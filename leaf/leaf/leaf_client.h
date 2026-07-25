#ifndef LEAF_CLIENT_H
#define LEAF_CLIENT_H

#include "leaf_codec.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * Pending QOS 1/2 management
 * ═══════════════════════════════════════════════════════════════════════════ */

static int leaf__pending_slot(leaf_t *c) {
    for (int i = 0; i < LEAF_MAX_PENDING; i++)
        if (c->pending[i].packet_id == 0) return i;
    return -1; /* full */
}

static uint16_t leaf__next_pid(leaf_t *c) {
    uint16_t pid = ++c->next_packet_id;
    if (pid == 0) pid = ++c->next_packet_id; /* skip 0 */
    return pid;
}

static void leaf__retry_pending(leaf_t *c) {
    uint64_t now = leaf__now_ms();
    for (int i = 0; i < LEAF_MAX_PENDING; i++) {
        leaf_pending_t *p = &c->pending[i];
        if (p->packet_id == 0) continue;
        if (now - p->sent_at_ms < 10000) continue; /* retry after 10s */
        if (p->retries >= 5) { p->packet_id = 0; continue; } /* give up */
        send(c->fd, p->payload, p->payload_len, MSG_NOSIGNAL);
        p->sent_at_ms = now;
        p->retries++;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Network I/O
 * ═══════════════════════════════════════════════════════════════════════════ */

static int leaf__connect_tcp(leaf_t *c) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(c->cfg.port);

    struct hostent *he = gethostbyname(c->cfg.broker_ip);
    if (!he) { c->state = LEAF_S_DISCONNECTED; return -1; }
    memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);

    c->fd = (int)socket(AF_INET, SOCK_STREAM, 0);
    if (c->fd < 0) { c->state = LEAF_S_DISCONNECTED; return -1; }

    leaf__nonblock(c->fd);
    leaf__nodelay(c->fd);

    int rc = connect(c->fd, (struct sockaddr*)&addr, sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        leaf__close_fd(c);
        c->state = LEAF_S_DISCONNECTED;
        return -1;
    }

    c->state = (rc == 0) ? LEAF_S_TCP_CONNECTED : LEAF_S_TCP_CONNECTING;
    c->last_recv_ms = leaf__now_ms();
    c->last_send_ms = c->last_recv_ms;
    return 0;
}

static void leaf__close_fd(leaf_t *c) {
    if (c->fd >= 0) { close(c->fd); c->fd = -1; }
}

static int leaf__send_pending(leaf_t *c) {
    if (c->send_off >= c->send_len) return 0;
    ssize_t n = send(c->fd, c->send_buf + c->send_off, c->send_len - c->send_off, MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1; /* connection lost */
    }
    c->send_off += (uint32_t)n;
    if (c->send_off >= c->send_len) { c->send_off = 0; c->send_len = 0; }
    c->last_send_ms = leaf__now_ms();
    return 0;
}

static int leaf__recv_and_dispatch(leaf_t *c) {
    /* Accumulate into recv_buf */
    uint32_t room = LEAF_BUF_SIZE - c->recv_len;
    ssize_t n = recv(c->fd, c->recv_buf + c->recv_len, room, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    c->recv_len += (uint32_t)n;
    c->last_recv_ms = leaf__now_ms();

    /* Try to dispatch complete packets */
    int dispatched = 0;
    while (c->recv_len >= 2) {
        /* Decode remaining length to find total packet size */
        uint32_t rem_len;
        int rl = leaf__decode_remaining(c->recv_buf + 1, 4, &rem_len);
        if (rl < 0) { c->recv_len = 0; return -1; }
        uint32_t hdr_len = (uint32_t)(1 + rl);
        uint32_t total = hdr_len + rem_len;
        if (total > LEAF_BUF_SIZE) { c->recv_len = 0; return -1; }
        if (c->recv_len < total) break; /* not yet complete */

        int rc = leaf__dispatch(c, (int)total);
        if (rc < 0) return -1;

        /* Shift remaining data down */
        uint32_t rem = c->recv_len - total;
        if (rem > 0) memmove(c->recv_buf, c->recv_buf + total, rem);
        c->recv_len = rem;
        dispatched++;
    }
    (void)dispatched;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Internal state helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static void leaf__reset(leaf_t *c) {
    leaf__close_fd(c);
    c->state = LEAF_S_DISCONNECTED;
    c->send_off = 0; c->send_len = 0;
    c->recv_len = 0;
    c->sub_count = 0;
    c->next_packet_id = 0;
    memset(c->pending, 0, sizeof(c->pending));
    memset(c->received_ids, 0, sizeof(c->received_ids));
    memset(c->subs, 0, sizeof(c->subs));
}

static void leaf__fire_disconnect(leaf_t *c) {
    if (c->state == LEAF_S_DISCONNECTED) return;
    leaf__reset(c);
    if (c->on_disconnect) c->on_disconnect(c->cb_userdata);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Public API
 * ═══════════════════════════════════════════════════════════════════════════ */

static leaf_t g_leaf;
static int    g_leaf_in_use = 0;

leaf_t* leaf_create(const leaf_config_t *cfg) {
    if (g_leaf_in_use) return NULL;
    g_leaf_in_use = 1;
    leaf_t *c = &g_leaf;
    memset(c, 0, sizeof(*c));
    c->cfg = *cfg;
    if (!c->cfg.port) c->cfg.port = 1883;
    c->fd = -1;
    c->state = LEAF_S_DISCONNECTED;
    c->reconnect_backoff_ms = LEAF_RECONNECT_MS;
    return c;
}

void leaf_destroy(leaf_t *c) {
    if (!c) return;
    if (c->state == LEAF_S_READY) leaf_disconnect(c);
    leaf__close_fd(c);
    memset(c, 0, sizeof(*c));
    g_leaf_in_use = 0;
}

int leaf_connect(leaf_t *c) {
    if (c->state != LEAF_S_DISCONNECTED) return -1;
    return leaf__connect_tcp(c);
}

int leaf_disconnect(leaf_t *c) {
    if (c->state == LEAF_S_DISCONNECTED || c->state == LEAF_S_DISCONNECTING) return 0;
    c->state = LEAF_S_DISCONNECTING;
    c->clean_disconnect = 1;
    uint8_t buf[2]; leaf__encode_disconnect(buf);
    send(c->fd, buf, 2, MSG_NOSIGNAL);
    leaf__fire_disconnect(c);
    return 0;
}

int leaf_subscribe(leaf_t *c, const char *topic, uint8_t qos) {
    if (c->state != LEAF_S_READY) return -1;
    if (c->sub_count >= LEAF_MAX_SUBS) return -1;
    uint16_t pid = leaf__next_pid(c);
    /* Store locally */
    uint16_t tl = (uint16_t)strlen(topic);
    if (tl >= LEAF_MAX_TOPIC) return -1;
    memcpy(c->subs[c->sub_count].topic, topic, tl + 1);
    c->subs[c->sub_count].qos = qos;
    c->sub_count++;

    /* Enqueue subscribe packet */
    c->send_len = (uint32_t)leaf__encode_subscribe(c, topic, qos, pid);
    c->send_off = 0;

    /* Track in pending until SUBACK arrives */
    int si = leaf__pending_slot(c);
    if (si >= 0) { c->pending[si].packet_id = pid; c->pending[si].type = LEAF_PKT_SUBSCRIBE; }
    return 0;
}

int leaf_unsubscribe(leaf_t *c, const char *topic) {
    if (c->state != LEAF_S_READY) return -1;
    /* Remove from local subs */
    for (uint8_t i = 0; i < c->sub_count; i++) {
        if (strcmp(c->subs[i].topic, topic) == 0) {
            c->subs[i] = c->subs[--c->sub_count]; break;
        }
    }
    uint16_t pid = leaf__next_pid(c);
    c->send_len = (uint32_t)leaf__encode_unsubscribe(c, topic, pid);
    c->send_off = 0;
    int si = leaf__pending_slot(c);
    if (si >= 0) { c->pending[si].packet_id = pid; c->pending[si].type = LEAF_PKT_UNSUBSCRIBE; }
    return 0;
}

int leaf_publish(leaf_t *c, const char *topic, const void *payload,
                 uint32_t len, uint8_t qos, uint8_t retain) {
    if (c->state != LEAF_S_READY) return -1;
    uint16_t pid = 0;
    if (qos > 0) {
        pid = leaf__next_pid(c);
        /* Store payload for potential retransmit */
        int si = leaf__pending_slot(c);
        if (si < 0) return -1;
        c->pending[si].packet_id = pid;
        c->pending[si].type = LEAF_PKT_PUBLISH;
        c->pending[si].sent_at_ms = leaf__now_ms();
        c->pending[si].retries = 0;
        /* Snapshot the encoded packet for retry */
        int plen = leaf__encode_publish(c, topic, (const uint8_t*)payload, len, qos, retain, pid);
        if (plen > 0 && plen <= (int)sizeof(c->pending[si].payload)) {
            memcpy(c->pending[si].payload, c->send_buf, plen);
            c->pending[si].payload_len = (uint32_t)plen;
        }
    }
    c->send_len = (uint32_t)leaf__encode_publish(c, topic, (const uint8_t*)payload, len, qos, retain, pid);
    c->send_off = 0;
    return 0;
}

int leaf_poll(leaf_t *c, int timeout_ms) {
    uint64_t now = leaf__now_ms();
    c->last_poll_ms = now;

    /* Reconnect backoff if disconnected */
    if (c->state == LEAF_S_DISCONNECTED) {
        static uint64_t last_attempt; /* static: shared across calls */
        if (now - last_attempt >= (uint64_t)c->reconnect_backoff_ms) {
            last_attempt = now;
            if (c->reconnect_backoff_ms < 60000) c->reconnect_backoff_ms *= 2;
            leaf__connect_tcp(c);
        }
    }

    /* Build fd_set */
    fd_set rfds, wfds;
    FD_ZERO(&rfds); FD_ZERO(&wfds);
    int maxfd = -1;

    if (c->fd >= 0) {
        FD_SET(c->fd, &rfds);
        maxfd = c->fd;
        /* Need to write if send buffer has data or TCP connection is pending */
        if (c->send_len > c->send_off || c->state == LEAF_S_TCP_CONNECTING)
            { FD_SET(c->fd, &wfds); }
    }

    if (maxfd < 0) { if (timeout_ms > 0) { struct timeval _tv = {timeout_ms/1000, (timeout_ms%1000)*1000}; select(0,0,0,0,&_tv); } return 0; }

    struct timeval tv, *ptv = NULL;
    if (timeout_ms >= 0) {
        tv.tv_sec = timeout_ms / 1000; tv.tv_usec = (timeout_ms % 1000) * 1000;
        ptv = &tv;
    }

    int rc = select(maxfd + 1, &rfds, &wfds, NULL, ptv);
    if (rc < 0) {
        if (errno != EINTR) {
            leaf__fire_disconnect(c);
        }
        return -1;
    }
    if (rc == 0) return 0; /* timeout */

    /* Check writable (TCP connect complete or send ready) */
    if (FD_ISSET(c->fd, &wfds)) {
        if (c->state == LEAF_S_TCP_CONNECTING) {
            int err = 0; socklen_t len = sizeof(err);
            getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len);
            if (err) { leaf__fire_disconnect(c); return -1; }
            c->state = LEAF_S_TCP_CONNECTED;
        }

        if (c->state == LEAF_S_TCP_CONNECTED) {
            /* Send CONNECT packet */
            c->send_len = (uint32_t)leaf__encode_connect(c);
            c->send_off = 0;
            c->state = LEAF_S_CONNACK_WAIT;
        }

        if (leaf__send_pending(c) < 0) { leaf__fire_disconnect(c); return -1; }
    }

    /* Check readable */
    if (FD_ISSET(c->fd, &rfds)) {
        if (leaf__recv_and_dispatch(c) < 0) { leaf__fire_disconnect(c); return -1; }
    }

    /* Keepalive: send PINGREQ if idle */
    if (c->state == LEAF_S_READY || c->state == LEAF_S_CONNACK_WAIT) {
        uint16_t ka = c->cfg.keepalive_sec ? c->cfg.keepalive_sec : LEAF_KEEPALIVE_SEC;
        if (ka > 0 && now - c->last_send_ms > (uint64_t)ka * 1000) {
            uint8_t ping[2]; leaf__encode_pingreq(ping);
            send(c->fd, ping, 2, MSG_NOSIGNAL);
            c->last_send_ms = now;
        }
        /* Check server alive: if no recv for 1.5x keepalive, disconnect */
        if (ka > 0 && now - c->last_recv_ms > (uint64_t)ka * 1500) {
            leaf__fire_disconnect(c);
            return -1;
        }
    }

    /* Retry QOS 1/2 pending */
    if (c->state == LEAF_S_READY) leaf__retry_pending(c);

    return 0;
}

/* ── Callbacks ── */
void leaf_set_on_connect(leaf_t *c, leaf_on_connect_cb cb, void *ud)   { c->on_connect=cb; c->cb_userdata=ud; }
void leaf_set_on_disconnect(leaf_t *c, leaf_on_disconnect_cb cb, void *ud){ c->on_disconnect=cb; c->cb_userdata=ud; }
void leaf_set_on_message(leaf_t *c, leaf_on_message_cb cb, void *ud)   { c->on_message=cb; c->cb_userdata=ud; }

/* ── Status ── */
int leaf_is_connected(leaf_t *c) { return c->state >= LEAF_S_TCP_CONNECTED ? 1 : 0; }
int leaf_is_ready(leaf_t *c)     { return c->state == LEAF_S_READY ? 1 : 0; }

#endif /* LEAF_CLIENT_H */
