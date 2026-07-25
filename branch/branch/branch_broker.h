#pragma once
#include "branch_codec.h"
#include "branch_match.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * Broker internal helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static uint16_t branch__alloc_pid(branch_t *t) {
    uint16_t pid = ++t->next_pid;
    if (pid == 0) pid = ++t->next_pid;
    return pid;
}

static void branch__close_client(branch_t *t, int ci) {
    branch_client_t *c = &t->clients[ci];
    if (c->fd >= 0) { close(c->fd); c->fd = -1; }
    /* Trigger will if not clean disconnect */
    if (c->has_will && c->state != CS_WILL_TRIGGERED) {
        branch_publish(t, c->will_topic, c->will_payload, c->will_payload_len,
                     c->will_qos, c->will_retain);
        c->state = CS_WILL_TRIGGERED;
    }
    /* Free subscriptions */
    for (uint8_t i = 0; i < c->sub_count; i++) {
        t->subs[c->sub_indices[i]].used = 0;
    }
    memset(c, 0, sizeof(*c));
    c->fd = -1;
    c->state = CS_DISCONNECTED;
}

static int branch__find_free_client(branch_t *t) {
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++)
        if (t->clients[i].fd < 0) return i;
    return -1;
}

static int branch__find_free_sub(branch_t *t) {
    for (int i = 0; i < BRANCH_MAX_SUBS; i++)
        if (!t->subs[i].used) return i;
    return -1;
}

static int branch__find_retained(branch_t *t, const char *topic) {
    for (int i = 0; i < BRANCH_MAX_RETAINED; i++)
        if (t->retained[i].used && strcmp(t->retained[i].topic, topic) == 0)
            return i;
    return -1;
}

static int branch__find_free_retained(branch_t *t) {
    for (int i = 0; i < BRANCH_MAX_RETAINED; i++)
        if (!t->retained[i].used) return i;
    return -1;
}

/* Find a client by client_id string (for session resumption) */
static int branch__find_client_by_id(branch_t *t, const char *client_id) {
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++)
        if (t->clients[i].fd >= 0 && strcmp(t->clients[i].client_id, client_id) == 0)
            return i;
    return -1;
}

static void branch__queue_and_flush(branch_client_t *c, const uint8_t *data, uint32_t len) {
    if (c->send_len + len <= BRANCH_BUF_SIZE) {
        memcpy(c->send_buf + c->send_len, data, len);
        c->send_len += len;
    }
    /* Try to send immediately — don't wait for next poll cycle */
    if (c->send_off < c->send_len) {
        ssize_t n = send(c->fd, c->send_buf + c->send_off,
                         c->send_len - c->send_off, MSG_NOSIGNAL);
        if (n > 0) {
            c->send_off += (uint32_t)n;
            if (c->send_off >= c->send_len) {
                c->send_off = 0; c->send_len = 0;
                if (c->state == CS_CONNACK_SENT) c->state = CS_READY;
            }
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Message forwarding (with QOS downgrade per subscription)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void branch__forward(branch_t *t, int from_ci, const char *topic,
                          const uint8_t *payload, uint32_t plen,
                          uint8_t qos, uint8_t retain) {
    for (int si = 0; si < BRANCH_MAX_SUBS; si++) {
        if (!t->subs[si].used) continue;
        if (!branch__match_topic(t->subs[si].topic_filter, topic)) continue;

        int ci = t->subs[si].client_idx;
        if (ci == from_ci) continue;  /* don't echo to sender */
        if (ci < 0 || ci >= BRANCH_MAX_CLIENTS) continue;

        branch_client_t *c = &t->clients[ci];
        if (c->state < CS_CONNACK_SENT) continue;

        /* Downgrade QOS to min(requested, published) */
        uint8_t fwd_qos = qos < t->subs[si].qos ? qos : t->subs[si].qos;
        uint16_t pid = 0;
        if (fwd_qos > 0) {
            pid = branch__alloc_pid(t);
            /* Track outgoing QOS 2 */
            if (fwd_qos == 2) {
                for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++) {
                    if (c->qos2[qi].state == Q2_FREE) {
                        c->qos2[qi].pid = pid;
                        c->qos2[qi].state = Q2_OUT_PUBLISH_SENT;
                        c->qos2[qi].t_ms = branch__now_ms();
                        break;
                    }
                }
            }
        }

        uint8_t pkt[BRANCH_BUF_SIZE];
        int pklen = branch__enc_publish(pkt, topic, payload, plen, fwd_qos, retain, pid);
        branch__queue_and_flush(c, pkt, (uint32_t)pklen);
    }

    /* Handle retain */
    if (retain && plen > 0) {
        int ri = branch__find_retained(t, topic);
        if (ri < 0) ri = branch__find_free_retained(t);
        if (ri >= 0) {
            branch_retained_t *r = &t->retained[ri];
            int tl = (int)strlen(topic);
            memcpy(r->topic, topic, tl < BRANCH_MAX_TOPIC - 1 ? tl + 1 : BRANCH_MAX_TOPIC - 1);
            r->topic[BRANCH_MAX_TOPIC - 1] = '\0';
            uint32_t n = plen < BRANCH_BUF_SIZE ? plen : BRANCH_BUF_SIZE - 1;
            memcpy(r->payload, payload, n);
            r->payload_len = n;
            r->qos = qos;
            r->used = 1;
        }
    } else if (retain && plen == 0) {
        /* Zero-length retain = delete retained message */
        int ri = branch__find_retained(t, topic);
        if (ri >= 0) t->retained[ri].used = 0;
    }
}

/* Send retained messages matching a new subscription */
static void branch__send_retained(branch_t *t, int ci, const char *topic_filter) {
    branch_client_t *c = &t->clients[ci];
    for (int ri = 0; ri < BRANCH_MAX_RETAINED; ri++) {
        if (!t->retained[ri].used) continue;
        if (!branch__match_topic(topic_filter, t->retained[ri].topic)) continue;
        uint8_t pkt[BRANCH_BUF_SIZE];
        int pklen = branch__enc_publish(pkt, t->retained[ri].topic,
                                      t->retained[ri].payload,
                                      t->retained[ri].payload_len,
                                      t->retained[ri].qos, 1, 0);
        branch__queue_and_flush(c, pkt, (uint32_t)pklen);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Per-client I/O
 * ═══════════════════════════════════════════════════════════════════════════ */

static int branch__client_recv(branch_t *t, int ci) {
    branch_client_t *c = &t->clients[ci];
    uint32_t room = BRANCH_BUF_SIZE - c->recv_len;
    if (room < 64) { c->recv_len = 0; return -1; }
    ssize_t n = recv(c->fd, c->recv_buf + c->recv_len, room, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    if (n == 0) return -1;
    c->recv_len += (uint32_t)n;
    c->last_recv_ms = branch__now_ms();

    /* Dispatch complete packets */
    while (c->recv_len >= 2) {
        uint32_t rem_len;
        int rl = branch__decode_remaining(c->recv_buf + 1, 4, &rem_len);
        if (rl < 0) { c->recv_len = 0; return -1; }
        uint32_t total = 1 + (uint32_t)rl + rem_len;
        if (total > BRANCH_BUF_SIZE) { c->recv_len = 0; return -1; }
        if (c->recv_len < total) break;

        int rc = branch__dispatch(t, ci, (int)total);
        if (rc < 0) return -1;

        uint32_t rem = c->recv_len - total;
        if (rem > 0) memmove(c->recv_buf, c->recv_buf + total, rem);
        c->recv_len = rem;
    }
    return 0;
}

static int branch__client_send(branch_client_t *c) {
    if (c->send_off >= c->send_len) return 0;
    ssize_t n = send(c->fd, c->send_buf + c->send_off,
                     c->send_len - c->send_off, MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    c->send_off += (uint32_t)n;
    if (c->send_off >= c->send_len) {
        c->send_off = 0; c->send_len = 0;
        if (c->state == CS_CONNACK_SENT) c->state = CS_READY;
    }
    c->last_send_ms = branch__now_ms();
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Keepalive check
 * ═══════════════════════════════════════════════════════════════════════════ */

static void branch__check_keepalive(branch_t *t) {
    uint64_t now = branch__now_ms();
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++) {
        branch_client_t *c = &t->clients[i];
        if (c->fd < 0) continue;
        uint32_t ka_ms = (uint32_t)c->keepalive_sec * 1500; /* 1.5x timeout */
        if (ka_ms == 0) continue;
        if (now - c->last_recv_ms > ka_ms) {
            if (t->cfg.on_event) {
                char buf[256]; snprintf(buf, sizeof(buf), "client '%s' keepalive timeout", c->client_id);
                t->cfg.on_event(buf, t->cfg.event_userdata);
            }
            branch__close_client(t, i);
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Public API
 * ═══════════════════════════════════════════════════════════════════════════ */

static branch_t g_branch;
static int    g_branch_in_use = 0;

branch_t* branch_create(const branch_config_t *cfg) {
    if (g_branch_in_use) return NULL;
    g_branch_in_use = 1;
    branch_t *t = &g_branch;
    memset(t, 0, sizeof(*t));
    t->cfg = *cfg;
    if (!t->cfg.port) t->cfg.port = 1883;
    t->listen_fd = -1;

    /* Initialize all client slots */
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++) {
        t->clients[i].fd = -1;
        t->clients[i].state = CS_DISCONNECTED;
    }

    /* Create listen socket */
    t->listen_fd = (int)socket(AF_INET, SOCK_STREAM, 0);
    if (t->listen_fd < 0) return t;

    branch__reuseaddr(t->listen_fd);
    branch__nonblock(t->listen_fd);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(t->cfg.port);

    if (bind(t->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { close(t->listen_fd); t->listen_fd = -1; return t; }
    if (listen(t->listen_fd, SOMAXCONN) < 0) { close(t->listen_fd); t->listen_fd = -1; return t; }

    t->start_ms = branch__now_ms();
    if (t->cfg.on_event) {
        char buf[128]; snprintf(buf, sizeof(buf), "broker listening on :%d", t->cfg.port);
        t->cfg.on_event(buf, t->cfg.event_userdata);
    }
    return t;
}

void branch_destroy(branch_t *t) {
    if (!t) return;
    if (t->cfg.on_event) t->cfg.on_event("broker shutting down", t->cfg.event_userdata);
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++)
        if (t->clients[i].fd >= 0) branch__close_client(t, i);
    if (t->listen_fd >= 0) { close(t->listen_fd); t->listen_fd = -1; }
    g_branch_in_use = 0;
}

int branch_poll(branch_t *t, int timeout_ms) {
    if (t->listen_fd < 0) return -1;

    fd_set rfds, wfds;
    FD_ZERO(&rfds); FD_ZERO(&wfds);
    FD_SET(t->listen_fd, &rfds);
    int maxfd = t->listen_fd;

    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++) {
        branch_client_t *c = &t->clients[i];
        if (c->fd < 0) continue;
        FD_SET(c->fd, &rfds);
        if (c->send_len > c->send_off) FD_SET(c->fd, &wfds);
        if (c->fd > maxfd) maxfd = c->fd;
    }

    struct timeval tv, *ptv = NULL;
    if (timeout_ms >= 0) {
        tv.tv_sec = timeout_ms / 1000; tv.tv_usec = (timeout_ms % 1000) * 1000;
        ptv = &tv;
    }

    int rc = select(maxfd + 1, &rfds, &wfds, NULL, ptv);
    if (rc < 0) { if (errno != EINTR) return -1; return 0; }
    if (rc == 0) { branch__check_keepalive(t); return 0; }

    /* Accept new connections */
    if (FD_ISSET(t->listen_fd, &rfds)) {
        struct sockaddr_in ca; socklen_t sl = sizeof(ca);
        int newfd = accept(t->listen_fd, (struct sockaddr*)&ca, &sl);
        if (newfd >= 0) {
            int ci = branch__find_free_client(t);
            if (ci >= 0) {
                branch__nonblock(newfd); branch__nodelay(newfd);
                memset(&t->clients[ci], 0, sizeof(branch_client_t));
                t->clients[ci].fd = newfd;
                t->clients[ci].state = CS_CONNACK_SENT; /* will become CONNACK_SENT after CONNECT */
                t->clients[ci].last_recv_ms = branch__now_ms();
                t->clients[ci].last_send_ms = t->clients[ci].last_recv_ms;
                /* client_id set on CONNECT */
            } else {
                close(newfd); /* no free slots */
            }
        }
    }

    /* I/O per client */
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++) {
        branch_client_t *c = &t->clients[i];
        if (c->fd < 0) continue;

        if (FD_ISSET(c->fd, &wfds)) {
            if (branch__client_send(c) < 0) { branch__close_client(t, i); continue; }
            /* If client was rejected (CONNACK with rc!=0), close after sending */
            if (c->state == CS_CONNACK_SENT && c->send_len == 0) {
                /* state unchanged = still waiting for first CONNECT, fine */
            }
        }

        if (FD_ISSET(c->fd, &rfds)) {
            if (branch__client_recv(t, i) < 0) { branch__close_client(t, i); continue; }
        }

        /* Close rejected clients after CONNACK sent */
        if (c->state == CS_DISCONNECTED) { branch__close_client(t, i); }
    }

    branch__check_keepalive(t);
    return 0;
}

int branch_publish(branch_t *t, const char *topic, const void *payload,
                 uint32_t len, uint8_t qos, uint8_t retain) {
    if (!t || !topic) return -1;
    branch__forward(t, -1, topic, (const uint8_t*)payload, len, qos, retain);
    return 0;
}

int branch_client_count(branch_t *t) {
    int n = 0;
    for (int i = 0; i < BRANCH_MAX_CLIENTS; i++)
        if (t->clients[i].fd >= 0) n++;
    return n;
}

int branch_get_clients(branch_t *t, branch_client_info_t *dst, int max) {
    int n = 0;
    for (int i = 0; i < BRANCH_MAX_CLIENTS && n < max; i++) {
        branch_client_t *c = &t->clients[i];
        if (c->fd < 0) continue;
        strncpy(dst[n].client_id, c->client_id, BRANCH_MAX_CLIENT_ID - 1);
        dst[n].client_id[BRANCH_MAX_CLIENT_ID - 1] = '\0';
        dst[n].connected_sec = (uint32_t)((branch__now_ms() - c->connected_at_ms) / 1000);
        dst[n].active = 1;
        n++;
    }
    return n;
}
