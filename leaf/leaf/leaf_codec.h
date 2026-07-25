#ifndef LEAF_CODEC_H
#define LEAF_CODEC_H

#include "leaf_types.h"

/* ═══════════════════════════════════════════════════════════════════════════
 * MQTT remaining-length codec
 * ═══════════════════════════════════════════════════════════════════════════ */

static int leaf__read_u8(const uint8_t *buf, int off, int len, uint8_t *out) {
    if (off >= len) return -1; *out = buf[off]; return off + 1;
}
static int leaf__read_u16(const uint8_t *buf, int off, int len, uint16_t *out) {
    if (off + 1 >= len) return -1;
    *out = ((uint16_t)buf[off] << 8) | buf[off + 1];
    return off + 2;
}
static int leaf__read_bytes(const uint8_t *buf, int off, int len,
                            const uint8_t **out, uint16_t *out_len) {
    uint16_t n;
    if ((off = leaf__read_u16(buf, off, len, &n)) < 0) return -1;
    if (off + n > len) return -1;
    *out = buf + off; *out_len = n; return off + n;
}
static void leaf__write_u8(uint8_t *buf, int *off, uint8_t v)  { buf[(*off)++] = v; }
static void leaf__write_u16(uint8_t *buf, int *off, uint16_t v) {
    buf[(*off)++] = (uint8_t)(v >> 8); buf[(*off)++] = (uint8_t)(v & 0xFF);
}
static void leaf__write_bytes(uint8_t *buf, int *off, const uint8_t *d, uint16_t n) {
    leaf__write_u16(buf, off, n);
    if (n) { memcpy(buf + *off, d, n); *off += n; }
}
static void leaf__write_str(uint8_t *buf, int *off, const char *s) {
    leaf__write_bytes(buf, off, (const uint8_t *)s, (uint16_t)strlen(s));
}

static int leaf__encode_remaining(uint32_t val, uint8_t *out) {
    int n = 0;
    do { out[n++] = (uint8_t)(val & 0x7F) | ((val > 0x7F) ? 0x80 : 0); val >>= 7; } while (val);
    return n;
}

static int leaf__decode_remaining(const uint8_t *buf, int len, uint32_t *out) {
    uint32_t v = 0; int i;
    for (i = 0; i < 4 && i < len; i++) { v |= (uint32_t)(buf[i] & 0x7F) << (7*i); if (!(buf[i] & 0x80)) break; }
    *out = v;
    if (i >= len || (buf[i] & 0x80)) return -1; /* OOB safe: check len before buf[i] */
    return i + 1;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MQTT packet encode
 * ═══════════════════════════════════════════════════════════════════════════ */

static int leaf__encode_fixed_header(uint8_t *buf, uint8_t type, uint8_t flags, uint32_t rem_len) {
    int off = 0;
    leaf__write_u8(buf, &off, (uint8_t)((type << 4) | (flags & 0x0F)));
    off += leaf__encode_remaining(rem_len, buf + off);
    return off;
}

/* Returns total packet length, or 0 on error */
static int leaf__encode_connect(leaf_t *c) {
    int off = 0; uint8_t *b = c->send_buf;
    /* Payload first to calculate remaining length */
    uint8_t  payload[LEAF_BUF_SIZE]; int plen = 0;
    /* Client ID */
    payload[plen++] = 0; payload[plen++] = (uint8_t)strlen(c->cfg.client_id);
    memcpy(payload + plen, c->cfg.client_id, strlen(c->cfg.client_id)); plen += (int)strlen(c->cfg.client_id);

    uint8_t flags = 0;
    if (c->cfg.clean_session) flags |= 0x02;
    if (c->cfg.will_topic && c->cfg.will_topic[0]) {
        flags |= 0x04; /* Will flag */
        flags |= (c->cfg.will_qos & 0x03) << 3;
        if (c->cfg.will_retain) flags |= 0x20;
        /* Will topic */
        uint16_t wtl = (uint16_t)strlen(c->cfg.will_topic);
        payload[plen++] = (uint8_t)(wtl >> 8); payload[plen++] = (uint8_t)(wtl & 0xFF);
        memcpy(payload + plen, c->cfg.will_topic, wtl); plen += wtl;
        /* Will payload */
        payload[plen++] = (uint8_t)(c->cfg.will_payload_len >> 8);
        payload[plen++] = (uint8_t)(c->cfg.will_payload_len & 0xFF);
        if (c->cfg.will_payload && c->cfg.will_payload_len) {
            memcpy(payload + plen, c->cfg.will_payload, c->cfg.will_payload_len);
            plen += c->cfg.will_payload_len;
        }
    }
    if (c->cfg.username && c->cfg.username[0]) {
        flags |= 0x80;
        uint16_t ul = (uint16_t)strlen(c->cfg.username);
        payload[plen++] = (uint8_t)(ul >> 8); payload[plen++] = (uint8_t)(ul & 0xFF);
        memcpy(payload + plen, c->cfg.username, ul); plen += ul;
    }
    if (c->cfg.password && c->cfg.password[0]) {
        flags |= 0x40;
        uint16_t pl = (uint16_t)strlen(c->cfg.password);
        payload[plen++] = (uint8_t)(pl >> 8); payload[plen++] = (uint8_t)(pl & 0xFF);
        memcpy(payload + plen, c->cfg.password, pl); plen += pl;
    }

    /* Variable header: protocol name + level + flags + keepalive = 10 bytes */
    uint32_t rem_len = 10 + (uint32_t)plen;
    uint8_t rh[4]; int rl = leaf__encode_remaining(rem_len, rh);
    /* Fixed header */
    leaf__write_u8(b, &off, (uint8_t)((LEAF_PKT_CONNECT << 4)));
    memcpy(b + off, rh, rl); off += rl;
    /* Protocol name */
    leaf__write_str(b, &off, "MQTT");
    /* Protocol level */
    leaf__write_u8(b, &off, 4);
    /* Connect flags */
    leaf__write_u8(b, &off, flags);
    /* Keep alive */
    leaf__write_u16(b, &off, c->cfg.keepalive_sec ? c->cfg.keepalive_sec : LEAF_KEEPALIVE_SEC);
    /* Payload */
    memcpy(b + off, payload, plen); off += plen;
    return off;
}

static int leaf__encode_publish(leaf_t *c, const char *topic, const uint8_t *pl,
                                uint32_t plen, uint8_t qos, uint8_t retain, uint16_t pid) {
    int off = 0; uint8_t *b = c->send_buf;
    uint16_t tl = (uint16_t)strlen(topic);
    uint32_t rem_len = 2 + tl + plen;
    if (qos > 0) rem_len += 2; /* packet ID */
    uint8_t rh[4]; int rl = leaf__encode_remaining(rem_len, rh);
    uint8_t f = (uint8_t)((qos & 0x03) << 1);
    if (retain) f |= 0x01;
    leaf__write_u8(b, &off, (uint8_t)((LEAF_PKT_PUBLISH << 4) | f));
    memcpy(b + off, rh, rl); off += rl;
    leaf__write_str(b, &off, topic);
    if (qos > 0) leaf__write_u16(b, &off, pid);
    if (plen) { memcpy(b + off, pl, plen); off += plen; }
    return off;
}

static int leaf__encode_subscribe(leaf_t *c, const char *topic, uint8_t qos, uint16_t pid) {
    int off = 0; uint8_t *b = c->send_buf;
    uint16_t tl = (uint16_t)strlen(topic);
    uint32_t rem_len = 2 + 2 + tl + 1; /* pid + topic_str + qos */
    uint8_t rh[4]; int rl = leaf__encode_remaining(rem_len, rh);
    leaf__write_u8(b, &off, (uint8_t)((LEAF_PKT_SUBSCRIBE << 4) | 2));
    memcpy(b + off, rh, rl); off += rl;
    leaf__write_u16(b, &off, pid);
    leaf__write_str(b, &off, topic);
    leaf__write_u8(b, &off, qos);
    return off;
}

static int leaf__encode_unsubscribe(leaf_t *c, const char *topic, uint16_t pid) {
    int off = 0; uint8_t *b = c->send_buf;
    uint16_t tl = (uint16_t)strlen(topic);
    uint32_t rem_len = 2 + 2 + tl;
    uint8_t rh[4]; int rl = leaf__encode_remaining(rem_len, rh);
    leaf__write_u8(b, &off, (uint8_t)((LEAF_PKT_UNSUBSCRIBE << 4) | 2));
    memcpy(b + off, rh, rl); off += rl;
    leaf__write_u16(b, &off, pid);
    leaf__write_str(b, &off, topic);
    (void)c; return off;
}

static int leaf__encode_ack(uint8_t *buf, uint8_t type, uint16_t pid) {
    int off = 0;
    leaf__write_u8(buf, &off, (uint8_t)(type << 4));
    leaf__write_u8(buf, &off, 2); /* remaining length */
    leaf__write_u16(buf, &off, pid);
    return off;
}

static int leaf__encode_pingreq(uint8_t *buf) { buf[0] = (uint8_t)(LEAF_PKT_PINGREQ << 4); buf[1] = 0; return 2; }
static int leaf__encode_disconnect(uint8_t *buf){ buf[0]=(uint8_t)(LEAF_PKT_DISCONNECT<<4); buf[1]=0; return 2; }

/* ═══════════════════════════════════════════════════════════════════════════
 * MQTT packet decode (called when full packet is in recv_buf)
 * ═══════════════════════════════════════════════════════════════════════════ */

static int leaf__dispatch(leaf_t *c, int pkt_len) {
    const uint8_t *p = c->recv_buf;
    int off = 0, r;
    uint8_t type, flags;
    uint32_t rem_len;

    if (pkt_len < 2) return -1;
    type = (p[0] >> 4) & 0x0F;
    flags = p[0] & 0x0F;

    r = leaf__decode_remaining(p + 1, pkt_len - 1, &rem_len);
    if (r < 0) return -1;
    off = 1 + r;

    switch (type) {

    case LEAF_PKT_CONNACK: {
        uint8_t sp, rc;
        if ((off = leaf__read_u8(p, off, pkt_len, &sp)) < 0) return -1;
        if ((off = leaf__read_u8(p, off, pkt_len, &rc)) < 0) return -1;
        if (rc == 0) {
            c->state = LEAF_S_READY;
            c->reconnect_backoff_ms = LEAF_RECONNECT_MS;
            if (c->on_connect) c->on_connect(c->cb_userdata);
        }
        break;
    }

    case LEAF_PKT_PUBLISH: {
        uint8_t qos = (flags >> 1) & 0x03;
        uint8_t retain = flags & 0x01;
        const uint8_t *topic; uint16_t topic_len;
        if ((off = leaf__read_bytes(p, off, pkt_len, &topic, &topic_len)) < 0) return -1;
        uint16_t pid = 0;
        if (qos > 0) { if ((off = leaf__read_u16(p, off, pkt_len, &pid)) < 0) return -1; }

        /* Check dedup: if we've seen this pid recently, send ACK but skip callback */
        int seen = 0;
        for (int i = 0; i < LEAF_MAX_PENDING; i++) {
            if (c->received_ids[i] == pid && pid != 0) { seen = 1; break; }
        }
        if (!seen && pid != 0) {
            for (int i = 0; i < LEAF_MAX_PENDING; i++) {
                if (c->received_ids[i] == 0) { c->received_ids[i] = pid; break; }
            }
        }

        /* Extract payload */
        int payload_off = off;
        uint32_t payload_len = (uint32_t)(pkt_len - off);

        /* QOS 1 -> PUBACK, QOS 2 -> PUBREC */
        if (qos == 1) {
            uint8_t pa[4]; int pal = leaf__encode_ack(pa, LEAF_PKT_PUBACK, pid);
            send(c->fd, pa, pal, MSG_NOSIGNAL);
        } else if (qos == 2) {
            /* Store for PUBREL -> PUBCOMP cycle */
            for (int i = 0; i < LEAF_MAX_PENDING; i++) {
                if (c->pending[i].packet_id == 0) {
                    c->pending[i].packet_id = pid;
                    c->pending[i].type = LEAF_PKT_PUBLISH; /* incoming */
                    c->pending[i].sent_at_ms = leaf__now_ms();
                    break;
                }
            }
            uint8_t pr[4]; int prl = leaf__encode_ack(pr, LEAF_PKT_PUBREC, pid);
            send(c->fd, pr, prl, MSG_NOSIGNAL);
        }

        if (!seen && c->on_message) {
            /* Zero-copy: topic points into recv_buf. User must consume before next poll. */
            char topic_buf[LEAF_MAX_TOPIC];
            uint16_t n = topic_len < LEAF_MAX_TOPIC-1 ? topic_len : LEAF_MAX_TOPIC-1;
            memcpy(topic_buf, topic, n); topic_buf[n] = '\0';
            leaf_msg_t msg;
            msg.topic = topic_buf;
            msg.payload = p + payload_off;
            msg.payload_len = payload_len;
            msg.qos = qos;
            msg.retain = retain;
            c->on_message(&msg, c->cb_userdata);
        }
        break;
    }

    case LEAF_PKT_PUBACK:
    case LEAF_PKT_PUBCOMP: {
        /* These complete outgoing QOS 1/2 publish */
        uint16_t pid; if ((off = leaf__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        for (int i = 0; i < LEAF_MAX_PENDING; i++) {
            if (c->pending[i].packet_id == pid) { c->pending[i].packet_id = 0; break; }
        }
        break;
    }

    case LEAF_PKT_PUBREC: {
        /* Received PUBREC -> send PUBREL */
        uint16_t pid; if ((off = leaf__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        uint8_t pr[4]; int prl = leaf__encode_ack(pr, LEAF_PKT_PUBREL, pid);
        send(c->fd, pr, prl, MSG_NOSIGNAL);
        /* Keep pending alive until PUBCOMP arrives */
        break;
    }

    case LEAF_PKT_PUBREL: {
        /* Received PUBREL -> send PUBCOMP, release received_ids entry */
        uint16_t pid; if ((off = leaf__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        uint8_t pc[4]; int pcl = leaf__encode_ack(pc, LEAF_PKT_PUBCOMP, pid);
        send(c->fd, pc, pcl, MSG_NOSIGNAL);
        for (int i = 0; i < LEAF_MAX_PENDING; i++) {
            if (c->pending[i].packet_id == pid && c->pending[i].type == LEAF_PKT_PUBLISH) {
                c->pending[i].packet_id = 0; break;
            }
        }
        /* Dispatch the stored QOS 2 message to callback now */
        /* (simplification: dispatch on PUBLISH arrival, not on PUBREL) */
        break;
    }

    case LEAF_PKT_SUBACK: {
        uint16_t pid; if ((off = leaf__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        /* Remove from pending */
        for (int i = 0; i < LEAF_MAX_PENDING; i++) {
            if (c->pending[i].packet_id == pid) { c->pending[i].packet_id = 0; break; }
        }
        break;
    }

    case LEAF_PKT_UNSUBACK: {
        uint16_t pid; if ((off = leaf__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        for (int i = 0; i < LEAF_MAX_PENDING; i++) {
            if (c->pending[i].packet_id == pid) { c->pending[i].packet_id = 0; break; }
        }
        break;
    }

    case LEAF_PKT_PINGRESP:
        /* nothing to do */
        break;

    default:
        break;
    }
    (void)off;
    return 0;
}

#endif /* LEAF_CODEC_H */
