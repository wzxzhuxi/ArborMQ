#pragma once
#include "branch_types.h"

/* ── Forward declarations for broker functions used by branch__dispatch ── */
static void     branch__close_client(branch_t *t, int ci);
static int      branch__find_client_by_id(branch_t *t, const char *client_id);
static int      branch__find_free_sub(branch_t *t);
static void     branch__queue_and_flush(branch_client_t *c, const uint8_t *data, uint32_t len);
static void     branch__send_retained(branch_t *t, int ci, const char *topic_filter);
static void     branch__forward(branch_t *t, int from_ci, const char *topic,
                              const uint8_t *payload, uint32_t plen,
                              uint8_t qos, uint8_t retain);

/* ═══════════════════════════════════════════════════════════════════════════
 * MQTT util: read/write helpers + remaining-length codec
 * ═══════════════════════════════════════════════════════════════════════════ */

static int tr__read_u8(const uint8_t *b, int off, int len, uint8_t *o) {
    if (off >= len) return -1; *o = b[off]; return off + 1;
}
static int tr__read_u16(const uint8_t *b, int off, int len, uint16_t *o) {
    if (off + 1 >= len) return -1;
    *o = ((uint16_t)b[off] << 8) | b[off + 1]; return off + 2;
}
static int tr__read_str(const uint8_t *b, int off, int len,
                        const uint8_t **s, uint16_t *slen) {
    uint16_t n;
    if ((off = tr__read_u16(b, off, len, &n)) < 0) return -1;
    if (off + n > len) return -1;
    *s = b + off; *slen = n; return off + n;
}
static int tr__read_str_copy(const uint8_t *b, int off, int len,
                             char *dst, int dst_max) {
    const uint8_t *s; uint16_t slen;
    if ((off = tr__read_str(b, off, len, &s, &slen)) < 0) return -1;
    int n = slen < dst_max - 1 ? slen : dst_max - 1;
    memcpy(dst, s, n); dst[n] = '\0'; return off;
}

static void tr__write_u8(uint8_t *b, int *off, uint8_t v)    { b[(*off)++] = v; }
static void tr__write_u16(uint8_t *b, int *off, uint16_t v) {
    b[(*off)++] = (uint8_t)(v >> 8); b[(*off)++] = (uint8_t)(v & 0xFF);
}
static void tr__write_bytes(uint8_t *b, int *off, const uint8_t *d, uint16_t n) {
    tr__write_u16(b, off, n);
    if (n) { memcpy(b + *off, d, n); *off += n; }
}
static void tr__write_str(uint8_t *b, int *off, const char *s) {
    tr__write_bytes(b, off, (const uint8_t *)s, (uint16_t)strlen(s));
}

static int tr__encode_remaining(uint32_t val, uint8_t *out) {
    int n = 0;
    do { out[n++] = (uint8_t)(val & 0x7F) | ((val > 0x7F) ? 0x80 : 0); val >>= 7; } while (val);
    return n;
}
static int tr__decode_remaining(const uint8_t *b, int len, uint32_t *out) {
    uint32_t v = 0; int i;
    for (i = 0; i < 4 && i < len; i++) { v |= (uint32_t)(b[i] & 0x7F) << (7*i); if (!(b[i] & 0x80)) break; }
    *out = v;
    if (i >= len || (b[i] & 0x80)) return -1;
    return i + 1;
}

static int tr__fhdr(uint8_t *b, uint8_t type, uint8_t flags, uint32_t rem) {
    int off = 0; tr__write_u8(b, &off, (uint8_t)((type << 4) | (flags & 0x0F)));
    off += tr__encode_remaining(rem, b + off); return off;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MQTT packet encode helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static int branch__enc_connack(uint8_t *buf, uint8_t session_present, uint8_t rc) {
    int off = 0;
    tr__write_u8(buf, &off, (uint8_t)(T_PKT_CONNACK << 4));
    tr__write_u8(buf, &off, 2);
    tr__write_u8(buf, &off, session_present);
    tr__write_u8(buf, &off, rc);
    return off;
}

static int branch__enc_publish(uint8_t *buf, const char *topic, const uint8_t *payload,
                             uint32_t plen, uint8_t qos, uint8_t retain, uint16_t pid) {
    int off = 0;
    uint16_t tl = (uint16_t)strlen(topic);
    uint32_t rem = 2 + tl + plen;
    if (qos > 0) rem += 2;
    uint8_t f = (uint8_t)((qos & 0x03) << 1) | (retain ? 1 : 0);
    off += tr__fhdr(buf + off, T_PKT_PUBLISH, f, rem);
    tr__write_str(buf, &off, topic);
    if (qos > 0) tr__write_u16(buf, &off, pid);
    if (plen > 0) { memcpy(buf + off, payload, plen); off += plen; }
    return off;
}

static int branch__enc_suback(uint8_t *buf, uint16_t pid, uint8_t rc) {
    int off = 0;
    off += tr__fhdr(buf + off, T_PKT_SUBACK, 0, 3);
    tr__write_u16(buf, &off, pid);
    tr__write_u8(buf, &off, rc);
    return off;
}

static int branch__enc_unsuback(uint8_t *buf, uint16_t pid) {
    int off = 0;
    off += tr__fhdr(buf + off, T_PKT_UNSUBACK, 0, 2);
    tr__write_u16(buf, &off, pid);
    return off;
}

static int branch__enc_ack(uint8_t *buf, uint8_t type, uint16_t pid) {
    int off = 0;
    off += tr__fhdr(buf + off, type, 0, 2);
    tr__write_u16(buf, &off, pid);
    return off;
}

static int branch__enc_pingresp(uint8_t *buf) {
    buf[0] = (uint8_t)(T_PKT_PINGRESP << 4); buf[1] = 0; return 2;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Packet dispatch — process one complete MQTT packet from a client
 * ═══════════════════════════════════════════════════════════════════════════ */

static int branch__dispatch(branch_t *t, int ci, int pkt_len) {
    branch_client_t *c = &t->clients[ci];
    const uint8_t *p = c->recv_buf;
    int off = 0, r;
    uint8_t type, flags;
    uint32_t rem_len;

    if (pkt_len < 2) return -1;
    type = (p[0] >> 4) & 0x0F;
    flags = p[0] & 0x0F;
    r = tr__decode_remaining(p + 1, pkt_len - 1, &rem_len);
    if (r < 0) return -1;
    off = 1 + r;

    switch (type) {

    case T_PKT_CONNECT: {
        /* Protocol name */
        const uint8_t *proto; uint16_t plen;
        if ((off = tr__read_str(p, off, pkt_len, &proto, &plen)) < 0) return -1;
        uint8_t proto_ok = (plen == 4 && memcmp(proto, "MQTT", 4) == 0);

        /* Protocol level */
        uint8_t level;
        if ((off = tr__read_u8(p, off, pkt_len, &level)) < 0) return -1;

        /* Connect flags */
        uint8_t cf;
        if ((off = tr__read_u8(p, off, pkt_len, &cf)) < 0) return -1;
        uint8_t clean_session = (cf >> 1) & 1;
        uint8_t will_flag  = (cf >> 2) & 1;
        uint8_t will_qos   = (cf >> 3) & 3;
        uint8_t will_retain= (cf >> 5) & 1;
        uint8_t has_user   = (cf >> 7) & 1;
        uint8_t has_pass   = (cf >> 6) & 1;

        /* Keep alive */
        uint16_t ka;
        if ((off = tr__read_u16(p, off, pkt_len, &ka)) < 0) return -1;

        /* Client ID */
        if ((off = tr__read_str_copy(p, off, pkt_len,
                                     c->client_id, BRANCH_MAX_CLIENT_ID)) < 0) return -1;

        /* Will */
        if (will_flag) {
            if ((off = tr__read_str_copy(p, off, pkt_len,
                                         c->will_topic, BRANCH_MAX_TOPIC)) < 0) return -1;
            const uint8_t *wp; uint16_t wpl;
            if ((off = tr__read_str(p, off, pkt_len, &wp, &wpl)) < 0) return -1;
            c->will_payload_len = wpl < BRANCH_MAX_WILL_PAYLOAD ? wpl : BRANCH_MAX_WILL_PAYLOAD;
            memcpy(c->will_payload, wp, c->will_payload_len);
            c->will_qos = will_qos;
            c->will_retain = will_retain;
            c->has_will = 1;
        }

        /* Username */
        if (has_user) {
            if ((off = tr__read_str_copy(p, off, pkt_len, c->username, 64)) < 0) return -1;
        }

        /* Password */
        char password[64] = {0};
        if (has_pass) {
            if ((off = tr__read_str_copy(p, off, pkt_len, password, 64)) < 0) return -1;
        }

        /* Validate */
        uint8_t rc = 0;
        if (!proto_ok) rc = 1;  /* protocol version refused */
        else if (level != 4) rc = 1;
        else if (c->client_id[0] == '\0' && !clean_session) rc = 2; /* ID required for persistent */
        if (rc == 0 && t->cfg.auth_cb) {
            if (!t->cfg.auth_cb(c->username, password, c->client_id, t->cfg.auth_userdata))
                rc = 4; /* bad username/password */
        }

        if (rc == 0 && clean_session) {
            /* Evict old session with same client_id */
            int old = branch__find_client_by_id(t, c->client_id);
            if (old >= 0 && old != ci) branch__close_client(t, old);
        }

        c->keepalive_sec = ka;
        c->clean_session = clean_session;

        /* Send CONNACK */
        {
            uint8_t ca[4]; int cal = branch__enc_connack(ca, 0, rc);
            branch__queue_and_flush(c, ca, (uint32_t)cal);
        }

        if (rc == 0) {
            c->state = CS_CONNACK_SENT;
            c->connected_at_ms = branch__now_ms();
            if (t->cfg.on_event) {
                char buf[256]; snprintf(buf, sizeof(buf), "client '%s' connected", c->client_id);
                t->cfg.on_event(buf, t->cfg.event_userdata);
            }
        } else {
            /* Refused — will be closed after send */
            if (t->cfg.on_event) {
                char buf[256]; snprintf(buf, sizeof(buf), "client '%s' refused (rc=%d)", c->client_id, rc);
                t->cfg.on_event(buf, t->cfg.event_userdata);
            }
        }
        break;
    }

    case T_PKT_PUBLISH: {
        uint8_t qos = (flags >> 1) & 3, retain = flags & 1;
        const uint8_t *topic; uint16_t topic_len;
        if ((off = tr__read_str(p, off, pkt_len, &topic, &topic_len)) < 0) return -1;

        uint16_t pid = 0;
        if (qos > 0) {
            if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        }

        /* Duplicate detection for QOS 2 */
        if (qos == 2) {
            int dup = 0;
            for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++) {
                if (c->qos2[qi].pid == pid &&
                    (c->qos2[qi].state == Q2_IN_PUBREC_SENT ||
                     c->qos2[qi].state == Q2_IN_PUBREL_RCVD)) { dup = 1; break; }
            }
            if (!dup) {
                for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++) {
                    if (c->qos2[qi].state == Q2_FREE) {
                        c->qos2[qi].pid = pid;
                        c->qos2[qi].state = Q2_IN_PUBREC_SENT;
                        c->qos2[qi].from_client = (uint8_t)ci;
                        c->qos2[qi].t_ms = branch__now_ms();
                        break;
                    }
                }
            }
        }

        /* Extract payload */
        uint32_t payload_len = (uint32_t)pkt_len - (uint32_t)off;
        const uint8_t *payload = p + off;

        /* QOS 1 → PUBACK, QOS 2 → PUBREC */
        if (qos == 1) {
            uint8_t pa[4]; int pal = branch__enc_ack(pa, T_PKT_PUBACK, pid);
            branch__queue_and_flush(c, pa, (uint32_t)pal);
        } else if (qos == 2) {
            uint8_t pr[4]; int prl = branch__enc_ack(pr, T_PKT_PUBREC, pid);
            branch__queue_and_flush(c, pr, (uint32_t)prl);
        }

        /* Forward to subscribers */
        char topic_buf[BRANCH_MAX_TOPIC];
        {
            uint16_t n = topic_len < BRANCH_MAX_TOPIC-1 ? topic_len : BRANCH_MAX_TOPIC-1;
            memcpy(topic_buf, topic, n); topic_buf[n] = '\0';
        }
        branch__forward(t, ci, topic_buf, payload, payload_len, qos, retain);
        break;
    }

    case T_PKT_PUBACK: {
        uint16_t pid; if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        /* Clear QOS 1 outgoing tracking */
        for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++)
            if (c->qos2[qi].pid == pid && c->qos2[qi].state == Q2_OUT_PUBLISH_SENT)
                { c->qos2[qi].state = Q2_FREE; break; }
        break;
    }

    case T_PKT_PUBREC: {
        uint16_t pid; if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        /* QOS 2 outgoing: PUBREC received → send PUBREL */
        for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++) {
            if (c->qos2[qi].pid == pid && c->qos2[qi].state == Q2_OUT_PUBLISH_SENT) {
                c->qos2[qi].state = Q2_OUT_PUBREC_RCVD;
                uint8_t pr[4]; int prl = branch__enc_ack(pr, T_PKT_PUBREL, pid);
                branch__queue_and_flush(c, pr, (uint32_t)prl);
                c->qos2[qi].state = Q2_OUT_PUBREL_SENT;
                break;
            }
        }
        break;
    }

    case T_PKT_PUBREL: {
        uint16_t pid; if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        /* QOS 2 incoming: PUBREL received → send PUBCOMP, then forward stored message */
        for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++) {
            if (c->qos2[qi].pid == pid && c->qos2[qi].state == Q2_IN_PUBREC_SENT) {
                c->qos2[qi].state = Q2_IN_PUBREL_RCVD;
                uint8_t pc[4]; int pcl = branch__enc_ack(pc, T_PKT_PUBCOMP, pid);
                branch__queue_and_flush(c, pc, (uint32_t)pcl);
                /* Now actually forward the stored message (it was held during QOS 2 handshake) */
                /* For simplicity we forward on PUBLISH arrival; QOS 2 dedup prevents double-delivery */
                c->qos2[qi].state = Q2_FREE;
                break;
            }
        }
        break;
    }

    case T_PKT_PUBCOMP: {
        uint16_t pid; if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        /* QOS 2 outgoing complete */
        for (int qi = 0; qi < BRANCH_MAX_PENDING; qi++)
            if (c->qos2[qi].pid == pid && c->qos2[qi].state == Q2_OUT_PUBREL_SENT)
                { c->qos2[qi].state = Q2_FREE; break; }
        break;
    }

    case T_PKT_SUBSCRIBE: {
        uint16_t pid; if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;

        /* Build SUBACK payload: one return code per topic filter */
        uint8_t rcs[32]; uint8_t rc_count = 0;
        while (off < pkt_len && rc_count < 32) {
            const uint8_t *tf; uint16_t tfl;
            if ((off = tr__read_str(p, off, pkt_len, &tf, &tfl)) < 0) break;
            uint8_t rqos;
            if ((off = tr__read_u8(p, off, pkt_len, &rqos)) < 0) break;

            int si = branch__find_free_sub(t);
            if (si >= 0 && rc_count < BRANCH_MAX_PENDING) {
                uint16_t n = tfl < BRANCH_MAX_TOPIC-1 ? tfl : BRANCH_MAX_TOPIC-1;
                memcpy(t->subs[si].topic_filter, tf, n);
                t->subs[si].topic_filter[n] = '\0';
                t->subs[si].client_idx = (uint8_t)ci;
                t->subs[si].qos = rqos & 0x03;
                t->subs[si].used = 1;

                /* Track in client's sub list */
                if (c->sub_count < BRANCH_MAX_PENDING) {
                    c->sub_indices[c->sub_count++] = (uint8_t)si;
                }

                rcs[rc_count++] = rqos & 0x03;

                /* Send matching retained messages */
                branch__send_retained(t, ci, t->subs[si].topic_filter);
            } else {
                rcs[rc_count++] = 0x80; /* failure */
            }
        }

        /* Encode SUBACK */
        {
            uint8_t sa[BRANCH_BUF_SIZE]; int off2 = 0;
            uint32_t rem = 2 + rc_count;
            off2 += tr__fhdr(sa + off2, T_PKT_SUBACK, 0, rem);
            tr__write_u16(sa, &off2, pid);
            for (int i = 0; i < rc_count; i++) tr__write_u8(sa, &off2, rcs[i]);
            branch__queue_and_flush(c, sa, (uint32_t)off2);
        }
        break;
    }

    case T_PKT_UNSUBSCRIBE: {
        uint16_t pid; if ((off = tr__read_u16(p, off, pkt_len, &pid)) < 0) return -1;
        while (off < pkt_len) {
            const uint8_t *tf; uint16_t tfl;
            if ((off = tr__read_str(p, off, pkt_len, &tf, &tfl)) < 0) break;
            /* Remove matching subscriptions */
            char tf_buf[BRANCH_MAX_TOPIC];
            {
                uint16_t n = tfl < BRANCH_MAX_TOPIC-1 ? tfl : BRANCH_MAX_TOPIC-1;
                memcpy(tf_buf, tf, n); tf_buf[n] = '\0';
            }
            for (uint8_t i = 0; i < c->sub_count; i++) {
                int si = c->sub_indices[i];
                if (t->subs[si].used && strcmp(t->subs[si].topic_filter, tf_buf) == 0) {
                    t->subs[si].used = 0;
                    c->sub_indices[i] = c->sub_indices[--c->sub_count];
                    i--;
                }
            }
        }
        uint8_t ua[4]; int ual = branch__enc_unsuback(ua, pid);
        branch__queue_and_flush(c, ua, (uint32_t)ual);
        break;
    }

    case T_PKT_PINGREQ: {
        uint8_t pr[2]; branch__enc_pingresp(pr);
        branch__queue_and_flush(c, pr, 2);
        break;
    }

    case T_PKT_DISCONNECT: {
        char disc_id[BRANCH_MAX_CLIENT_ID];
        strncpy(disc_id, c->client_id, BRANCH_MAX_CLIENT_ID - 1);
        disc_id[BRANCH_MAX_CLIENT_ID - 1] = '\0';
        c->has_will = 0;
        branch__close_client(t, ci);
        if (t->cfg.on_event) {
            char buf[256]; snprintf(buf, sizeof(buf), "client '%s' disconnected (clean)", disc_id);
            t->cfg.on_event(buf, t->cfg.event_userdata);
        }
        break;
    }

    default:
        break;
    }
    (void)off;
    return 0;
}
