/*
 * leaf.h — MQTT 3.1.1 Client Library (single-header, zero-allocation)
 *
 * Usage:
 *   #define LEAF_IMPLEMENTATION
 *   #include "leaf.h"
 *
 * Optional compile-time tuning (define before include):
 *   LEAF_BUF_SIZE       — recv/send buffer size       (default 4096)
 *   LEAF_MAX_PENDING    — max in-flight QOS 1/2 msgs  (default 8)
 *   LEAF_MAX_SUBS       — max subscriptions           (default 8)
 *   LEAF_MAX_CLIENT_ID  — max client ID length        (default 64)
 *   LEAF_MAX_TOPIC      — max topic filter length     (default 256)
 *   LEAF_KEEPALIVE_SEC  — default keepalive interval  (default 60)
 *   LEAF_RECONNECT_MS   — reconnect backoff start     (default 5000)
 *
 * License: MIT / 0BSD (dual-licensed at your option)
 */
#ifndef LEAF_H
#define LEAF_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Tuning ── */
#ifndef LEAF_BUF_SIZE
#define LEAF_BUF_SIZE       4096
#endif
#ifndef LEAF_MAX_PENDING
#define LEAF_MAX_PENDING    8
#endif
#ifndef LEAF_MAX_SUBS
#define LEAF_MAX_SUBS       8
#endif
#ifndef LEAF_MAX_CLIENT_ID
#define LEAF_MAX_CLIENT_ID  64
#endif
#ifndef LEAF_MAX_TOPIC
#define LEAF_MAX_TOPIC      256
#endif
#ifndef LEAF_KEEPALIVE_SEC
#define LEAF_KEEPALIVE_SEC  60
#endif
#ifndef LEAF_RECONNECT_MS
#define LEAF_RECONNECT_MS   5000
#endif

/* ── Public types ── */
typedef struct leaf_s  leaf_t;

typedef struct {
    const char *broker_ip;
    uint16_t    port;
    const char *client_id;
    const char *username;          /* NULL = anonymous */
    const char *password;
    uint16_t    keepalive_sec;
    uint8_t     clean_session;
    /* Will message */
    const char  *will_topic;
    const uint8_t *will_payload;
    uint32_t     will_payload_len;
    uint8_t      will_qos;
    uint8_t      will_retain;
} leaf_config_t;

typedef struct {
    const char    *topic;
    const uint8_t *payload;
    uint32_t       payload_len;
    uint8_t        qos;
    uint8_t        retain;
} leaf_msg_t;

typedef void (*leaf_on_connect_cb)(void *userdata);
typedef void (*leaf_on_disconnect_cb)(void *userdata);
typedef void (*leaf_on_message_cb)(const leaf_msg_t *msg, void *userdata);

/* ── Lifecycle ── */
leaf_t* leaf_create(const leaf_config_t *cfg);
void    leaf_destroy(leaf_t *c);

/* ── Commands (non-blocking; results come via callbacks in leaf_poll) ── */
int leaf_connect(leaf_t *c);
int leaf_disconnect(leaf_t *c);
int leaf_subscribe(leaf_t *c, const char *topic_filter, uint8_t qos);
int leaf_unsubscribe(leaf_t *c, const char *topic_filter);
int leaf_publish(leaf_t *c, const char *topic, const void *payload,
                 uint32_t len, uint8_t qos, uint8_t retain);

/* ── Event loop ── */
int leaf_poll(leaf_t *c, int timeout_ms); /* -1=forever, 0=non-block, >0=ms */

/* ── Callbacks ── */
void leaf_set_on_connect(leaf_t *c, leaf_on_connect_cb cb, void *userdata);
void leaf_set_on_disconnect(leaf_t *c, leaf_on_disconnect_cb cb, void *userdata);
void leaf_set_on_message(leaf_t *c, leaf_on_message_cb cb, void *userdata);

/* ── Status ── */
int  leaf_is_connected(leaf_t *c);
int  leaf_is_ready(leaf_t *c);        /* CONNACK received, can pub/sub */

#ifdef __cplusplus
}
#endif
#endif /* LEAF_H */

/* ═══════════════════════════════════════════════════════════════════════════
 * Implementation
 * ═══════════════════════════════════════════════════════════════════════════ */
#ifdef LEAF_IMPLEMENTATION
#include "leaf/leaf_types.h"
#include "leaf/leaf_codec.h"
#include "leaf/leaf_client.h"
#endif
