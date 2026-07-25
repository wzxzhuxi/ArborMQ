/*
 * branch.h — MQTT 3.1.1 Broker (single-header, zero-allocation)
 *
 * Usage:
 *   #define BRANCH_IMPLEMENTATION
 *   #include "branch.h"
 *
 * Tuning macros (define before include):
 *   BRANCH_MAX_CLIENTS    — max concurrent connections      (default 32)
 *   BRANCH_MAX_SUBS       — total subscription slots        (default 128)
 *   BRANCH_MAX_RETAINED   — max retained messages           (default 64)
 *   BRANCH_MAX_PENDING    — max in-flight QOS 1/2 per client (default 8)
 *   BRANCH_BUF_SIZE       — per-client recv/send buffer     (default 4096)
 *   BRANCH_MAX_TOPIC      — max topic length                (default 256)
 *   BRANCH_MAX_CLIENT_ID  — max client ID length            (default 128)
 *   BRANCH_MAX_WILL_PAYLOAD — max will payload              (default 512)
 *
 * License: MIT / 0BSD
 */
#ifndef BRANCH_H
#define BRANCH_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Tuning ── */
#ifndef BRANCH_MAX_CLIENTS
#define BRANCH_MAX_CLIENTS     32
#endif
#ifndef BRANCH_MAX_SUBS
#define BRANCH_MAX_SUBS       128
#endif
#ifndef BRANCH_MAX_RETAINED
#define BRANCH_MAX_RETAINED    64
#endif
#ifndef BRANCH_MAX_PENDING
#define BRANCH_MAX_PENDING      8
#endif
#ifndef BRANCH_BUF_SIZE
#define BRANCH_BUF_SIZE      4096
#endif
#ifndef BRANCH_MAX_TOPIC
#define BRANCH_MAX_TOPIC      256
#endif
#ifndef BRANCH_MAX_CLIENT_ID
#define BRANCH_MAX_CLIENT_ID  128
#endif
#ifndef BRANCH_MAX_WILL_PAYLOAD
#define BRANCH_MAX_WILL_PAYLOAD 512
#endif

/* ── Public types ── */
typedef struct branch_s branch_t;

typedef struct {
    uint16_t port;
    int (*auth_cb)(const char *username, const char *password,
                   const char *client_id, void *userdata);
    void *auth_userdata;
    void (*on_event)(const char *msg, void *userdata);  /* optional event log */
    void *event_userdata;
} branch_config_t;

typedef struct {
    char     client_id[BRANCH_MAX_CLIENT_ID];
    uint32_t connected_sec;
    uint8_t  active;
} branch_client_info_t;

/* ── Lifecycle ── */
branch_t* branch_create(const branch_config_t *cfg);
void    branch_destroy(branch_t *t);

/* ── Event loop ── */
int branch_poll(branch_t *t, int timeout_ms);  /* -1=forever, 0=不等待, >0=毫秒 */

/* ── Broker-side publish ── */
int branch_publish(branch_t *t, const char *topic, const void *payload,
                 uint32_t len, uint8_t qos, uint8_t retain);

/* ── Query ── */
int branch_client_count(branch_t *t);
int branch_get_clients(branch_t *t, branch_client_info_t *dst, int max);

#ifdef __cplusplus
}
#endif
#endif /* BRANCH_H */

#ifdef BRANCH_IMPLEMENTATION
#include "branch/branch_types.h"
#include "branch/branch_codec.h"
#include "branch/branch_match.h"
#include "branch/branch_broker.h"
#endif
