#ifndef LEAF_TYPES_H
#define LEAF_TYPES_H

/* leaf.h is included first by the implementation guard — its public types
 * and tuning macros (leaf_t, LEAF_BUF_SIZE, LEAF_MAX_TOPIC, ...) are
 * already visible when this file is processed.  No circular include. */
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* ── MQTT packet types ── */
enum { LEAF_PKT_CONNECT=1,  LEAF_PKT_CONNACK=2,    LEAF_PKT_PUBLISH=3,
       LEAF_PKT_PUBACK=4,   LEAF_PKT_PUBREC=5,     LEAF_PKT_PUBREL=6,
       LEAF_PKT_PUBCOMP=7,  LEAF_PKT_SUBSCRIBE=8,  LEAF_PKT_SUBACK=9,
       LEAF_PKT_UNSUBSCRIBE=10, LEAF_PKT_UNSUBACK=11,
       LEAF_PKT_PINGREQ=12, LEAF_PKT_PINGRESP=13,  LEAF_PKT_DISCONNECT=14 };

/* ── Internal client state ── */
enum { LEAF_S_DISCONNECTED, LEAF_S_TCP_CONNECTING, LEAF_S_TCP_CONNECTED,
       LEAF_S_CONNACK_WAIT,  LEAF_S_READY,          LEAF_S_DISCONNECTING };

/* ── Per-QOS>0 pending slot ── */
typedef struct {
    uint16_t packet_id;
    uint8_t  type;          /* original packet type (PUBLISH/PUBREC/PUBREL) */
    uint8_t  retries;
    uint8_t  payload[LEAF_BUF_SIZE];
    uint32_t payload_len;
    uint64_t sent_at_ms;
} leaf_pending_t;

/* ── Subscription slot ── */
typedef struct {
    char     topic[LEAF_MAX_TOPIC];
    uint8_t  qos;
} leaf_sub_t;

/* ── Main client struct (all statically-sized, no malloc) ── */
struct leaf_s {
    /* Config */
    leaf_config_t cfg;

    /* Network */
    int      fd;
    uint8_t  state;
    uint8_t  clean_disconnect;  /* DISCONNECT sent -> don't will */

    /* Buffers */
    uint8_t  recv_buf[LEAF_BUF_SIZE];
    uint32_t recv_len;
    uint8_t  send_buf[LEAF_BUF_SIZE];
    uint32_t send_len;
    uint32_t send_off;

    /* QOS 1/2 tracking */
    uint16_t         next_packet_id;
    leaf_pending_t   pending[LEAF_MAX_PENDING];
    uint16_t         received_ids[LEAF_MAX_PENDING]; /* dedup bitmap-ish */

    /* Subscriptions */
    leaf_sub_t subs[LEAF_MAX_SUBS];
    uint8_t    sub_count;

    /* Keepalive */
    uint64_t last_send_ms;
    uint64_t last_recv_ms;
    uint64_t last_poll_ms;

    /* Callbacks */
    leaf_on_connect_cb    on_connect;
    leaf_on_disconnect_cb on_disconnect;
    leaf_on_message_cb    on_message;
    void                 *cb_userdata;

    /* Reconnect */
    int      reconnect_backoff_ms;
};

/* ── Forward decls ── */
static void leaf__reset(leaf_t *c);
static int  leaf__connect_tcp(leaf_t *c);
static void leaf__close_fd(leaf_t *c);
static int  leaf__send_pending(leaf_t *c);
static int  leaf__recv_and_dispatch(leaf_t *c);
static void leaf__fire_disconnect(leaf_t *c);

/* ═══════════════════════════════════════════════════════════════════════════
 * Platform helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static uint64_t leaf__now_ms(void) {
    struct timeval tv; gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void leaf__nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static void leaf__nodelay(int fd) {
    int v = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &v, sizeof(v));
}

#endif /* LEAF_TYPES_H */
