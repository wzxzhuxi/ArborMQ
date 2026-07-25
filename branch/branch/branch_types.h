#pragma once
/* tree.h public types (branch_t, branch_config_t, macros) are visible via BRANCH_IMPLEMENTATION */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* ── Packet types ── */
enum { T_PKT_CONNECT=1, T_PKT_CONNACK=2, T_PKT_PUBLISH=3,
       T_PKT_PUBACK=4, T_PKT_PUBREC=5, T_PKT_PUBREL=6, T_PKT_PUBCOMP=7,
       T_PKT_SUBSCRIBE=8, T_PKT_SUBACK=9, T_PKT_UNSUBSCRIBE=10, T_PKT_UNSUBACK=11,
       T_PKT_PINGREQ=12, T_PKT_PINGRESP=13, T_PKT_DISCONNECT=14 };

/* ── QOS 2 flow state for broker ── */
enum { Q2_FREE, Q2_IN_PUBREC_SENT, Q2_IN_PUBREL_RCVD,  /* incoming QOS2 */
           Q2_OUT_PUBLISH_SENT, Q2_OUT_PUBREC_RCVD, Q2_OUT_PUBREL_SENT }; /* outgoing QOS2 */

typedef struct {
    uint16_t pid;
    uint8_t  state;      /* Q2_* */
    uint8_t  payload[BRANCH_BUF_SIZE];
    uint32_t payload_len;
    char     topic[BRANCH_MAX_TOPIC];
    uint8_t  from_client; /* index into clients[] */
    uint64_t t_ms;
} branch_qos2_t;

/* ── Per-client state ── */
enum { CS_DISCONNECTED, CS_CONNACK_SENT, CS_READY, CS_WILL_TRIGGERED };

typedef struct {
    int      fd;
    uint8_t  state;
    uint8_t  clean_session;

    /* Buffers */
    uint8_t  recv_buf[BRANCH_BUF_SIZE];
    uint32_t recv_len;
    uint8_t  send_buf[BRANCH_BUF_SIZE];
    uint32_t send_len;
    uint32_t send_off;

    /* MQTT identity */
    char     client_id[BRANCH_MAX_CLIENT_ID];
    char     username[64];
    uint16_t keepalive_sec;
    uint64_t last_recv_ms;
    uint64_t last_send_ms;

    /* Will */
    char     will_topic[BRANCH_MAX_TOPIC];
    uint8_t  will_payload[BRANCH_MAX_WILL_PAYLOAD];
    uint32_t will_payload_len;
    uint8_t  will_qos;
    uint8_t  will_retain;
    uint8_t  has_will;

    /* QOS 2 tracking */
    branch_qos2_t qos2[BRANCH_MAX_PENDING];

    /* Subscriptions held by this client (indices into branch_t::subs[]) */
    uint8_t  sub_indices[BRANCH_MAX_PENDING];  /* reused count; rename conceptually */
    uint8_t  sub_count;

    /* Connect time */
    uint64_t connected_at_ms;
} branch_client_t;

/* ── Subscription entry ── */
typedef struct {
    char    topic_filter[BRANCH_MAX_TOPIC];
    uint8_t client_idx;
    uint8_t qos;
    uint8_t used;
} branch_sub_t;

/* ── Retained message ── */
typedef struct {
    char     topic[BRANCH_MAX_TOPIC];
    uint8_t  payload[BRANCH_BUF_SIZE];
    uint32_t payload_len;
    uint8_t  qos;
    uint8_t  used;
} branch_retained_t;

/* ── Main broker struct ── */
struct branch_s {
    branch_config_t  cfg;
    int            listen_fd;
    branch_client_t  clients[BRANCH_MAX_CLIENTS];
    branch_sub_t     subs[BRANCH_MAX_SUBS];
    branch_retained_t retained[BRANCH_MAX_RETAINED];
    uint16_t       next_pid; /* global packet ID allocator */
    uint64_t       start_ms;
};

/* ═══════════════════════════════════════════════════════════════════════════
 * Platform helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static uint64_t branch__now_ms(void) {
    struct timeval tv; gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void branch__nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static void branch__reuseaddr(int fd) {
    int v = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(v));
}

static void branch__nodelay(int fd) {
    int v = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &v, sizeof(v));
}
