/*
 * test_leaf.c — Unit tests for leaf.h MQTT client protocol encoding/decoding
 *
 * Build:  cc -std=c99 -o test_leaf test_leaf.c
 * Run:    ./test_leaf
 *
 * Tests remaining-length codec, CONNECT/PUBLISH/SUBSCRIBE packet encoding,
 * packet dispatch (CONNACK/PUBLISH received from broker), and edge cases.
 */
#define LEAF_IMPLEMENTATION
#include "leaf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name)  static void test_##name(void)
#define RUN(name)   do { printf("  %-50s", #name); test_##name(); \
                         printf("  PASS\n"); g_passed++; } while(0)

/* ── Utils ── */

static void assert_buf_eq(const uint8_t *a, const uint8_t *b, int len, const char *msg) {
    if (memcmp(a, b, len) != 0) {
        fprintf(stderr, "  FAIL: %s\n  expected: ", msg);
        for (int i = 0; i < len; i++) fprintf(stderr, "%02x ", a[i]);
        fprintf(stderr, "\n  got:      ");
        for (int i = 0; i < len; i++) fprintf(stderr, "%02x ", b[i]);
        fprintf(stderr, "\n");
        g_failed++;
        abort();
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Remaining-length codec
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(rem_len_roundtrip_small) {
    uint32_t vals[] = {0, 1, 127, 128, 255, 16383, 16384, 2097151, 2097152, 268435455};
    for (int i = 0; i < (int)(sizeof(vals)/sizeof(vals[0])); i++) {
        uint8_t enc[4];
        int elen = leaf__encode_remaining(vals[i], enc);
        uint32_t dec;
        int dlen = leaf__decode_remaining(enc, elen, &dec);
        assert(dlen == elen && dec == vals[i]);
    }
}

TEST(rem_len_encode_known) {
    /* 0   → 0x00 */
    /* 127 → 0x7F */
    /* 128 → 0x80 0x01 */
    /* 16383 → 0xFF 0x7F */
    uint8_t buf[4];
    assert(leaf__encode_remaining(0, buf) == 1 && buf[0] == 0x00);
    assert(leaf__encode_remaining(127, buf) == 1 && buf[0] == 0x7F);
    int n = leaf__encode_remaining(128, buf);
    assert(n == 2 && buf[0] == 0x80 && buf[1] == 0x01);
    n = leaf__encode_remaining(16383, buf);
    assert(n == 2 && buf[0] == 0xFF && buf[1] == 0x7F);
}

TEST(rem_len_decode_incomplete) {
    uint8_t buf[] = {0x80, 0x80, 0x80}; /* needs 4th byte */
    uint32_t out;
    int n = leaf__decode_remaining(buf, 3, &out);
    assert(n == -1); /* incomplete */
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MQTT string encoding
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(mqtt_str_encode) {
    uint8_t buf[10];
    int off = 0;
    leaf__write_str(buf, &off, "MQTT");
    /* MQTT = 4 chars → 0x00 0x04 'M' 'Q' 'T' 'T' */
    uint8_t expect[] = {0x00, 0x04, 'M', 'Q', 'T', 'T'};
    assert(off == 6);
    assert_buf_eq(expect, buf, 6, "MQTT string");
}

/* ═══════════════════════════════════════════════════════════════════════════
 * CONNECT packet encoding
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(connect_minimal) {
    leaf_config_t cfg = {0};
    cfg.client_id = "test";
    cfg.keepalive_sec = 60;
    cfg.clean_session = 1;

    leaf_t *c = leaf_create(&cfg);
    assert(c != NULL);

    int len = leaf__encode_connect(c);
    assert(len > 4);
    assert((c->send_buf[0] >> 4) == 1); /* CONNECT type */

    /* Verify protocol name "MQTT" somewhere after the fixed header */
    int found_mqtt = 0;
    for (int i = 2; i < len - 3; i++) {
        if (memcmp(c->send_buf + i, "\x00\x04MQTT", 6) == 0) { found_mqtt = 1; break; }
    }
    assert(found_mqtt);

    leaf_destroy(c);
}

TEST(connect_with_auth) {
    leaf_config_t cfg = {0};
    cfg.client_id = "dev1";
    cfg.username = "admin";
    cfg.password = "secret";
    cfg.keepalive_sec = 30;
    cfg.clean_session = 1;

    leaf_t *c = leaf_create(&cfg);
    assert(c != NULL);

    int len = leaf__encode_connect(c);
    assert(len > 10);

    /* Find the connect flags byte: after fixed header + protocol name(6) + level(1) */
    uint8_t *p = c->send_buf;
    int off = 1; /* skip type byte */
    /* skip remaining length (1 byte for small packets) */
    off += 1;
    /* skip protocol name (6 bytes: 00 04 M Q T T) + level (1 byte) */
    off += 6 + 1;
    uint8_t flags = p[off];
    assert((flags & 0xC0) == 0xC0); /* username + password flags */

    leaf_destroy(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PUBLISH packet encoding
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(publish_qos0) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    uint8_t payload[] = "hello";
    c->send_len = (uint32_t)leaf__encode_publish(c, "sensor/temp", payload, 5, 0, 0, 0);

    /* First byte: PUBLISH type (3 << 4) | flags (0) = 0x30 */
    assert((c->send_buf[0] >> 4) == 3);
    assert((c->send_buf[0] & 0x0F) == 0); /* no DUP/QOS/RETAIN */

    leaf_destroy(c);
}

TEST(publish_qos1) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    uint8_t payload[] = "hello";
    c->send_len = (uint32_t)leaf__encode_publish(c, "a/b", payload, 5, 1, 0, 42);

    /* Flags: QOS=1 → (1 << 1) = 0x02 */
    assert((c->send_buf[0] & 0x06) == 0x02);
    /* Packet ID 42 should be in the packet */
    int found_pid = 0;
    for (uint32_t i = 0; i < c->send_len - 1; i++) {
        if (c->send_buf[i] == 0 && c->send_buf[i+1] == 42) { found_pid = 1; break; }
    }
    assert(found_pid);

    leaf_destroy(c);
}

TEST(publish_retain) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    c->send_len = (uint32_t)leaf__encode_publish(c, "x", (uint8_t*)"y", 1, 0, 1, 0);
    assert((c->send_buf[0] & 0x01) == 0x01); /* retain flag */
    leaf_destroy(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SUBSCRIBE packet encoding
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(subscribe_basic) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    c->send_len = (uint32_t)leaf__encode_subscribe(c, "sensor/#", 1, 100);

    /* First byte: SUBSCRIBE type (8 << 4) | flags (2) = 0x82 */
    assert((c->send_buf[0] >> 4) == 8);
    assert((c->send_buf[0] & 0x0F) == 2);

    leaf_destroy(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Fixed header helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(pingreq_pingresp) {
    uint8_t ping[4];
    int len = leaf__encode_pingreq(ping);
    assert(len == 2 && ping[0] == 0xC0 && ping[1] == 0x00);

    /* PINGRESP is same structure, different type */
    /* leaf__encode_pingresp doesn't exist, but disconnect does: */
}

TEST(disconnect_packet) {
    uint8_t disc[4];
    int len = leaf__encode_disconnect(disc);
    assert(len == 2 && disc[0] == 0xE0 && disc[1] == 0x00);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Packet dispatch (decode) tests
 * ═══════════════════════════════════════════════════════════════════════════ */

static int g_connack_rcvd = 0;
static int g_publish_rcvd = 0;
static char g_last_topic[256];
static char g_last_payload[256];

static void test_on_connect(void *ud)   { (void)ud; g_connack_rcvd = 1; }
static void test_on_disconnect(void *ud){ (void)ud; }
static void test_on_message(const leaf_msg_t *m, void *ud) {
    (void)ud;
    g_publish_rcvd = 1;
    uint32_t n = m->payload_len < 255 ? m->payload_len : 255;
    memcpy(g_last_payload, m->payload, n); g_last_payload[n] = '\0';
    strncpy(g_last_topic, m->topic, 255); g_last_topic[255] = '\0';
}

TEST(dispatch_connack_accepted) {
    leaf_config_t cfg = {.client_id = "t", .keepalive_sec = 30, .clean_session = 1};
    leaf_t *c = leaf_create(&cfg);
    leaf_set_on_connect(c, test_on_connect, NULL);
    leaf_set_on_disconnect(c, test_on_disconnect, NULL);
    leaf_set_on_message(c, test_on_message, NULL);

    /* Craft CONNACK: type=2, flags=0, remaining=2, session_present=0, return_code=0 */
    uint8_t connack[] = {0x20, 0x02, 0x00, 0x00};
    memcpy(c->recv_buf, connack, 4);
    c->recv_len = 4;

    g_connack_rcvd = 0;
    int rc = leaf__dispatch(c, 4);
    assert(rc == 0);
    assert(g_connack_rcvd == 1);
    assert(leaf_is_ready(c));

    leaf_destroy(c);
}

TEST(dispatch_connack_refused) {
    leaf_config_t cfg = {.client_id = "t", .keepalive_sec = 30, .clean_session = 1};
    leaf_t *c = leaf_create(&cfg);
    leaf_set_on_connect(c, test_on_connect, NULL);
    leaf_set_on_disconnect(c, test_on_disconnect, NULL);
    leaf_set_on_message(c, test_on_message, NULL);

    /* CONNACK with return_code=4 (bad username/password) */
    uint8_t connack[] = {0x20, 0x02, 0x00, 0x04};
    memcpy(c->recv_buf, connack, 4);
    c->recv_len = 4;

    g_connack_rcvd = 0;
    int rc = leaf__dispatch(c, 4);
    assert(rc == 0);
    assert(g_connack_rcvd == 0);  /* on_connect should NOT fire */
    assert(!leaf_is_ready(c));

    leaf_destroy(c);
}

TEST(dispatch_publish_qos0) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    leaf_set_on_connect(c, test_on_connect, NULL);
    leaf_set_on_disconnect(c, test_on_disconnect, NULL);
    leaf_set_on_message(c, test_on_message, NULL);
    c->state = LEAF_S_READY; /* must be connected to receive */

    /* PUBLISH: type=3, flags=0, topic "a/b" (3 chars), payload "hi" (2 chars)
     * remaining = 2(topic_len) + 3(topic) + 2(payload) = 7 */
    uint8_t publish[] = {0x30, 0x07, 0x00, 0x03, 'a', '/', 'b', 'h', 'i'};
    memcpy(c->recv_buf, publish, 9);
    c->recv_len = 9;

    g_publish_rcvd = 0;
    int rc = leaf__dispatch(c, 9);
    assert(rc == 0);
    assert(g_publish_rcvd == 1);
    assert(strcmp(g_last_topic, "a/b") == 0);
    assert(strcmp(g_last_payload, "hi") == 0);

    leaf_destroy(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * State machine transitions
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(state_initial) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    assert(c != NULL);
    assert(!leaf_is_connected(c));
    assert(!leaf_is_ready(c));
    leaf_destroy(c);
}

TEST(state_after_connect) {
    leaf_config_t cfg = {.broker_ip = "127.0.0.1", .port = 19999, .client_id = "t",
                          .keepalive_sec = 30, .clean_session = 1};
    leaf_t *c = leaf_create(&cfg);
    /* connect to a closed port — should fail gracefully */
    int rc = leaf_connect(c);
    /* connect should return 0 (async start) or -1 (immediate failure) */
    assert(rc == 0 || rc == -1);
    assert(!leaf_is_ready(c)); /* won't be ready without broker */
    leaf_destroy(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Edge cases
 * ═══════════════════════════════════════════════════════════════════════════ */

TEST(empty_client_id) {
    leaf_config_t cfg = {.client_id = "", .clean_session = 0};
    leaf_t *c = leaf_create(&cfg);
    /* MQTT allows empty client ID only with clean_session=1 */
    /* leaf should allow creation, broker will reject */
    assert(c != NULL);
    leaf_destroy(c);
}

TEST(max_topic_length) {
    leaf_config_t cfg = {.client_id = "t"};
    leaf_t *c = leaf_create(&cfg);
    /* Topic longer than LEAF_MAX_TOPIC should be truncated by subscribe */
    char long_topic[512];
    memset(long_topic, 'a', 500); long_topic[500] = '\0';
    int rc = leaf_subscribe(c, long_topic, 0);
    /* Should fail because too long for internal buffer */
    assert(rc == -1);
    leaf_destroy(c);
}

TEST(double_create) {
    leaf_config_t cfg = {.client_id = "a"};
    leaf_t *c1 = leaf_create(&cfg);
    assert(c1 != NULL);
    leaf_t *c2 = leaf_create(&cfg);
    assert(c2 == NULL); /* singleton — second create fails */
    leaf_destroy(c1);
}

/* ═══════════════════════════════════════════════════════════════════════════ */

int main(void) {
    printf("\n=== leaf.h unit tests ===\n\n");

    printf("Remaining-length codec:\n");
    RUN(rem_len_roundtrip_small);
    RUN(rem_len_encode_known);
    RUN(rem_len_decode_incomplete);

    printf("MQTT string encoding:\n");
    RUN(mqtt_str_encode);

    printf("CONNECT packet:\n");
    RUN(connect_minimal);
    RUN(connect_with_auth);

    printf("PUBLISH packet:\n");
    RUN(publish_qos0);
    RUN(publish_qos1);
    RUN(publish_retain);

    printf("SUBSCRIBE packet:\n");
    RUN(subscribe_basic);

    printf("Control packets:\n");
    RUN(pingreq_pingresp);
    RUN(disconnect_packet);

    printf("Packet dispatch:\n");
    RUN(dispatch_connack_accepted);
    RUN(dispatch_connack_refused);
    RUN(dispatch_publish_qos0);

    printf("State machine:\n");
    RUN(state_initial);
    RUN(state_after_connect);

    printf("Edge cases:\n");
    RUN(empty_client_id);
    RUN(max_topic_length);
    RUN(double_create);

    printf("\n=== %d passed, %d failed ===\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
