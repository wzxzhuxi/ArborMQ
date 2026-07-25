/*
 * test_tree.c — Unit tests for tree.h MQTT broker
 *
 * Build:  cc -std=c99 -o test_tree test_tree.c
 * Run:    ./test_tree
 */
#define BRANCH_IMPLEMENTATION
#include "branch.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name)  static void test_##name(void)
#define RUN(name)   do { printf("  %-55s", #name); test_##name(); \
                         printf("  PASS\n"); g_passed++; } while(0)

/* ── Topic filter matching ── */

TEST(match_exact) {
    assert(branch__match_topic("a/b/c", "a/b/c") == 1);
    assert(branch__match_topic("foo", "foo") == 1);
    assert(branch__match_topic("foo", "bar") == 0);
    assert(branch__match_topic("a/b", "a/b/c") == 0);
    assert(branch__match_topic("a/b/c", "a/b") == 0);
}

TEST(match_single_wildcard) {
    assert(branch__match_topic("+/b/c", "a/b/c") == 1);
    assert(branch__match_topic("a/+/c", "a/b/c") == 1);
    assert(branch__match_topic("a/b/+", "a/b/c") == 1);
    assert(branch__match_topic("+/+/+", "a/b/c") == 1);
    assert(branch__match_topic("+/c", "a/b/c") == 0);
    assert(branch__match_topic("a/+", "a") == 0);
}

TEST(match_multi_wildcard) {
    assert(branch__match_topic("a/#", "a/b") == 1);
    assert(branch__match_topic("a/#", "a/b/c") == 1);
    assert(branch__match_topic("a/#", "a/b/c/d/e") == 1);
    assert(branch__match_topic("#", "anything") == 1);
    assert(branch__match_topic("#", "a/b/c") == 1);
    assert(branch__match_topic("a/#/c", "a/b/c") == 0);
}

TEST(match_combined) {
    assert(branch__match_topic("+/+/#", "a/b/c/d") == 1);
    assert(branch__match_topic("sensor/+/temp", "sensor/kitchen/temp") == 1);
    assert(branch__match_topic("sensor/+/temp", "sensor/bedroom/temp") == 1);
    assert(branch__match_topic("sensor/+/temp", "sensor/kitchen/humidity") == 0);
    assert(branch__match_topic("sensor/#", "sensor/kitchen/temp") == 1);
}

TEST(match_edge_cases) {
    assert(branch__match_topic("", "") == 1);
    assert(branch__match_topic("a/b/#", "a/b") == 1);
    assert(branch__match_topic("a/b/", "a/b/") == 1);
}

/* ── Broker lifecycle ── */

TEST(broker_create_destroy) {
    branch_config_t cfg = {.port = 19998};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);
    assert(branch_client_count(t) == 0);
    branch_destroy(t);
}

TEST(broker_publish_no_clients) {
    branch_config_t cfg = {.port = 19997};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);
    int rc = branch_publish(t, "test/hello", "world", 5, 0, 0);
    assert(rc == 0);
    branch_destroy(t);
}

/* ── Retained messages ── */

TEST(retained_store_and_retrieve) {
    branch_config_t cfg = {.port = 19996};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);
    branch_publish(t, "sensor/temp", "25.5", 4, 0, 1);
    int ri = branch__find_retained(t, "sensor/temp");
    assert(ri >= 0);
    assert(t->retained[ri].used == 1);
    assert(t->retained[ri].payload_len == 4);
    branch_destroy(t);
}

TEST(retained_delete_zero_len) {
    branch_config_t cfg = {.port = 19995};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);
    branch_publish(t, "x/y", "data", 4, 0, 1);
    int ri = branch__find_retained(t, "x/y");
    assert(ri >= 0);
    branch_publish(t, "x/y", "", 0, 0, 1);
    ri = branch__find_retained(t, "x/y");
    assert(ri < 0);
    branch_destroy(t);
}

TEST(retained_overwrite) {
    branch_config_t cfg = {.port = 19994};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);
    branch_publish(t, "status", "online", 6, 0, 1);
    branch_publish(t, "status", "offline", 7, 0, 1);
    int ri = branch__find_retained(t, "status");
    assert(ri >= 0);
    assert(t->retained[ri].payload_len == 7);
    assert(memcmp(t->retained[ri].payload, "offline", 7) == 0);
    branch_destroy(t);
}

/* ── CONNECT dispatch ── */

static int g_event_connect = 0;

static void capture_event(const char *msg, void *ud) {
    (void)ud;
    if (strstr(msg, "connected")) g_event_connect++;
}

TEST(dispatch_connect_valid) {
    branch_config_t cfg = {.port = 1880, .on_event = capture_event};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);

    branch_client_t *c = &t->clients[0];
    c->fd = 999;
    c->state = CS_CONNACK_SENT;

    uint8_t conn[] = {
        0x10, 0x0E,
        0x00, 0x04, 'M','Q','T','T',
        0x04, 0x02,
        0x00, 0x3C,
        0x00, 0x02, 'u','t'
    };
    memcpy(c->recv_buf, conn, sizeof(conn));
    c->recv_len = sizeof(conn);

    g_event_connect = 0;
    int rc = branch__dispatch(t, 0, (int)sizeof(conn));
    assert(rc == 0);
    assert(g_event_connect == 1);
    assert(c->send_len > 0); /* CONNACK queued */

    branch_destroy(t);
}

TEST(dispatch_connect_with_auth) {
    branch_config_t cfg = {.port = 1880, .on_event = capture_event};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);

    branch_client_t *c = &t->clients[0];
    c->fd = 998;
    c->state = CS_CONNACK_SENT;

    uint8_t conn[] = {
        0x10, 0x1C,
        0x00, 0x04, 'M','Q','T','T',
        0x04, 0xC2,
        0x00, 0x1E,
        0x00, 0x03, 'u','t','1',
        0x00, 0x05, 'a','d','m','i','n',
        0x00, 0x02, 'p','w'
    };
    memcpy(c->recv_buf, conn, sizeof(conn));
    c->recv_len = sizeof(conn);

    g_event_connect = 0;
    int rc = branch__dispatch(t, 0, (int)sizeof(conn));
    assert(rc == 0);
    assert(g_event_connect == 1);
    assert(strcmp(c->username, "admin") == 0);

    branch_destroy(t);
}

/* ── PUBLISH dispatch + forwarding ── */

TEST(dispatch_publish_qos0) {
    branch_config_t cfg = {.port = 1880, .on_event = capture_event};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);

    /* Set up subscriber on client 1 */
    t->clients[1].fd = 997;
    t->clients[1].state = CS_READY;
    strcpy(t->clients[1].client_id, "sub1");
    t->subs[0].used = 1;
    strcpy(t->subs[0].topic_filter, "test/#");
    t->subs[0].client_idx = 1;
    t->subs[0].qos = 0;
    t->clients[1].sub_indices[0] = 0;
    t->clients[1].sub_count = 1;

    /* Publisher on client 0 */
    t->clients[0].fd = 996;
    t->clients[0].state = CS_READY;
    strcpy(t->clients[0].client_id, "pub1");

    uint8_t pub[] = {
        0x30, 0x0D,
        0x00, 0x0A,
        't','e','s','t','/','h','e','l','l','o',
        'h','i'
    };
    memcpy(t->clients[0].recv_buf, pub, sizeof(pub));
    t->clients[0].recv_len = sizeof(pub);
    t->clients[1].send_len = 0;
    t->clients[1].send_off = 0;

    int rc = branch__dispatch(t, 0, (int)sizeof(pub));
    assert(rc == 0);
    assert(t->clients[1].send_len > 0);
    assert((t->clients[1].send_buf[0] >> 4) == 3); /* PUBLISH type */

    branch_destroy(t);
}

/* ── SUBSCRIBE dispatch ── */

TEST(dispatch_subscribe) {
    branch_config_t cfg = {.port = 1880, .on_event = capture_event};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);

    branch_client_t *c = &t->clients[0];
    c->fd = 995;
    c->state = CS_READY;
    strcpy(c->client_id, "sub1");

    uint8_t sub[] = {
        0x82, 0x0E,
        0x00, 0x01,
        0x00, 0x08,
        's','e','n','s','o','r','/','+',
        0x01
    };
    memcpy(c->recv_buf, sub, sizeof(sub));
    c->recv_len = sizeof(sub);

    int rc = branch__dispatch(t, 0, (int)sizeof(sub));
    assert(rc == 0);

    int found = 0;
    for (int i = 0; i < BRANCH_MAX_SUBS; i++) {
        if (t->subs[i].used && strcmp(t->subs[i].topic_filter, "sensor/+") == 0) {
            assert(t->subs[i].qos == 1);
            found = 1;
            break;
        }
    }
    assert(found == 1);
    assert(c->send_len > 0); /* SUBACK queued */

    branch_destroy(t);
}

/* ── Will message ── */

TEST(will_on_connect) {
    branch_config_t cfg = {.port = 1880, .on_event = capture_event};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);

    branch_client_t *c = &t->clients[0];
    c->fd = 994;
    c->state = CS_CONNACK_SENT;

    uint8_t conn[] = {
        0x10, 0x2A,
        0x00, 0x04, 'M','Q','T','T',
        0x04, 0x06,
        0x00, 0x3C,
        0x00, 0x03, 'u','t','2',
        0x00, 0x0D, 'c','l','i','e','n','t','/','s','t','a','t','u','s',
        0x00, 0x07, 'o','f','f','l','i','n','e'
    };
    memcpy(c->recv_buf, conn, sizeof(conn));
    c->recv_len = sizeof(conn);

    g_event_connect = 0;
    int rc = branch__dispatch(t, 0, (int)sizeof(conn));
    assert(rc == 0);
    assert(g_event_connect == 1);
    assert(c->has_will == 1);
    assert(strcmp(c->will_topic, "client/status") == 0);
    assert(c->will_payload_len == 7);

    branch_destroy(t);
}

/* ── PINGREQ dispatch ── */

TEST(dispatch_pingreq) {
    branch_config_t cfg = {.port = 1880, .on_event = capture_event};
    branch_t *t = branch_create(&cfg);
    assert(t != NULL);

    branch_client_t *c = &t->clients[0];
    c->fd = 993;
    c->state = CS_READY;

    uint8_t ping[] = {0xC0, 0x00};
    memcpy(c->recv_buf, ping, 2);
    c->recv_len = 2;
    c->send_len = 0;

    int rc = branch__dispatch(t, 0, 2);
    assert(rc == 0);
    assert(c->send_len >= 2);
    assert((c->send_buf[0] >> 4) == 13); /* PINGRESP */

    branch_destroy(t);
}

int main(void) {
    printf("\n=== tree.h unit tests ===\n\n");

    printf("Topic matching:\n");
    RUN(match_exact);
    RUN(match_single_wildcard);
    RUN(match_multi_wildcard);
    RUN(match_combined);
    RUN(match_edge_cases);

    printf("Broker lifecycle:\n");
    RUN(broker_create_destroy);
    RUN(broker_publish_no_clients);

    printf("Retained messages:\n");
    RUN(retained_store_and_retrieve);
    RUN(retained_delete_zero_len);
    RUN(retained_overwrite);

    printf("CONNECT dispatch:\n");
    RUN(dispatch_connect_valid);
    RUN(dispatch_connect_with_auth);

    printf("PUBLISH dispatch + forwarding:\n");
    RUN(dispatch_publish_qos0);

    printf("SUBSCRIBE dispatch:\n");
    RUN(dispatch_subscribe);

    printf("Will message:\n");
    RUN(will_on_connect);

    printf("PINGREQ dispatch:\n");
    RUN(dispatch_pingreq);

    printf("\n=== %d passed, %d failed ===\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
