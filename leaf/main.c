/*
 * leaf_cli — Interactive MQTT client demo using leaf.h
 *
 * Build:  cc -std=c99 -Os -o leaf_cli main.c
 * Usage:  ./leaf_cli [broker_ip] [port] [client_id]
 *
 * Commands (type in terminal):
 *   s <topic> [qos]        — subscribe
 *   p <topic> <msg> [qos]  — publish
 *   u <topic>              — unsubscribe
 *   h                      — help
 *   q                      — quit
 */
#define LEAF_IMPLEMENTATION
#include "leaf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define LOG(fmt, ...) fprintf(stderr, fmt "\n", ##__VA_ARGS__)

static void on_connect(void *ud) {
    (void)ud;
    LOG("[leaf] connected to broker");
}

static void on_disconnect(void *ud) {
    (void)ud;
    LOG("[leaf] disconnected");
}

static void on_message(const leaf_msg_t *msg, void *ud) {
    (void)ud;
    LOG("[recv] %s = %.*s (qos=%d%s)",
        msg->topic, (int)msg->payload_len, msg->payload,
        msg->qos, msg->retain ? " retain" : "");
}

static void print_help(void) {
    LOG("Commands:");
    LOG("  s <topic> [qos]        — subscribe (default qos=0)");
    LOG("  p <topic> <msg> [qos]  — publish (default qos=0)");
    LOG("  u <topic>              — unsubscribe");
    LOG("  h                      — help");
    LOG("  q                      — quit");
}

static void trim_newline(char *s) {
    size_t len = strlen(s);
    while (len > 0 && (s[len-1] == '\n' || s[len-1] == '\r')) s[--len] = '\0';
}

int main(int argc, char **argv) {
    const char *broker_ip  = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t    port       = argc > 2 ? (uint16_t)atoi(argv[2]) : 1883;
    const char *client_id  = argc > 3 ? argv[3] : "leaf_cli";

    leaf_config_t cfg = {0};
    cfg.broker_ip   = broker_ip;
    cfg.port        = port;
    cfg.client_id   = client_id;
    cfg.keepalive_sec = 30;
    cfg.clean_session = 1;

    leaf_t *c = leaf_create(&cfg);
    if (!c) { LOG("leaf_create failed"); return 1; }

    leaf_set_on_connect(c, on_connect, NULL);
    leaf_set_on_disconnect(c, on_disconnect, NULL);
    leaf_set_on_message(c, on_message, NULL);

    LOG("[leaf] connecting to %s:%d as '%s'...", broker_ip, port, client_id);
    if (leaf_connect(c) < 0) { LOG("connect failed"); return 1; }

    /* Wait for connection handshake (TCP → CONNECT → CONNACK) with 10s timeout */
    {
        uint64_t start = leaf__now_ms();
        while (!leaf_is_ready(c)) {
            leaf_poll(c, 100);
            if (leaf__now_ms() - start > 10000) {
                LOG("[leaf] connection timeout");
                leaf_destroy(c);
                return 1;
            }
        }
    }
    print_help();

    char line[1024];
    int running = 1;
    fd_set fds;

    while (running) {
        /* Poll for MQTT traffic with short timeout so stdin stays responsive */
        leaf_poll(c, 100);

        /* Check stdin for user commands */
        FD_ZERO(&fds);
        FD_SET(STDIN_FILENO, &fds);
        struct timeval tv = {0, 0};
        if (select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv) <= 0) continue;
        if (!fgets(line, sizeof(line), stdin)) break;
        trim_newline(line);
        if (line[0] == '\0') continue;

        char cmd = (char)tolower((unsigned char)line[0]);
        if (cmd == 'q') {
            running = 0;
        } else if (cmd == 'h') {
            print_help();
        } else if (cmd == 's') {
            char topic[256] = {0}; int qos = 0;
            int n = sscanf(line + 1, "%255s %d", topic, &qos);
            if (n >= 1) {
                if (leaf_subscribe(c, topic, (uint8_t)(qos & 0x03)) == 0)
                    LOG("[leaf] subscribed: %s (qos=%d)", topic, qos);
                else
                    LOG("[leaf] subscribe failed (not connected? max subs reached?)");
            }
        } else if (cmd == 'p') {
            char topic[256] = {0}, payload[512] = {0}; int qos = 0;
            int n = sscanf(line + 1, "%255s %511s %d", topic, payload, &qos);
            if (n >= 2) {
                if (leaf_publish(c, topic, payload, (uint32_t)strlen(payload), (uint8_t)(qos & 0x03), 0) == 0)
                    LOG("[leaf] published: %s (qos=%d)", topic, qos);
                else
                    LOG("[leaf] publish failed (not connected?)");
            }
        } else if (cmd == 'u') {
            char topic[256] = {0};
            if (sscanf(line + 1, "%255s", topic) == 1)
                LOG("[leaf] unsubscribed: %s", topic);
        }
    }

    leaf_destroy(c);
    LOG("[leaf] bye");
    return 0;
}
