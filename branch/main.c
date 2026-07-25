/*
 * branch_broker — Standalone MQTT 3.1.1 broker using tree.h
 *
 * Build:  cc -std=c99 -Os -o branch_broker main.c
 * Usage:  ./branch_broker [port]
 */
#define BRANCH_IMPLEMENTATION
#include "branch.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

static volatile int g_running = 1;

static void on_signal(int s) { (void)s; g_running = 0; }

static void on_event(const char *msg, void *ud) {
    (void)ud;
    fprintf(stderr, "[branch] %s\n", msg);
    fflush(stderr);
}

int main(int argc, char **argv) {
    uint16_t port = argc > 1 ? (uint16_t)atoi(argv[1]) : 1883;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    branch_config_t cfg = {0};
    cfg.port   = port;
    cfg.on_event = on_event;

    branch_t *t = branch_create(&cfg);
    if (!t) { fprintf(stderr, "branch_create failed\n"); return 1; }

    printf("[branch] MQTT broker started on port %d\n", port);
    printf("[branch] max %d clients, %d subs, %d retained\n",
           BRANCH_MAX_CLIENTS, BRANCH_MAX_SUBS, BRANCH_MAX_RETAINED);

    while (g_running) {
        branch_poll(t, 1000);
    }

    printf("[branch] done. client count at exit: %d\n", branch_client_count(t));
    branch_destroy(t);
    return 0;
}
