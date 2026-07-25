#pragma once

/* ═══════════════════════════════════════════════════════════════════════════
 * Topic filter matching (supports + and # wildcards)
 * ═══════════════════════════════════════════════════════════════════════════ */

static int branch__match_topic(const char *filter, const char *topic) {
    while (*filter && *topic) {
        if (*filter == '+') {
            /* Match single level: skip to next '/' or end */
            filter++;
            while (*topic && *topic != '/') topic++;
            if (*topic == '/' && *filter == '\0') return 0; /* + can't match empty trailing level */
            continue;
        }
        if (*filter == '#') {
            /* Multi-level wildcard — matches everything remaining */
            filter++;
            if (*filter == '\0') return 1; /* # at end matches all */
            /* # must be last char per spec */
            return 0;
        }
        if (*filter != *topic) return 0;
        filter++; topic++;
    }
    /* Both must end together, or filter ends with # (handled above) */
    if (*filter == '#' && *(filter + 1) == '\0') return 1;
    /* filter = "/#" + topic exhausted → # matches zero remaining levels */
    if (*topic == '\0' && *filter == '/' && *(filter + 1) == '#' && *(filter + 2) == '\0') return 1;
    return (*filter == '\0' && *topic == '\0') ? 1 : 0;
}
