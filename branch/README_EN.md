[中文](README.md)

# branch — MQTT 3.1.1 Broker (KB-size)

Zero-allocation, single-entry MQTT 3.1.1 broker. Full protocol support under 19KB.

## File Layout

```
branch/
├── branch.h              Public entry point (users only include this)
├── branch/
│   ├── branch_types.h    Internal types + platform helpers
│   ├── branch_codec.h    MQTT protocol encode/decode
│   ├── branch_match.h    Topic wildcard matching
│   └── branch_broker.h   Subscription routing + forwarding
├── test_branch.c         16 unit tests
├── main.c              Standalone broker binary
└── CMakeLists.txt
```

## Quick Start

```c
#define BRANCH_IMPLEMENTATION
#include "branch.h"

int main() {
    branch_config_t cfg = { .port = 1883 };
    branch_t *t = branch_create(&cfg);
    while (1) branch_poll(t, 1000);
    branch_destroy(t);
}
```

```bash
cc -std=c99 -Os -s -o broker main.c
./broker
```

## API

| Function | Description |
|----------|-------------|
| `branch_create(&cfg)` | Create broker (singleton) |
| `branch_poll(t, ms)` | Event loop (-1=forever, 0=non-block, >0=ms timeout) |
| `branch_publish(t, topic, data, len, qos, retain)` | Publish from broker side |
| `branch_client_count(t)` | Number of connected clients |
| `branch_get_clients(t, dst, max)` | List connected clients |
| `branch_destroy(t)` | Close all connections, release resources |

## Features

- Full MQTT 3.1.1: QOS 0/1/2, Retain, Will, Session, Auth
- Wildcard topic filters (`+` single-level, `#` multi-level)
- Keepalive timeout detection (1.5x keepalive interval)
- Zero malloc — all buffers statically sized
- Single-threaded select() event loop
- ~19KB ELF, ~11KB text (x86_64 -Os)

## Architecture

```
branch_t (statically allocated)
├── listen_fd
├── clients[BRANCH_MAX_CLIENTS]     # Per-client state machine + buffers
├── subs[BRANCH_MAX_SUBS]           # Global subscription table (linear scan)
├── retained[BRANCH_MAX_RETAINED]   # Retained message store
└── packet_id_counter             # 16-bit cyclic ID
```

Subscription routing uses linear scan with wildcard matching — no hash table (saves code size).

## Tuning

`#define` before `#include "branch.h"`:

| Macro | Default | Description |
|-------|---------|-------------|
| BRANCH_MAX_CLIENTS | 32 | Max concurrent connections |
| BRANCH_MAX_SUBS | 128 | Total subscription slots |
| BRANCH_MAX_RETAINED | 64 | Max retained messages |
| BRANCH_MAX_PENDING | 8 | In-flight QOS 1/2 per client |
| BRANCH_BUF_SIZE | 4096 | Per-client recv/send buffer |
| BRANCH_MAX_TOPIC | 256 | Max topic length |
| BRANCH_MAX_CLIENT_ID | 128 | Max client ID length |
| BRANCH_MAX_WILL_PAYLOAD | 512 | Max will payload length |

## Running Tests

```bash
cd branch/build && cmake .. && make && ./test_branch
```

## License

MIT / 0BSD
