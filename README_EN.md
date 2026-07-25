[中文](README.md)

# ArborMQ — KB-size MQTT 3.1.1 in C

Zero-dependency, production-grade MQTT 3.1.1 implementation. Full protocol support under 20KB compiled.

## Structure

```
ArborMQ/
├── leaf/               MQTT Client
│   ├── leaf.h          Public entry point
│   ├── leaf/           Internal modules
│   │   ├── leaf_types.h    Types + platform helpers
│   │   ├── leaf_codec.h    MQTT protocol codec
│   │   └── leaf_client.h   State machine + network I/O
│   ├── test_leaf.c     20 unit tests
│   └── main.c          Interactive CLI demo
│
└── branch/               MQTT Broker
    ├── branch.h          Public entry point
    ├── branch/           Internal modules
    │   ├── branch_types.h    Types + platform helpers
    │   ├── branch_codec.h    MQTT protocol codec
    │   ├── branch_match.h    Topic wildcard matching
    │   └── branch_broker.h   Subscription routing + forwarding
    ├── test_branch.c     16 unit tests
    └── main.c          Standalone broker binary
```

## Quick Start

### Branch

```bash
cd branch && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel && make
./branch_broker 1883
```

### Client

```bash
cd leaf && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel && make
./leaf_cli 127.0.0.1 1883 mydevice
```

### Embedding

```c
#define LEAF_IMPLEMENTATION
#include "leaf.h"

int main() {
    leaf_config_t cfg = { .broker_ip = "127.0.0.1", .port = 1883, .client_id = "dev1" };
    leaf_t *c = leaf_create(&cfg);
    leaf_connect(c);
    while (!leaf_is_ready(c)) leaf_poll(c, 100);
    leaf_subscribe(c, "test/#", 0);
    leaf_publish(c, "test/hello", "world", 5, 0, 0);
    while (1) leaf_poll(c, 1000);
}
```

## Features

| | leaf | branch |
|---|---|---|
| QOS 0 / 1 / 2 | [+] | [+] |
| Retained messages | [+] | [+] |
| Will messages | [+] | [+] |
| Authentication | [+] | [+] |
| Keepalive | [+] | [+] |
| Wildcards + / # | — | [+] |
| Session persistence | [+] | [+] |
| Zero malloc | [+] | [+] |
| Binary size (x86_64 -Os) | ~19KB | ~19KB |

## Protocol Coverage

All 14 MQTT 3.1.1 packet types:

| Packet | leaf | branch |
|--------|------|--------|
| CONNECT | Encode | Decode + validate |
| CONNACK | Decode | Encode |
| PUBLISH QOS 0/1/2 | Encode/decode | Encode/decode + forward |
| PUBACK/PUBREC/PUBREL/PUBCOMP | Full flow | Full flow (bidirectional) |
| SUBSCRIBE/SUBACK | Encode/decode | Decode + route + encode |
| UNSUBSCRIBE/UNSUBACK | Encode/decode | Decode + remove route |
| PINGREQ/PINGRESP | Encode/decode | Decode/encode |
| DISCONNECT | Encode | Decode + cleanup |

## Design

- **Zero malloc** — all buffers statically sized at compile time
- **Zero copy** — received topic/payload point into recv buffer
- **Non-blocking** — event-driven polling, single thread
- **Pure C99** — only POSIX socket + select, no third-party dependencies

## Tuning

Override defaults with `#define` before include. See subdirectory READMEs for details.

## Running Tests

```bash
cd leaf/build && cmake .. && make && ./test_leaf   # 20 tests
cd branch/build && cmake .. && make && ./test_branch   # 16 tests
```

## License

MIT / 0BSD
