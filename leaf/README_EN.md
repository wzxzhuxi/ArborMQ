[中文](README.md)

# leaf — MQTT 3.1.1 Client (KB-size)

Zero-allocation, non-blocking, single-entry MQTT 3.1.1 client. Full protocol support under 19KB.

## File Layout

```
leaf/
├── leaf.h              Public entry point (users only include this)
├── leaf/
│   ├── leaf_types.h    Internal types + platform helpers
│   ├── leaf_codec.h    MQTT protocol encode/decode
│   └── leaf_client.h   State machine + network I/O
├── test_leaf.c         20 unit tests
├── main.c              Interactive CLI demo
└── CMakeLists.txt
```

## Quick Start

```c
#define LEAF_IMPLEMENTATION
#include "leaf.h"

void on_msg(const leaf_msg_t *m, void *ud) {
    printf("[%s] %.*s\n", m->topic, m->payload_len, m->payload);
}

int main() {
    leaf_config_t cfg = { .broker_ip = "127.0.0.1", .port = 1883, .client_id = "demo" };
    leaf_t *c = leaf_create(&cfg);
    leaf_set_on_message(c, on_msg, NULL);
    leaf_connect(c);
    while (!leaf_is_ready(c)) leaf_poll(c, 100);
    leaf_subscribe(c, "test/#", 0);
    leaf_publish(c, "test/hello", "world", 5, 0, 0);
    while (1) leaf_poll(c, 1000);
}
```

```bash
cc -std=c99 -Os -s -o client main.c
./client
```

## API

| Function | Description |
|----------|-------------|
| `leaf_create(&cfg)` | Create client (singleton) |
| `leaf_connect(c)` | Start non-blocking TCP connect |
| `leaf_poll(c, ms)` | Event loop (-1=forever, 0=non-block, >0=ms timeout) |
| `leaf_subscribe(c, topic, qos)` | Subscribe to topic filter |
| `leaf_unsubscribe(c, topic)` | Unsubscribe |
| `leaf_publish(c, topic, data, len, qos, retain)` | Publish message |
| `leaf_is_ready(c)` | CONNACK handshake complete |
| `leaf_is_connected(c)` | TCP connection alive |
| `leaf_disconnect(c)` | Send DISCONNECT and close |
| `leaf_set_on_connect(c, cb, ud)` | Set connect callback |
| `leaf_set_on_disconnect(c, cb, ud)` | Set disconnect callback |
| `leaf_set_on_message(c, cb, ud)` | Set message callback |
| `leaf_destroy(c)` | Destroy instance, release resources |

## Features

- Full MQTT 3.1.1: QOS 0/1/2, Will, Auth, Retain
- Non-blocking state machine with auto-reconnect
- Zero-copy message dispatch
- Zero malloc — single static instance, all buffers embedded
- ~19KB ELF, ~10KB text (x86_64 -Os)

## Tuning

`#define` before `#include "leaf.h"`:

| Macro | Default | Description |
|-------|---------|-------------|
| LEAF_BUF_SIZE | 4096 | Recv/send buffer size |
| LEAF_MAX_PENDING | 8 | In-flight QOS 1/2 messages |
| LEAF_MAX_SUBS | 8 | Max subscriptions |
| LEAF_MAX_CLIENT_ID | 64 | Max client ID length |
| LEAF_MAX_TOPIC | 256 | Max topic length |
| LEAF_KEEPALIVE_SEC | 60 | Keepalive interval (seconds) |
| LEAF_RECONNECT_MS | 5000 | Reconnect backoff start (ms) |

## Running Tests

```bash
cd leaf/build && cmake .. && make && ./test_leaf
```

## License

MIT / 0BSD
