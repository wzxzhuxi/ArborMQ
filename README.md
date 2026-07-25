[English](README_EN.md)

# ArborMQ — KB 级 MQTT 3.1.1 in C

零依赖、生产级 MQTT 3.1.1 实现。完整协议支持，不到 20KB 编译体积。

## 项目结构

```
ArborMQ/
├── leaf/               MQTT 客户端（leaf — 轻如叶片）
│   ├── leaf.h          公共入口
│   ├── leaf/           内部模块
│   │   ├── leaf_types.h    类型定义 + 平台抽象
│   │   ├── leaf_codec.h    MQTT 协议编解码
│   │   └── leaf_client.h   状态机 + 网络 I/O
│   ├── test_leaf.c     20 项单元测试
│   └── main.c          CLI 交互式 demo
│
└── branch/               MQTT Broker（branch — 枝状路由）
    ├── tree.h          公共入口
    ├── branch/           内部模块
    │   ├── tree_types.h    类型定义 + 平台抽象
    │   ├── tree_codec.h    MQTT 协议编解码
    │   ├── tree_match.h    通配符匹配 (+ / #)
    │   └── branch_broker.h   订阅路由 + 消息转发
    ├── test_tree.c     16 项单元测试
    └── main.c          独立 broker 入口
```

## 快速开始

### Branch

```bash
cd tree && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel && make
./branch_broker 1883
```

### Client

```bash
cd leaf && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel && make
./leaf_cli 127.0.0.1 1883 mydevice
```

### 嵌入式部署

```c
#define LEAF_IMPLEMENTATION
#include "leaf.h"

void on_msg(const leaf_msg_t *m, void *ud) {
    printf("[%s] %.*s\n", m->topic, m->payload_len, m->payload);
}

int main() {
    leaf_config_t cfg = { .broker_ip = "127.0.0.1", .port = 1883, .client_id = "dev1" };
    leaf_t *c = leaf_create(&cfg);
    leaf_set_on_message(c, on_msg, NULL);
    leaf_connect(c);
    while (!leaf_is_ready(c)) leaf_poll(c, 100);
    leaf_subscribe(c, "test/#", 0);
    leaf_publish(c, "test/hello", "world", 5, 0, 0);
    while (1) leaf_poll(c, 1000);
}
```

## 功能

| | leaf | tree |
|---|---|---|
| QOS 0 / 1 / 2 | [+] | [+] |
| Retain 保留消息 | [+] | [+] |
| Will 遗嘱消息 | [+] | [+] |
| Auth 认证 | [+] | [+] |
| Keepalive 心跳 | [+] | [+] |
| 通配符 + / # | — | [+] |
| Session 持久化 | [+] | [+] |
| 零 malloc | [+] | [+] |
| 编译体积 (x86_64 -Os) | ~19KB | ~19KB |

## 协议覆盖

MQTT 3.1.1 全部 14 种报文类型：

| 报文 | leaf | tree |
|------|------|------|
| CONNECT | 编码 | 解码 + 验证 |
| CONNACK | 解码 | 编码 |
| PUBLISH QOS 0/1/2 | 编解码 | 编解码 + 转发 |
| PUBACK / PUBREC / PUBREL / PUBCOMP | 全流程 | 全流程双向 |
| SUBSCRIBE / SUBACK | 编码 / 解码 | 解码 + 路由 + 编码 |
| UNSUBSCRIBE / UNSUBACK | 编码 / 解码 | 解码 + 删除路由 |
| PINGREQ / PINGRESP | 编码 / 解码 | 解码 / 编码 |
| DISCONNECT | 编码 | 解码 + 清理会话 |

## 设计原则

- **零 malloc** — 所有 buffer 编译期静态定长，适合长期运行的嵌入式设备
- **零拷贝** — 收到消息的 topic/payload 直接指向 recv buffer 内部
- **非阻塞** — 事件驱动轮询，单线程可管理多个连接
- **纯 C99** — 仅依赖 POSIX socket + select，无需任何第三方库

## 调参

通过 `#define` 在 include 前重写默认值。详见各子目录的 README。

## 运行测试

```bash
cd leaf/build && cmake .. && make && ./test_leaf   # 20 tests
cd branch/build && cmake .. && make && ./test_tree   # 16 tests
```

## 许可证

MIT / 0BSD
