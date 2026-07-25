[English](README_EN.md)

# leaf — MQTT 3.1.1 客户端（KB 级）

零动态分配、非阻塞、单入口 MQTT 3.1.1 客户端。完整协议支持，~19KB 编译体积。

## 文件结构

```
leaf/
├── leaf.h              公共入口（用户只 include 这个）
├── leaf/
│   ├── leaf_types.h    内部类型 + 平台抽象
│   ├── leaf_codec.h    MQTT 协议编解码
│   └── leaf_client.h   状态机 + 网络 I/O
├── test_leaf.c         20 项单元测试
├── main.c              CLI 交互式 demo
└── CMakeLists.txt
```

## 快速开始

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

| 函数 | 说明 |
|------|------|
| `leaf_create(&cfg)` | 创建客户端（单例） |
| `leaf_connect(c)` | 发起 TCP 连接（非阻塞） |
| `leaf_poll(c, ms)` | 事件循环（-1=阻塞，0=非阻塞，>0=超时毫秒） |
| `leaf_subscribe(c, topic, qos)` | 订阅主题过滤器 |
| `leaf_unsubscribe(c, topic)` | 取消订阅 |
| `leaf_publish(c, topic, data, len, qos, retain)` | 发布消息 |
| `leaf_is_ready(c)` | CONNACK 握手是否完成 |
| `leaf_is_connected(c)` | TCP 连接是否存活 |
| `leaf_disconnect(c)` | 发送 DISCONNECT 并关闭连接 |
| `leaf_set_on_connect(c, cb, ud)` | 注册连接成功回调 |
| `leaf_set_on_disconnect(c, cb, ud)` | 注册断连回调 |
| `leaf_set_on_message(c, cb, ud)` | 注册消息回调 |
| `leaf_destroy(c)` | 销毁实例，释放资源 |

## 功能

- 完整 MQTT 3.1.1：QOS 0/1/2、Will、Auth、Retain
- 非阻塞状态机，自动重连
- 零拷贝消息分发（topic 直接指向 recv buffer 内部）
- 零 malloc — 全局单例，所有 buffer 内嵌于 struct
- ~19KB ELF，~10KB text（x86_64 -Os）

## 调参

在 `#include "leaf.h"` 之前 `#define` 以下宏：

| 宏 | 默认值 | 说明 |
|----|--------|------|
| LEAF_BUF_SIZE | 4096 | 收发缓冲区大小 |
| LEAF_MAX_PENDING | 8 | QOS 1/2 待确认消息数 |
| LEAF_MAX_SUBS | 8 | 最大订阅数 |
| LEAF_MAX_CLIENT_ID | 64 | Client ID 最大长度 |
| LEAF_MAX_TOPIC | 256 | Topic 最大长度 |
| LEAF_KEEPALIVE_SEC | 60 | 心跳间隔（秒） |
| LEAF_RECONNECT_MS | 5000 | 重连退避起始（毫秒） |

## 运行测试

```bash
cd leaf/build && cmake .. && make && ./test_leaf
```

## 许可证

MIT / 0BSD
