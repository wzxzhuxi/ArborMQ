[English](README_EN.md)

# branch — MQTT 3.1.1 Broker（KB 级）

零动态分配、单入口 MQTT 3.1.1 Broker。完整协议支持，~19KB 编译体积。

## 文件结构

```
branch/
├── branch.h              公共入口（用户只 include 这个）
├── branch/
│   ├── branch_types.h    内部类型 + 平台抽象
│   ├── branch_codec.h    MQTT 协议编解码
│   ├── branch_match.h    通配符匹配 (+ / #)
│   └── branch_broker.h   订阅路由 + 消息转发
├── test_branch.c         16 项单元测试
├── main.c              独立 broker 入口
└── CMakeLists.txt
```

## 快速开始

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

| 函数 | 说明 |
|------|------|
| `branch_create(&cfg)` | 创建 broker 实例（单例） |
| `branch_poll(t, ms)` | 事件循环（-1=阻塞，0=非阻塞，>0=超时毫秒） |
| `branch_publish(t, topic, data, len, qos, retain)` | Broker 端发布消息 |
| `branch_client_count(t)` | 当前连接数 |
| `branch_get_clients(t, dst, max)` | 获取客户端列表 |
| `branch_destroy(t)` | 关闭所有连接，释放资源 |

## 功能

- 完整 MQTT 3.1.1：QOS 0/1/2、Retain、Will、Session、Auth
- 通配符主题匹配（`+` 单层 / `#` 多层）
- Keepalive 超时检测（1.5x 心跳间隔）
- 零 malloc — 所有 buffer 编译期静态定长
- 单线程 select() 事件循环
- ~19KB ELF，~11KB text（x86_64 -Os）

## 架构

```
branch_t (静态分配)
├── listen_fd
├── clients[BRANCH_MAX_CLIENTS]     # 每客户端独立状态机 + 收发缓冲
├── subs[BRANCH_MAX_SUBS]           # 全局订阅表（线性扫描 O(n)）
├── retained[BRANCH_MAX_RETAINED]   # 保留消息存储
└── packet_id_counter             # 16-bit 循环 ID
```

订阅路由使用线性扫描 + 通配符匹配，不建 hash table（省代码体积）。

## 调参

在 `#include "branch.h"` 之前 `#define` 以下宏：

| 宏 | 默认值 | 说明 |
|----|--------|------|
| BRANCH_MAX_CLIENTS | 32 | 最大并发连接数 |
| BRANCH_MAX_SUBS | 128 | 全局订阅槽位数 |
| BRANCH_MAX_RETAINED | 64 | 保留消息上限 |
| BRANCH_MAX_PENDING | 8 | 每客户端 QOS 1/2 待确认数 |
| BRANCH_BUF_SIZE | 4096 | 每客户端收发缓冲区 |
| BRANCH_MAX_TOPIC | 256 | Topic 最大长度 |
| BRANCH_MAX_CLIENT_ID | 128 | Client ID 最大长度 |
| BRANCH_MAX_WILL_PAYLOAD | 512 | 遗嘱消息最大长度 |

## 运行测试

```bash
cd branch/build && cmake .. && make && ./test_branch
```

## 许可证

MIT / 0BSD
