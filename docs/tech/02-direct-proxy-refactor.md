# 02 Direct Proxy Refactor

## 背景

Stage 1 把共享协议层抽出来以后，`client.rs` 其实还没有真正使用这层能力。

原来的 DirectProxy 路径仍然在自己做三件事：

- 自己解析目标字符串
- 自己发送 fake header
- 自己拼接 `target\n` 或 `UDP\n`

与此同时，`server.rs` 也还在自己逐字节读 fake header 和目标地址。

这会产生一个问题：

- 共享层虽然存在
- 但真实流量并没有经过共享层
- 一旦后续继续改 `client_tun.rs`，就会出现“两套协议逻辑并存”的老毛病

所以 Stage 2 的目标很明确：

- 让 DirectProxy 真正切到共享握手层
- 让 server 真正切到共享请求解析

## 这一阶段改了什么

### `client.rs`

做了两件核心事：

1. 把 SOCKS5 目标地址解析收口成 `read_socks_target()`
2. 拿到 `TargetAddr` 后，不再手写握手，而是直接构造：

- `RelayRequest::Tcp { target }`
- `RelayRequest::Udp { target: None }`

然后统一走：

- `open_remote_session(&mut ctrl, &request)`

这样 DirectProxy 终于变成了“业务表达意图，共享层负责协议字节”的架构。

### `server.rs`

也同步切换到了共享读取接口：

- 不再手写 fake header 校验
- 不再手写逐字节读取目标地址
- 直接使用 `read_relay_request()`

解析结果变成：

- `RelayRequest::Tcp { target }`
- `RelayRequest::Udp { .. }`

然后再进入原来的 TCP/UDP 业务分支。

## 为什么必须 client/server 一起改

这一步很容易被低估。

如果只改 `client.rs`，不改 `server.rs`：

- client 发的是共享协议
- server 读的还是旧逻辑

表面上都还是文本协议，但只要一边先演进，另一边就会开始漂移。

所以正确做法不是“先改客户端，服务端以后再说”，而是：

- 共享握手一旦落地
- client 和 server 同时接入

这样协议边界只有一处定义，后面才敢继续重构。

## 一个关键细节：为什么要先修 `read_relay_request()`

在开始 Stage 2 之前，我先修了 `read_relay_request()` 的读取方式。

原因是第一版实现用了 `BufReader::read_line()`。

这个写法在“只测握手、不接真实 payload”时看不出问题，但一旦真实流量在握手后立刻跟上，`BufReader` 可能会提前多读，把后续 payload 吞进自己的内部缓冲。

这对多路复用流是危险的，因为握手后面马上就是业务数据。

所以现在改成了：

- 先读 fixed-size fake header
- 再逐字节读取直到 `\n`

这样可以保证：

- 握手只消费握手本身
- 后续 payload 仍然留在流里，等业务层继续读

这也是为什么我在 Stage 2 一开始没有直接改 `client.rs`，而是先补了共享协议层的一个边界修复。

## 数据流对比

### 改造前

```text
client.rs
-> parse target string
-> open yamux stream
-> write fake header
-> write "host:port\n" or "UDP\n"

server.rs
-> read fake header manually
-> read target manually
-> branch by raw string
```

### 改造后

```text
client.rs
-> read_socks_target()
-> TargetAddr
-> RelayRequest
-> open_remote_session()

server.rs
-> read_relay_request()
-> RelayRequest
-> branch by typed request
```

## 这一步的工程价值

DirectProxy 改造完成后，系统开始真正具备“共享协议层”的价值：

- TUN 模式后续可以直接复用同一套握手
- server 不再关心请求来自 DirectProxy 还是 TunGateway
- 目标地址不再是到处乱飞的裸字符串
- 后续换成二进制协议时，只需要改共享层

## 当前阶段有意没做什么

这一阶段没有去动：

- `client_tun.rs` 的主循环结构
- ListenerPool
- `SocketCtx`
- `SocketState`

原因很简单：

- 先把 DirectProxy 这一条最短路径切到共享层
- 再去改 TUN 路径，回归成本最低

## 下一步

下一阶段应该开始改 `client_tun.rs` 的骨架，而不是继续往 `client.rs` 里堆逻辑。

重点方向会是：

- 从单个 `socket_handle` 过渡到可扩展结构
- 抽出 `SocketCtx`
- 抽出公共查房逻辑
- 让 TUN 路径开始真正复用 `RelayRequest`
