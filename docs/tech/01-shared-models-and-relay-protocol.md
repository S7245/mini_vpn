# 01 Shared Models And Relay Protocol

## 背景

在这一阶段之前，`client.rs` 与 `client_tun.rs` 都各自维护了一份“远端会话开场白”：

- 自己拼目标地址字符串
- 自己发送 fake header
- 自己决定 TCP/UDP 的请求格式

这种写法在 demo 阶段能跑，但一旦要支持：

- 任意 `host:port`
- `TCP/UDP` 共存
- `TUN` 可选
- 后续连接池

问题就会同时暴露出来：

- 协议逻辑重复RelayRequest
- 分支容易漂移
- 错误处理分散
- 后续很难统一演进

## 本阶段做了什么

这一阶段先不碰大规模 runtime 重构，只把“共享协议层”抽出来，落成一组稳定接口：

- `src/shared/errors.rs`
  - 新增 `ClientError`
- `src/shared/target.rs`
  - 新增 `TargetAddr`
- `src/shared/relay_protocol.rs`
  - 新增 `RelayRequest`
  - 新增 `FAKE_HTTP_HEADER`
  - 新增 `write_relay_request()`
  - 新增 `read_relay_request()`
- `src/shared/tunnel.rs`
  - 新增 `open_remote_session()`
- `tests/shared_relay_protocol.rs`
  - 新增共享协议测试

## 为什么先做这一层

先讲为什么，不先讲代码。

当前真正卡住后续架构演进的，不是 `smoltcp`，也不是 `yamux`，而是“协议入口没有收口”。

只要 fake header、目标地址、TCP/UDP 协商还散落在多个分支里，后面你无论做：

- DirectProxy
- TunGateway
- ListenerPool
- SocketCtx

都会重复搬运一遍协议逻辑，最后把问题复制得更大。

所以第一阶段的目标很克制：

- 不急着改大循环
- 不急着改连接池
- 先把共享协议层钉死

## 核心数据结构

### `TargetAddr`

它负责把目标地址从“随手拼接的字符串”升级成“结构化数据”。

支持两种目标：

- `IpPort(SocketAddr)`
- `DomainPort { host, port }`

它的价值有三层：

- 表达能力更强：既能表示 `127.0.0.1:7897`，也能表示 `www.figma.com:443`
- 后续更容易校验和日志记录
- 协议层不用再在多个地方做字符串拼接

### `RelayRequest`

它是共享隧道层的请求模型：

- `RelayRequest::Tcp { target }`
- `RelayRequest::Udp { target }`

这个抽象的好处是，外层调用者以后只需要表达：

- 我要开一个 TCP 远端会话
- 或者我要开一个 UDP 远端会话

而不再需要关心 fake header、文本格式、换行符这些细节。

### `ClientError`

它把共享层的失败统一收口：

- I/O 错误
- 目标地址错误
- 协议解析错误
- Yamux 开流错误

这个动作很基础，但很关键。
没有统一错误面，后面连接生命周期收口会非常痛苦。

## 数据流

这一阶段完成后，共享握手的数据流变成：

```text
caller
-> build TargetAddr
-> build RelayRequest
-> open_remote_session()
-> open Yamux substream
-> write fake header
-> write request line
-> server reads shared protocol
```

也就是说，外层业务代码只负责“描述意图”，共享层负责“把意图变成线上的字节协议”。

## 协议格式为什么暂时保留文本

当前实现没有一步到位切换到二进制帧，而是继续使用：

- fake header
- 一行文本请求

例如：

```text
TCP 34.107.238.235:443
UDP
UDP mtalk.google.com:5228
```

这样做不是因为文本协议最好，而是因为第一阶段的目标是：

- 先统一
- 再演进

如果在第一阶段同时改：

- 共享模型
- 共享错误
- 二进制 framing
- server 解析逻辑

那改动面会过大，回归成本也会明显上升。

所以这里采取的是更稳的工程策略：

- 先统一协议入口
- 再在后续阶段替换传输格式

## 关键测试

这一阶段新增了 6 个共享协议测试，覆盖：

- IPv4 目标解析
- 域名目标解析
- 缺失端口拒绝
- TCP 请求 round-trip
- UDP 请求 round-trip
- fake header 前缀校验

这些测试的意义不是“测得很多”，而是：

- 把共享层的最小契约钉住
- 让后续重构 `client.rs` 与 `client_tun.rs` 时有回归保护

## 当前阶段的边界

这一阶段有意不做下面这些事：

- 不重构 `client.rs` 主体
- 不重构 `client_tun.rs` 主体
- 不上线 ListenerPool
- 不改 server 解析格式
- 不处理 TUN 生命周期

因为这不是偷懒，而是控制改动半径。

## 你接下来会看到什么

下一阶段会从 `client.rs` 开始，把 direct proxy 路径切换到共享模型：

- 不再手写 fake header
- 不再手工拼 `target_addr\n`
- 不再让 TCP/UDP 的握手散落在业务分支里

等 direct proxy 这条线跑顺之后，再去改 `client_tun.rs`，风险最小。