# 03 TUN Runtime Skeleton

## 背景

在 Stage 2 之后，共享握手层已经稳定了，但 `client_tun.rs` 还有一个明显的结构问题：

- 主循环过大
- 两段“查房逻辑”几乎重复
- 整个文件围绕一个裸 `socket_handle` 运转
- TUN 路径虽然能工作，但完全不利于后续连接池扩展

如果这时候直接上 `pool_size > 1`，本质上只是在复制复杂度。

所以 Stage 3 的目标不是马上打开连接池，而是先把 runtime 骨架改造成“连接池友好”的形态。

## 这一阶段做了什么

### 1. 引入基础结构类型

在 `client_tun.rs` 里新增了几组基础类型：

- `ListenerSpec`
- `ListenerPool`
- `SocketState`
- `SocketCtx`

这些类型的作用不是“功能变多”，而是把原来散落在局部变量和注释里的运行时状态显式化。

现在单个 TUN 监听槽位已经能表达：

- 监听哪个本地端口
- 当前处于什么状态
- 对应的远端目标是谁
- 当前是否已经持有上行发送通道

### 2. 把主循环从“裸逻辑”改成“helper 驱动”

原来 `client_tun.rs` 里最难维护的部分，是网卡事件分支和定时器分支里各自复制了一整套：

- 从 `TcpSocket` 取 payload
- 判断是否已有通道
- 新建 Yamux 子流
- 启动后台 relay

Stage 3 把这部分拆成了几个 helper：

- `build_listener_socket()`
- `extract_socket_payload()`
- `rearm_socket()`
- `process_listener_activity()`
- `handle_local_payload()`
- `handle_remote_payload()`
- `spawn_remote_relay()`

这样主循环的心智模型就变成了：

```text
poll iface
-> flush TUN
-> iterate listener handles
-> process_listener_activity()
```

而不是在主循环里直接堆满细节。

### 3. TUN 路径开始复用共享握手层

这是 Stage 3 最重要的实际收益。

之前 TUN 路径仍然手写：

- fake header
- `httpbin.org:80\n`

现在新建远端会话时，已经改成：

- 构造 `RelayRequest::Tcp`
- 调用 `open_remote_session()`

也就是说，TUN 路径已经开始和 DirectProxy 共用同一套握手协议。

这一步非常关键，因为后面不管继续改：

- ListenerPool
- SocketCtx
- SocketState

都不需要再碰 fake header 和协议协商细节了。

## 为什么现在仍然是 `pool_size = 1`

这不是功能没做完，而是刻意分阶段。

当前已经有：

- `ListenerSpec`
- `ListenerPool`
- `handles: Vec<SocketHandle>`

即使现在只放了一个 handle，主循环的写法已经从“面向单句柄”变成“遍历槽位”。

这意味着下一阶段再把：

- `pool_size` 从 `1` 调到 `4` 或 `8`

时，改动会非常小，因为主循环已经不依赖单一 `socket_handle` 了。

这就是“先搭脚手架，再扩容量”的工程思路。

## 数据流变化

### Stage 2 之前的 TUN 逻辑

```text
TUN packet
-> poll
-> 在主循环里直接操作 socket_handle
-> 原地判断是否有活动连接
-> 原地开 Yamux
-> 原地 spawn relay
```

### Stage 3 之后的 TUN 逻辑

```text
TUN packet
-> poll
-> iterate listener_pool.handles
-> process_listener_activity()
-> handle_local_payload()
-> open_remote_session()
-> spawn_remote_relay()
```

这个变化的关键不在于少写了几行，而在于：

- 状态和职责开始分层
- 主循环开始退回到“调度者”角色

## 这一步解决了什么

Stage 3 解决的不是“并发能力”，而是“并发准备度”。

具体来说，它解决了 3 个长期问题：

### 1. 重复逻辑开始收口

原来两段几乎一样的“查房逻辑”很容易漂移。

现在这部分已经开始往公共 helper 汇聚，后续继续改时，不需要再在两个分支同时复制粘贴。

### 2. 状态开始显式化

以前 `active_connections` 只能表达“有没有 sender”，表达不了：

- 这个槽位是不是在监听
- 是不是正在开远端会话
- 是不是正在回收

现在 `SocketState` 已经把这些概念立起来了。

### 3. 协议层和运行时层终于接上了

这一步之后，TUN 路径不再是“另一套独立世界”，而是开始和共享协议层对齐。

这会直接降低后面继续做连接池和 UDP 对齐的成本。

## 当前阶段有意保留的限制

Stage 3 还没有做这些事：

- 还没有把 `pool_size` 真正扩到多槽位
- 还没有把默认目标从 `httpbin.org:80` 拔成配置项
- 还没有引入 `UdpCtx`
- 还没有彻底清理所有 TUN 热路径里的 `unwrap()`

这些都是后续阶段要继续推进的，但现在先把骨架立住更重要。

## 下一步

下一阶段应该做真正的 ListenerPool 激活：

- 允许 `pool_size > 1`
- 按槽位初始化多个监听 socket
- 逐槽位回收和重挂监听

那时我们就不是“逻辑上支持池化”，而是“运行时真正开始池化”了。
