# 04 Real Listener Pool Activation

## 背景

Stage 3 做的事情，本质上是把 `client_tun.rs` 从“大循环硬编码单个 handle”改造成“池化友好骨架”。

那一步很重要，但它还没有真正激活多个监听槽位。

换句话说，Stage 3 的重点是：

- 先把骨架立住
- 先把 helper 收口
- 先让 TUN 路径复用共享握手层

它还不是：

- 真正的 4 槽位监听池

Stage 4 才是把这个骨架真正点亮的一步。

## 这一步真正改变了什么

这次的核心变化，不是“把一个数字从 1 改到 4”这么简单。

真正变化的是：

- `ListenerSpec` 现在默认使用 `pool_size = 4`
- `build_listener_pool()` 会一次性创建 4 个独立的 `TcpSocket`
- 每个 `SocketHandle` 都会拥有自己的 `SocketCtx`
- 主循环遍历 `listener_pool.handles`，而不是盯着某一个裸 `socket_handle`
- 某个 handle 收到 EOF 后，只会回收并重新监听它自己，不会影响其他槽位

这就是从“单间酒店”真正升级到“4 间房的小型连锁酒店”。

## 为什么不能只把数量从 1 改到 4

如果只是简单把数量调大，但不去做这些配套动作：

- 每个 handle 一份上下文
- 每个 handle 独立 rearm
- 每个 handle 独立日志

最后得到的不是并发能力，而是把旧问题复制四份。

所以 Stage 4 的重点其实是两件事一起完成：

1. 真正创建 4 个监听槽位
2. 真正把生命周期管理做到“按 handle 隔离”

## 关键结构

### `build_listener_pool()`

这是 Stage 4 新增的关键入口。

它负责：

- 根据 `ListenerSpec` 创建多个监听 socket
- 把每个生成的 handle 放进 `ListenerPool`
- 为每个 handle 建立对应的 `SocketCtx`

这意味着后续无论做：

- 日志定位
- EOF 回收
- 重新 `listen(local_port)`

都可以围绕 handle 做，而不是围绕全局变量猜状态。

### `SocketCtx`

现在每个房间上下文都明确记录：

- `local_port`
- `state`
- `target`
- `uplink_tx`

它的价值不是字段变多，而是“连接生命周期终于有归属了”。

### `SocketState`

Stage 4 继续沿用并强化这几个状态：

- `Listening`
- `OpeningRemote`
- `Relaying`
- `Closing`
- `Rearming`

现在这些状态不仅存在，而且会和日志一起帮助你观察：

- 哪个 handle 正在开远端子流
- 哪个 handle 正在中继
- 哪个 handle 已经退房并重新挂上监听

## 这一步怎么验证

### 静态验证

这一阶段已经通过：

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

### 运行时验证

理想情况下，应该这样测：

```bash
./target/debug/mini_vpn server
./target/debug/mini_vpn client-tun
curl 10.0.0.2:80
curl 10.0.0.2:80
curl 10.0.0.2:80
curl 10.0.0.2:80
seq 1 4 | xargs -I{} -P4 sh -c 'curl -s 10.0.0.2:80 >/tmp/mini_vpn_curl_{}.out'
```

### 这次实测遇到的真实边界

这次在当前环境里：

- `server` 二进制可以启动
- `client-tun` 在创建 TUN 设备时被系统权限拦住

报错是：

```text
无法创建 TUN 设备: Io(Os { code: 1, kind: PermissionDenied, message: "Operation not permitted" })
```

这说明：

- Stage 4 的编译和静态校验是通的
- 运行时已经走到了 TUN 创建点
- 当前阻塞点是本机权限，不是这次 ListenerPool 代码本身的编译错误

## 你在本机如何复测

如果你的本机具备创建 TUN 设备的权限，可以按下面顺序跑：

1. 启动服务端

```bash
./target/debug/mini_vpn server
```

2. 启动 TUN 客户端

```bash
./target/debug/mini_vpn client-tun
```

3. 连续执行 4 次短连接

```bash
curl 10.0.0.2:80
curl 10.0.0.2:80
curl 10.0.0.2:80
curl 10.0.0.2:80
```

4. 再做轻并发验证

```bash
seq 1 4 | xargs -I{} -P4 sh -c 'curl -s 10.0.0.2:80 >/tmp/mini_vpn_curl_{}.out'
```

5. 观察客户端日志，重点看：

- 是否打印 4 个 listener slot 创建日志
- 是否出现不同的 handle
- 是否出现某个 handle 的 `remote session opened`
- 是否出现 `rearmed on local port 80`

## 这一步解决了什么

Stage 4 真正解决的是：

- 单槽位模型不再是唯一接入路径
- 连接池从“结构上支持”变成“运行时真实存在”
- handle 级别的状态、回收、日志开始独立

还没解决的事情也要实话实说：

- UDP over TUN 还没做池化
- TUN 目标地址还不是配置项
- 热路径里还有可继续收紧的 `unwrap()`
- 真正的端到端验证还需要具备 TUN 权限的本机环境

## 小结

Stage 3 是“把前台系统搭起来”。

Stage 4 是“真的把 4 间房开门营业”。

从这里往后，再继续做：

- 更大的 listener pool
- TUN UDP 路径
- 配置化目标与端口

都会比之前稳得多，因为主循环已经不再依赖“单个 socket 的运气”。
