# 2026-06-01 Stage 10 Yamux Auto-Reconnect Spec

## 背景

到 Stage 9 为止，client-tun 启动时建立一条 TLS + Yamux 长连接到 Upstream。
若该连接断开（server 重启、网络抖动、TLS/TCP 中断），后台 poll task 只打印
"与服务端的 Yamux 长连接已断开，请重启 Client" 然后默默退出。此后所有新连接的
`open_remote_session` 都会失败，客户端实际已废，必须手动重启。

中文要点：单条长连接是单点故障。Stage 10 让 client-tun 在断开后自动重连，
无需人工干预。

## 术语（见 CONTEXT.md）

- **Upstream**：客户端穿过的代理/中继服务器。
- **Reconnect epoch**：上游连接的"代"。每成功建立一次上游连接，epoch +1。
  用于让属于旧连接的 relay task 不会把数据误塞进新连接的 socket（防串话）。

## 目标

1. client-tun 的上游 TLS+Yamux 连接断开后，自动重连，**无限重试**。
2. 重连使用**指数退避 + full jitter**，抑制 5000+ 客户端同时重连的惊群。
3. 重连成功后，把所有在途（Relaying）的本地 smoltcp socket 复位回 Listening，
   依赖上层 TCP 自愈（curl/浏览器自行重连），不尝试迁移旧子流。
4. server 端零改动。

## 非目标（记入 TODO.md）

- 多 upstream 地址 / failover（轮换、择优）。
- 控制面 / 服务发现。
- 应用层心跳主动探活半开连接（本阶段靠 open_stream/poll task 的错误检测）。
- env 可配的退避参数（本阶段用编译期常量）。
- 完整 epoch 防串话加固（最小实现靠 rearm + 旧 task 自然退出；epoch 仅作 socket 归属校验）。

## 架构边界

### 重连所有权（grill 决策 A）

- `connect_upstream(...)` 抽成一个 async fn：建 TCP → TLS → `Connection::new(Mode::Client)`
  → 返回 `(yamux::Control, JoinHandle)`，并向主循环回传"断开"信号。
- 主循环持有 `let mut ctr: yamux::Control`，断开时进入重连流程，成功后**替换** `ctr`。
- 后台 poll task（`while next_stream().await` 循环）退出即代表断开；它通过一个
  `mpsc::Sender<()>`（或 `tokio::sync::Notify`）通知主循环。
- 主循环 `tokio::select!` 新增一个分支监听该断开信号。

### 退避（grill 决策，规模驱动）

```text
backoff_delay(attempt) = random(0, min(CAP, BASE * 2^attempt))
BASE = 500ms
CAP  = 30s
无限重试；成功后 attempt 清零。
```

- full jitter：下界取 0，最大程度摊平惊群。
- 纯函数签名（可单测）：`fn backoff_delay(attempt: u32, rand_unit: f64) -> Duration`
  其中 `rand_unit ∈ [0,1)` 由调用方注入（测试可传固定值，运行时传 `rand::random`）。

### 在途连接处理（grill 决策 A）

重连成功后，遍历 registry 所有 handle：
- 处于 `Relaying`（`uplink_tx.is_some()`）的 → `rearm_socket`（abort 本地 socket、清
  uplink_tx、重新 listen、回 Listening）。
- 旧的 `spawn_remote_relay` task：其 `write_all`/`read` 在旧子流上报错，自行 break 退出。
- 本地应用（curl/浏览器）那条 TCP 被复位 → 自行重连 → 新 SYN 进来 → 在新 Yamux 上开全新子流。

### Reconnect epoch（最小防串话）

- 全局 `epoch: u64`，每次 `connect_upstream` 成功 +1。
- `spawn_remote_relay` 捕获当时的 epoch；回程数据经 global_rx 送到主循环时带上 epoch。
- `handle_remote_payload` 校验：payload 的 epoch != 当前 epoch 则丢弃（旧连接的迟到数据）。
- 最小实现：若实现成本过高，可先只做 rearm（旧 task 自然退出已足够覆盖绝大多数情况），
  epoch 作为加固，spec 保留、plan 中标注可降级。

## 失败语义

- `connect_upstream` 失败（TCP/TLS/任意一步）→ 不返回错误中止进程，而是进入退避重试。
- 重连期间，新到的本地 SYN 仍会被 SYN inspector 接住、建 socket、但 `open_remote_session`
  会失败（无可用 control）→ 该 handle 复位/稍后随上层 TCP 重试。**不 panic**。
- 进程永不因上游断开而退出。

## 日志与可观测性

- `🔌 上游连接断开，准备重连`
- `⏳ 第 {n} 次重连，等待 {ms}ms`
- `✅ 上游重连成功 (epoch={e})`
- 复位在途连接时：`♻️ 重连后复位 {count} 条在途连接`

## 测试策略

### 单元测试

- `backoff_delay`:
  - attempt=0, rand_unit=1.0 → 接近 BASE（500ms）
  - attempt 增大，上界按 2^n 增长但被 CAP 钳制（如 attempt=10 → 上界=CAP）
  - rand_unit=0.0 → Duration::ZERO（full jitter 下界）
  - 上界恒 <= CAP
- 复位逻辑可借现有 `rearm_socket` 测试覆盖（已存在）。

### 本机/跨机手动联调（验收 recipe）

跨机拓扑（沿用 Stage 9，US Upstream）：

1. 客户端连上，`curl http://1.1.1.1/` 正常返回。
2. **重启 US server**（kill 后重新启动）。
3. 观察客户端日志：`🔌 ... 断开` → `⏳ 第 n 次重连` → `✅ 重连成功`。
4. server 重新起来后，再次 `curl http://1.1.1.1/` 应正常返回（无需重启客户端）。
5. 反复 kill/restart server 多次，客户端始终自愈。

## 文件范围

- `src/client_tun.rs`：抽 `connect_upstream`；主循环加断开信号分支 + 重连退避循环；
  `backoff_delay` 纯函数；重连后复位在途连接；epoch 字段。
- `docs/tech/2026-06-01-stage-10-yamux-reconnect-plan.md`
- `docs/tech/10-yamux-auto-reconnect.md`（教学笔记）
- `CONTEXT.md`（新增 Reconnect epoch 术语）
- `TODO.md`（落扩展方案：failover / 控制面 / LB / 滚动重启 / 心跳）

## 验收标准

1. 不重启客户端的前提下，kill+重启 US server 后客户端自动重连并恢复转发。
2. 反复多次 server 重启，客户端持续自愈，不退出、不 panic。
3. `cargo test` / `cargo check` / `cargo clippy --all-targets --all-features -- -D warnings`
   / `cargo doc --no-deps` 全过（CI 双平台亦绿）。
