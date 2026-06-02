# 10 Yamux Auto-Reconnect

## 背景

到 Stage 9，client-tun 启动时建一条 TLS+Yamux 长连接到 Upstream。这条连接是
**单点故障**：server 重启、网络抖动、TLS/TCP 中断都会让它断开，旧实现只打印
"请重启 Client" 然后后台 task 退出，此后所有新连接的 `open_remote_session` 失败，
客户端实际已废。Stage 10 让它断开后自动重连。

## 所有权模型（grill 决策 A）

- `connect_upstream(connector, server_addr, domain, disconnect_tx) -> Result<Control>`
  把"建 TCP→TLS→Yamux"收敛到一处，返回可替换的 `yamux::Control`，并 spawn 后台
  poll task。poll task 的 `while next_stream().await` 退出即代表连接断开，此时通过
  `disconnect_tx.send(())` 通知主循环。
- 主循环持有 `let mut ctr`，`tokio::select!` 新增"断开"分支。重连成功后用新 Control
  **替换** `ctr`，下一轮迭代 `ctr.clone()` 自然用上新连接。无锁、单一重连点。
- server 端**零改动**：它本就被动 accept，旧连接的 task 在 `next_stream` 返回 None
  时自然退出，新连接走 `listener.accept` 进新 task。

## 退避：full jitter（规模驱动）

5000+ 客户端 + server 重启 = 经典惊群。若所有客户端同时、同节奏重连，会把刚起来的
server 再次打垮。所以退避必须带**随机抖动**摊开重连时刻。

```
backoff_delay(attempt) = random(0, min(CAP, BASE * 2^attempt))
BASE = 500ms, CAP = 30s
无限重试；成功后 attempt 清零。
```

- **full jitter**（下界取 0）比"指数±少量抖动"更能摊平惊群（AWS 经典退避文的推荐）。
- 纯函数 `backoff_delay(attempt, rand_unit)`，`rand_unit ∈ [0,1)` 由调用方注入：
  运行时传 `rand::random`，测试传固定值 → 可单测。
- BASE/CAP 是编译期常量，本阶段不开 env。

> ⚠️ 客户端 jitter 是"地基防线"，但抑制惊群**最有效**的其实是服务端
> **滚动重启 + 优雅 drain**（把"5000 同时断"摊成"每批几十个断"）。见 TODO.md
> "Scale & reconnection resilience"。

## 在途连接处理（grill 决策 A）

连接断的瞬间，可能有 N 条正在中继的 TCP（每条一个 `spawn_remote_relay` task +
一个本地 smoltcp socket）。重连流程：

1. 遍历 registry 所有 handle，把处于 Relaying（`uplink_tx.is_some()`）的
   `rearm_socket` → abort 本地 socket、清 uplink_tx、重新 listen、回 Listening。
2. 旧 relay task 的 `write_all`/`read` 在废子流上报错，自行 break 退出。
3. 本地应用（curl/浏览器）那条 TCP 被复位 → **自行重连** → 新 SYN 进来 → 在
   **新** Yamux 上开全新子流。

不尝试迁移旧子流（远端 `target_stream` 已随旧连接销毁，无法续传）。透明代理就该
让上层 TCP 自愈。

### 防串话 / 防 panic（epoch guard 的轻量降级版）

重连 rearm 后，旧 relay task 可能有一条**迟到的回程 payload** 还在 global channel
里排队。主循环处理它时该 handle 已是 Listening，若直接 `send_slice` 会报错（旧版本
`.unwrap()` 会 panic）。`handle_remote_payload` 现在先检查 `ctx.uplink_tx.is_none()`：
是则这是上一代连接的迟到数据，丢弃；`send_slice` 也改为优雅处理 Err 不 panic。

`epoch`（连接代际计数）目前用于日志与该 state guard 的概念基础；完整的 payload 级
epoch 标记（global channel 带 epoch 字段）作为加固项留待将来（见 spec 非目标）。

## 手动验收 recipe（跨机，沿用 Stage 9）

US Upstream + 深圳客户端按 Stage 9 起好。然后：

1. `curl http://1.1.1.1/` 正常返回 301。
2. 在 US server 上 `Ctrl-C` 杀掉 server，再重新启动它。
3. 观察客户端日志：
   - `🔌 上游连接断开，准备重连`
   - `♻️ 重连后复位 N 条在途连接`
   - `⏳ 第 n 次重连，等待 {ms}ms`（注意 ms 是随机抖动值，每次不同）
   - server 起来后 `✅ 上游重连成功 (epoch=2)`
4. 无需重启客户端，再次 `curl http://1.1.1.1/` 应正常返回 301。
5. 反复 kill/restart server 多次，客户端 epoch 递增、始终自愈、不退出、不 panic。
