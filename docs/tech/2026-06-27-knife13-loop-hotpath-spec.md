# 刀13 spec + plan：主循环热路径净化（① trace 门控 + ② 非阻塞上行）

> 基线 main `c3b2416`（+ handoff `459310b`）。分支 `claude/knife13-loop-hotpath`。
> 设计输入：刀12 OS `sample` 三证裁决（ADR-0013「Update」+「Sample confirms it」）+ 本刀
> design-audit workflow（4 路：println 全量盘点 / acceptance 信号交叉引用 / ② 重构对抗审计 / harness 可行性）。
> 一刀两 commit（① 与 ② 各自独立增量、各自全绿）。

## 0. 背景与裁决（为什么是这两个改动）

刀12 的 OS `sample`（44.5M 隧道压测期）锁死了三件事：
- 进程**非 CPU-bound**：`cvwait 29149 / kevent 7283`（parked）压倒，smoltcp poll 仅 17 采样 → #4 实测推翻、墙是 #3/WAN。
- 主循环 **#1 on-CPU 成本 = 热路径 `println!`**：最肥栈 `process_listener_activity → _print → write` ~183 采样
  （22000 事件/秒，每次一个阻塞 `write` syscall），远超 poll（17）。
- 真 bug：上行 `tx.send(payload).await`（有界 1024 channel）在上游拥塞时**阻塞整个事件循环** →
  一条慢流 head-of-line 阻塞其它所有流 / 下行回程 / 5ms 定时器 / DNS。

→ **① 删/门控热路径 println（最便宜）**；**② 非阻塞上行（结构性，治跨流 HoL）**。本刀两者都做（用户裁决合并一刀）。

---

## 1. ① trace 门控（commit 1）

### 1.1 机制（仿 `rdbg!`，但**缓存 env 读**——关键差异）

`src/reality/handshake.rs:68` 的 `rdbg!` 每次调用都 `env::var_os(...)`——对 REALITY 握手（低频）无害，但
本刀门控的是 **22000/s 的每包路径**，绝不能每包一次 env syscall。所以：env 只读**一次**入 `OnceLock<bool>`。

在 `src/client_tun.rs` 顶部（imports 之后、首个使用点 ~276 之前）定义：

```rust
/// 热路径诊断日志总开关（env `MINI_VPN_TRACE` 非空即开）。**只读一次**缓存进 OnceLock：
/// 门控的是每包/每连接路径（22000/s），绝不能像 rdbg! 那样每次 env syscall。默认关 → 热路径零 stdout。
fn trace_enabled() -> bool {
    static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRACE.get_or_init(|| std::env::var_os("MINI_VPN_TRACE").is_some())
}

/// 热路径诊断 println 门控宏：`trace_enabled()` 为真才打印（默认静默）。保留 `println!`（stdout，
/// acceptance 重定向到日志文件）语义不变；翻 `MINI_VPN_TRACE=1` 恢复全部诊断（零信息损失）。
macro_rules! trace_log {
    ($($arg:tt)*) => {
        if trace_enabled() {
            println!($($arg)*);
        }
    };
}
```

`macro_rules!` 文本作用域：定义点必须在所有使用点之前（~第 30-60 行）。模块内使用，`trace_enabled()`
裸名同模块可达，不 `#[macro_export]`。

### 1.2 门控清单（已交叉核验 acceptance 信号，不误伤）

**改成 `trace_log!`（默认静默），21 处**：

| 类 | 行 |
|---|---|
| 真·每包（实际成本） | 727 📬、740、874、878、990、1351 🔄(上行每包)、1362、1502、1511、1668 🚫(UDP) |
| 每连接 churn（无 acceptance grep） | 276 🆕、1242 ♻️、1286 🚫(refuse)、1375、1382 🎯、1383 🔄、1385 🚪(inline open)、1414、1448 🗑️、1465、1694 🌊(new assoc) |

> 注：1351（已建 relay 上行每包 🔄 entering）会在 commit 2 随 ② 重构**移出 `handle_local_payload`**
> （已建 relay 改走 `process_listener_activity` 的 try_reserve 分支，无 per-packet println）。commit 1 先门控它，
> commit 2 把那条 dead 分支删掉——两次都干净。

**保留不门控（acceptance load-bearing / 启动 / 错误 / lifecycle）**：
- **acceptance 信号（审计实锤被 grep）**：🪪 DNS `1156/1164`（`scripts/knife35-acceptance.sh:326` grep `🪪 DNS`；
  per-query 低频，sample 从未列为成本；用户明确要保留）、🛡️ `1306`（knife1 K4 grep）、
  `spawn 握手` `1416/1473/1477`（`scripts/knife9-failover-acceptance.sh:127` status grep）、DNS flush 错误 `1194`（rare error）。
- **启动**：469、473、478、484、494、511、517、521、526、545、549、570、576、580、585、589、593、600。
- **cap/饱和错误**：796、809。**周期 📊**：915（metrics.rs:191，ADR-0013 钦点 load-bearing）。
- **run_relay lifecycle（后台 task、每 relay 一次）**：1579、1585、1593、1606、1613。

### 1.3 ① 测试

门控本质是"热路径不打印"，难直接断言。最小 TDD：
- 单测 `trace_enabled()` 行为可被验证（env 设/未设 → bool；只读一次幂等）。因 `OnceLock` 是进程级单例、
  并行测试会污染，**抽纯函数** `fn parse_trace(v: Option<&std::ffi::OsStr>) -> bool` 单测（仿
  `parse_profile_loop`/`parse_metrics_secs` 既有惯用法），`trace_enabled()` 只是它 + OnceLock 包壳。
- 主质量门：盘点正确性（workflow 已核验 51 处全分类）+ clippy 0 + release 绿 + 既有测全绿。

---

## 2. ② 非阻塞上行（commit 2）

### 2.1 核心：try_reserve **在 extract 之前**

当前 `extract_socket_payload`（`src/client_tun.rs:1204`，`socket.recv()`）在 `tx.send` **之前**已把字节从
smoltcp 排空。所以朴素 `s/send().await/try_send/` 是错的（满了字节已在手、无处可去 → 丢或塞新缓存，都破)。
正解：**先 `try_reserve` 抢 permit、再 `recv`**。

`process_listener_activity` 重构（**flush_downlink 之后**分流）：

```text
process_listener_activity(handle):
  1. flush_downlink(socket, ctx)            # 无条件最先（所有路径，含 Full）—— audit blocker
  2. 取 ctx；若 ctx.uplink_tx.is_some()（已建 relay）→ 走 established-fast-path：
       match ctx.uplink_tx.as_mut().unwrap().try_reserve():
         Err(Full)      → 不 recv、不打印、不分配，return Ok（保持 dirty：can_recv()仍true → still_active）
                          # smoltcp rx 满 → TCP 零窗口 → app 端到端背压；主循环永不阻塞；5ms timer 重试
         Err(Closed)    → 不 extract（不把字节排进虚空）；rearm_socket(socket, ctx, fake_pool, now)；return Ok
         Ok(permit)     → let Some(payload)=extract_socket_payload(socket) else return Ok（无字节，permit drop 无害）
                          permit.send(payload)         # infallible，**消费 permit 释放 ctx 借用**
                          # send 之后再 touch ctx.state（borrow 顺序，audit blocker #2）
                          ctx.state = Relaying; return Ok
       # 已建 relay 跳过 resolve_target：endpoint 不可变 + refcount 钉住 fake-IP → Block/Refuse 不可能翻（audit high）
  3. 否则（未建：首包 / HandshakePending / inline OpeningRemote）→ 原路径不变：
       extract + local_endpoint → resolve_target → handle_local_payload
```

`handle_local_payload` 的已建分支（`if let Some(tx)=ctx.uplink_tx.as_mut()`，1350-1357）在重构后对
`process_listener_activity` 调用方**变 dead**（已建已在步骤 2 截走）→ **删除该分支**（含 1351 println）。
保留 first-open 与 HandshakePending 分支。

### 2.2 first-open send 改 try_send（1388）

fresh channel（cap 1024、rx 尚未 move 进 spawn）→ `try_send` 必成。但**不假设永有位**：
`Err(_)`（Full|Closed）→ log + `rearm_socket`（镜像握手 flush 的 1462-1469，**绝不静默丢首包**=ClientHello/首请求）。
`debug_assert!(RELAY_CHANNEL_CAPACITY >= 1)`。

### 2.3 必守不变量（audit must-handle 汇总）

1. **flush_downlink 无条件最先**（所有路径含 Full）——否则拥塞上行流的下行 stall。
2. **借用顺序**：`permit.send()` 消费 permit → 之后才 `ctx.state=`。permit 存活期不得改 ctx。
3. **Full 早返**：任何 println / extract / 分配**之前** return；靠 5ms timer 重试（① 已门控该路径所有 println）。
4. **跳 resolve_target 严格 gate 在 `uplink_tx.is_some()`**；rearm 清 uplink_tx → 复位槽自动回到 resolve 路径。
5. **Err(Closed) → 不 extract + 行内 rearm**（比现状 log-only 更严，是 deliberate 改进，配回归测 + 文档）。
6. **extract↔send 无 fallible op**：`Ok(permit) → extract(None则早返) → permit.send`，中间无 `?`/早返（防丢/截断）。
7. **不破无锁单写者模型**；**Ok(permit) 路径不碰 conn_epoch**（仅 rearm bump）。

### 2.4 ② 测试（TDD 红→绿，harness 集成）

harness 扩展（`src/harness.rs`）：
- **MockUpstream stall 模式**：加 `stall_after: Option<usize>` + `stall_port: Option<u16>` + 可恢复 `Arc<Notify>`。
  `open_tcp` 按 `target` 端口判定：命中 stall_port 的 echo task 读够 `stall_after` 字节后**停读**（`select!` 读 vs
  release-notify），**`far` 不 drop**（保持 duplex 开 = "拥塞但未关"，否则触发 Closed 而非 Full）。
- **per-flow 定向**：A→stall_port、B→普通端口（GenConn 已用不同 dst port → 不同 TargetAddr）。
- **位置编码 payload**：现 `vec![0xAB; n]` 改 `byte i = (i & 0xff)`（否则丢/重/乱序在全 0xAB 中不可检）；
  GenConn 累积收到字节做逐字节核对。
- **`run_tcp_hol_scenario`**（仿 run_tcp_scenario，多线程 runtime）：2 流，payload 足够大（256KB+，
  > RELAY_CHANNEL_CAPACITY×MTU + smoltcp rx buf，确保 channel 真填满触发 Full）；echo_buf 小（8KB）使 duplex 快阻塞。

断言：
- **(red→green) 无跨流 HoL**：A stall 期间，B 在紧超时（5s）内 `recvd>=payload_len` 跑完，且 A 未完成。
  当前阻塞 `tx.send().await` → 整个 loop 停在 A 的 send，B 永不完成 = **red**；改后 = **green**。
- **无字节丢失**：release stall 后 A 也跑完，A 收到字节流 == 发出字节流（逐字节）。
- **无 spurious rearm**：stall 期间 `mock.tcp_opens()` 不增（证明 Full 路径保持 dirty、未排空丢弃也未拆 relay）。

gotchas（harness 审计）：multi_thread runtime（单线程掩 HoL）；`far` 不 drop；echo_buf 小 + payload 大；
全程 < 90s（RELAY_IDLE_TIMEOUT）；不用 start_paused（与 busy generator-poll 不合）、用真时钟 + 松超时 + 顺序不变量断言。

---

## 3. commit 计划

1. **commit 1 `perf(knife13): gate hot-path println behind MINI_VPN_TRACE`**：trace_enabled/trace_log 宏 + 21 处门控 +
   parse_trace 单测。质量门全绿。
2. **commit 2 `perf(knife13): non-blocking uplink send (try_reserve) — fix cross-flow HoL`**：process_listener_activity
   established-fast-path 重构 + first-open try_send + 删 dead 分支 + harness stall 模式 + HoL/byte-loss 测。质量门全绿。

每 commit 后立即 `git push`。

## 4. 质量门（每 commit）

- `cargo test`（lib）+ `cargo test --features harness`（含新 HoL 测）全绿。
- `cargo clippy --all-targets --features harness` 0 warning。
- `cargo build --release` 绿。
- `/code-review` + 对抗式核验（② 重构 ≥3 lens）。

## 5. 真出口 acceptance（尽力而为如实记录）

用 `sudo -E env MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 bash scripts/knife35-acceptance.sh soak` 起隧道：
- **金标准进隧道**：`curl ipinfo.io` 显美国出口 IP + `📊 TCP relay 累计 >0`（验证 ① 没误门控 📊）。
- **① 验证**：默认 soak（无 MINI_VPN_TRACE）日志**无** 📬/🔄 每包洪水，仍见 🪪 DNS / 📊 / 🛡️（acceptance 信号在）；
  `sample <pid> 10` 压测期 `_print→write` 栈应从 ~183 大幅下降。`MINI_VPN_TRACE=1` 重起 → 诊断打印全回来。
- **② 验证**：混合大并发（一条慢流 + 多条快流，如 `iperf3 -P` 混不同目标）下快流不被慢流拖死；
  `🔬 relay` 占比下降（不再阻塞在 send().await）；不丢字节（curl 大文件 md5 一致）。
- 真 HoL 难在真出口精确诱发 → 以 harness 多线程 stall 测为高保真替身，真出口尽力而为如实记录（同刀10 KeyUpdate 纪律）。
