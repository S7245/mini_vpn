# 刀13 交接（新 session 冷启动）

> 一刀一 session。从 main 起新分支。中文回复（代码/术语/commit 英文）。

## 当前状态
- **刀12（多核逼近 100M·量化定位）已完成 + 合入 main**（HEAD `c3b2416`，2026-06-27）。裁决见
  `docs/adr/0013-client-loop-not-the-100m-bottleneck.md`（尤其「Update」段）。
- **三证裁决**：#4（单核 smoltcp poll = 100M 天花板）**实测证伪**（`🔬` poll 段 ≤8% / `sample` poll 仅 17 采样）；
  墙是 **#3 = 单 QUIC 连接 CC / 跨太平洋 WAN**，客户端**非 CPU-bound**（`sample` 全是 `cvwait`/`kevent` parked）。
  **100M 在深圳↔美国这条路物理不可达**（网口 100M ≠ 跨洋可达吞吐；单流 ~22M / 并行聚合 ~46M）。
- 量化仪器 `LoopProfiler` 已在 main（`MINI_VPN_PROFILE_LOOP=1` 开，默认 `NoopSink` 零开销），随时可复测 #4/#3。

## 刀13 做什么（sample 实测定的两个真 client bug，按便宜→结构性）
1. **删/门控热路径 `println!`（最便宜，先做）** —— `sample` 揪出主循环 **#1 on-CPU 成本是 relay 热路径的
   `println!` → `write`**（22000 事件/秒，每次一个阻塞 `write` syscall；soak 下写日志文件），远超 smoltcp poll。
   - 起点：`grep -n 'println!' src/client_tun.rs`，重点审 `run_event_loop` / `process_dirty_relay` /
     `process_listener_activity` / `handle_remote_payload`（如 `📬 从大邮筒收到…` `src/client_tun.rs:658`）/
     `handle_local_payload` / `rearm_socket` / `forge_dns_reply`。
   - 项目已有「热路径勿 println」纪律（`src/device.rs` 注释），这些是漏网的。门控到 debug flag 或删掉 per-packet 的；
     **保留低频/启动/错误日志**。
2. **非阻塞上行发送（结构性，治 HoL）** —— 上行 `tx.send(payload).await`（有界 `RELAY_CHANNEL_CAPACITY=1024`，
   `src/client_tun.rs:1350`）在 QUIC 上游拥塞时**阻塞整个事件循环** → 一条慢流 head-of-line 阻塞其它所有流 /
   下行回程 / 5ms 定时器 / DNS（大并发混合流下慢流拖死快流）。
   - 改法：`try_send`，满了**不从 smoltcp 取数据、留在 socket rx + 保持 handle dirty**，靠 smoltcp TCP 窗口
     端到端背压，主循环永不阻塞。**不破无锁单写者模型。**

> 建议 ①②**分两刀**（各自独立增量），grill 时定。①小而快；②要小心 smoltcp 背压语义 + 回归（别丢字节）。
> 吞吐天花板仍是 WAN/#3——连接池只在**低 RTT 胖链路**（同区域出口，非深圳↔美国）才有意义，留以后。

## 节奏（HANDOFF Rhythm，必守）
- 从 main `c3b2416` 起新分支，逐 commit 后立即 `git push`；一刀一 session、一个分支一个 writer。
- **grill（先对齐范围，别直接写码）→ spec+plan（docs/tech/，TDD 分解）→ TDD 红→绿→commit→push →
  `/code-review` + 对抗式核验 → 真出口 acceptance（尽力而为如实记录）**。
- 质量门：lib+harness 全绿、`clippy --all-targets --features harness` 0 warning、release 绿。原则：**系统稳定 > 代码漂亮**。
- cwd 陷阱：每条 git/cargo 命令前 `git branch --show-current` 确认在本 worktree、用绝对路径。

## 真出口 acceptance 的坑（刀12 血泪，必记）
- **必用 `soak` 起**：`sudo -E env MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 bash scripts/knife35-acceptance.sh soak`
  ——别裸跑 `./mini_vpn client-tun`（裸二进制只建 utun + TUIC，**不配 macOS 路由/DNS** → 流量走 en0 不进隧道）。
- **金标准验证「真进隧道」**：`curl ipinfo.io` 必须显示**美国出口 IP**（显示深圳 = 没进隧道，吞吐/丢包数全作废）；
  并确认 `📊 TCP relay 累计` `>0`。
- soak 把 client 输出重定向到 `/tmp/mvpn_accept.log`，看 `🔬`/`📊` 要 `tail -f /tmp/mvpn_accept.log`（不在终端）。
- 量瓶颈三件套：`🔬`（loop-active/poll/relay 占比——**注意它把 CPU 与 arm 内 `.await` 背压等待混在一起**）、
  `📊`（relay 累计 / leg）、`sample <pid> 10`（OS 线程 CPU，区分 CPU-bound vs 背压等待）。

## 必读
`Rules.md`、仓库根 `HANDOFF.md`（「刀12 完成（2026-06-27）」段）、`docs/adr/0013-*`、
`docs/tech/2026-06-12-knife1-bottleneck-findings.md`（末节「刀12」+「刀12 续」）。
