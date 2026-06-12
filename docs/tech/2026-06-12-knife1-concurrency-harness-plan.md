# 刀1 — 大并发压测 harness（plan / TDD 分解）

> 配套 spec：`2026-06-12-knife1-concurrency-harness-spec.md`。
> 节奏：每个 task 写失败测试 → red → 实现 → green → commit → **立即 `git push`**（一分支一 writer）。
> 分支：`claude/knife1-concurrency-harness`（从 main / cea29f1 起）。

## Task 0 — 搬主循环模块进 library（机械重构，零行为变化）

**为什么先做**：`tests/` 整合测试只能用 lib crate 公开 API；`client_tun` 现是 binary-only。

- `src/lib.rs` 加 `pub mod client_tun; pub mod device; pub mod dns; pub mod fake_ip;`
- `src/main.rs` 删 4 个 `mod`，改成 `use mini_vpn::client_tun;` + `client_tun::start_tun_proxy().await`。
- `src/client_tun.rs` 把 4 行 `mini_vpn::` 改 `crate::`（shared/tuic/upstream/udp_relay）。
- 核 device/dns/fake_ip 是否有 `mini_vpn::` 自引用（grep 显示无）。
- **测试**：`cargo build` + `cargo test`（现有 52 单测全绿）即为通过门（无新测试，纯搬迁）。
- clippy 0 warning。**commit + push**。

## Task 1 — `TunIo` trait 抽象 device

- **red**：新增单测：`VirtualTunDevice` 实现 `TunIo`（`rx_peek`/`rx_take` 与既有 `rx_buffer` 语义一致；
  `inject_ip_packet` 入队、`rx_take` 取走后 `rx_peek` 为 None）。先写断言，未实现 → red。
- **green**：定义 `TunIo`（spec 草案），给 `VirtualTunDevice` impl；内部把 `rx_buffer` 字段存取收敛到
  `rx_peek`/`rx_take`。不改主循环行为。
- 现有 52 单测无回归。**commit + push**。

## Task 2 — 抽 `run_event_loop`（生产/测试同一份循环）+ `MetricsSink`

- **red**：`MetricsSink` trait + `NoopSink`；单测断言 `NoopSink` 各回调可调用且零状态。
  （`run_event_loop` 的行为等价由 Task 4/5 的整合测试兜底；此处先立骨架与 sink 接口。）
- **green**：把 `start_tun_proxy` 的 `loop { select! {...} }` 体逐分支搬进
  `run_event_loop<D: TunIo, ...>(device, tcp_upstream, udp_downlink_rx, cfg, metrics)`；
  三段（poll / relay-sweep / 可选 DNS）包 `metrics.enter_*()/leave_*()`。
  `start_tun_proxy` 改为：建真 utun + 真 `TuicUpstream` + `start_udp()` → `run_event_loop(..., NoopSink)`。
- **关键**：逐分支人工 diff，确保与原循环**语义等价**（global_rx / rx 分流 / tuic_downlink / udp_sweep / timer
  五分支一字不差地迁移）。系统稳定优先。
- 现有 52 单测无回归 + clippy。**commit + push**。

## Task 3 — `DatagramUpstream` trait + 接进 run_event_loop

- **red**：单测：一个 mock impl `DatagramUpstream`，`send_udp` 把 datagram 推进内部 channel，可读回。
- **green**：定义 `DatagramUpstream { async fn send_udp(&self, dg: Vec<u8>); }`；`TuicUpstream` impl 之
  （转调既有 `send_udp`）；`run_event_loop` 的 UDP 上行改调 trait；下行 receiver 作参数传入。
  `handle_tuic_udp_uplink` 改吃 `&dyn DatagramUpstream`（或泛型）。
- 现有 52 单测无回归 + clippy。**commit + push**。

## Task 4 — 回环 device + mock 上游 + 第二 smoltcp 发生器（test 支撑件）

- **red**：先写一个**最小**整合用例（单连接）：发生器 smoltcp 经 `LoopbackTunDevice` 连到 SUT
  `run_event_loop`（mock echo 上游），发 1 条 TCP，收到 echo。未实现支撑件 → red。
- **green**：
  - `LoopbackTunDevice`：内存双向包队列，impl `TunIo` + smoltcp `Device`（rx 来自对端 tx，反之亦然）。
  - `MockUpstream`：impl `ProxyUpstream`（`open_tcp` 返回内存 echo duplex）+ `DatagramUpstream`
    （`send_udp` 回环喂下行 channel，做 UDP echo/计数）。
  - 发生器：第二个 `Interface`+`SocketSet` 当 app，open TCP client socket 驱动握手+收发。
  - 单连接 echo 往返通过。
- **commit + push**。

## Task 5 — `tests/concurrency_harness.rs`：N sweep + 指标 + 正确性断言

- **red**：N=64 用例：64 路并发连接跨多端口，断言 64/64 全 echo 往返成功。未跑通 → red。
- **green**：参数化 N∈{64,256,1024}；多端口（≥64 个目标端口）；`RecordingSink` 采三段耗时；
  聚合吞吐/延迟分布/内存估算；打印指标表（`--nocapture`）。大 N 用例 `#[ignore]` 门控（默认 `cargo test`
  快；压测显式 `cargo test -- --ignored --nocapture`）。N/N 正确性断言常跑。
- 加一个**轻量 UDP 用例**：少量并发 datagram 经 mock echo 上/下行往返，断言不被 TCP 饿死 + drop=0。
- clippy 0 warning。**commit + push**。

## Task 6 — 瓶颈定位结论 + 收尾

- 跑 N sweep，把三段耗时/吞吐/延迟/内存数据落进 `docs/tech/2026-06-12-knife1-bottleneck-findings.md`：
  - 每个怀疑瓶颈 #1/#2/#4/#5 的**数据 + 判定**（是/否瓶颈、量级）。
  - **指向刀2 的优化项排序**（按实测影响）。
  - #3 deferred 标注 + **手动端到端 probe 配方**（用现成 sing-box env 跑 N 路真 TUIC 的步骤）。
- `/code-review` over the diff → 修。
- 更新 `HANDOFF.md`（刀1 完成、刀2 起点）与 `TODO.md`（Scale 段）状态。**commit + push**。

## 风险与缓解

- **主循环搬迁回归**：Task 2 逐分支人工 diff + 现有 52 单测 + Task 4/5 整合回归三重兜底；必要时跨机 smoke。
- **harness 自身成瓶颈**（Stage 12 教训：echo 端/日志拖垮）：mock echo 用内存 duplex（非 fork）、
  发生器与 SUT 各自 task、压测路径**无 per-packet 日志**。
- **async fn in trait 的 Send 约束**：先原生 + 泛型；遇阻退 `async_trait`（与 ProxyUpstream 一致）。
- **第二 smoltcp 发生器复杂度**：先单连接打通（Task 4）再放大到 N（Task 5），增量验证。
