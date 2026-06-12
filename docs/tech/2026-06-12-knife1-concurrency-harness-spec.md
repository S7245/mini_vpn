# 刀1 — 大并发压测 harness（spec）

> Core 路线第一刀。目标：Rules.md ③「大并发连接」当前**不知道真瓶颈在哪**——先量化、事实先行，
> 为刀2（对症优化）提供数据地基。纪律：**localize before fixing**（.learnings 反复强调）。

## 目标

一个**可复现、CI 可跑、无需 root / 无需 sing-box** 的并发压测 harness，在 N 路并发 TCP + 一个轻量
UDP 用例下，把 mini_vpn **客户端主循环 + smoltcp + relay 调度** 的并发瓶颈从网络中**隔离**出来，
并 instrument 出**每段耗时 / 吞吐 / 延迟 / 内存**，把瓶颈 localize 到主循环的具体环节。

## 非目标（本刀不做）

- **不做端到端真出口压测**（真 TUIC→sing-box）。#3「单条 QUIC 连接」是网络/上游属性，mock 本地回环
  不会出现，标注 deferred + 附手动 probe 配方，留给后续/刀2 acceptance。
- 不做任何性能**优化**（那是刀2）。本刀只测量 + 给结论。
- 不做前端意义的 library 化（local-control 接入等）——本刀只为测试可达性做**最小**主循环暴露。
- 不碰 UDP 直播吞吐硬化（quic-stream fallback / MSS-MTU）——刀3。

## 怀疑瓶颈映射（哪些本刀能钉死、哪些 deferred）

| # | 怀疑瓶颈 | mock 回环能否暴露 | 本刀产出 |
|---|---|---|---|
| 1 | 主循环每 tick `registry.all_handles()` **O(n) 全量遍历** | ✅ | 三段插桩里 relay-scheduling 段随 N 的耗时曲线 |
| 2 | `MAX_INTERCEPTED_PORTS=64` 端口上限 | ✅ | 多端口 sweep 跨 ≥64 端口，记录建池失败/被拒 SYN |
| 3 | **单条 TUIC QUIC 连接**承载所有 TCP flow（拥塞/队头） | ❌（网络属性） | **deferred**，结论里标注 + 手动 probe 配方 |
| 4 | 单线程 `tokio::select!` 主循环串行上限 | ✅ | 总吞吐随 N 的饱和点 + 每 tick 总耗时 |
| 5 | 每 socket 64KB×2 缓冲的内存/poll 成本 | ✅ | per-socket 缓冲内存解析估算 + poll 段耗时随 N |

## 8 项设计决策（grill 对齐结果）

1. **Harness 形态 = 全栈回环 + mock 上游**。内存 device（两个 smoltcp 对接：一个是被测主循环 SUT，
   一个当 N 个 app 的流量发生器）+ mock `ProxyUpstream`（echo/计数，不走网络）。
2. **重构 = 抽 `run_event_loop` + `TunIo` trait**。生产与测试跑**同一份**循环代码；device 用 `TunIo`
   抽象（`wait_for_rx`/`flush_tx`/`inject_ip_packet`/rx-buffer 存取 + 实现 smoltcp `Device`）。
   `start_tun_proxy` 退化为「建真 utun + 真 `TuicUpstream` → 调 `run_event_loop`」薄壳。零回归。
3. **流量发生器 = 第二个 smoltcp 栈**（SUT 消费 IP 包，内存管道另一端放不了内核 socket）。**TCP 为主、
   多端口 sweep**：N∈{64,256,1024} 并发连接，目标端口跨 ≥64 个以压 #2；每连接跑固定负载 echo 往返。
   外加**一个轻量 UDP 吞吐用例**（验证 datagram 上/下行不被 TCP 饿死；UDP 主体压测留刀3）。
4. **度量 = 循环分段插桩 + 聚合指标 + N/N 正确性断言**。给主循环 `poll` / `all_handles-sweep` /
   `process_listener_activity`(relay 调度) 三段插桩计时计数；叠加总吞吐 / 连接延迟分布 / per-socket
   缓冲内存估算；并跑 **N/N 全连通**断言（兼做 Stage 12 那种 loopback 并发回归测试）。
5. **插桩接线 = metrics-sink hook**。`run_event_loop` 收一个轻量 `MetricsSink`；生产传 `NoopSink`
   （内联后零开销），harness 传 `RecordingSink`。生产/测试同一份循环、同一个 sink 接口。
6. **UDP 上游抽象 = 新增 `DatagramUpstream` trait**（`async fn send_udp`），下行 receiver 作独立参数
   传进 `run_event_loop`。`ProxyUpstream` 原封不动；mock 同时 impl 两 trait + 自带 echo 回环 channel。
7. **本刀边界 = 只交 mock，#3 标注 deferred**（见上表）。
8. **harness 位置 = 搬主循环进 library + `tests/` 整合测试**。`run_event_loop`/`TunIo`/`DatagramUpstream`
   暴露到 `mini_vpn` lib；回环 device / mock / 发生器作 lib 内 `#[cfg(test)]` 或 `pub(crate)` test 支撑；
   harness 本体在 `tests/concurrency_harness.rs`。这是**窄范围测试面暴露**，非前端 library 化。

## TunIo trait（草案）

```rust
pub trait TunIo: smoltcp::phy::Device {
    async fn wait_for_rx(&mut self) -> std::io::Result<()>;
    fn rx_peek(&self) -> Option<&[u8]>;       // classify_inbound / inspect_inbound_syn 只读窥视
    fn rx_take(&mut self) -> Option<BytesMut>; // UDP relay take 走
    async fn flush_tx(&mut self) -> std::io::Result<()>;
    fn inject_ip_packet(&mut self, pkt: &[u8]);
}
```
- `run_event_loop<D: TunIo, ...>` 用**泛型单态化**（smoltcp `Device` 非对象安全，且避免 per-tick boxing）。
- async fn in trait 走原生（generic，非 dyn）；若 `Send` 约束遇阻再退回 `async_trait`（与 `ProxyUpstream` 一致）。
- `VirtualTunDevice` 现有 `rx_buffer: Option<BytesMut>` 字段 → 用 `rx_peek`/`rx_take` 存取（trait 不暴露字段）。

## run_event_loop 签名（草案）

```rust
pub async fn run_event_loop<D, U, M>(
    device: D,
    tcp_upstream: Arc<U>,                  // ProxyUpstream
    udp_upstream: Arc<dyn DatagramUpstream>, // 或泛型；mock & TuicUpstream 各 impl
    udp_downlink_rx: mpsc::Receiver<Vec<u8>>,
    cfg: TunRuntimeConfig,
    metrics: M,                            // MetricsSink（NoopSink / RecordingSink）
) where D: TunIo, U: ProxyUpstream, M: MetricsSink
```
（确切泛型/dyn 形态在 plan 里随 TDD 定；原则：生产热路径零额外开销、mock 可注入。）

## 度量指标（RecordingSink 采集 + harness 聚合）

- **三段耗时**：每 tick 在 `poll` / `relay-sweep`(`all_handles`+`process_listener_activity`) /（可选 DNS）
  段累计耗时与调用次数 → 随 N 的曲线。
- **吞吐**：echo 往返总字节 / 墙钟。
- **延迟**：每连接 connect→首字节 echo 的分布（p50/p95/max）。
- **正确性/丢失**：TCP N/N 全往返成功（TCP 可靠，"丢失"= 连接 refused/卡死）；UDP 用例记 drop 计数。
- **内存**：per-socket 缓冲 = N × (TCP_SOCKET_BUFFER_SIZE × 2) 解析估算（+ 可选实测增量）。

## 验收（acceptance）

1. `cargo test` 绿；现有 52 单测无回归；clippy 0 warning；release build 绿。
2. 生产路径（`start_tun_proxy`）行为不变——主循环逻辑逐分支等价（人工 diff 复核 + 可选跨机 smoke）。
3. harness 在 N∈{64,256,1024} 跑出 N/N 全连通 + 三段耗时/吞吐/延迟/内存表，可重复。
4. 产出 `docs/tech` 一份**瓶颈定位结论**：数据 + 指向刀2 的优化项排序 + #3 deferred 标注与手动 probe 配方。
