# 刀11 — 数据面可观测性（observability）spec

> 配套：plan(同目录 `2026-06-26-knife11-observability-plan.md`)、ADR `docs/adr/0012-data-plane-observability-metrics.md`、
> seed `docs/tech/2026-06-26-knife11-observability-seed.md`。分支 `claude/knife11-observability`(从 main `6ba6d42` 起)。
> 北极星：**为后续「数据面多线程逼近 100M」铺量化底座**——先把 DNS forge / datagram drop / 背压 / TCP 活跃连接 /
> fake-IP 池用量 / failover 当班腿这些「现在看不见」的数据面状态变成**原子计数 + 周期快照**，量化先行、别凭猜改。

## TL;DR

| 面 | 缺口（现状） | 本刀做法 |
|---|---|---|
| **DNS forge** | `forge_dns_reply` Some=伪造/None=丢弃，**无任何计数** | caller `handle_dns_hijack` 处 `dns_forged++`(Some)/`dns_dropped++`(None)，**不碰纯函数** |
| **UDP 上行 drop** | `udp_drops`(AtomicU64)已有但 conflate 三种上行失败 | **不动**（零回归），snapshot 经现成 `udp_drop_count()` 读出 |
| **UDP 下行 drop** | accept-uni 信号量溢出（无 else）+ `read_uni_packet` None **静默丢、无计数** | 新 `udp_drops_down` 计数（与上行**严格分离**） |
| **datagram 背压** | `is_datagram_pressured` 是 level 信号，仅 30s 打印、不计数 | 新 `datagram_pressure_events` 计数（**false→true 上升沿**，非每 tick） |
| **TCP 活跃连接/relay** | `socket_ctxs` 有 `state==Relaying`，**无聚合数** | 30s tick 重算 `active_relays` gauge + 累计 `relays_spawned` |
| **fake-IP 池用量** | `FakeIpPool` 有 per-映射 refcount，**无 usage 访问器** | 新 `usage()->(total,active)` + 30s tick 采样为 gauge |
| **failover 当班腿** | `active_leg()->TcpLeg` 已可读 | 30s tick 经 trait 采样为 gauge（degrade gracefully 非 failover） |
| **统一接口** | 无 | 新 `Metrics`(Arc 原子) + `MetricsSnapshot`(纯值契约) + 30s `📊` 快照日志 |

## 现状（代码事实，已逐一查证 — grounding workflow 2026-06-26）

数据面是**两个 task** 各自持状态，**当前无任何共享 metrics 句柄桥接二者**：

1. **`run_event_loop`**（[client_tun.rs:518](../../src/client_tun.rs)，`<D:TunIo, U:ProxyUpstream+DatagramUpstream, M:MetricsSink>`）= **单 task** 跑 `tokio::select!`，**独占无锁**持有 `socket_ctxs: HashMap<SocketHandle,SocketCtx>`([:546](../../src/client_tun.rs))、`fake_pool: FakeIpPool`([:556](../../src/client_tun.rs))。DNS 劫持 caller `handle_dns_hijack`([:646](../../src/client_tun.rs) 调用点) 在此 task。**当前无 30s tick**（只有 timer 5ms[:582]、udp_sweep 1s[:590]、fake_ip_sweep 60s[:592]）。
2. **`TuicUpstream::start_udp`**（[tuic.rs:944](../../src/tuic.rs)，`self:&Arc<Self>`）spawn 的下行泵/心跳/统计 task = **另一个 task**，持 `conn`(quinn) + clone 的 `Arc<TuicUpstream>`。**既有 30s `📊` 日志在此**（[:1000](../../src/tuic.rs) `stats.tick`，`UDP_STATS_LOG_SECS=30`），读 `conn.stats().path`(RTT/cwnd/lost/sent) + `udp_drops`/`udp_stream_fallbacks` 原子 + `datagram_send_buffer_space`，门控 `udp_active||fb>0||drops>0`（空闲静默防刷屏）。
3. **`MetricsSink` trait**（[client_tun.rs:35](../../src/client_tun.rs)）= knife1 的**逐段计时**接缝（`enter_poll/leave_poll/enter_relay/leave_relay/note_listeners`，全默认空）；生产 `NoopSink` 单态化后**零开销、热路径无 `Instant::now()`**。**它是计时导向、且只活在 run_event_loop 这一 task，看不见 TuicUpstream 的原子** → 不适合做累计计数器载体（见决策 D1）。

已核实的接缝（符号定位，行号会变）：

- **DNS forge**：`forge_dns_reply(udp,&mut fake_pool,now)->Option<Vec<u8>>`（[:992](../../src/client_tun.rs)，**自由纯函数**，`None`=`parse_query` 失败=唯一 drop 分支；`Some`=A 伪造 / AAAA·other 的 NODATA，**都算 forge**）；caller `handle_dns_hijack`（[:1018](../../src/client_tun.rs)）的 `if let Some(reply)=…{…}else{…}` 是**两个结局都可见**的唯一插桩点。
- **UDP 上行**（已有，不动）：`udp_drops:AtomicU64`([tuic.rs:671](../../src/tuic.rs))在 send_udp no-conn/datagram-fail/uni-stream-fail 三处 `fetch_add`；访问器 `udp_drop_count()`([:929])、`udp_stream_fallback_count()`([:934])。这些原子是 `TuicUpstream` 的**直接字段、不在 conn Mutex 下**，Relaxed 读写。
- **UDP 下行**（缺口）：`start_udp` 的 accept-uni 分支 `if let Ok(permit)=…try_acquire_owned(){…}`（[tuic.rs:987](../../src/tuic.rs)）**无 else** → 信号量(`MAX_CONCURRENT_DOWNLINK_STREAMS=256`)耗尽时静默丢 stream；`read_uni_packet`([:1055])超长/reset/读错/空 → `None` → 静默丢。**两处均无计数**。
- **背压**：`is_datagram_pressured(space,mtu)->bool`([tuic.rs:621](../../src/tuic.rs)) = `space<mtu` 纯 level 信号，仅 30s tick 读一次打印。
- **fake-IP 池**：`FakeIpPool{…, ip_to_mapping:HashMap<Ipv4Addr,Mapping>}`，`Mapping{domain,refcount,last_used}`（**私有**，[fake_ip.rs:9](../../src/fake_ip.rs)）。`alloc` 不改 refcount；`acquire`/`release` 调 refcount±1；`sweep` 回收 refcount==0 且超 TTL。**无 len/usage 访问器**。`resolve(&self)`([:138]) 是「便宜只读 &self」的模板。**单 task 独占无锁**。
- **failover**：`FailoverState::active_leg(&self)->TcpLeg`([failover.rs:136](../../src/failover.rs)，O(1) Relaxed load，`Arc<FailoverState>` 下)；`TcpLeg{Tuic=0,Reality=1}`，`as_u8`/`from_u8` **当前私有**([:96])。`FailoverUpstream::state()->&Arc<FailoverState>`([:254])。**铁律**（[:356](../../src/failover.rs)）：`send_udp` 永不读 `active_leg`/冷却——本刀的 leg 采样是**独立周期 Relaxed 读、不在 UDP 数据路径**，不破铁律。
- **TCP 活跃 relay**：`SocketState{Listening,OpeningRemote,HandshakePending,Relaying,Closing,Rearming}`([:64])；`Relaying`=有活的 `run_relay` task。`spawn_remote_relay`([:1359]) 在 2 处调用（inline TUIC [:1228]、spawn 握手成功 [:1303]）= relay 启动唯一入口。
- **trait**：`ProxyUpstream`([upstream.rs:21]) 有 `open_is_cheap()->bool` **默认方法先例**；`DatagramUpstream`([:41]) 只有 `send_udp`。二者 `#[async_trait]`。

## 设计决策（基于 grounding + 设计综合 workflow，2026-06-26；详见 ADR-0012）

- **D1 独立 `Arc<Metrics>`，不扩 `MetricsSink`**。`MetricsSink` 是逐段计时（扩它会让 `NoopSink` 长出真状态、丢掉「单态化零开销」承诺），且只活在 run_event_loop task、**看不见 TuicUpstream 的 UDP 原子**。唯一能被**两个 task** 各 clone 一份、无锁桥接的载体 = 进程级 `Arc<Metrics>`（各字段 `Atomic*`，热路径 `fetch_add(Relaxed)`），正是 `TuicUpstream.udp_drops:AtomicU64` 既有模式的推广。`MetricsSink` 原样保留（计时正交）。
- **D2 计数器 vs 发布式 gauge 两套取值**。
  - **累计 counter**（`dns_forged/dns_dropped/udp_drops_down/datagram_pressure_events/relays_spawned`）= `Arc<Metrics>` 的 `AtomicU64`，事件点 `fetch_add(Relaxed)`，`snapshot()` 仅 `load`。
  - **瞬时 gauge**（`active_relays/fake_ip_active/fake_ip_total/failover_leg`）来自 run_event_loop **单 task 独占状态**，**不能跨 task 读**（`socket_ctxs`/`fake_pool` 无锁、严禁套 `Arc<Mutex>`）→ 由 loop 在 **30s tick 重算**后 `store(Relaxed)` 进**同一 `Arc<Metrics>`** 的 `AtomicU32/U8`；`snapshot()` 读最新已发布值。**O(n) 扫描只在 30s tick，绝不进每包热路径**。
  - **上行 `udp_drops`/`udp_stream_fallbacks` 不迁移**（保留在 `TuicUpstream`、零回归），snapshot 经 trait 访问器读出。
- **D3 背压 = 上升沿计「集」**。`is_datagram_pressured` 是 level；每 tick 为真就 ++ 会把一段持续背压重复计数。改：start_udp task 内持**普通局部 `prev_pressured:bool`**（单 task 无需原子），仅 `pressured && !prev` 时 `datagram_pressure_events++`，counts distinct episodes。
- **D4 failover leg 经 trait 默认方法采样**。给 `ProxyUpstream` 加默认 `failover_leg_u8(&self)->u8 { NO_FAILOVER }`（同构既有 `open_is_cheap`），仅 `FailoverUpstream` override 返回 `state().active_leg().as_u8()`（`as_u8` 改 `pub(crate)`）。纯 TUIC/纯 REALITY 继承哨兵 `NO_FAILOVER`(=255) → **degrade gracefully、不 panic**。读发生在 30s tick（`upstream:Arc<U>` 在 scope），**不破 send_udp 铁律**。
- **D5 统一 `📊` 快照行由 run_event_loop 发**（不在 start_udp）。理由：run_event_loop **永远存在**（start_udp 仅 UDP relay 启动后才有；纯 REALITY 模式无 start_udp → 否则就没可观测输出），且**独占 gauge 源**。新 30s tick：重算+发布 gauge → 经 trait 读上行计数 → `snapshot()` → `format_metrics_snapshot()` **无门控打印**（TCP/DNS/failover 在 UDP 空闲时也有意义）。既有 start_udp 的 UDP-path `📊` 行（RTT/cwnd/背压）**原样保留、不动其门控**。→ **两行日志、各司其职**（两个独立 30s interval 轻微漂移，benign）。
- **D6 前端契约边界**：本刀只导出 `MetricsSnapshot`（纯值 `Copy` struct，pub 字段）+ `snapshot()` + `📊` 日志。**不建 IPC/local-control 读通道**（前端 session 主导，契约先行）。**不引入外部依赖**（无 prometheus/metrics/serde——serde 派生留前端按需加，struct 形状即契约）。

## 组件设计

### C1 `src/metrics.rs`（新模块）— TDD 核心叶子

```rust
// 进程级共享句柄（Arc<Metrics>）：两个 task 各 clone 一份。仅 std atomics，无外部依赖。
pub struct Metrics {
    // 累计 counter（事件点 fetch_add(Relaxed)）
    dns_forged: AtomicU64, dns_dropped: AtomicU64,
    udp_drops_down: AtomicU64, datagram_pressure_events: AtomicU64,
    relays_spawned: AtomicU64,
    // 发布式 gauge（loop 30s tick store；snapshot load）
    active_relays: AtomicU32, fake_ip_active: AtomicU32, fake_ip_total: AtomicU32,
    failover_leg: AtomicU8, // 0=Tuic 1=Reality 255=NO_FAILOVER
}
pub const NO_FAILOVER: u8 = u8::MAX;

// 前端契约：纯值、Copy、无原子无锁、pub 字段。
pub struct MetricsSnapshot {
    pub dns_forged: u64, pub dns_dropped: u64,
    pub udp_drops_up: u64, pub udp_drops_down: u64, pub udp_stream_fallbacks: u64,
    pub datagram_pressure_events: u64,
    pub relays_spawned: u64, pub active_relays: u32,
    pub fake_ip_active: u32, pub fake_ip_total: u32,
    pub failover_leg: FailoverLegView, // {Tuic,Reality,None}
}
impl Metrics {
    pub fn new() -> Self;                 // 全 0，failover_leg=NO_FAILOVER
    // 上行计数由 caller 从 upstream trait 读出后传入（Metrics 不持 TuicUpstream 句柄）。
    pub fn snapshot(&self, udp_drops_up: u64, udp_stream_fallbacks: u64) -> MetricsSnapshot;
    // 各 counter 的 fetch_add helper（或直接 pub(crate) 字段 + Relaxed）+ gauge 的 store helper。
}
pub fn format_metrics_snapshot(s: &MetricsSnapshot) -> String; // 纯，可单测（仿 format_udp_stats）
```
- `Metrics` 字段建议 `pub(crate)` 或提供 `inc_*()`/`set_*()` 薄方法，使写点（client_tun/tuic）与读点（snapshot）解耦于具体字段。**snapshot() 一律 `load(Relaxed)`，O(1)、可在任意 task 调**。
- `lib.rs` 加 `pub mod metrics;`。

### C2 `FakeIpPool::usage(&self)->(usize,usize)`（`fake_ip.rs`，紧邻 `resolve`）
```rust
pub fn usage(&self) -> (usize, usize) {
    let total = self.ip_to_mapping.len();                                   // O(1)
    let active = self.ip_to_mapping.values().filter(|m| m.refcount > 0).count(); // O(n) 一遍
    (total, active)
}
```
- `&self` 只读、不触 `last_used`、不分配（不像 `resolve` clone domain）。`Mapping.refcount` 私有 → **必须是 `FakeIpPool` 的 inherent 方法**。**只在 30s tick 调**（worst-case ~131k 映射，O(n) 进每包热路径不可接受）。

### C3 upstream trait 观测访问器（`upstream.rs` + 各 impl）
```rust
// ProxyUpstream（仿 open_is_cheap 默认方法）
fn failover_leg_u8(&self) -> u8 { crate::metrics::NO_FAILOVER }
// DatagramUpstream（默认 0）
fn udp_drops_up(&self) -> u64 { 0 }
fn udp_stream_fallbacks(&self) -> u64 { 0 }
```
- override：`TuicUpstream`→`udp_drops_up=udp_drop_count()`、`udp_stream_fallbacks=udp_stream_fallback_count()`（leg 继承 NO_FAILOVER）；`FailoverUpstream`→`failover_leg_u8=state().active_leg().as_u8()`、UDP 两项**转发 self.tuic**（UDP 恒 TUIC）；`RealityUpstream` 全继承默认(0/0/NO_FAILOVER)。
- `failover.rs`：`TcpLeg::as_u8` 改 `pub(crate)`（单一编码源，勿在别处重写 match）。

### C4 DNS forge 计数（`handle_dns_hijack`，**不碰 `forge_dns_reply` 纯函数**）
- `handle_dns_hijack` 加 `metrics:&Metrics` 参（[client_tun.rs:1018]）：`Some(reply)`臂 `metrics.dns_forged++`、`else`臂 `metrics.dns_dropped++`。调用点[:646]传 `&metrics_handle`。
- **不计** [:1024] 的 `parse_inbound_udp…else return`（生产里 `classify_inbound` 已保证 parse 成功 → dead，计了反而与 forge None 双计）。

### C5 `relays_spawned` + 三 gauge + 30s tick + 快照发射（`run_event_loop`）
- **param**：`run_event_loop` 加 `metrics_handle: Arc<Metrics>`（**保留** `mut metrics:M`，二者正交）。6 调用点（生产 [client_tun.rs:434/451/490]、harness [harness.rs:532/678/811]）各传 clone。
- **`relays_spawned`**：`spawn_remote_relay` 加 `&Arc<Metrics>` 参，内 `fetch_add(1,Relaxed)`，一处覆盖 2 个 spawn 站。
- **新 30s tick**：`let mut metrics_tick = interval(Duration::from_secs(30));`（≈[:592] 旁），select! 加（≈[:765] fake_ip_sweep 臂后）：
```rust
_ = metrics_tick.tick() => {
    let active = socket_ctxs.values().filter(|c| c.state == SocketState::Relaying).count();
    metrics_handle.set_active_relays(active as u32);
    let (total, act) = fake_pool.usage();
    metrics_handle.set_fake_ip(total as u32, act as u32);
    metrics_handle.set_failover_leg(upstream.failover_leg_u8());
    let snap = metrics_handle.snapshot(upstream.udp_drops_up(), upstream.udp_stream_fallbacks());
    println!("{}", format_metrics_snapshot(&snap)); // 无门控
}
```
- gauge 重算/扫描**只在此 task、此 tick**；发布进 `Arc<Metrics>` 供未来前端读；snapshot 同源读出。

### C6 UDP 下行 drop + 背压计数（`TuicUpstream`/`start_udp`）
- `TuicUpstream` 加字段 `metrics: Arc<Metrics>`（[tuic.rs:663]），`connect` 加 `metrics:Arc<Metrics>` 参（[:687]），`Ok(Self{…})` 初始化。`start_udp` 已 clone `Arc<Self>` 为 `me` → `me.metrics` 在 spawned task 可达。
- **下行 drop**：accept-uni `if let Ok(permit)=…{…}` 加 `else { me.metrics.inc_udp_drops_down(); }`（[:987]）；`read_uni_packet` 返回 `None` 处（[:991] call）`me.metrics.inc_udp_drops_down()`。
- **背压上升沿**：stats tick 内已算 `pressured`（[:1020]）；持局部 `prev_pressured`，`if pressured && !prev { me.metrics.inc_datagram_pressure_events(); } prev = pressured;`。
- 提取纯 helper `note_pressure_edge(pressured,&mut prev)->bool`（返回是否计一次）以便单测沿语义。

## 测试边界（诚实分层）

- **纯单元（TDD red→green，主战场）**：
  - **C1**：`snapshot()` 与各 `inc_*/set_*` 一致；**N 路并发 `fetch_add` 不丢计数**（spawn N task 各 +K，断言 = N·K）；`failover_leg` u8↔`FailoverLegView`(0→Tuic/1→Reality/255→None)；`format_metrics_snapshot` 含全字段。
  - **C2** `FakeIpPool::usage()`：空(0,0)；`alloc` 后(1,0)（alloc 不 acquire）；`acquire` 后(1,1)；`release` 归零(1,0)；`sweep` 回收后 total 降。
  - **C3** trait：`FailoverUpstream::failover_leg_u8()` 随 `state().set_leg()` 变(Tuic→0/Reality→1)；非 failover mock → `NO_FAILOVER`；`FailoverUpstream::udp_drops_up()` 转发 mock tuic 腿。
  - **C6 沿语义**：`note_pressure_edge` false→true 计一次、true→true 不计、true→false→true 再计一次。
- **C4 DNS 计数（单元/半集成）**：构 mock `TunIo` + `FakeIpPool` + `Metrics`，喂可解析 `:53` 查询 → `dns_forged==1`；喂截断查询 → `dns_dropped==1`。
- **harness 集成（复用 knife1 harness + 读 Arc<Metrics>）**：scenario 注入 `Arc<Metrics>`、跑 TCP/DNS 流、`.abort()` 后读快照（仿 [harness.rs:630] drain）断言 `dns_forged>0`、`relays_spawned>0`、`active_relays`/`fake_ip_*` 随负载>0。**UDP 下行 drop/背压**（C6）走真 quinn → harness mock echo 路径不触发 → 归 tuic 单元（沿 helper）+ acceptance。
- **真出口 acceptance（关键，见 plan T9，尽力而为如实记录）**：真隧道一段，`📊` 快照行随负载变化——`dns_forged` 随 DNS 查询↑、`active_relays`/`fake_ip_*` 随并发↑、`failover_leg` 随 cut/restore 翻 Tuic↔Reality、大流量下观察 `udp_drops_down`/`datagram_pressure_events`（难强制触发，如实记录）。复用 `scripts/knife9-failover-acceptance.sh`（两腿，验 leg 翻转）+ `scripts/knife35-acceptance.sh`（UDP 负载）。

## 风险 / 已知边界

- **单写者不变量**：`socket_ctxs`/`fake_pool` 无锁单 task。gauge **只能在 loop task 重算后发布原子**，**严禁** `Arc<Mutex>` 包它们给别的 task 读（会把锁压进每包/每 relay 热路径）。
- **send_udp 铁律**：leg 采样是独立周期 Relaxed 读，不进 UDP 数据路径；`FailoverUpstream::send_udp` 仍无条件转发 tuic（D4 不破铁律）。
- **热路径开销**：`fetch_add(Relaxed)` 与既有 `udp_drops` 同点同法，可忽略；O(n) gauge 扫描严格限 30s tick；`snapshot()` 全 `load`、勿进热路径。
- **NODATA 语义**：AAAA/other 返回 Some(NODATA) → `Some=forge` 约定下计入 `dns_forged`（与 seed §3 一致）。若前端要区分「真 fake-IP 分配 vs NODATA」需另加 `dns_nodata`——**defer**（会破 `forge_dns_reply` 纯性或需改返回枚举）。
- **两 interval 漂移**：run_event_loop metrics_tick 与 start_udp stats 各自 30s、不同 task → 触发时刻略漂；advisory 指标 benign（稳定优先）。
- **REALITY-only 模式无 UDP-path 行**：纯 REALITY 无 start_udp → 只有 run_event_loop 的统一快照行（这正是 D5 让 run_event_loop 发的原因）；`udp_drops_down`/`pressure_events` 恒 0（无 UDP）。
- **ADR**：「第二 metrics 接缝（Arc<Metrics> 与 MetricsSink 并存）+ counter/published-gauge 拆分 + 背压上升沿」是未来读者会问「为何不直接扩 MetricsSink」的非显然决策 → `docs/adr/0012-data-plane-observability-metrics.md`（T0 落库）。
