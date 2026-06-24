use crate::device::{TunIo, VirtualTunDevice};
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::{
    AssocTable, FragReassembler, TuicClientConfig, TuicUpstream, decode_packet_meta, encode_packet,
};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use crate::reality_upstream::RealityUpstream;
use crate::failover::FailoverUpstream;
use crate::udp_relay::{
    FourTuple, UDP_FLOW_IDLE_SECS, UdpInbound, build_udp_ip_packet, parse_inbound_udp,
};
use crate::dns::{self, Answer};
use crate::fake_ip::FakeIpPool;
use std::net::Ipv4Addr;
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer as TcpSocketBuffer, State as TcpState};
use smoltcp::wire::{IpAddress, IpCidr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

pub(crate) const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const RELAY_CHANNEL_CAPACITY: usize = 1024;
/// L2（刀9 F4）：一条 relay 双向静默多久判 idle → 退出 + shutdown。防慢/卡死上游（尤其 REALITY
/// TCP-only 手写 TLS 遇 server 不返回）长期挂住 relay task 泄漏。90s 偏宽松保稳（长轮询/SSE 不误杀）。
const RELAY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// 主循环分段插桩接缝（knife1：并发压测定位瓶颈）。
///
/// 中文要点：生产传 [`NoopSink`]（空方法，单态化内联后**零开销**，热路径无 `Instant::now()`）；
/// 并发压测 harness 传 RecordingSink，在每个回调里采集每段耗时/调用次数。计时逻辑全部留在 sink
/// 实现内，主循环只做平凡方法调用——生产与测试**同一份循环**。
pub trait MetricsSink {
    /// 进入 smoltcp poll 段（poll → flush_tx）。
    fn enter_poll(&mut self) {}
    /// 离开 poll 段。
    fn leave_poll(&mut self) {}
    /// 进入 relay 调度段（`all_handles()` 全量遍历 + `process_listener_activity`）。
    fn enter_relay(&mut self) {}
    /// 离开 relay 调度段。
    fn leave_relay(&mut self) {}
    /// 记录本 tick relay 段遍历的 listener handle 数（量化怀疑瓶颈 #1：O(n) 全量遍历）。
    fn note_listeners(&mut self, _n: usize) {}
}

/// 生产用空插桩：所有回调空实现，单态化后零开销。
pub struct NoopSink;
impl MetricsSink for NoopSink {}
// 中文要点：Stage 9 起按"每端口"配 pool，64 端口 * 2 槽 * 2 缓冲 ≈ 16MB。
const DEFAULT_TUN_POOL_SIZE: usize = 2;

/// One listener socket's binding parameters.
/// 中文要点：Stage 9 起 pool_size 已上移到 `ListenerRegistry`，这里只剩端口。
#[derive(Debug, Clone, Copy)]
struct ListenerSpec {
    /// Local TCP port intercepted on the TUN-side smoltcp stack.
    local_port: u16,
}

/// Explicit lifecycle state for one listener slot.
/// 中文要点：每个 handle 都要有自己的状态，避免“一个槽位出错、全局都混乱”。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SocketState {
    /// Ready to accept a new intercepted local TCP session.
    Listening,
    /// Opening the remote Yamux substream for the first local payload.
    OpeningRemote,
    /// Local and remote sides are actively relaying payloads.
    Relaying,
    /// The current slot is closing after EOF or transport failure.
    Closing,
    /// The slot is re-entering the listening state after cleanup.
    Rearming,
}

/// Per-handle runtime context owned by a single listener slot.
/// 中文要点：这是“房间上下文”，每个 handle 都有一份，专门存本槽位的状态和上行通道。
#[derive(Debug)]
struct SocketCtx {
    /// The local port that must be re-listened after this slot is rearmed.
    local_port: u16,
    /// Current lifecycle state for this listener slot.
    state: SocketState,
    /// Sender used to push local payloads into the remote relay task for this slot only.
    uplink_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// Downlink bytes not yet accepted by the smoltcp tx buffer.
    /// 中文要点：smoltcp send_slice 可能只写一部分（tx buffer 受 TCP ACK 释放制约），
    /// 写不下的字节必须留在这里、由后续 poll 持续 flush，否则丢字节 → TLS bad decrypt。
    downlink_pending: Vec<u8>,
    /// 本槽位当前 flow 占用的 fake-IP（若 target 经 fake-IP 改写）。
    /// 中文要点：刀2 引用计数——首次开远端时 `acquire`、rearm 时 `release`，
    /// 保证该 fake-IP 映射在本 flow 存活期间不被 sweep 回收（否则 resolve 失败 → 断连）。
    fake_ip: Option<Ipv4Addr>,
}

impl SocketCtx {
    /// Create the initial per-slot runtime context.
    /// 中文要点：每个新建的监听槽位一开始都处于 Listening，没有绑定上行通道。
    /// Target 不再预置——它在首包到达时从被拦截连接的 `local_endpoint()` 提取。
    fn new(local_port: u16) -> Self {
        Self {
            local_port,
            state: SocketState::Listening,
            uplink_tx: None,
            downlink_pending: Vec::new(),
            fake_ip: None,
        }
    }
}

/// Push as much of the handle's downlink backlog into the smoltcp tx buffer as fits;
/// keep the rest for the next poll. Partial `send_slice` writes are normal.
/// 中文要点：这是修 bad decrypt 的关键——绝不丢弃写不下的字节。
fn flush_downlink(tcp_socket: &mut TcpSocket, ctx: &mut SocketCtx) {
    if ctx.downlink_pending.is_empty() || !tcp_socket.can_send() {
        return;
    }
    match tcp_socket.send_slice(&ctx.downlink_pending) {
        Ok(0) => {}
        Ok(n) => {
            ctx.downlink_pending.drain(..n);
        }
        Err(_) => {
            // socket 不可发（已关闭/复位）：丢弃残留，避免无限堆积。
            ctx.downlink_pending.clear();
        }
    }
}

/// Hard cap on the number of distinct destination ports we will intercept.
/// 中文要点：防止 SYN flood 下 socket / 缓冲区无限增长，到顶就拒新端口。
const MAX_INTERCEPTED_PORTS: usize = 64;

/// 全局 listener socket 总数上限（#2 弹性扩容的兜底）。
/// 中文要点：放开了「每端口 pool_size 固定上限」后，仍需一个全局闸防 SYN flood 把内存撑爆。
/// 4096 槽 × 128KB ≈ 512MB 上界；实际按需扩容远小于此。
const MAX_TOTAL_LISTENERS: usize = 4096;

/// SYN 命中时为该端口保证的空闲 listening 槽数（#2 弹性扩容触发阈值）。
/// 中文要点：每个新 SYN 到来前确保该端口恒有 ≥2 个空闲 listening 槽，吸收突发并发，
/// 避免「所有槽都 Relaying 时新 SYN 无 socket 可握手 → SYN 退避重传 stall」。
const MIN_SPARE_LISTENERS: usize = 2;

/// fake-IP 映射回收 TTL（秒）：idle 且 refcount==0 超此时长才回收。
/// 中文要点：远大于 DNS A 记录 TTL（5s）。review #4：取 30min（而非 300s），给「应用已解析并缓存
/// fake-IP、但尚未发起连接」留足窗口——这段 refcount==0，过早回收会让后续用缓存 IP 的连接被 Refuse。
/// 活跃 flow（refcount>0）任何情况都不回收（见 FakeIpPool::sweep）。
const FAKE_IP_TTL: u64 = 1800;

/// Failure mode for `ListenerRegistry::ensure_port`.
/// 中文要点：到顶时优雅拒绝，不能 panic，已注册端口的 socket 不受影响。
#[derive(Debug, PartialEq, Eq)]
enum RegistryError {
    Capped,
}

/// Dynamic per-port listener pools.
/// 中文要点：一个目的端口对应一组（`pool_size` 个）smoltcp 监听 socket，
/// 由 SYN inspector 按需创建；主循环遍历所有 handle 处理首包。
#[derive(Debug)]
struct ListenerRegistry {
    ports: HashMap<u16, Vec<SocketHandle>>,
    pool_size: usize,
    /// 全局 listener socket 总数上限（#2 弹性扩容兜底；默认 [`MAX_TOTAL_LISTENERS`]）。
    max_total: usize,
    /// 当前 listener socket 总数（review #6：O(1) 计数器，避免每 SYN 在扩容循环里 O(端口) 求和）。
    /// 中文要点：槽只增不删（rearm/reap 复用回 Listen，从不从 ports 移除），故计数器随建槽单调累加。
    total: usize,
}

impl ListenerRegistry {
    fn new(pool_size: usize) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            max_total: MAX_TOTAL_LISTENERS,
            total: 0,
        }
    }

    #[cfg(test)]
    fn with_max_total(pool_size: usize, max_total: usize) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            max_total,
            total: 0,
        }
    }

    /// 当前所有端口的 listener socket 总数（O(1)，维护计数器）。
    fn total_handles(&self) -> usize {
        self.total
    }

    #[cfg(test)]
    fn port_count(&self) -> usize {
        self.ports.len()
    }

    /// Idempotently ensure a listener pool exists for `port`.
    /// 中文要点：端口在册时不重复建；首次建则一次性创建 `pool_size` 个监听槽位
    /// 并同步登记 `SocketCtx`，到顶返回 `Capped`。
    fn ensure_port(
        &mut self,
        port: u16,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if self.ports.contains_key(&port) {
            return Ok(());
        }
        if self.ports.len() >= MAX_INTERCEPTED_PORTS {
            return Err(RegistryError::Capped);
        }
        let spec = ListenerSpec { local_port: port };
        let mut handles = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            let h = sockets.add(build_listener_socket(&spec));
            socket_ctxs.insert(h, SocketCtx::new(port));
            handles.push(h);
        }
        self.total += self.pool_size;
        println!(
            "🆕 listener pool created for port {port} (pool_size={})",
            self.pool_size
        );
        self.ports.insert(port, handles);
        Ok(())
    }

    /// Iterate every smoltcp handle across all currently intercepted ports.
    /// 中文要点：脏集合驱动后热路径不再用它；仅低频「死槽回收」(reap_dead_slots) + 测试遍历。
    fn all_handles(&self) -> impl Iterator<Item = SocketHandle> + '_ {
        self.ports.values().flatten().copied()
    }

    /// All listener handles registered for `port`（未注册端口返回空 slice）。
    /// 中文要点：#1 脏集合驱动——按 inbound 包的 dst_port 取该端口 pool 全部 handle 标脏，
    /// 替代每 tick 全量 `all_handles()` 遍历。
    fn handles_for_port(&self, port: u16) -> &[SocketHandle] {
        self.ports.get(&port).map(Vec::as_slice).unwrap_or(&[])
    }

    /// #2 弹性扩容：保证 `port` 当前至少有 `min_spare` 个 Listening 槽，不足则按需补建。
    ///
    /// 中文要点：这是放开「每端口 pool_size 固定上限」的核心——热门端口（如 :443）突发时，
    /// 已有槽都进了 Relaying，新 SYN 没有 listening socket 可握手就会 stall。每个 SYN 到来前
    /// 补足空闲槽即可吸收突发。rearm 回 Listening 的旧槽计入空闲、优先复用，不无限增长；
    /// 全局 `max_total` 兜底防 SYN flood，到顶返回 `Capped`（退回旧行为，不 panic）。
    /// 未注册端口（无 SYN 命中过）→ no-op，建池仍由 `ensure_port` 负责。
    fn ensure_spare_listeners(
        &mut self,
        port: u16,
        min_spare: usize,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if !self.ports.contains_key(&port) {
            return Ok(());
        }
        // 「空闲」必须看 smoltcp socket 的真实状态（accept 后立即离开 Listen），不能看
        // SocketCtx.state——后者直到 process 首包才更新，SYN 刚被 accept 时仍是 Listening，
        // 计数虚高会导致永不补建（实测单端口仍 stall）。
        let listening = self.ports[&port]
            .iter()
            .filter(|h| sockets.get::<TcpSocket>(**h).state() == TcpState::Listen)
            .count();
        if listening >= min_spare {
            return Ok(());
        }
        let spec = ListenerSpec { local_port: port };
        for _ in listening..min_spare {
            if self.total_handles() >= self.max_total {
                return Err(RegistryError::Capped);
            }
            let h = sockets.add(build_listener_socket(&spec));
            socket_ctxs.insert(h, SocketCtx::new(port));
            self.ports.get_mut(&port).unwrap().push(h);
            self.total += 1;
        }
        Ok(())
    }
}

/// Local listener-side startup configuration for the TUN runtime.
/// 中文要点：这一层只关心本地拦截面，不关心怎么连上游 TLS/Yamux 服务。
#[derive(Debug, Clone)]
pub struct TunListenerConfig {
    /// Per-port pool size: number of smoltcp listener slots created for each
    /// intercepted destination port.
    /// 中文要点：Stage 9 起 pool 按"每端口"算，决定单个端口能并发承接多少条连接。
    pub pool_size: usize,
}

impl TunListenerConfig {
    /// Build listener config from optional string sources.
    /// 中文要点：Stage 9 起本地不再固定监听端口，端口由 SYN inspector 按需注册，
    /// 这里只保留 `pool_size` 一个旋钮。
    fn from_sources(pool_size: Option<&str>) -> Result<Self, ClientError> {
        let pool_size = match pool_size {
            Some(value) => value
                .parse::<usize>()
                .map_err(|_| ClientError::InvalidTarget(format!("invalid pool size: {value}")))?,
            None => DEFAULT_TUN_POOL_SIZE,
        };

        if pool_size == 0 {
            return Err(ClientError::InvalidTarget(
                "invalid pool size: must be at least 1".to_string(),
            ));
        }

        Ok(Self { pool_size })
    }
}

/// Startup configuration for the TUN runtime.
/// 中文要点：Stage 13d 退役 legacy 上游后，运行时只剩本地监听池配置；
/// TUIC 出口配置走 `MINI_VPN_TUIC_*`（见 tuic.rs），不在这里。
#[derive(Debug, Clone)]
pub struct TunRuntimeConfig {
    pub listener: TunListenerConfig,
}

impl TunRuntimeConfig {
    /// Build config from optional string sources.
    pub fn from_sources(pool_size: Option<&str>) -> Result<Self, ClientError> {
        Ok(Self {
            listener: TunListenerConfig::from_sources(pool_size)?,
        })
    }

    /// Read config from process environment（`MINI_VPN_TUN_POOL_SIZE`）。
    fn from_env() -> Result<Self, ClientError> {
        Self::from_sources(std::env::var("MINI_VPN_TUN_POOL_SIZE").ok().as_deref())
    }
}

/// 生产入口：建真 utun + 真 TUIC 上游，然后跑共享的 [`run_event_loop`]。
///
/// 中文要点：knife1 起把主循环抽成 `run_event_loop`（泛型 over [`TunIo`] 设备 + [`MetricsSink`]），
/// 生产与并发压测 harness **跑同一份循环代码**。本薄壳只负责构造真依赖；循环逻辑零回归。
pub async fn start_tun_proxy() {
    let runtime_config = match TunRuntimeConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            println!("加载 TUN 运行时配置失败: {e}");
            return;
        }
    };
    println!(
        "🚀 TUN runtime started with pool_size={}",
        runtime_config.listener.pool_size
    );

    // 1. 初始化 TUN 设备 / 创建操作系统的原生异步虚拟网卡。
    let raw_tun = match create_tun_device().await {
        Ok(device) => device,
        Err(e) => {
            println!("无法创建 TUN 设备: {e}");
            return;
        }
    };
    let device = VirtualTunDevice::new(raw_tun);

    // 2. 选上游 Transport：MINI_VPN_UPSTREAM=tuic（默认）| reality（VLESS over REALITY over TCP，刀8）。
    //    两分支各自单态化 run_event_loop（device 只在选中的分支被 move，互斥）。
    match select_upstream_kind(std::env::var("MINI_VPN_UPSTREAM").ok().as_deref()) {
        UpstreamKind::Tuic => {
            let cfg = match TuicClientConfig::from_env() {
                Ok(c) => c,
                Err(e) => {
                    println!("加载 TUIC 客户端配置失败（启动中止）: {e}");
                    return;
                }
            };
            let upstream = match TuicUpstream::connect(&cfg).await {
                Ok(u) => {
                    println!("✅ 已连接 TUIC 出口 {} (sing-box)", cfg.server);
                    Arc::new(u)
                }
                Err(e) => {
                    println!("连接 TUIC 出口失败（启动中止）: {e:?}");
                    return;
                }
            };
            // 3. UDP over TUIC Packet 下行接收端（断线自愈，见 13b/13c）。
            println!("🌊 UDP relay 数据面就绪（TUIC Packet datagram → sing-box）");
            let tuic_downlink_rx = upstream.start_udp();
            // 4. 进入共享主循环（生产传 NoopSink：零插桩开销）。
            run_event_loop(device, upstream, tuic_downlink_rx, runtime_config, NoopSink).await;
        }
        UpstreamKind::Reality => {
            // REALITY 是 **TCP-only**（force-reality）：UDP no-op（DatagramUpstream 静默丢）+
            // **空 downlink channel**——持有 tx 永不 send → run_event_loop 的下行 select 分支永久 pending
            // （REALITY 无 UDP 下行；分离上游/UDP-over-VLESS/failover 留刀9）。
            let upstream = match RealityUpstream::from_env() {
                Ok(u) => {
                    println!("✅ 已配置 REALITY 出口（VLESS over REALITY over TCP；TCP-only，UDP no-op）");
                    Arc::new(u)
                }
                Err(e) => {
                    println!("加载 REALITY 客户端配置失败（启动中止）: {e:?}");
                    return;
                }
            };
            let (_dummy_tx, dummy_rx) = mpsc::channel::<Vec<u8>>(1); // 持 tx 不 drop → 下行分支永挂
            run_event_loop(device, upstream, dummy_rx, runtime_config, NoopSink).await;
        }
        UpstreamKind::Failover => {
            // 刀9：健康感知 TUIC↔REALITY。两腿都建——TUIC 既承 TCP relay 又是 **UDP 唯一出口**，
            // REALITY 是 TCP-only 备路。`FailoverUpstream::open_tcp` 按健康态选腿；`send_udp` 恒走 tuic。
            let tuic_cfg = match TuicClientConfig::from_env() {
                Ok(c) => c,
                Err(e) => {
                    println!("加载 TUIC 客户端配置失败（failover 启动中止）: {e}");
                    return;
                }
            };
            let tuic = match TuicUpstream::connect(&tuic_cfg).await {
                Ok(u) => {
                    println!("✅ 已连接 TUIC 出口 {} (failover 主腿)", tuic_cfg.server);
                    Arc::new(u)
                }
                Err(e) => {
                    println!("连接 TUIC 出口失败（failover 启动中止）: {e:?}");
                    return;
                }
            };
            // UDP 下行接收端来源端 = TUIC（独立于 TCP 选腿；UDP 永久绑 TUIC）。
            println!("🌊 UDP relay 数据面就绪（TUIC Packet datagram → sing-box；UDP 永留 QUIC）");
            let tuic_downlink_rx = tuic.start_udp();
            let reality = match RealityUpstream::from_env() {
                Ok(u) => {
                    println!("✅ 已配置 REALITY 出口（failover 备腿；VLESS over REALITY over TCP）");
                    Arc::new(u)
                }
                Err(e) => {
                    println!("加载 REALITY 客户端配置失败（failover 需两腿都配齐，启动中止）: {e:?}");
                    return;
                }
            };
            let upstream = Arc::new(FailoverUpstream::new(tuic, reality));
            println!("🔀 failover 就绪：TCP relay 健康感知 TUIC↔REALITY，UDP 恒走 TUIC");
            run_event_loop(device, upstream, tuic_downlink_rx, runtime_config, NoopSink).await;
        }
    }
}

/// 选哪个上游 Transport（纯函数，便于单测）。默认 + 未知值 → TUIC（零回归）。
/// 刀9：新增 `failover`（健康感知 TUIC↔REALITY，需两腿都配齐）。`tuic`/`reality` 仍作强制单腿调试旁路。
/// **failover 设为 opt-in**（非默认）：默认/未设保持纯 TUIC，对既有 TUIC-only 部署零回归（稳定优先）。
#[derive(Debug, PartialEq, Eq)]
enum UpstreamKind {
    Tuic,
    Reality,
    Failover,
}

fn select_upstream_kind(env: Option<&str>) -> UpstreamKind {
    match env.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("reality") => UpstreamKind::Reality,
        Some("failover") => UpstreamKind::Failover,
        _ => UpstreamKind::Tuic,
    }
}

/// 共享主循环：生产（真 utun + 真 TUIC）与并发压测 harness（内存回环 device + mock 上游）共用。
///
/// 中文要点：泛型 over [`TunIo`]（设备接缝）与 [`MetricsSink`]（分段插桩）。所有 smoltcp 装置
/// （sockets / iface / registry / fake DNS / fake_pool / assoc_table）在此内部构造，与生产逐字一致，
/// 使 harness 也忠实地走同一套 SYN inspector / DNS / relay 调度路径。
pub async fn run_event_loop<D, U, M>(
    mut device: D,
    upstream: Arc<U>,
    mut tuic_downlink_rx: mpsc::Receiver<Vec<u8>>,
    runtime_config: TunRuntimeConfig,
    mut metrics: M,
) where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream,
    M: MetricsSink,
{
    let pool_size = runtime_config.listener.pool_size;

    // 全局回信通道（TCP relay 通用回程）：接收端 global_rx 留在主循环，发送端 global_tx 克隆给每个后台车厢。
    let (global_tx, mut global_rx) =
        tokio::sync::mpsc::channel::<(SocketHandle, Vec<u8>)>(RELAY_CHANNEL_CAPACITY);

    // =========== 初始化 smoltcp 酒店和路由器 ===========
    let mut sockets = SocketSet::new(vec![]);

    // Stage 9: 监听端口不再固定，由 SYN inspector 在 rx 热路径按需注册。
    // 中文要点：启动时 registry 是空的；第一条到任意端口的 SYN 会触发该端口建池。
    let mut registry = ListenerRegistry::new(pool_size);
    let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();

    // #1 脏集合驱动：只有「本 tick 有活动」的 listener handle 进集合，relay 段仅处理它们，
    // 替代每 tick O(总槽) 全量 sweep。入集 = inbound TCP 包标脏其端口 pool / 回程残留下行 pending；
    // 出集 = 处理后无 pending 且不再 can_recv（见 process_dirty_relay）。
    let mut dirty: HashSet<SocketHandle> = HashSet::new();

    // 刀5: fake-IP DNS 不再用 smoltcp socket。任意 resolver 的明文 :53 在 rx 热路径被
    // classify_inbound 判 Dns → handle_dns_hijack 裸包伪造 fake-IP 回包（绕过 smoltcp，
    // 源 = 被查询的 resolver，故不依赖系统 DNS 指向 198.18.0.1，见 ADR-0007）。
    let mut fake_pool = FakeIpPool::new();

    // 3. 初始化 smoltcp 的“虚拟路由器”
    let config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
    // 这里传入了包装好的 &mut device
    let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::now());

    // 给虚拟路由器配置 IP 地址 (10.0.0.1/24)
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
        // 刀5：不再把 198.18.0.1/32 配成接口 IP——DNS 回包改由 handle_dns_hijack 裸包注入
        // （src = 被查询的 resolver），不经 smoltcp 选源，故无需本接口持有 resolver 地址。
    });

    // AnyIP：接收目的 IP 不是本接口自身地址的包（即被拦截连接真正想去的 Target）。
    // 中文要点：默认路由的网关填本接口自己的 IP 10.0.0.2 是 smoltcp AnyIP 接收判定的
    // 硬性要求（routes.lookup(dst) 必须命中一个本接口 IP 才放行），不是笔误。
    iface.set_any_ip(true);
    iface
        .routes_mut()
        .add_default_ipv4_route(smoltcp::wire::Ipv4Address::new(10, 0, 0, 2))
        .unwrap();

    // 初始化定时器 (每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));

    // Stage 13b：UDP over TUIC Packet。AssocTable 主循环独占；upstream 与下行 rx 由调用方注入
    // （生产=真 TuicUpstream::start_udp()；harness=mock echo 回环）。
    let mut assoc_table = AssocTable::new();
    // 刀3：native 下行分片重组器（主循环独占、无锁，与 AssocTable 同寿）。
    let mut reassembler = FragReassembler::new();
    let udp_clock = std::time::Instant::now();
    let mut udp_sweep = tokio::time::interval(std::time::Duration::from_secs(1));
    // review #7：fake-IP 池回收单独走低频 tick（TTL=300s，无需每秒全表扫）。
    let mut fake_ip_sweep = tokio::time::interval(std::time::Duration::from_secs(60));

    loop {
        tokio::select! {
            // TCP relay 回程：后台车厢把远端回传字节送回主循环 → 注入对应 smoltcp socket。
            //   TUIC 自重连（live_conn），不需要 legacy 的 disconnect/复位分支。
            Some((handle, payload)) = global_rx.recv() =>{
                println!("📬 从大邮筒收到 {} 字节数据，准备送往房间 {:?}", payload.len(), handle);
                if let Err(e) = handle_remote_payload(
                    handle,
                    payload,
                    &mut sockets,
                    &mut socket_ctxs,
                    &mut iface,
                    &mut device,
                    &mut fake_pool,
                    udp_clock.elapsed().as_secs(),
                )
                .await
                {
                    println!("处理回程数据失败: {e}");
                }
                // #1：回程写不下的下行字节留在 downlink_pending（tx buffer 满）→ 标脏，
                // 让 relay 段后续 tick 持续 flush，直到排空才出集（绝不丢字节）。
                if socket_ctxs
                    .get(&handle)
                    .map(|c| !c.downlink_pending.is_empty())
                    .unwrap_or(false)
                {
                    dirty.insert(handle);
                }
            }
            // 分支 1: 全局回信通道接收到了新数据包
            // 分支 1: 物理网卡接收到了新数据包
            res = device.wait_for_rx() =>{
                if res.is_ok(){
                    // rx 分流（stage-12 D1 + 刀5）：任意 :53 → 裸包 DNS 劫持；其它 UDP → 裸 relay；
                    // 非 UDP → 既有 smoltcp 路径。前两类 take 走、不进 iface.poll。
                    let class = device.rx_peek().map(classify_inbound);
                    if class == Some(Inbound::UdpRelay) {
                        if let Some(pkt) = device.rx_take() {
                            // Stage 13b: UDP → 编码 TUIC Packet → send_udp。
                            handle_tuic_udp_uplink(
                                &pkt,
                                &mut assoc_table,
                                &mut fake_pool,
                                &*upstream,
                                udp_clock.elapsed().as_secs(),
                            )
                            .await;
                        }
                    } else if class == Some(Inbound::Dns) {
                        // 刀5：任意 resolver 的明文 :53 → 裸包伪造 fake-IP 回包（绕过 smoltcp）。
                        if let Some(pkt) = device.rx_take() {
                            handle_dns_hijack(
                                &pkt,
                                &mut fake_pool,
                                &mut device,
                                udp_clock.elapsed().as_secs(),
                            )
                            .await;
                        }
                    } else {
                        // 1) SYN inspector + 脏集合标脏：在 iface.poll 之前看一眼包。
                        //    - 干净 SYN 去往新端口 → 立刻建监听池，smoltcp 同一帧就能 accept。
                        //    - 任意去往拦截端口的 TCP 包 → 把该端口 pool 标脏（#1），覆盖 SYN 之后
                        //      让 listener can_recv 的首个 data 包；relay 段只处理脏集合，不再全扫。
                        if let Some(buf) = device.rx_peek()
                            && let Some((port, is_clean_syn)) = inspect_inbound_tcp(buf)
                        {
                            if is_clean_syn {
                                if let Err(e) =
                                    registry.ensure_port(port, &mut sockets, &mut socket_ctxs)
                                {
                                    println!(
                                        "⚠️ intercepted port cap reached, drop SYN to port {port}: {:?}",
                                        e
                                    );
                                }
                                // #2 弹性扩容：SYN accept 前确保该端口有空闲 listening 槽吸收突发，
                                // 打掉「每端口 pool_size 固定上限」导致的热门端口 stall。全局 cap 兜底。
                                if let Err(e) = registry.ensure_spare_listeners(
                                    port,
                                    MIN_SPARE_LISTENERS,
                                    &mut sockets,
                                    &mut socket_ctxs,
                                ) {
                                    println!(
                                        "⚠️ global listener cap reached, 端口 {port} 无法弹性扩容: {:?}",
                                        e
                                    );
                                }
                            }
                            // 任意去往拦截端口的 TCP 包 → 标脏该端口 pool（覆盖 SYN 之后的首个 data 包）。
                            for &h in registry.handles_for_port(port) {
                                dirty.insert(h);
                            }
                        }

                        metrics.enter_poll();
                        let timestamp = smoltcp::time::Instant::now();
                        iface.poll(timestamp, &mut device, &mut sockets);
                        device.flush_tx().await.unwrap();
                        metrics.leave_poll();

                        process_dirty_relay(
                            &mut dirty,
                            &mut sockets,
                            &mut socket_ctxs,
                            &*upstream,
                            &global_tx,
                            &mut fake_pool,
                            udp_clock.elapsed().as_secs(),
                            &mut metrics,
                        )
                        .await;
                    }
                }
            }
            // Stage 13b/刀3: TUIC 下行（datagram 或 uni-stream）→ decode_packet_meta → 分片重组
            // → AssocTable 解路由 → 造回程 IP/UDP 注入 TUN。FRAG_TOTAL==1 直通；>1 集齐才注入。
            Some(dg) = tuic_downlink_rx.recv() => {
                if let Some(meta) = decode_packet_meta(&dg) {
                    let assoc_id = meta.assoc_id;
                    // 分片重组：单帧直通；多帧集齐返回整包，否则缓存等后续帧。
                    if let Some(payload) = reassembler.accept(&meta, udp_clock.elapsed().as_secs()) {
                        // 先取出路由信息(Copy),释放 assoc_table 借用后再 touch。
                        let routed = assoc_table
                            .resolve(assoc_id)
                            .map(|e| (e.target_src(), e.app_endpoint()));
                        if let Some((src, dst)) = routed {
                            let pkt = build_udp_ip_packet(src, dst, &payload);
                            device.inject_ip_packet(&pkt);
                            assoc_table.touch(assoc_id, udp_clock.elapsed().as_secs());
                            if let Err(e) = device.flush_tx().await {
                                println!("UDP 下行 flush 失败: {e}");
                            }
                        } else {
                            // assoc 已回收/未知 → 丢弃该回程(应用会重发/重查,自愈)。
                            println!("🗑️ TUIC UDP↓ assoc={assoc_id} 无映射，丢弃 {}B", payload.len());
                        }
                    }
                }
            }
            // Stage 13b: 周期回收空闲 UDP assoc。刀2：同时回收 fake-IP 引用 + sweep fake-IP 池。
            _ = udp_sweep.tick() => {
                let now = udp_clock.elapsed().as_secs();
                // 被回收的 UDP assoc → release 其占用的 fake-IP（引用计数归零，进可回收候选）。
                for ip in assoc_table.sweep(now, UDP_FLOW_IDLE_SECS) {
                    fake_pool.release(ip, now);
                }
                // 刀3：回收未集齐且超时的下行分片包（丢片自愈，防内存泄漏）。
                reassembler.sweep(now, crate::tuic::FRAG_REASSEMBLY_TTL_SECS);
                // review #1/#2：回收已死/卡住的 TCP listener 槽（本地关闭/开远端失败的 teardown 缺口），
                // 释放其 fake-IP refcount 并让槽回 Listen 复用，防 refcount 泄漏 + 槽数涨到 Capped。
                reap_dead_slots(&registry, &mut sockets, &mut socket_ctxs, &mut fake_pool, now);
            }
            // review #7：低频回收 idle 且 refcount==0 超 TTL 的 fake-IP 映射（长稳防泄漏）。
            _ = fake_ip_sweep.tick() => {
                fake_pool.sweep(udp_clock.elapsed().as_secs(), FAKE_IP_TTL);
            }
            // 分支 2: 时钟滴答，处理超时重传等后台任务
            _ = timer.tick() =>{
                metrics.enter_poll();
                let timestamp = smoltcp::time::Instant::now();
                iface.poll(timestamp, &mut device, &mut sockets);
                device.flush_tx().await.unwrap();
                metrics.leave_poll();

                // #1：timer tick 无新 inbound 包，只续推进脏集合（主要是下行 pending flush +
                // smoltcp 超时重传释放 tx buffer 后继续写）。不再全量 sweep。
                process_dirty_relay(
                    &mut dirty,
                    &mut sockets,
                    &mut socket_ctxs,
                    &*upstream,
                    &global_tx,
                    &mut fake_pool,
                    udp_clock.elapsed().as_secs(),
                    &mut metrics,
                )
                .await;
            }
        }
    }
}

/// #1 脏集合驱动的 relay 调度段：只处理本 tick 标脏的 handle，替代每 tick 全量 `all_handles()`。
///
/// 中文要点：把 relay 段成本从 O(总 listener 槽数) 降到 O(活跃 handle)。处理完一个 handle 后，
/// 若它既无下行 pending、smoltcp 侧也不再 `can_recv`（首包已 drain、已开远端进 Relaying），
/// 就出脏集合——后续回程走 `global_rx` 分支，残留 pending 时会被重新标脏。仍有活就留在集合里下个 tick 续处理。
#[allow(clippy::too_many_arguments)]
async fn process_dirty_relay<U, M>(
    dirty: &mut HashSet<SocketHandle>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &U,
    global_tx: &mpsc::Sender<(SocketHandle, Vec<u8>)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
    metrics: &mut M,
) where
    U: ProxyUpstream,
    M: MetricsSink,
{
    metrics.enter_relay();
    // 快照后处理：边遍历边 `dirty.remove` 会与迭代借用冲突。dirty 规模 = O(活跃)，分配可忽略。
    let snapshot: Vec<SocketHandle> = dirty.iter().copied().collect();
    metrics.note_listeners(snapshot.len());
    for handle in snapshot {
        if let Err(e) = process_listener_activity(
            handle,
            sockets,
            socket_ctxs,
            upstream,
            global_tx,
            fake_pool,
            now_secs,
        )
        .await
        {
            println!("处理本地房间 {:?} 失败: {e}", handle);
        }
        let still_active = {
            let has_recv = sockets.get_mut::<TcpSocket>(handle).can_recv();
            let has_pending = socket_ctxs
                .get(&handle)
                .map(|c| !c.downlink_pending.is_empty())
                .unwrap_or(false);
            has_recv || has_pending
        };
        if !still_active {
            dirty.remove(&handle);
        }
    }
    metrics.leave_relay();
}

/// 回收「已用过但已死/卡住」的 listener 槽（review #1/#2 修复）：本地 FIN/RST 关闭、双向关闭完成、
/// 或开远端失败卡住的槽，热路径的 rearm 只在「远端 EOF / Refuse」触发，覆盖不到这些路径——
/// 不回收则 ① 它持有的 fake-IP refcount 永不归零 → 映射永不被 sweep 回收（泄漏）；
/// ② 槽停在非 Listen，`ensure_spare_listeners` 不断新建 → `total_handles` 涨到 `MAX_TOTAL_LISTENERS`
/// → Capped → #2 修好的热门端口 stall 又回来。低频（1s tick）调用，非每包热路径。
///
/// 死槽判定（仅对「被用过」的槽，即 `ctx.state != Listening`；空闲 Listen 槽 ctx.state==Listening 永不命中）：
/// - `!is_active()`：Closed / TimeWait（RST、双向关闭完成）；
/// - `CloseWait`：被拦截应用已发 FIN（主动关闭）→ teardown，绝不会再有上行；
/// - `OpeningRemote && uplink_tx.is_none()`：`open_tcp` 失败后状态卡在 OpeningRemote。
fn reap_dead_slots(
    registry: &ListenerRegistry,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> usize {
    let handles: Vec<SocketHandle> = registry.all_handles().collect();
    let mut reaped = 0;
    for h in handles {
        let dead = {
            let st = sockets.get::<TcpSocket>(h).state();
            let active = sockets.get::<TcpSocket>(h).is_active();
            match socket_ctxs.get(&h) {
                Some(ctx) if ctx.state != SocketState::Listening => {
                    !active
                        || st == TcpState::CloseWait
                        || (ctx.state == SocketState::OpeningRemote && ctx.uplink_tx.is_none())
                }
                _ => false,
            }
        };
        if dead {
            let sock = sockets.get_mut::<TcpSocket>(h);
            if let Some(ctx) = socket_ctxs.get_mut(&h) {
                rearm_socket(sock, ctx, fake_pool, now_secs);
                reaped += 1;
            }
        }
    }
    reaped
}

/// Allocate a fresh smoltcp TCP listener socket for one pool slot.
/// 中文要点：每次调用都创建一间独立房间，并立即挂上 listen 牌子。
fn build_listener_socket(spec: &ListenerSpec) -> TcpSocket<'static> {
    let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; TCP_SOCKET_BUFFER_SIZE]);
    let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; TCP_SOCKET_BUFFER_SIZE]);
    let mut tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
    tcp_socket.listen(spec.local_port).unwrap();
    tcp_socket
}

/// 解析入站 IPv4+TCP 包：返回 `(目的端口, 是否干净 SYN)`。一次解析同时供 SYN 建池与脏集合标脏。
/// 中文要点：review #5——原 `inspect_inbound_syn` + `inbound_tcp_dst_port` 对同一包解析两遍，
/// 每个入站 TCP 包在热路径白跑一次 etherparse。合并为一次解析：`is_clean_syn = syn && !ack`
/// 用于建池/扩容；端口对任意 TCP 包都返回，用于标脏（覆盖 SYN 之后让 listener can_recv 的首个 data 包）。
/// 非 IPv4 / 非 TCP / 解析失败 → None。
fn inspect_inbound_tcp(packet: &[u8]) -> Option<(u16, bool)> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    Some((tcp.destination_port, tcp.syn && !tcp.ack))
}

/// Convert a smoltcp endpoint into a relay Target.
/// 中文要点：TUN 链路上目的地址在 IP 层已是裸 IP（域名早被 DNS 解析掉），
/// 这里统一转成 `TargetAddr::IpPort`。当前 crate 只开 `proto-ipv4`，故必为 IPv4。
fn target_from_endpoint(endpoint: smoltcp::wire::IpEndpoint) -> TargetAddr {
    let ip = std::net::IpAddr::from(endpoint.addr);
    TargetAddr::IpPort(std::net::SocketAddr::new(ip, endpoint.port))
}

/// fake-IP target 改写结果。
enum TargetResolve {
    /// 正常转发：IpPort 直连（`fake_ip=None`），或 fake-IP 查回的 DomainPort（`fake_ip=Some`）。
    /// 中文要点：刀2 透出 fake_ip，供上层在首开远端时 `acquire`、rearm 时 `release`（引用计数回收）。
    Direct {
        target: TargetAddr,
        fake_ip: Option<Ipv4Addr>,
    },
    /// fake-IP 段内但查不到映射（如客户端重启丢表、应用用旧缓存 IP）：拒绝，让应用重查。
    Refuse,
    /// 加密 DNS 端点（DoT/DoQ :853、DoH/DoH3 :443 命中名单）：阻断，逼应用回落明文 DNS。
    /// 中文要点（刀4）：TCP 发 RST（rearm）、UDP 丢包；应用回落 :53 → 我方伪造 fake-IP → 进隧道。
    Block,
}

/// 把提取出的 endpoint 解析成 relay target。
/// 中文要点：fake-IP → 查表得域名 → DomainPort（出口解析、绕污染）；非 fake → IpPort
/// （Stage 8/9 行为不变）；fake 但无映射 → Refuse（拒绝连接）。
fn resolve_target(endpoint: smoltcp::wire::IpEndpoint, fake_pool: &FakeIpPool) -> TargetResolve {
    // 刀4/刀5：DNS 端口拦截(先于常规解析)。:853 = DoT/DoQ(任意 IP)→ Block；
    // :53 = TCP 明文 DNS(UDP :53 已被 classify 截到劫持路径、不到此)→ Block(RST 逼回落 UDP :53)。
    if crate::dns_block::is_dns_relay_port(endpoint.port) {
        return TargetResolve::Block;
    }
    let std::net::IpAddr::V4(v4) = std::net::IpAddr::from(endpoint.addr) else {
        return TargetResolve::Direct {
            target: target_from_endpoint(endpoint),
            fake_ip: None,
        };
    };
    if fake_pool.is_fake(v4) {
        match fake_pool.resolve(v4) {
            Some(domain) => {
                // 刀4：DoH/DoH3 经 fake-IP——:443 且域名命中 DoH 名单 → Block（不碰普通 :443）。
                if endpoint.port == 443 && crate::dns_block::is_doh_domain(&domain) {
                    return TargetResolve::Block;
                }
                // 不在此 println!——resolve_target 在每个 UDP 包/每条 TCP 首包都会走到，
                // 热路径同步 stdout 会拖垮大并发。flow 创建的可观测性放在服务端日志。
                TargetResolve::Direct {
                    target: TargetAddr::DomainPort {
                        host: domain,
                        port: endpoint.port,
                    },
                    fake_ip: Some(v4),
                }
            }
            None => TargetResolve::Refuse,
        }
    } else {
        // 刀4：DoH/DoH3 硬编 bootstrap IP——:443 且 IP 命中 DoH-IP 名单 → Block。
        if endpoint.port == 443 && crate::dns_block::is_doh_ip(v4) {
            return TargetResolve::Block;
        }
        TargetResolve::Direct {
            target: target_from_endpoint(endpoint),
            fake_ip: None,
        }
    }
}

/// 刀5：把发往**任意** resolver 的明文 DNS 查询，本地伪造成 fake-IP 回包（裸包构造）。
/// 中文要点：A 查询 → 分配 fake-IP 并回伪造 A 记录；AAAA/其它 → NODATA；不可解析 → `None`
/// （调用方丢弃，**绝不转发真 DNS**——转发即泄漏真实 IP，绕过 fake-IP）。回包源 = app 当初查询的
/// resolver（`udp.dst_ip:53`），目的 = app 原端点；否则 app 的 socket 认不出回包而丢弃。裸包能任意
/// 设 src（smoltcp 受限于本接口 IP、对无界 resolver 集合做不到，见 ADR-0007）。纯逻辑（只依赖
/// `UdpInbound` + `&mut FakeIpPool`），无 device/async，便于单测。
fn forge_dns_reply(udp: &UdpInbound<'_>, fake_pool: &mut FakeIpPool, now_secs: u64) -> Option<Vec<u8>> {
    let q = dns::parse_query(udp.payload)?;
    let resp = if q.qtype == dns::QTYPE_A {
        let ip = fake_pool.alloc(&q.qname, now_secs);
        println!("🪪 DNS {} (A) → fake-IP {}", q.qname, ip);
        dns::build_response(&q, Answer::A(ip, 5))
    } else {
        let kind = if q.qtype == dns::QTYPE_AAAA {
            "AAAA"
        } else {
            "other"
        };
        println!("🪪 DNS {} ({}) → NODATA", q.qname, kind);
        dns::build_response(&q, Answer::NoData)
    };
    // 回包：src = 被查询的 resolver:53，dst = app 原端点（src/dst 对调）。
    Some(build_udp_ip_packet(
        (udp.dst_ip, 53),
        (udp.src_ip, udp.src_port),
        &resp,
    ))
}

/// 刀5：rx 热路径的裸包 DNS 劫持薄壳——解析入站 :53 包 → `forge_dns_reply` → 注入回包到 TUN。
/// 中文要点：`forge_dns_reply` 返回 `None`（不可解析）→ 静默丢弃（app 重查自愈，绝不转发真 DNS）。
/// 与 UDP relay 下行注入同款（`inject_ip_packet` + `flush_tx`）；泛型 `D: TunIo` 使生产/harness 共用。
async fn handle_dns_hijack<D: TunIo>(
    pkt: &[u8],
    fake_pool: &mut FakeIpPool,
    device: &mut D,
    now_secs: u64,
) {
    let Some(udp) = parse_inbound_udp(pkt) else {
        return;
    };
    if let Some(reply) = forge_dns_reply(&udp, fake_pool, now_secs) {
        device.inject_ip_packet(&reply);
        if let Err(e) = device.flush_tx().await {
            println!("DNS 劫持回包 flush 失败: {e}");
        }
    }
}

/// Drain the currently available local payload from one listener slot.
/// 中文要点：这里只负责把 smoltcp 缓冲区里的数据取出来，不做任何异步外联动作。
fn extract_socket_payload(socket: &mut TcpSocket<'_>) -> Option<Vec<u8>> {
    if !socket.can_recv() {
        return None;
    }

    let mut payload = None;
    socket
        .recv(|data| {
            payload = Some(data.to_vec());
            (data.len(), ())
        })
        .unwrap();
    payload
}

/// Reset a slot back into the listening state after the current relay ends.
/// 中文要点：单个 handle 退房只影响自己，不能误清理其他房间的状态。
fn rearm_socket(
    socket: &mut TcpSocket<'_>,
    ctx: &mut SocketCtx,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) {
    ctx.state = SocketState::Closing;
    socket.abort();
    ctx.uplink_tx = None;
    ctx.downlink_pending.clear();
    // 刀2 引用计数：本 flow 占用的 fake-IP 释放（归零后该映射进入可回收候选，sweep 才回收）。
    if let Some(ip) = ctx.fake_ip.take() {
        fake_pool.release(ip, now_secs);
    }
    ctx.state = SocketState::Rearming;
    socket.listen(ctx.local_port).unwrap();
    ctx.state = SocketState::Listening;
    println!("♻️ handle slot rearmed on local port {}", ctx.local_port);
}

/// Process one listener slot after iface polling.
/// 中文要点：主循环只负责遍历 handle，真正的房间处理逻辑都收口在这里。
async fn process_listener_activity<U: ProxyUpstream>(
    handle: SocketHandle,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &U,
    global_tx: &mpsc::Sender<(SocketHandle, Vec<u8>)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> Result<(), ClientError> {
    // 每轮先推进该 handle 的下行 pending：TCP ACK 释放 tx buffer 空间后继续写，
    // 直到把上一轮没写完的回程字节全部交付，绝不丢字节（修 bad decrypt 的另一半）。
    {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        if let Some(ctx) = socket_ctxs.get_mut(&handle) {
            flush_downlink(tcp_socket, ctx);
        }
    }

    // 取首包的同时读 local_endpoint：它就是被拦截连接真正想去的目的 endpoint。
    // 中文要点：两者都需要 socket，合并在这一处借用里读出，避免二次借用。
    let extracted = {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        let payload = extract_socket_payload(tcp_socket);
        let endpoint = tcp_socket.local_endpoint();
        payload.map(|p| (p, endpoint))
    };

    // 只有"有首包"且"有 local_endpoint"才继续；否则跳过。
    let Some((payload, Some(endpoint))) = extracted else {
        return Ok(());
    };

    // Stage 11：fake-IP → 查表换域名（DomainPort）；非 fake → IpPort；fake 无映射 → 拒绝。
    let (target, fake_ip) = match resolve_target(endpoint, fake_pool) {
        TargetResolve::Direct { target, fake_ip } => (target, fake_ip),
        TargetResolve::Refuse => {
            println!(
                "🚫 fake-IP {} 无映射，拒绝连接（请重新解析）",
                endpoint.addr
            );
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                rearm_socket(tcp_socket, ctx, fake_pool, now_secs);
            }
            return Ok(());
        }
        // 刀4：加密 DNS（DoT :853 / DoH :443）→ RST（rearm），逼应用回落明文 DNS。
        TargetResolve::Block => {
            // 解析 fake-IP 回域名供日志（low-rate TCP block 路径，便于核对命中端点 / 调 DoH 名单）；
            // :853/DoH-IP（非 fake-IP）则显示 IP。
            let who = match std::net::IpAddr::from(endpoint.addr) {
                std::net::IpAddr::V4(v4) => {
                    fake_pool.resolve(v4).unwrap_or_else(|| endpoint.addr.to_string())
                }
                _ => endpoint.addr.to_string(),
            };
            println!("🛡️ 阻断加密 DNS {who} (@{}:{})（→ RST，逼回落明文 DNS）", endpoint.addr, endpoint.port);
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                rearm_socket(tcp_socket, ctx, fake_pool, now_secs);
            }
            return Ok(());
        }
    };

    handle_local_payload(
        handle,
        payload,
        Some(target),
        fake_ip,
        socket_ctxs,
        upstream,
        global_tx,
        fake_pool,
        now_secs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_local_payload<U: ProxyUpstream>(
    handle: SocketHandle,
    payload: Vec<u8>,
    target: Option<TargetAddr>,
    fake_ip: Option<Ipv4Addr>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &U,
    global_tx: &mpsc::Sender<(SocketHandle, Vec<u8>)>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> Result<(), ClientError> {
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(());
    };

    if let Some(tx) = ctx.uplink_tx.as_mut() {
        println!("🔄 handle {:?} entering {:?}", handle, ctx.state);
        tx.send(payload)
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "本地中继通道已关闭"))?;
        ctx.state = SocketState::Relaying;
        return Ok(());
    }

    // 首次开远端必须有提取出的 Target；理论上首包时连接已 Established，local_endpoint 不应为 None。
    // 中文要点：缺 Target 时记录并跳过，绝不 panic、绝不退回写死地址。
    let Some(target) = target else {
        println!("⚠️ handle {:?} 无 local_endpoint，跳过开远端", handle);
        return Ok(());
    };

    ctx.state = SocketState::OpeningRemote;
    println!(
        "🎯 handle {:?} extracted target {}",
        handle,
        target.to_wire_string()
    );
    println!("🔄 handle {:?} entering {:?}", handle, ctx.state);
    let stream = upstream.open_tcp(&target).await?;
    println!("🚪 handle {:?} remote session opened", handle);

    let (tx, rx) = tokio::sync::mpsc::channel(RELAY_CHANNEL_CAPACITY);
    tx.send(payload)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "新建中继通道写入失败"))?;
    ctx.uplink_tx = Some(tx);
    ctx.state = SocketState::Relaying;
    // 刀2 引用计数：首次开远端成功后才 acquire（开失败走 `?` 早返回，不会泄漏 refcount）。
    if let Some(ip) = fake_ip {
        fake_pool.acquire(ip, now_secs);
        ctx.fake_ip = Some(ip);
    }

    spawn_remote_relay(handle, stream, rx, global_tx.clone());
    Ok(())
}

/// 处理远端回信
#[allow(clippy::too_many_arguments)]
async fn handle_remote_payload<D: TunIo>(
    handle: SocketHandle,
    payload: Vec<u8>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    device: &mut D,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> std::io::Result<()> {
    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(());
    };

    if payload.is_empty() {
        println!("🔄 handle {:?} entering {:?}", handle, SocketState::Closing);
        rearm_socket(tcp_socket, ctx, fake_pool, now_secs);
        return Ok(());
    }

    // 防串话 / 防 panic（epoch guard 的轻量降级版）：若该 handle 已被重连流程复位回
    // Listening（uplink_tx 被清空），说明这是上一代上游连接的迟到回程数据，直接丢弃，
    // 绝不能往非 Established 的 socket 写（否则 send_slice 报错、旧版本会 unwrap panic）。
    if ctx.uplink_tx.is_none() {
        println!(
            "🗑️ handle {:?} 已复位，丢弃旧连接迟到回程 {} 字节",
            handle,
            payload.len()
        );
        return Ok(());
    }

    // 不直接 send_slice（会丢写不下的字节）：先入下行 pending，再尽量 flush；
    // 剩余字节由主循环每轮 poll 持续推进（TCP ACK 释放 buffer 后继续）。
    ctx.downlink_pending.extend_from_slice(&payload);
    flush_downlink(tcp_socket, ctx);
    ctx.state = SocketState::Relaying;

    let timestamp = smoltcp::time::Instant::now();
    iface.poll(timestamp, device, sockets);
    device.flush_tx().await
}

fn spawn_remote_relay(
    handle: SocketHandle,
    stream: RelayStream,
    rx: mpsc::Receiver<Vec<u8>>,
    back_tx: mpsc::Sender<(SocketHandle, Vec<u8>)>,
) {
    tokio::spawn(run_relay(handle, stream, rx, back_tx));
}

/// 一条 TCP relay 的双向泵（独立 task body；抽出便于 idle 超时单测）。
/// 中文要点：L2（刀9 F4）select 加 idle 超时分支——双向 `RELAY_IDLE_TIMEOUT` 无活动 → 退出 + shutdown。
/// 任一方向有活动（本地→上游 write 成功 / 上游→本地 read）即重置（每轮 select 重建 sleep，计「距上次活动」）。
/// 适用 TUIC/REALITY 两种 RelayStream，与连接级 failover 探测无关（那是连接级，这是单 relay 级）。
async fn run_relay(
    handle: SocketHandle,
    mut stream: RelayStream,
    mut rx: mpsc::Receiver<Vec<u8>>,
    back_tx: mpsc::Sender<(SocketHandle, Vec<u8>)>,
) {
    let mut buf = [0u8; 65_536];
    loop {
        tokio::select! {
            local_msg = rx.recv() => {
                match local_msg {
                    Some(payload) => {
                        match stream.write_all(&payload).await {
                            Ok(_) => {}
                            Err(e) => {
                                println!("写入上游流失败: {:?}", e);
                                break;
                            }
                        }
                    }
                    None => {
                        println!("本地房间 {:?} 已关闭通道", handle);
                        break;
                    }
                }
            }
            remote_msg = stream.read(&mut buf) => {
                match remote_msg {
                    Ok(0) => {
                        println!("远端服务器关闭了车厢 {:?}", handle);
                        if back_tx.send((handle, vec![])).await.is_err() {
                            break;
                        }
                        break;
                    }
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        if back_tx.send((handle, data)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        println!("读取 Yamux 流失败（从远端读取失败）: {:?}", e);
                        break;
                    }
                }
            }
            // L2：双向静默超时 → 主动清理（防泄漏）。有活动的 select 分支会重启 loop → 重建 sleep。
            _ = tokio::time::sleep(RELAY_IDLE_TIMEOUT) => {
                println!("⏱️ relay {:?} 双向静默 {}s，idle 超时关闭（L2）", handle, RELAY_IDLE_TIMEOUT.as_secs());
                break;
            }
        }
    }
    // M2：关流前 shutdown——驱动 poll_shutdown 排空应用层缓冲（RealityStream 的 write_pending 尾部密文）
    // + 底层发 FIN；否则 best-effort poll_write 残留的 TLS app-record 尾部密文随 drop 丢弃 → 对端 AEAD
    // 停在 record 中途 + 收到提前 FIN（bad-decrypt / 被 REALITY 当异常流量）。对 TUIC 也是干净 finish。
    let _ = stream.shutdown().await;
}

/// rx 热路径分流结果。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Inbound {
    /// 任意 resolver 的明文 UDP/53 —— 走裸包 DNS 劫持（本地伪造 fake-IP，绕过 smoltcp，见 ADR-0007）。
    Dns,
    /// 其它 UDP —— 走裸包 UDP relay（绕过 smoltcp）。
    UdpRelay,
    /// 非 UDP（TCP/ICMP…）—— 走既有 smoltcp 路径。
    Other,
}

/// 给一个入站裸 IP 包分类（见 stage-12 spec 的 D1 规则；刀5 起任意 :53 → Dns）。
/// **load-bearing 不变量**：UDP :53 在此被判 `Dns`、`rx_take` 走劫持路径，**永不进 iface.poll /
/// resolve_target**——这正是 `resolve_target` 的 `is_dns_relay_port`（port==53 Block）只命中 **TCP** :53
/// 的依据（见 ADR-0007 / `dns_block::is_dns_relay_port`）。改动此分支前务必同步那条 Block 语义。
fn classify_inbound(pkt: &[u8]) -> Inbound {
    match parse_inbound_udp(pkt) {
        // 刀5：任意 resolver 的明文 :53 → Dns（裸包伪造 fake-IP，不依赖系统 DNS 指向 198.18.0.1）。
        Some(udp) if udp.dst_port == 53 => Inbound::Dns,
        Some(_) => Inbound::UdpRelay,
        None => Inbound::Other,
    }
}

/// 处理一个被拦截的 UDP 上行包：解析 → fake-IP 改写 target → 铸 assoc-id →
/// 编码 TUIC Packet → `TuicUpstream::send_udp`。
/// 中文要点：fake 无映射 → 丢弃(短 TTL 自愈)；send_udp 自带丢弃计数(UDP 语义)。
async fn handle_tuic_udp_uplink<U: DatagramUpstream>(
    pkt: &[u8],
    assoc_table: &mut AssocTable,
    fake_pool: &mut FakeIpPool,
    upstream: &U,
    now_secs: u64,
) {
    let Some(udp) = parse_inbound_udp(pkt) else {
        return;
    };
    let dst_ep = smoltcp::wire::IpEndpoint::new(
        IpAddress::Ipv4(smoltcp::wire::Ipv4Address::from_bytes(&udp.dst_ip.octets())),
        udp.dst_port,
    );
    let (target, fake_ip) = match resolve_target(dst_ep, fake_pool) {
        TargetResolve::Direct { target, fake_ip } => (target, fake_ip),
        TargetResolve::Refuse => {
            println!("🚫 UDP fake-IP {} 无映射，丢弃（待应用重新解析）", udp.dst_ip);
            return;
        }
        // 刀4：加密 DNS（DoQ :853 / DoH3 :443）→ **静默丢包**，逼应用回落明文 DNS。
        // 中文要点：此处每个入站 UDP datagram 必经，**不在热路径 println!**（同 resolve_target 的纪律：
        // 同步 stdout 会拖垮大并发；DoQ/DoH3 被丢后 QUIC 会重传一串包 → 逐包打印即洪水）。丢弃即正确行为；
        // 需要可观测时另加计数器周期汇报（cheap follow-up，本刀从简）。
        TargetResolve::Block => return,
    };
    let tuple = FourTuple {
        src_ip: udp.src_ip,
        src_port: udp.src_port,
        dst_ip: udp.dst_ip,
        dst_port: udp.dst_port,
    };
    // 仅在「新 flow」时打日志（每流一次，不是每包），与 Stage 12 一致。
    let is_new = !assoc_table.contains(&tuple);
    let assoc_id = assoc_table.intern(tuple);
    assoc_table.touch(assoc_id, now_secs);
    if is_new {
        // 刀2 引用计数：UDP 新 flow 占用 fake-IP → 登记到 assoc + acquire，
        // 保证该映射在 flow 存活期间不被 fake_pool sweep 回收（回收会让回程 resolve 失败）。
        if let Some(ip) = fake_ip {
            assoc_table.set_fake_ip(assoc_id, ip);
            fake_pool.acquire(ip, now_secs);
        }
        println!(
            "🌊 TUIC UDP↑ new assoc={assoc_id} → {} (first {}B)",
            target.to_wire_string(),
            udp.payload.len()
        );
    }
    // intern 可能 LRU 驱逐旧 assoc → 立即 release 其占用的 fake-IP（引用计数平衡）。
    for ip in assoc_table.take_reclaimed_fake_ips() {
        fake_pool.release(ip, now_secs);
    }
    upstream.send_udp(encode_packet(assoc_id, &target, udp.payload)).await;
}

pub async fn create_tun_device() -> tun::Result<tun::AsyncDevice> {
    let mut config = tun::Configuration::default();

    config
        .address((10, 0, 0, 1)) // 网卡的 IP 地址
        .destination((10, 0, 0, 2)) // 🌟 新增：告诉 OS 水管另一头是谁！
        .netmask((255, 255, 255, 0)) // 子网掩码
        .up(); // 启动网卡

    #[cfg(target_os = "macos")]
    config.layer(tun::Layer::L3); // macOS 通常需要显式指定三层（IP层）

    // Create the async TUN device with an explicit error path.
    // 中文要点：这里不要 panic，启动失败应当以可观测的错误返回给上层。
    tun::create_as_async(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::iface::SocketSet;

    /// 刀8/刀9：上游选择器——reality→Reality、failover→Failover（大小写/空白不敏感）；
    /// 其余（含 tuic/缺省/未知）→ Tuic（零回归，failover opt-in）。
    #[test]
    fn upstream_kind_selector() {
        assert_eq!(select_upstream_kind(Some("reality")), UpstreamKind::Reality);
        assert_eq!(select_upstream_kind(Some("  REALITY ")), UpstreamKind::Reality);
        assert_eq!(select_upstream_kind(Some("failover")), UpstreamKind::Failover);
        assert_eq!(select_upstream_kind(Some(" Failover ")), UpstreamKind::Failover);
        assert_eq!(select_upstream_kind(Some("tuic")), UpstreamKind::Tuic);
        assert_eq!(select_upstream_kind(None), UpstreamKind::Tuic, "缺省 → TUIC（failover opt-in）");
        assert_eq!(select_upstream_kind(Some("bogus")), UpstreamKind::Tuic, "未知 → TUIC（零回归）");
    }

    // ---- 刀9 F4：relay idle 超时（L2）----
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};

    /// 一条永不产数据的 mock 上游流：read 恒 Pending、write/flush 即成、shutdown 记账。
    /// 用于驱动 run_relay 的 idle 超时分支（唯一能 fire 的分支）。
    struct IdleStream {
        shutdown_called: Arc<AtomicBool>,
    }
    impl tokio::io::AsyncRead for IdleStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending // 永不产数据/永不 EOF：只有 idle sleep 分支能完成
        }
    }
    impl tokio::io::AsyncWrite for IdleStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len())) // 上行 write 即成（活动 → 重置 idle）
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdown_called.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    fn mk_test_handle(sockets: &mut SocketSet<'static>) -> SocketHandle {
        sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }))
    }

    /// idle：双向 90s 无活动 → relay task 退出 + stream.shutdown 被调（L2）。
    #[tokio::test(start_paused = true)]
    async fn relay_idle_timeout_shuts_down_stream() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(IdleStream { shutdown_called: flag.clone() });
        let (_tx, rx) = mpsc::channel::<Vec<u8>>(8); // 持 _tx → rx 不关、永不收（无活动）
        let (back_tx, _back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, stream, rx, back_tx));

        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        assert!(!task.is_finished(), "89s < 90s idle 阈值，relay 不应退出");
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        task.await.unwrap();
        assert!(flag.load(Ordering::SeqCst), "idle 超时应退出并调用 stream.shutdown（L2）");
    }

    /// 活动重置：临近阈值前来一次上行活动 → idle 计时重置，不退出；再静默满 90s 才退出。
    #[tokio::test(start_paused = true)]
    async fn relay_activity_resets_idle_timer() {
        let mut sockets = SocketSet::new(vec![]);
        let handle = mk_test_handle(&mut sockets);
        let flag = Arc::new(AtomicBool::new(false));
        let stream: RelayStream = Box::new(IdleStream { shutdown_called: flag.clone() });
        let (tx, rx) = mpsc::channel::<Vec<u8>>(8);
        let (back_tx, _back_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_relay(handle, stream, rx, back_tx));

        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        tx.send(vec![1, 2, 3]).await.unwrap(); // 活动（上行 write）→ 重置 idle 计时
        tokio::task::yield_now().await; // 让 relay 消费该活动并重建 sleep
        tokio::time::advance(std::time::Duration::from_secs(89)).await;
        assert!(!task.is_finished(), "活动重置了 idle 计时，第二个 89s 窗口内不应退出");
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        task.await.unwrap();
        assert!(flag.load(Ordering::SeqCst), "重置后再满 90s 静默才退出");
    }

    #[test]
    fn target_from_endpoint_builds_ipv4_target() {
        let ep = smoltcp::wire::IpEndpoint::new(IpAddress::v4(93, 184, 216, 34), 80);
        let target = target_from_endpoint(ep);
        assert_eq!(target.to_wire_string(), "93.184.216.34:80");
    }

    /// MetricsSink 契约：自定义 sink 能覆写默认空实现，逐回调被调用、listener 计数透传。
    /// 中文要点：锁住 run_event_loop 的插桩接缝形状（生产 NoopSink 零开销、harness 可记录）。
    #[derive(Default)]
    struct CountingSink {
        poll_enters: usize,
        relay_enters: usize,
        last_listeners: usize,
    }
    impl MetricsSink for CountingSink {
        fn enter_poll(&mut self) {
            self.poll_enters += 1;
        }
        fn enter_relay(&mut self) {
            self.relay_enters += 1;
        }
        fn note_listeners(&mut self, n: usize) {
            self.last_listeners = n;
        }
    }

    #[test]
    fn metrics_sink_records_per_phase_calls() {
        let mut sink = CountingSink::default();
        // 模拟一个 tick：poll 段 + relay 段（遍历 7 个 listener）。
        sink.enter_poll();
        sink.leave_poll();
        sink.enter_relay();
        sink.note_listeners(7);
        sink.leave_relay();
        assert_eq!(sink.poll_enters, 1);
        assert_eq!(sink.relay_enters, 1);
        assert_eq!(sink.last_listeners, 7);
    }

    #[test]
    fn noop_sink_is_zero_state() {
        // NoopSink 全空实现：可被反复调用且无副作用（生产热路径零开销的依据）。
        let mut sink = NoopSink;
        sink.enter_poll();
        sink.leave_poll();
        sink.enter_relay();
        sink.note_listeners(1024);
        sink.leave_relay();
    }

    /// Build a minimal IPv4+TCP packet with the requested flags for SYN-inspector tests.
    fn build_ipv4_tcp(
        src: [u8; 4],
        dst: [u8; 4],
        src_port: u16,
        dst_port: u16,
        syn: bool,
        ack: bool,
    ) -> Vec<u8> {
        let b = etherparse::PacketBuilder::ipv4(src, dst, 64).tcp(src_port, dst_port, 0, 1024);
        let b = if syn { b.syn() } else { b };
        let b = if ack { b.ack(0) } else { b };
        let mut buf = Vec::new();
        let payload: [u8; 0] = [];
        b.write(&mut buf, &payload).unwrap();
        buf
    }

    #[test]
    fn inspect_inbound_tcp_flags_clean_syn() {
        // 干净 SYN → (端口, is_clean_syn=true)。
        let syn = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(inspect_inbound_tcp(&syn), Some((443, true)));
        // SYN-ACK → 端口仍返回（用于标脏），但非干净 SYN（不建池）。
        let synack = build_ipv4_tcp([1, 1, 1, 1], [10, 0, 0, 1], 443, 60000, true, true);
        assert_eq!(inspect_inbound_tcp(&synack), Some((60000, false)));
        // 纯 ACK / data 包 → 端口返回，非 SYN（首包数据让 listener can_recv 的那一刻）。
        let ack = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 80, false, true);
        assert_eq!(inspect_inbound_tcp(&ack), Some((80, false)));
    }

    #[test]
    fn inspect_inbound_tcp_rejects_non_tcp_and_garbage() {
        // 非 TCP（UDP）/ 垃圾 → None。
        assert_eq!(inspect_inbound_tcp(&udp_pkt([8, 8, 8, 8], 53)), None);
        assert_eq!(inspect_inbound_tcp(&[0u8; 4]), None);
    }

    #[test]
    fn registry_ensure_port_is_idempotent_and_capped() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);

        for i in 0..MAX_INTERCEPTED_PORTS as u16 {
            reg.ensure_port(i + 1, &mut sockets, &mut ctxs).unwrap();
        }
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
        // pool_size * port_count handles registered
        assert_eq!(ctxs.len(), 2 * MAX_INTERCEPTED_PORTS);

        // idempotent: re-adding an existing port does not grow the registry
        reg.ensure_port(1, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
        assert_eq!(ctxs.len(), 2 * MAX_INTERCEPTED_PORTS);

        // capped: a new port beyond the cap is rejected, existing state preserved
        let err = reg.ensure_port(9999, &mut sockets, &mut ctxs).unwrap_err();
        assert!(matches!(err, RegistryError::Capped));
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
    }

    /// #2 弹性扩容：端口 Listening 槽不足 min_spare 时按需补建，已够则幂等不动，
    /// 未注册端口 no-op；rearm 回 Listening 的槽计入空闲、可复用。
    #[test]
    fn ensure_spare_listeners_grows_and_is_idempotent() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 2);

        // 占满已有 2 槽（abort → 离开 Listen 状态，模拟被 accept 占用）→ 无空闲 listening。
        let hs: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        for h in &hs {
            sockets.get_mut::<TcpSocket>(*h).abort();
        }
        // 要求 ≥2 空闲 → 补建 2 个。
        reg.ensure_spare_listeners(443, 2, &mut sockets, &mut ctxs)
            .unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 4);
        // 幂等：已有 2 空闲，不再建。
        reg.ensure_spare_listeners(443, 2, &mut sockets, &mut ctxs)
            .unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 4);
        // 未注册端口 → no-op（建池仍由 ensure_port 负责）。
        reg.ensure_spare_listeners(8080, 2, &mut sockets, &mut ctxs)
            .unwrap();
        assert!(reg.handles_for_port(8080).is_empty());
    }

    /// #2 全局总槽上限：弹性扩容受 `max_total` 兜底，达上限返回 Capped、不再增长（防 SYN flood）。
    #[test]
    fn ensure_spare_listeners_respects_global_cap() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        // cap=3：pool_size=2 首建占 2，弹性最多再加 1。
        let mut reg = ListenerRegistry::with_max_total(2, 3);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        let hs: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        for h in &hs {
            sockets.get_mut::<TcpSocket>(*h).abort();
        }
        // 要 4 空闲，但 cap=3：建到 total=3 即停，返回 Capped。
        let err = reg
            .ensure_spare_listeners(443, 4, &mut sockets, &mut ctxs)
            .unwrap_err();
        assert!(matches!(err, RegistryError::Capped));
        assert_eq!(reg.handles_for_port(443).len(), 3);
    }

    /// review #1/#2：reap_dead_slots 回收已用过且已死的槽（abort→Closed），rearm 回 Listen +
    /// release 其 fake-IP；空闲 Listen 槽（ctx.state==Listening）不被回收。
    #[test]
    fn reap_dead_slots_rearms_closed_slot_and_releases_fake_ip() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("x.com", 0);
        pool.acquire(ip, 0);

        let handles: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
        let dead = handles[0];
        let idle_listen = handles[1];
        // dead 槽：模拟被用过后本地关闭（abort → Closed）+ 持有 fake-IP。
        sockets.get_mut::<TcpSocket>(dead).abort();
        {
            let ctx = ctxs.get_mut(&dead).unwrap();
            ctx.state = SocketState::Relaying;
            ctx.fake_ip = Some(ip);
        }
        // idle_listen 槽：空闲监听（ctx.state 默认 Listening）→ 不该被回收。

        let reaped = reap_dead_slots(&reg, &mut sockets, &mut ctxs, &mut pool, 1);
        assert_eq!(reaped, 1, "只回收 1 个死槽");
        let dead_ctx = ctxs.get(&dead).unwrap();
        assert_eq!(dead_ctx.state, SocketState::Listening, "死槽回 Listening");
        assert!(dead_ctx.fake_ip.is_none(), "死槽 fake-IP 已 release");
        assert_eq!(pool.sweep(1000, 300), 1, "release 后映射可回收");
        assert_eq!(
            ctxs.get(&idle_listen).unwrap().state,
            SocketState::Listening,
            "空闲 Listen 槽不动"
        );
    }

    #[test]
    fn rearm_socket_restores_listening_state_and_releases_fake_ip() {
        let spec = ListenerSpec { local_port: 80 };
        let mut socket = build_listener_socket(&spec);
        let (tx, _rx) = mpsc::channel(1);
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("x.com", 0);
        pool.acquire(ip, 0); // 模拟本 flow 已 acquire
        let mut ctx = SocketCtx {
            local_port: 80,
            state: SocketState::Relaying,
            uplink_tx: Some(tx),
            downlink_pending: Vec::new(),
            fake_ip: Some(ip),
        };

        rearm_socket(&mut socket, &mut ctx, &mut pool, 1);

        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.uplink_tx.is_none());
        assert!(ctx.fake_ip.is_none(), "rearm 应清空 fake_ip");
        // refcount 已归零 → idle 超 TTL 可回收（证明 rearm 走了 release）。
        assert_eq!(pool.sweep(1000, 300), 1);
    }

    #[test]
    fn tun_runtime_config_defaults_match_stage9_behavior() {
        let config = TunRuntimeConfig::from_sources(None).expect("config should load");
        // Stage 9 drops local_port; pool_size default lowered to 2 (per-port now).
        assert_eq!(config.listener.pool_size, 2);
    }

    #[test]
    fn tun_runtime_config_rejects_zero_pool_size() {
        let err = TunRuntimeConfig::from_sources(Some("0")).expect_err("zero pool size should fail");
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn tun_runtime_config_accepts_pool_size_override() {
        let config = TunRuntimeConfig::from_sources(Some("3")).expect("valid config should load");
        assert_eq!(config.listener.pool_size, 3);
    }

    fn udp_pkt(dst: [u8; 4], dst_port: u16) -> Vec<u8> {
        let mut v = Vec::new();
        etherparse::PacketBuilder::ipv4([10, 0, 0, 1], dst, 64)
            .udp(50000, dst_port)
            .write(&mut v, &[])
            .unwrap();
        v
    }

    /// 刀5：classify_inbound 把**任意** :53 路由到 Dns（裸包伪造），:853/:443 仍走 UdpRelay。
    #[test]
    fn classify_routes_dns_relay_and_other() {
        // 任意 resolver 的 :53 → Dns（不再只限 198.18.0.1）。
        assert_eq!(classify_inbound(&udp_pkt([198, 18, 0, 1], 53)), Inbound::Dns);
        assert_eq!(classify_inbound(&udp_pkt([8, 8, 8, 8], 53)), Inbound::Dns);
        assert_eq!(classify_inbound(&udp_pkt([1, 1, 1, 1], 53)), Inbound::Dns);
        // 其它 UDP（含 DoT/DoQ :853、DoH3/视频 :443）→ UdpRelay（刀4 Block 由 resolve_target 判）。
        assert_eq!(
            classify_inbound(&udp_pkt([198, 18, 0, 5], 443)),
            Inbound::UdpRelay
        );
        assert_eq!(classify_inbound(&udp_pkt([1, 1, 1, 1], 853)), Inbound::UdpRelay);
        // TCP SYN / 垃圾 → Other。
        let pkt = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(classify_inbound(&pkt), Inbound::Other);
        assert_eq!(classify_inbound(&[0u8; 4]), Inbound::Other);
    }

    /// 构造一个最小 DNS 查询(单 question, RD=1, QCLASS=IN)——刀5 forge 测试用。
    fn dns_query(id: u16, qname: &str, qtype: u16) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&id.to_be_bytes());
        v.extend_from_slice(&0x0100u16.to_be_bytes()); // RD=1
        v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
        v.extend_from_slice(&[0u8; 6]); // AN/NS/AR COUNT = 0
        for label in qname.split('.') {
            v.push(label.len() as u8);
            v.extend_from_slice(label.as_bytes());
        }
        v.push(0);
        v.extend_from_slice(&qtype.to_be_bytes());
        v.extend_from_slice(&1u16.to_be_bytes()); // QCLASS IN
        v
    }

    /// 取一个伪造回包的 A 记录 RDATA（响应末 4 字节 = fake-IP）。
    fn reply_rdata_ip(reply: &[u8]) -> Ipv4Addr {
        let g = parse_inbound_udp(reply).expect("回包应是合法 IPv4/UDP");
        let p = g.payload;
        Ipv4Addr::new(p[p.len() - 4], p[p.len() - 3], p[p.len() - 2], p[p.len() - 1])
    }

    /// 刀5 T1：任意 resolver 的明文 A 查询 → 本地伪造 fake-IP 回包；
    /// 回包 src=被查询的 resolver:53、dst=app、RDATA=fake-IP（不依赖 198.18.0.1）。
    #[test]
    fn forge_dns_reply_forges_fake_ip_for_any_resolver() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50000);
        // app 把 A 查询发给 8.8.8.8:53（非我方 resolver）。
        let query = dns_query(0x1234, "example.com", dns::QTYPE_A);
        let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(8, 8, 8, 8), 53), &query);
        let udp = parse_inbound_udp(&pkt).unwrap();

        let reply = forge_dns_reply(&udp, &mut pool, 0).expect("A 查询应被伪造");
        let r = parse_inbound_udp(&reply).expect("回包应是合法 IPv4/UDP");
        // 源 = 被查询的 resolver（否则 app socket 丢弃），目的 = app 原端点。
        assert_eq!((r.src_ip, r.src_port), (Ipv4Addr::new(8, 8, 8, 8), 53));
        assert_eq!((r.dst_ip, r.dst_port), app);
        // RDATA = fake-IP，落 198.18/15 且能 resolve 回域名。
        let fake = reply_rdata_ip(&reply);
        assert!(pool.is_fake(fake), "RDATA 应是 fake-IP, got {fake}");
        assert_eq!(pool.resolve(fake).as_deref(), Some("example.com"));
    }

    /// 刀5 T1：dst 落 fake-IP 段（app 把 resolver 配成被 fake 的域名）也照样伪造，不 Refuse。
    #[test]
    fn forge_dns_reply_forges_even_for_fake_range_resolver() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50001);
        let resolver = (Ipv4Addr::new(198, 18, 0, 9), 53); // fake 段内
        let query = dns_query(1, "foo.com", dns::QTYPE_A);
        let pkt = build_udp_ip_packet(app, resolver, &query);
        let udp = parse_inbound_udp(&pkt).unwrap();
        let reply = forge_dns_reply(&udp, &mut pool, 0).expect("应伪造");
        let r = parse_inbound_udp(&reply).unwrap();
        assert_eq!((r.src_ip, r.src_port), resolver);
        assert!(pool.is_fake(reply_rdata_ip(&reply)));
    }

    /// 刀5 T1：AAAA 查询 → NODATA（ANCOUNT=0），不分配 fake-IP。
    #[test]
    fn forge_dns_reply_aaaa_is_nodata() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50002);
        let query = dns_query(2, "example.com", dns::QTYPE_AAAA);
        let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(1, 1, 1, 1), 53), &query);
        let udp = parse_inbound_udp(&pkt).unwrap();
        let reply = forge_dns_reply(&udp, &mut pool, 0).expect("AAAA 应回 NODATA（非 None）");
        let r = parse_inbound_udp(&reply).unwrap();
        // 响应 payload 偏移 6..8 = ANCOUNT。
        assert_eq!(u16::from_be_bytes([r.payload[6], r.payload[7]]), 0, "NODATA ANCOUNT=0");
    }

    /// 刀5 T1：不可解析的 :53 payload → None（调用方丢弃，绝不转发真 DNS = 不泄漏）。
    #[test]
    fn forge_dns_reply_unparseable_is_none() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50003);
        let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(8, 8, 8, 8), 53), &[0u8; 4]); // 截断
        let udp = parse_inbound_udp(&pkt).unwrap();
        assert!(forge_dns_reply(&udp, &mut pool, 0).is_none());
    }

    /// 刀5 T1：同域名两次查询 → 同一 fake-IP（稳定复用，DNS 给的 IP 与 TCP 时查表一致）。
    #[test]
    fn forge_dns_reply_stable_fake_ip_per_domain() {
        let mut pool = FakeIpPool::new();
        let app = (Ipv4Addr::new(10, 0, 0, 2), 50004);
        let res = (Ipv4Addr::new(8, 8, 8, 8), 53);
        let p1 = build_udp_ip_packet(app, res, &dns_query(1, "stable.com", dns::QTYPE_A));
        let p2 = build_udp_ip_packet(app, res, &dns_query(2, "stable.com", dns::QTYPE_A));
        let r1 = forge_dns_reply(&parse_inbound_udp(&p1).unwrap(), &mut pool, 0).unwrap();
        let r2 = forge_dns_reply(&parse_inbound_udp(&p2).unwrap(), &mut pool, 1).unwrap();
        assert_eq!(reply_rdata_ip(&r1), reply_rdata_ip(&r2));
    }

    /// 刀4：resolve_target 对加密 DNS 端点返回 Block，且精确不误伤普通 :443 / 零回归 Refuse。
    #[test]
    fn resolve_target_blocks_encrypted_dns() {
        use smoltcp::wire::IpEndpoint;
        let mut pool = FakeIpPool::new();
        let ep = |ip: [u8; 4], port: u16| {
            IpEndpoint::new(IpAddress::v4(ip[0], ip[1], ip[2], ip[3]), port)
        };

        // DoT/DoQ :853（任意 IP）→ Block。
        assert!(matches!(resolve_target(ep([1, 1, 1, 1], 853), &pool), TargetResolve::Block));
        assert!(matches!(
            resolve_target(ep([93, 184, 216, 34], 853), &pool),
            TargetResolve::Block
        ));

        // 刀5：TCP :53（明文 DNS over TCP，任意 IP）→ Block（RST，逼回落 UDP :53）。
        // 不变量：UDP :53 已被 classify_inbound 截到 Dns 路径、不到 resolve_target，故 port==53 只命中 TCP。
        assert!(matches!(resolve_target(ep([8, 8, 8, 8], 53), &pool), TargetResolve::Block));
        assert!(matches!(
            resolve_target(ep([198, 18, 0, 1], 53), &pool),
            TargetResolve::Block
        ));

        // DoH 经 fake-IP：dns.google:443 → Block；普通域名:443 → Direct；DoH 域名但 :80 → Direct（仅 :443）。
        let doh_fake = pool.alloc("dns.google", 0);
        assert!(matches!(resolve_target(ep(doh_fake.octets(), 443), &pool), TargetResolve::Block));
        assert!(matches!(resolve_target(ep(doh_fake.octets(), 80), &pool), TargetResolve::Direct { .. }));
        let normal_fake = pool.alloc("example.com", 0);
        assert!(matches!(
            resolve_target(ep(normal_fake.octets(), 443), &pool),
            TargetResolve::Direct { .. }
        ));

        // DoH 硬编 IP 1.1.1.1:443 → Block；普通真实 IP:443 → Direct。
        assert!(matches!(resolve_target(ep([1, 1, 1, 1], 443), &pool), TargetResolve::Block));
        assert!(matches!(
            resolve_target(ep([93, 184, 216, 34], 443), &pool),
            TargetResolve::Direct { .. }
        ));

        // fake-IP 段内无映射 → Refuse（零回归，不被 Block 吞掉）。
        assert!(matches!(
            resolve_target(IpEndpoint::new(IpAddress::v4(198, 18, 99, 99), 443), &pool),
            TargetResolve::Refuse
        ));
    }

    /// #1 脏集合驱动：`handles_for_port` 返回该端口 pool 的全部 handle；未注册端口空 slice。
    #[test]
    fn handles_for_port_returns_pool_handles() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(3);
        reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.handles_for_port(443).len(), 3);
        assert!(reg.handles_for_port(8080).is_empty());
    }
}
