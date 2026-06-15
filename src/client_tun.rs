use crate::device::{TunIo, VirtualTunDevice};
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::{AssocTable, TuicClientConfig, TuicUpstream, decode_packet, encode_packet};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use crate::udp_relay::{FourTuple, UDP_FLOW_IDLE_SECS, build_udp_ip_packet, parse_inbound_udp};
use crate::dns::{self, Answer};
use crate::fake_ip::FakeIpPool;
use std::net::Ipv4Addr;
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer as TcpSocketBuffer};
use smoltcp::socket::udp;
use smoltcp::wire::{IpAddress, IpCidr, IpListenEndpoint};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

pub(crate) const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const RELAY_CHANNEL_CAPACITY: usize = 1024;

/// 主循环分段插桩接缝（knife1：并发压测定位瓶颈）。
///
/// 中文要点：生产传 [`NoopSink`]（空方法，单态化内联后**零开销**，热路径无 `Instant::now()`）；
/// 并发压测 harness 传 RecordingSink，在每个回调里采集每段耗时/调用次数。计时逻辑全部留在 sink
/// 实现内，主循环只做平凡方法调用——生产与测试**同一份循环**。
pub trait MetricsSink {
    /// 进入 smoltcp poll 段（poll → drain_dns → poll → flush_tx）。
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

/// fake-IP DNS resolver 地址：去往它的 :53 UDP 走本地 fake-IP 应答，不进 UDP relay。
const FAKE_DNS_RESOLVER: Ipv4Addr = Ipv4Addr::new(198, 18, 0, 1);

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
}

impl ListenerRegistry {
    fn new(pool_size: usize) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
        }
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
        println!(
            "🆕 listener pool created for port {port} (pool_size={})",
            self.pool_size
        );
        self.ports.insert(port, handles);
        Ok(())
    }

    /// Iterate every smoltcp handle across all currently intercepted ports.
    /// 中文要点：主循环用它替代 `ListenerPool.handles`，跨所有端口轮询首包。
    #[cfg(test)]
    fn all_handles(&self) -> impl Iterator<Item = SocketHandle> + '_ {
        self.ports.values().flatten().copied()
    }

    /// All listener handles registered for `port`（未注册端口返回空 slice）。
    /// 中文要点：#1 脏集合驱动——按 inbound 包的 dst_port 取该端口 pool 全部 handle 标脏，
    /// 替代每 tick 全量 `all_handles()` 遍历。
    fn handles_for_port(&self, port: u16) -> &[SocketHandle] {
        self.ports.get(&port).map(Vec::as_slice).unwrap_or(&[])
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

    // 2. 仅 TUIC 上游（legacy yamux / 自研 server 已退役，见 ADR-0004）。
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

    // Stage 11: fake-IP DNS。监听 UDP :53（AnyIP 收发往 198.18.0.1:53 的查询），
    // 本地伪造 A 响应（fake-IP）；TCP 时凭 fake-IP 查回域名走 DomainPort relay。
    let dns_handle = {
        // 中文要点：DNS 查询常突发（应用并发解析 + 系统后台噪声），buffer 太小会丢查询 →
        // getaddrinfo 失败 → 连接发不出。给足 64 槽 / 64KB，吸收突发。
        let rx = udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 64], vec![0u8; 65_536]);
        let tx = udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 64], vec![0u8; 65_536]);
        let mut s = udp::Socket::new(rx, tx);
        // 必须 bind 到具体的 198.18.0.1，而非通配 None：否则 smoltcp 回复 DNS 响应时
        // 按子网匹配选源地址（dst=10.0.0.1 → src=10.0.0.2），源与查询目的不一致，
        // 系统 resolver 会丢弃响应。bind 到 198.18.0.1 才能让响应 src=198.18.0.1。
        s.bind(IpListenEndpoint {
            addr: Some(IpAddress::v4(198, 18, 0, 1)),
            port: 53,
        })
        .unwrap();
        sockets.add(s)
    };
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
        // Stage 11: 把 fake DNS resolver 地址也配成本接口 IP，否则 smoltcp 无法以
        // src=198.18.0.1 发回 DNS 响应（egress 选不到合法 src），系统收不到应答。
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(198, 18, 0, 1), 32))
            .unwrap();
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
    let udp_clock = std::time::Instant::now();
    let mut udp_sweep = tokio::time::interval(std::time::Duration::from_secs(1));

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
                    // rx 分流（stage-12 D1）：去往 fake DNS 的 :53 → smoltcp；其它 UDP → 裸 relay；
                    // 非 UDP → 既有 smoltcp 路径。UDP relay 包 take 走、不进 iface.poll。
                    let class = device.rx_peek().map(classify_inbound);
                    if class == Some(Inbound::UdpRelay) {
                        if let Some(pkt) = device.rx_take() {
                            // Stage 13b: UDP → 编码 TUIC Packet → send_udp。
                            handle_tuic_udp_uplink(
                                &pkt,
                                &mut assoc_table,
                                &fake_pool,
                                &*upstream,
                                udp_clock.elapsed().as_secs(),
                            )
                            .await;
                        }
                    } else {
                        // 1) SYN inspector + 脏集合标脏：在 iface.poll 之前看一眼包。
                        //    - 干净 SYN 去往新端口 → 立刻建监听池，smoltcp 同一帧就能 accept。
                        //    - 任意去往拦截端口的 TCP 包 → 把该端口 pool 标脏（#1），覆盖 SYN 之后
                        //      让 listener can_recv 的首个 data 包；relay 段只处理脏集合，不再全扫。
                        if let Some(buf) = device.rx_peek() {
                            if let Some(syn_port) = inspect_inbound_syn(buf)
                                && let Err(e) =
                                    registry.ensure_port(syn_port, &mut sockets, &mut socket_ctxs)
                            {
                                println!(
                                    "⚠️ intercepted port cap reached, drop SYN to port {syn_port}: {:?}",
                                    e
                                );
                            }
                            if let Some(port) = inbound_tcp_dst_port(buf) {
                                for &h in registry.handles_for_port(port) {
                                    dirty.insert(h);
                                }
                            }
                        }

                        metrics.enter_poll();
                        let timestamp = smoltcp::time::Instant::now();
                        iface.poll(timestamp, &mut device, &mut sockets);
                        // 处理 DNS 查询并伪造响应；再 poll 一次把响应变成 IP 包入发货队列。
                        drain_dns(&mut sockets, dns_handle, &mut fake_pool);
                        iface.poll(timestamp, &mut device, &mut sockets);
                        device.flush_tx().await.unwrap();
                        metrics.leave_poll();

                        process_dirty_relay(
                            &mut dirty,
                            &mut sockets,
                            &mut socket_ctxs,
                            &*upstream,
                            &global_tx,
                            &fake_pool,
                            &mut metrics,
                        )
                        .await;
                    }
                }
            }
            // Stage 13b: TUIC 下行 datagram → decode_packet → AssocTable 解路由 → 造回程 IP/UDP 注入 TUN。
            Some(dg) = tuic_downlink_rx.recv() => {
                if let Some((assoc_id, payload)) = decode_packet(&dg) {
                    // 先取出路由信息(Copy),释放 assoc_table 借用后再 touch。
                    let routed = assoc_table
                        .resolve(assoc_id)
                        .map(|e| (e.target_src(), e.app_endpoint()));
                    if let Some((src, dst)) = routed {
                        let pkt = build_udp_ip_packet(src, dst, payload);
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
            // Stage 13b: 周期回收空闲 UDP assoc。
            _ = udp_sweep.tick() => {
                assoc_table.sweep(udp_clock.elapsed().as_secs(), UDP_FLOW_IDLE_SECS);
            }
            // 分支 2: 时钟滴答，处理超时重传等后台任务
            _ = timer.tick() =>{
                metrics.enter_poll();
                let timestamp = smoltcp::time::Instant::now();
                iface.poll(timestamp, &mut device, &mut sockets);
                drain_dns(&mut sockets, dns_handle, &mut fake_pool);
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
                    &fake_pool,
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
async fn process_dirty_relay<U, M>(
    dirty: &mut HashSet<SocketHandle>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &U,
    global_tx: &mpsc::Sender<(SocketHandle, Vec<u8>)>,
    fake_pool: &FakeIpPool,
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

/// Allocate a fresh smoltcp TCP listener socket for one pool slot.
/// 中文要点：每次调用都创建一间独立房间，并立即挂上 listen 牌子。
fn build_listener_socket(spec: &ListenerSpec) -> TcpSocket<'static> {
    let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; TCP_SOCKET_BUFFER_SIZE]);
    let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; TCP_SOCKET_BUFFER_SIZE]);
    let mut tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
    tcp_socket.listen(spec.local_port).unwrap();
    tcp_socket
}

/// Identify a clean inbound TCP SYN and return its destination port.
/// 中文要点：纯解析、无副作用。仅 IPv4+TCP+`syn && !ack` 才返回 Some(dst_port);
/// 解析失败或非 IPv4 / 非 TCP / 不是干净 SYN 一律 None。
fn inspect_inbound_syn(packet: &[u8]) -> Option<u16> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    if tcp.syn && !tcp.ack {
        Some(tcp.destination_port)
    } else {
        None
    }
}

/// Extract the destination TCP port from *any* inbound IPv4+TCP packet (not only a clean SYN).
/// 中文要点：#1 脏集合驱动用——任何去往拦截端口的 TCP 包都把该端口 pool 标脏，覆盖
/// SYN 之后让 listener `can_recv` 的首个 data 包（`inspect_inbound_syn` 只认干净 SYN，太窄）。
/// 非 IPv4 / 非 TCP / 解析失败 → None。
fn inbound_tcp_dst_port(packet: &[u8]) -> Option<u16> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    Some(tcp.destination_port)
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
    /// 正常转发：IpPort 直连，或 fake-IP 查回的 DomainPort。
    Direct(TargetAddr),
    /// fake-IP 段内但查不到映射（如客户端重启丢表、应用用旧缓存 IP）：拒绝，让应用重查。
    Refuse,
}

/// 把提取出的 endpoint 解析成 relay target。
/// 中文要点：fake-IP → 查表得域名 → DomainPort（出口解析、绕污染）；非 fake → IpPort
/// （Stage 8/9 行为不变）；fake 但无映射 → Refuse（拒绝连接）。
fn resolve_target(endpoint: smoltcp::wire::IpEndpoint, fake_pool: &FakeIpPool) -> TargetResolve {
    let std::net::IpAddr::V4(v4) = std::net::IpAddr::from(endpoint.addr) else {
        return TargetResolve::Direct(target_from_endpoint(endpoint));
    };
    if fake_pool.is_fake(v4) {
        match fake_pool.resolve(v4) {
            Some(domain) => {
                // 不在此 println!——resolve_target 在每个 UDP 包/每条 TCP 首包都会走到，
                // 热路径同步 stdout 会拖垮大并发。flow 创建的可观测性放在服务端日志。
                TargetResolve::Direct(TargetAddr::DomainPort {
                    host: domain,
                    port: endpoint.port,
                })
            }
            None => TargetResolve::Refuse,
        }
    } else {
        TargetResolve::Direct(target_from_endpoint(endpoint))
    }
}

/// 处理 DNS socket 上排队的查询：A → 分配 fake-IP 并伪造 A 响应；其它 → NODATA。
/// 中文要点：纯本地应答，不外发真实 DNS；解析失败的查询直接忽略，绝不 panic。
fn drain_dns(sockets: &mut SocketSet<'_>, dns_handle: SocketHandle, fake_pool: &mut FakeIpPool) {
    let sock = sockets.get_mut::<udp::Socket>(dns_handle);
    while sock.can_recv() {
        let (data, remote) = match sock.recv() {
            Ok((d, meta)) => (d.to_vec(), meta.endpoint),
            Err(_) => break,
        };
        let Some(q) = dns::parse_query(&data) else {
            continue;
        };
        let resp = if q.qtype == dns::QTYPE_A {
            let ip = fake_pool.alloc(&q.qname);
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
        if let Err(e) = sock.send_slice(&resp, remote) {
            println!("DNS 响应发送失败: {:?}", e);
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
fn rearm_socket(socket: &mut TcpSocket<'_>, ctx: &mut SocketCtx) {
    ctx.state = SocketState::Closing;
    socket.abort();
    ctx.uplink_tx = None;
    ctx.downlink_pending.clear();
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
    fake_pool: &FakeIpPool,
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
    let target = match resolve_target(endpoint, fake_pool) {
        TargetResolve::Direct(t) => t,
        TargetResolve::Refuse => {
            println!(
                "🚫 fake-IP {} 无映射，拒绝连接（请重新解析）",
                endpoint.addr
            );
            let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
            if let Some(ctx) = socket_ctxs.get_mut(&handle) {
                rearm_socket(tcp_socket, ctx);
            }
            return Ok(());
        }
    };

    handle_local_payload(handle, payload, Some(target), socket_ctxs, upstream, global_tx).await
}

async fn handle_local_payload<U: ProxyUpstream>(
    handle: SocketHandle,
    payload: Vec<u8>,
    target: Option<TargetAddr>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    upstream: &U,
    global_tx: &mpsc::Sender<(SocketHandle, Vec<u8>)>,
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

    spawn_remote_relay(handle, stream, rx, global_tx.clone());
    Ok(())
}

/// 处理远端回信
async fn handle_remote_payload<D: TunIo>(
    handle: SocketHandle,
    payload: Vec<u8>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    device: &mut D,
) -> std::io::Result<()> {
    let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
    let Some(ctx) = socket_ctxs.get_mut(&handle) else {
        return Ok(());
    };

    if payload.is_empty() {
        println!("🔄 handle {:?} entering {:?}", handle, SocketState::Closing);
        rearm_socket(tcp_socket, ctx);
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
    mut stream: RelayStream,
    mut rx: mpsc::Receiver<Vec<u8>>,
    back_tx: mpsc::Sender<(SocketHandle, Vec<u8>)>,
) {
    tokio::spawn(async move {
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
            }
        }
    });
}

/// rx 热路径分流结果。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Inbound {
    /// 去往 fake-IP DNS resolver 的 UDP/53 —— 交给 smoltcp（本地 fake-IP 应答）。
    Dns,
    /// 其它 UDP —— 走裸包 UDP relay（绕过 smoltcp）。
    UdpRelay,
    /// 非 UDP（TCP/ICMP…）—— 走既有 smoltcp 路径。
    Other,
}

/// 给一个入站裸 IP 包分类（见 stage-12 spec 的 D1 规则）。
fn classify_inbound(pkt: &[u8]) -> Inbound {
    match parse_inbound_udp(pkt) {
        Some(udp) => {
            if udp.dst_ip == FAKE_DNS_RESOLVER && udp.dst_port == 53 {
                Inbound::Dns
            } else {
                Inbound::UdpRelay
            }
        }
        None => Inbound::Other,
    }
}

/// 处理一个被拦截的 UDP 上行包：解析 → fake-IP 改写 target → 铸 assoc-id →
/// 编码 TUIC Packet → `TuicUpstream::send_udp`。
/// 中文要点：fake 无映射 → 丢弃(短 TTL 自愈)；send_udp 自带丢弃计数(UDP 语义)。
async fn handle_tuic_udp_uplink<U: DatagramUpstream>(
    pkt: &[u8],
    assoc_table: &mut AssocTable,
    fake_pool: &FakeIpPool,
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
    let target = match resolve_target(dst_ep, fake_pool) {
        TargetResolve::Direct(t) => t,
        TargetResolve::Refuse => {
            println!("🚫 UDP fake-IP {} 无映射，丢弃（待应用重新解析）", udp.dst_ip);
            return;
        }
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
        println!(
            "🌊 TUIC UDP↑ new assoc={assoc_id} → {} (first {}B)",
            target.to_wire_string(),
            udp.payload.len()
        );
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
    fn inspect_inbound_syn_returns_dst_port_for_clean_syn() {
        let pkt = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(inspect_inbound_syn(&pkt), Some(443));
    }

    #[test]
    fn inspect_inbound_syn_rejects_syn_ack() {
        let pkt = build_ipv4_tcp([1, 1, 1, 1], [10, 0, 0, 1], 443, 60000, true, true);
        assert_eq!(inspect_inbound_syn(&pkt), None);
    }

    #[test]
    fn inspect_inbound_syn_rejects_plain_ack() {
        let pkt = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 80, false, true);
        assert_eq!(inspect_inbound_syn(&pkt), None);
    }

    #[test]
    fn inspect_inbound_syn_rejects_garbage() {
        assert_eq!(inspect_inbound_syn(&[0u8; 4]), None);
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

    #[test]
    fn rearm_socket_restores_listening_state_and_clears_sender() {
        let spec = ListenerSpec { local_port: 80 };
        let mut socket = build_listener_socket(&spec);
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx {
            local_port: 80,
            state: SocketState::Relaying,
            uplink_tx: Some(tx),
            downlink_pending: Vec::new(),
        };

        rearm_socket(&mut socket, &mut ctx);

        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.uplink_tx.is_none());
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

    #[test]
    fn classify_routes_dns_relay_and_other() {
        assert_eq!(classify_inbound(&udp_pkt([198, 18, 0, 1], 53)), Inbound::Dns);
        assert_eq!(
            classify_inbound(&udp_pkt([198, 18, 0, 5], 443)),
            Inbound::UdpRelay
        );
        // UDP/53 到非 fake-resolver 仍走 relay（D1：只本地应答 198.18.0.1:53）。
        assert_eq!(classify_inbound(&udp_pkt([8, 8, 8, 8], 53)), Inbound::UdpRelay);
        // TCP SYN / 垃圾 → Other。
        let pkt = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(classify_inbound(&pkt), Inbound::Other);
        assert_eq!(classify_inbound(&[0u8; 4]), Inbound::Other);
    }

    /// #1 脏集合驱动：`inbound_tcp_dst_port` 对任意 TCP 包（不只干净 SYN）返回 dst_port，
    /// 用于把该端口 pool 标脏。非 TCP / 垃圾返回 None。
    #[test]
    fn inbound_tcp_dst_port_returns_port_for_any_tcp() {
        // 干净 SYN。
        let syn = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
        assert_eq!(inbound_tcp_dst_port(&syn), Some(443));
        // SYN-ACK（inspect_inbound_syn 会拒，但脏集合驱动要认它的 dst_port）。
        let synack = build_ipv4_tcp([1, 1, 1, 1], [10, 0, 0, 1], 443, 60000, true, true);
        assert_eq!(inbound_tcp_dst_port(&synack), Some(60000));
        // 纯 ACK / data 包（首包数据让 listener can_recv 的那一刻）。
        let ack = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 80, false, true);
        assert_eq!(inbound_tcp_dst_port(&ack), Some(80));
        // 非 TCP（UDP）/ 垃圾 → None。
        assert_eq!(inbound_tcp_dst_port(&udp_pkt([8, 8, 8, 8], 53)), None);
        assert_eq!(inbound_tcp_dst_port(&[0u8; 4]), None);
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
