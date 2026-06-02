use crate::device::VirtualTunDevice;
use mini_vpn::shared::{ClientError, RelayRequest, TargetAddr, open_remote_session};
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer as TcpSocketBuffer};
use smoltcp::wire::{IpAddress, IpCidr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use yamux::{Config as YamuxConfig, Connection, Mode};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::rustls::Certificate;

use std::collections::HashMap;
use tokio::sync::mpsc;

const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const RELAY_CHANNEL_CAPACITY: usize = 1024;
// 中文要点：Stage 9 起按"每端口"配 pool，64 端口 * 2 槽 * 2 缓冲 ≈ 16MB。
const DEFAULT_TUN_POOL_SIZE: usize = 2;
const DEFAULT_TUN_SERVER_ADDR: &str = "127.0.0.1:8081";
const DEFAULT_TUN_TLS_SNI: &str = "localhost";
const DEFAULT_TUN_CA_PATH: &str = "cert.pem";

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
        }
    }
}

/// Hard cap on the number of distinct destination ports we will intercept.
/// 中文要点：防止 SYN flood 下 socket / 缓冲区无限增长，到顶就拒新端口。
const MAX_INTERCEPTED_PORTS: usize = 64;

/// Reconnect backoff bounds (full-jitter exponential).
/// 中文要点：base/cap 设为编译期常量，本阶段不开 env；cap 限单连接最长重试间隔。
const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_CAP_MS: u64 = 30_000;

/// Full-jitter exponential backoff delay: `random(0, min(CAP, BASE * 2^attempt))`.
/// 中文要点：下界取 0 是 full jitter，最大程度摊平 5000+ 客户端的重连惊群。
/// `rand_unit ∈ [0,1)` 由调用方注入（运行时 `rand::random`，测试传固定值）。
fn backoff_delay(attempt: u32, rand_unit: f64) -> std::time::Duration {
    let exp = RECONNECT_BASE_MS.saturating_mul(1u64.checked_shl(attempt).unwrap_or(u64::MAX));
    let upper = exp.min(RECONNECT_CAP_MS);
    let ms = (upper as f64 * rand_unit) as u64;
    std::time::Duration::from_millis(ms)
}

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
    fn all_handles(&self) -> impl Iterator<Item = SocketHandle> + '_ {
        self.ports.values().flatten().copied()
    }
}

/// Local listener-side startup configuration for the TUN runtime.
/// 中文要点：这一层只关心本地拦截面，不关心怎么连上游 TLS/Yamux 服务。
#[derive(Debug, Clone)]
struct TunListenerConfig {
    /// Per-port pool size: number of smoltcp listener slots created for each
    /// intercepted destination port.
    /// 中文要点：Stage 9 起 pool 按"每端口"算，决定单个端口能并发承接多少条连接。
    pool_size: usize,
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

/// Upstream TLS/Yamux connection configuration for the TUN runtime.
/// 中文要点：这一层只描述“连谁”和“用什么 SNI”，不参与本地监听池逻辑。
#[derive(Debug, Clone)]
struct TunUpstreamConfig {
    /// TCP address of the upstream proxy server.
    /// 中文要点：TUN 客户端实际要连的上游服务地址。
    server_addr: String,
    /// TLS SNI value used during the upstream handshake.
    /// 中文要点：TLS 握手时发送给服务端的 Server Name。
    tls_sni: String,
}

impl TunUpstreamConfig {
    /// Build upstream config from optional string sources.
    /// 中文要点：外联配置在启动时完成校验，避免把坏值带到 TLS 热路径。
    fn from_sources(
        server_addr: Option<&str>,
        tls_sni: Option<&str>,
    ) -> Result<Self, ClientError> {
        let server_addr = server_addr.unwrap_or(DEFAULT_TUN_SERVER_ADDR).to_string();
        let tls_sni = tls_sni.unwrap_or(DEFAULT_TUN_TLS_SNI).to_string();

        server_addr.parse::<std::net::SocketAddr>().map_err(|_| {
            ClientError::InvalidTarget(format!("invalid upstream server addr: {server_addr}"))
        })?;

        ServerName::try_from(tls_sni.as_str()).map_err(|_| {
            ClientError::InvalidTarget(format!("invalid upstream tls sni: {tls_sni}"))
        })?;

        Ok(Self {
            server_addr,
            tls_sni,
        })
    }
}

/// TLS trust material configuration for the TUN client.
/// 中文要点：这一层只负责“信任哪份 CA 证书”，不参与监听池和上游地址解析。
#[derive(Debug, Clone)]
struct TunTlsConfig {
    /// PEM CA certificate path loaded into the rustls root store.
    /// 中文要点：客户端用它校验服务端证书链。
    ca_path: String,
}

impl TunTlsConfig {
    /// Build TLS trust config from optional string sources.
    /// 中文要点：当前最小配置面只开放 CA 路径，空字符串直接视为非法输入。
    fn from_sources(ca_path: Option<&str>) -> Result<Self, ClientError> {
        let ca_path = ca_path.unwrap_or(DEFAULT_TUN_CA_PATH).to_string();

        if ca_path.trim().is_empty() {
            return Err(ClientError::InvalidTarget(
                "invalid tun ca path: empty".to_string(),
            ));
        }

        Ok(Self { ca_path })
    }
}

/// Startup configuration for the TUN runtime.
/// 中文要点：总配置壳负责把 listener、upstream、tls 三类配置组合起来。
#[derive(Debug, Clone)]
struct TunRuntimeConfig {
    listener: TunListenerConfig,
    upstream: TunUpstreamConfig,
    tls: TunTlsConfig,
}

impl TunRuntimeConfig {
    /// Build config from optional string sources.
    /// 中文要点：测试和环境变量入口共享同一套组合逻辑，避免行为漂移。
    fn from_sources(
        pool_size: Option<&str>,
        server_addr: Option<&str>,
        tls_sni: Option<&str>,
        ca_path: Option<&str>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            listener: TunListenerConfig::from_sources(pool_size)?,
            upstream: TunUpstreamConfig::from_sources(server_addr, tls_sni)?,
            tls: TunTlsConfig::from_sources(ca_path)?,
        })
    }

    /// Read config from process environment.
    /// 中文要点：Stage 9 起 `MINI_VPN_TUN_LOCAL_PORT` 已删除，端口由 SYN inspector 动态注册。
    fn from_env() -> Result<Self, ClientError> {
        let pool_size = std::env::var("MINI_VPN_TUN_POOL_SIZE").ok();
        let server_addr = std::env::var("MINI_VPN_TUN_SERVER_ADDR").ok();
        let tls_sni = std::env::var("MINI_VPN_TUN_TLS_SNI").ok();
        let ca_path = std::env::var("MINI_VPN_TUN_CA_PATH").ok();

        Self::from_sources(
            pool_size.as_deref(),
            server_addr.as_deref(),
            tls_sni.as_deref(),
            ca_path.as_deref(),
        )
    }
}

pub async fn start_tun_proxy() {
    let runtime_config = match TunRuntimeConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            println!("加载 TUN 运行时配置失败: {e}");
            return;
        }
    };
    let pool_size = runtime_config.listener.pool_size;
    let upstream_server_addr = runtime_config.upstream.server_addr.clone();
    let upstream_tls_sni = runtime_config.upstream.tls_sni.clone();
    let tls_ca_path = runtime_config.tls.ca_path.clone();

    let mut root_cert_store = RootCertStore::empty();
    let cert_file = match File::open(tls_ca_path.as_str()) {
        Ok(file) => file,
        Err(e) => {
            println!("打开客户端 CA 证书失败 {}: {e}", tls_ca_path);
            return;
        }
    };
    let cert_file = &mut BufReader::new(cert_file);
    let certs = rustls_pemfile::certs(cert_file).unwrap();
    for cert in certs {
        root_cert_store.add(&Certificate(cert)).unwrap();
    }
    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    // 全局回信通道：接收端 global_rx 留在主循环，发送端 global_tx 会被克隆给每个后台车厢
    let (global_tx, mut global_rx) =
        tokio::sync::mpsc::channel::<(SocketHandle, Vec<u8>)>(RELAY_CHANNEL_CAPACITY);
    // =========== 1. 初始化底层网卡和通道 ===========

    println!(
        "🚀 TUN runtime started with pool_size={}, server_addr={}, tls_sni={}, ca_path={}",
        pool_size, upstream_server_addr, upstream_tls_sni, tls_ca_path
    );

    // 1. 初始化 TUN 设备(化底层网卡和通道) / 创建操作系统的原生异步虚拟网卡
    let raw_tun = match create_tun_device().await {
        Ok(device) => device,
        Err(e) => {
            println!("无法创建 TUN 设备: {e}");
            return;
        }
    };
    // 使用 raw_tun 实例化我们的包装器
    let mut device = VirtualTunDevice::new(raw_tun);

    // =========== 2. 初始化 smoltcp 酒店和路由器 ===========
    // 2. 初始化 smoltcp 的“酒店”
    let mut sockets = SocketSet::new(vec![]);

    // Stage 9: 监听端口不再固定，由 SYN inspector 在 rx 热路径按需注册。
    // 中文要点：启动时 registry 是空的；第一条到任意端口的 SYN 会触发该端口建池。
    let mut registry = ListenerRegistry::new(pool_size);
    let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();

    // 3. 初始化 smoltcp 的“虚拟路由器”
    let config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
    // 这里传入了包装好的 &mut device
    let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::now());

    // 给虚拟路由器配置 IP 地址 (10.0.0.1/24)
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
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

    // 3. 初始化定时器 (例如每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));

    let domain = match ServerName::try_from(upstream_tls_sni.as_str()) {
        Ok(domain) => domain,
        Err(e) => {
            println!("解析 SNI 域名失败: {e:?}");
            return;
        }
    };

    // 上游连接 + 断开信号通道。epoch 记录连接代际（每成功一次 +1）。
    let (disconnect_tx, mut disconnect_rx) = mpsc::channel::<()>(1);
    let mut epoch: u64 = 0;
    let mut ctr = match connect_upstream(
        &connector,
        &upstream_server_addr,
        domain.clone(),
        disconnect_tx.clone(),
    )
    .await
    {
        Ok(c) => {
            epoch += 1;
            println!("✅ 成功连接到洛杉矶代理服务器！(epoch={epoch})");
            c
        }
        Err(e) => {
            println!("首次连接代理服务端失败: {e:?}");
            return;
        }
    };

    loop {
        let mut ctrl = ctr.clone();
        tokio::select! {
            // 分支 0: 上游连接断开 → 复位在途连接 + 带 full-jitter 退避重连（无限重试）。
            _ = disconnect_rx.recv() => {
                println!("🔌 上游连接断开，准备重连");
                let handles: Vec<SocketHandle> = registry.all_handles().collect();
                let mut reset = 0usize;
                for h in handles {
                    if let Some(c) = socket_ctxs.get_mut(&h)
                        && c.uplink_tx.is_some()
                    {
                        let sock = sockets.get_mut::<TcpSocket>(h);
                        rearm_socket(sock, c);
                        reset += 1;
                    }
                }
                println!("♻️ 重连后复位 {reset} 条在途连接");
                let mut attempt = 0u32;
                loop {
                    let delay = backoff_delay(attempt, rand::random::<f64>());
                    println!("⏳ 第 {} 次重连，等待 {}ms", attempt + 1, delay.as_millis());
                    tokio::time::sleep(delay).await;
                    match connect_upstream(
                        &connector,
                        &upstream_server_addr,
                        domain.clone(),
                        disconnect_tx.clone(),
                    )
                    .await
                    {
                        Ok(new_ctr) => {
                            ctr = new_ctr;
                            epoch += 1;
                            println!("✅ 上游重连成功 (epoch={epoch})");
                            break;
                        }
                        Err(e) => {
                            println!("重连失败: {e:?}");
                            attempt = attempt.saturating_add(1);
                        }
                    }
                }
            }
            // 🌟 新增分支：监听洛杉矶回传的信件
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
            }
            // 分支 1: 全局回信通道接收到了新数据包
            // 分支 1: 物理网卡接收到了新数据包
            res = device.wait_for_rx() =>{
                if res.is_ok(){
                    // 1) SYN inspector：在 iface.poll 之前看一眼包，若是去往新端口的干净 SYN，
                    //    立刻为该端口建监听池，这样 smoltcp 同一帧就能 accept。
                    // 中文要点：到顶时优雅拒绝并日志告警，绝不 panic。
                    if let Some(buf) = &device.rx_buffer {
                        println!("📡 收到来自操作系统的包，大小: {} 字节", buf.len());
                        println!("🔍 包的前 4 字节: {:?}", &buf[..4.min(buf.len())]);
                        if let Some(port) = inspect_inbound_syn(buf)
                            && let Err(e) =
                                registry.ensure_port(port, &mut sockets, &mut socket_ctxs)
                        {
                            println!(
                                "⚠️ intercepted port cap reached, drop SYN to port {port}: {:?}",
                                e
                            );
                        }
                    }

                    let timestamp = smoltcp::time::Instant::now();
                    iface.poll(timestamp, &mut device, &mut sockets);
                    device.flush_tx().await.unwrap();

                    let handles: Vec<SocketHandle> = registry.all_handles().collect();
                    for handle in handles {
                        if let Err(e) = process_listener_activity(
                            handle,
                            &mut sockets,
                            &mut socket_ctxs,
                            &mut ctrl,
                            &global_tx,
                        )
                        .await
                        {
                            println!("处理本地房间 {:?} 失败: {e}", handle);
                        }
                    }
                }
            }
            // 分支 2: 时钟滴答，处理超时重传等后台任务
            _ = timer.tick() =>{
                let timestamp = smoltcp::time::Instant::now();
                iface.poll(timestamp, &mut device, &mut sockets);
                device.flush_tx().await.unwrap();

                let handles: Vec<SocketHandle> = registry.all_handles().collect();
                for handle in handles {
                    if let Err(e) = process_listener_activity(
                        handle,
                        &mut sockets,
                        &mut socket_ctxs,
                        &mut ctrl,
                        &global_tx,
                    )
                    .await
                    {
                        println!("处理本地房间 {:?} 失败: {e}", handle);
                    }
                }
            }
        }
    }
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

/// Establish one upstream TLS + Yamux connection and spawn its background poll task.
/// 中文要点：把"建 TCP→TLS→Yamux"收敛成一处，返回可替换的 Control；后台 poll task
/// 退出（即连接断开）时通过 disconnect_tx 给主循环发信号，驱动重连。
async fn connect_upstream(
    connector: &TlsConnector,
    server_addr: &str,
    domain: ServerName,
    disconnect_tx: mpsc::Sender<()>,
) -> Result<yamux::Control, ClientError> {
    let server_stream = TcpStream::connect(server_addr).await?;
    let tls_stream = connector.clone().connect(domain, server_stream).await?;
    let mut yamux_conn = Connection::new(tls_stream.compat(), YamuxConfig::default(), Mode::Client);
    let ctr = yamux_conn.control();
    tokio::spawn(async move {
        while let Ok(Some(_)) = yamux_conn.next_stream().await {}
        // 连接断开：通知主循环（receiver 可能已关闭，忽略错误）。
        let _ = disconnect_tx.send(()).await;
    });
    Ok(ctr)
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

/// Convert a smoltcp endpoint into a relay Target.
/// 中文要点：TUN 链路上目的地址在 IP 层已是裸 IP（域名早被 DNS 解析掉），
/// 这里统一转成 `TargetAddr::IpPort`。当前 crate 只开 `proto-ipv4`，故必为 IPv4。
fn target_from_endpoint(endpoint: smoltcp::wire::IpEndpoint) -> TargetAddr {
    let ip = std::net::IpAddr::from(endpoint.addr);
    TargetAddr::IpPort(std::net::SocketAddr::new(ip, endpoint.port))
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
    ctx.state = SocketState::Rearming;
    socket.listen(ctx.local_port).unwrap();
    ctx.state = SocketState::Listening;
    println!("♻️ handle slot rearmed on local port {}", ctx.local_port);
}

/// Process one listener slot after iface polling.
/// 中文要点：主循环只负责遍历 handle，真正的房间处理逻辑都收口在这里。
async fn process_listener_activity(
    handle: SocketHandle,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ctrl: &mut yamux::Control,
    global_tx: &mpsc::Sender<(SocketHandle, Vec<u8>)>,
) -> Result<(), ClientError> {
    // 取首包的同时读 local_endpoint：它就是被拦截连接真正想去的 Target。
    // 中文要点：两者都需要 socket，合并在这一处借用里读出，避免二次借用。
    let extracted = {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        let payload = extract_socket_payload(tcp_socket);
        let target = tcp_socket.local_endpoint().map(target_from_endpoint);
        payload.map(|p| (p, target))
    };

    if let Some((payload, target)) = extracted {
        handle_local_payload(handle, payload, target, socket_ctxs, ctrl, global_tx).await?;
    }

    Ok(())
}

async fn handle_local_payload(
    handle: SocketHandle,
    payload: Vec<u8>,
    target: Option<TargetAddr>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ctrl: &mut yamux::Control,
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
    let request = RelayRequest::Tcp { target };
    let stream = open_remote_session(ctrl, &request).await?;
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
async fn handle_remote_payload(
    handle: SocketHandle,
    payload: Vec<u8>,
    sockets: &mut SocketSet<'_>,
    socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    iface: &mut Interface,
    device: &mut VirtualTunDevice,
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

    if let Err(e) = tcp_socket.send_slice(&payload) {
        println!("写本地 socket 失败 {:?}: {:?}，丢弃该回程", handle, e);
        return Ok(());
    }
    ctx.state = SocketState::Relaying;
    println!("✅ 成功将远端回信发给本地浏览器！");

    let timestamp = smoltcp::time::Instant::now();
    iface.poll(timestamp, device, sockets);
    device.flush_tx().await
}

fn spawn_remote_relay(
    handle: SocketHandle,
    mut tokio_yamux_stream: Compat<yamux::Stream>,
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
                            match tokio_yamux_stream.write_all(&payload).await {
                                Ok(_) => {
                                    println!("✅ 成功发送 {} 字节数据到远端", payload.len());
                                }
                                Err(e) => {
                                    println!("写入 Yamux 流失败: {:?}", e);
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
                remote_msg = tokio_yamux_stream.read(&mut buf) => {
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
    fn backoff_delay_full_jitter_lower_bound_is_zero() {
        assert_eq!(backoff_delay(0, 0.0), std::time::Duration::ZERO);
        assert_eq!(backoff_delay(10, 0.0), std::time::Duration::ZERO);
    }

    #[test]
    fn backoff_delay_attempt_zero_upper_is_base() {
        let d = backoff_delay(0, 1.0_f64.next_down());
        assert!(d < std::time::Duration::from_millis(RECONNECT_BASE_MS));
        assert!(d >= std::time::Duration::from_millis(RECONNECT_BASE_MS * 99 / 100));
    }

    #[test]
    fn backoff_delay_is_capped() {
        let d = backoff_delay(30, 1.0_f64.next_down());
        assert!(d <= std::time::Duration::from_millis(RECONNECT_CAP_MS));
        assert!(d >= std::time::Duration::from_millis(RECONNECT_CAP_MS * 99 / 100));
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
        };

        rearm_socket(&mut socket, &mut ctx);

        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.uplink_tx.is_none());
    }

    #[test]
    fn tun_runtime_config_defaults_match_stage9_behavior() {
        let config = TunRuntimeConfig::from_sources(None, None, None, None)
            .expect("config should load");

        // Stage 9 drops local_port; pool_size default lowered to 2 (per-port now).
        assert_eq!(config.listener.pool_size, 2);
    }

    #[test]
    fn tun_runtime_config_rejects_zero_pool_size() {
        let err = TunRuntimeConfig::from_sources(Some("0"), None, None, None)
            .expect_err("zero pool size should fail");
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn tun_runtime_config_accepts_pool_size_override() {
        let config = TunRuntimeConfig::from_sources(Some("3"), None, None, None)
            .expect("valid config should load");

        assert_eq!(config.listener.pool_size, 3);
    }

    #[test]
    fn tun_runtime_config_defaults_include_upstream_values() {
        let config = TunRuntimeConfig::from_sources(None, None, None, None)
            .expect("config should load");

        assert_eq!(config.listener.pool_size, 2);
        assert_eq!(config.upstream.server_addr, "127.0.0.1:8081");
        assert_eq!(config.upstream.tls_sni, "localhost");
        assert_eq!(config.tls.ca_path, "cert.pem");
    }

    #[test]
    fn tun_runtime_config_accepts_listener_and_upstream_overrides() {
        let config = TunRuntimeConfig::from_sources(
            Some("4"),
            Some("127.0.0.1:9000"),
            Some("example.com"),
            Some("certs/dev/ca-cert.pem"),
        )
        .expect("config should load");

        assert_eq!(config.listener.pool_size, 4);
        assert_eq!(config.upstream.server_addr, "127.0.0.1:9000");
        assert_eq!(config.upstream.tls_sni, "example.com");
        assert_eq!(config.tls.ca_path, "certs/dev/ca-cert.pem");
    }

    #[test]
    fn tun_runtime_config_rejects_invalid_upstream_server_addr() {
        let err = TunRuntimeConfig::from_sources(None, Some("bad-addr"), None, None)
            .expect_err("invalid upstream server addr should fail");
        assert!(err
            .to_string()
            .contains("invalid upstream server addr"));
    }

    #[test]
    fn tun_runtime_config_rejects_invalid_upstream_tls_sni() {
        let err = TunRuntimeConfig::from_sources(None, None, Some("bad sni"), None)
            .expect_err("invalid upstream tls sni should fail");
        assert!(err.to_string().contains("invalid upstream tls sni"));
    }

    #[test]
    fn tun_tls_config_defaults_match_existing_behavior() {
        let config = TunTlsConfig::from_sources(None).expect("config should load");
        assert_eq!(config.ca_path, "cert.pem");
    }

    #[test]
    fn tun_tls_config_accepts_override_path() {
        let config = TunTlsConfig::from_sources(Some("certs/dev/ca-cert.pem"))
            .expect("config should load");
        assert_eq!(config.ca_path, "certs/dev/ca-cert.pem");
    }
}
