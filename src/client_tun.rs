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

const DEFAULT_TUN_LISTEN_PORT: u16 = 80;
const DEFAULT_TUN_TARGET: &str = "httpbin.org:80";
const TCP_SOCKET_BUFFER_SIZE: usize = 65_535;
const RELAY_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_TUN_POOL_SIZE: usize = 4;
const DEFAULT_TUN_SERVER_ADDR: &str = "127.0.0.1:8081";
const DEFAULT_TUN_TLS_SNI: &str = "localhost";
const DEFAULT_TUN_CA_PATH: &str = "cert.pem";

/// Describes how many local TCP listener slots the TUN runtime should create.
/// 中文要点：这是监听池的蓝图，不代表连接本身，只描述“开几间房、监听哪个端口”。
#[derive(Debug, Clone, Copy)]
struct ListenerSpec {
    /// Local TCP port intercepted on the TUN-side smoltcp stack.
    /// 中文要点：这是虚拟网卡内侧被 smoltcp 截获的本地端口。
    local_port: u16,
    /// Number of independent listener slots created for the same local port.
    /// 中文要点：用多个监听槽位模拟 backlog，避免单 socket 退房时堵住后续连接。
    pool_size: usize,
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
    /// Default remote target used by the current TCP-over-TUN demo path.
    target: TargetAddr,
    /// Sender used to push local payloads into the remote relay task for this slot only.
    uplink_tx: Option<mpsc::Sender<Vec<u8>>>,
}

impl SocketCtx {
    /// Create the initial per-slot runtime context.
    /// 中文要点：每个新建的监听槽位一开始都处于 Listening，没有绑定上行通道。
    fn new(local_port: u16, target: TargetAddr) -> Self {
        Self {
            local_port,
            state: SocketState::Listening,
            target,
            uplink_tx: None,
        }
    }
}

/// Holds every smoltcp listener handle that belongs to the same logical local port pool.
/// 中文要点：后续主循环只遍历这个池，而不是盯着单个裸 `socket_handle`。
#[derive(Debug)]
struct ListenerPool {
    handles: Vec<SocketHandle>,
}

/// Local listener-side startup configuration for the TUN runtime.
/// 中文要点：这一层只关心本地拦截面，不关心怎么连上游 TLS/Yamux 服务。
#[derive(Debug, Clone)]
struct TunListenerConfig {
    /// Local TCP port intercepted by the TUN-side smoltcp stack.
    /// 中文要点：虚拟网卡这一侧实际监听的本地端口。
    local_port: u16,
    /// Default remote relay target for the current TCP-over-TUN demo path.
    /// 中文要点：当前 TUN demo 默认转发到的远端目标。
    target_addr: TargetAddr,
    /// Number of listener slots created for the same local port.
    /// 中文要点：监听池槽位数，决定会创建多少个独立的监听房间。
    pool_size: usize,
}

impl TunListenerConfig {
    /// Build listener config from optional string sources.
    /// 中文要点：本地监听配置与上游外联配置分开解析，避免职责混淆。
    fn from_sources(
        local_port: Option<&str>,
        target_addr: Option<&str>,
        pool_size: Option<&str>,
    ) -> Result<Self, ClientError> {
        let local_port = match local_port {
            Some(value) => value
                .parse::<u16>()
                .map_err(|_| ClientError::InvalidTarget(format!("invalid local port: {value}")))?,
            None => DEFAULT_TUN_LISTEN_PORT,
        };

        let target_addr = match target_addr {
            Some(value) => TargetAddr::parse(value)?,
            None => TargetAddr::parse(DEFAULT_TUN_TARGET)?,
        };

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

        Ok(Self {
            local_port,
            target_addr,
            pool_size,
        })
    }

    /// Derive the listener-pool blueprint from startup config.
    /// 中文要点：监听池蓝图依然只从 listener 配置派生，不受 upstream 字段影响。
    fn listener_spec(&self) -> ListenerSpec {
        ListenerSpec {
            local_port: self.local_port,
            pool_size: self.pool_size,
        }
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
        local_port: Option<&str>,
        target_addr: Option<&str>,
        pool_size: Option<&str>,
        server_addr: Option<&str>,
        tls_sni: Option<&str>,
        ca_path: Option<&str>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            listener: TunListenerConfig::from_sources(local_port, target_addr, pool_size)?,
            upstream: TunUpstreamConfig::from_sources(server_addr, tls_sni)?,
            tls: TunTlsConfig::from_sources(ca_path)?,
        })
    }

    /// Read config from process environment.
    /// 中文要点：Stage 6 在 Stage 5 基础上新增 upstream 配置入口，但仍保持最小环境变量方案。
    fn from_env() -> Result<Self, ClientError> {
        let local_port = std::env::var("MINI_VPN_TUN_LOCAL_PORT").ok();
        let target_addr = std::env::var("MINI_VPN_TUN_TARGET_ADDR").ok();
        let pool_size = std::env::var("MINI_VPN_TUN_POOL_SIZE").ok();
        let server_addr = std::env::var("MINI_VPN_TUN_SERVER_ADDR").ok();
        let tls_sni = std::env::var("MINI_VPN_TUN_TLS_SNI").ok();
        let ca_path = std::env::var("MINI_VPN_TUN_CA_PATH").ok();

        Self::from_sources(
            local_port.as_deref(),
            target_addr.as_deref(),
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
    let listener_spec = runtime_config.listener.listener_spec();
    let default_target = runtime_config.listener.target_addr.clone();
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
        "🚀 TUN runtime started with local_port={}, pool_size={}, target={}, server_addr={}, tls_sni={}, ca_path={}",
        listener_spec.local_port,
        listener_spec.pool_size,
        default_target.to_wire_string(),
        upstream_server_addr,
        upstream_tls_sni,
        tls_ca_path
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

    let (listener_pool, mut socket_ctxs) =
        build_listener_pool(&mut sockets, &listener_spec, &default_target);

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

    // 3. 初始化定时器 (例如每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));

    let domain = match ServerName::try_from(upstream_tls_sni.as_str()) {
        Ok(domain) => domain,
        Err(e) => {
            println!("解析 SNI 域名失败: {e:?}");
            return;
        }
    };

    let server_stream = match TcpStream::connect(upstream_server_addr.as_str()).await {
        Ok(stream) => stream,
        Err(e) => {
            println!("连接代理服务端失败 {upstream_server_addr}: {e}");
            return;
        }
    };

     let tls_stream = match connector.clone().connect(domain, server_stream).await {
        Ok(s) => s,
        Err(e) => {
            println!("与代理服务端 TLS 握手失败: {:?}", e);
            return;
        }
    };
    println!("✅ 成功连接到洛杉矶代理服务器！");
    let mut yamux_conn =
        Connection::new(tls_stream.compat(), YamuxConfig::default(), Mode::Client);
    // 获取遥控器 ctr
    let ctr = yamux_conn.control();

    //使用 tokio::spawn 把 Yamux 引擎放到后台 poll 运行。🚂
    tokio::spawn(async move {
        while let Ok(Some(_)) = yamux_conn.next_stream().await {}
        println!("与服务端的 Yamux 长连接已断开，请重启 Client");
    });

    loop {
        let mut ctrl = ctr.clone();
        tokio::select! {
            // 在 tokio::select! 里面，模仿网卡或定时器的写法，增加一个新的监听分支来接收 global_rx 里的数据吗？
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
                    // 🌟 加上这行雷达代码
                    if let Some(buf) = &device.rx_buffer {
                        println!("📡 收到来自操作系统的包，大小: {} 字节", buf.len());
                        // 🌟 新增：打印前 4 个字节，看看是不是 macOS 偷偷加的料
                        println!("🔍 包的前 4 字节: {:?}", &buf[..4.min(buf.len())]);
                    }
                    let timestamp = smoltcp::time::Instant::now();

                    // ❓ 任务 1: 调用 iface 的推进方法，依次传入 timestamp, &mut device 和 &mut sockets
                    iface.poll(timestamp, &mut device, &mut sockets);

                    // ❓ 任务 2: 异步调用 device 的发货方法，把 smoltcp 产生的回包真正发给网卡
                    // 提示: 这是一个 async 方法，别忘了 .await 和错误处理 (比如 .unwrap())
                    device.flush_tx().await.unwrap();

                    for handle in &listener_pool.handles {
                        if let Err(e) = process_listener_activity(
                            *handle,
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
                // ❓ 任务 3: 这里需要做和上面完全一样的推进和发货操作
                iface.poll(timestamp, &mut device, &mut sockets);
                device.flush_tx().await.unwrap();

                for handle in &listener_pool.handles {
                    if let Err(e) = process_listener_activity(
                        *handle,
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

/// Build the real smoltcp listener pool for the TUN runtime.
/// 中文要点：一次性创建多个监听槽位，让后续连接不再依赖单个 socket 反复复位。
fn build_listener_pool(
    sockets: &mut SocketSet<'_>,
    spec: &ListenerSpec,
    default_target: &TargetAddr,
) -> (ListenerPool, HashMap<SocketHandle, SocketCtx>) {
    let mut handles = Vec::with_capacity(spec.pool_size);
    let mut socket_ctxs = HashMap::with_capacity(spec.pool_size);

    for slot_index in 0..spec.pool_size {
        let handle = sockets.add(build_listener_socket(spec));
        let ctx = SocketCtx::new(spec.local_port, default_target.clone());
        println!(
            "🧩 listener slot {} created on local port {} with handle {:?}",
            slot_index, spec.local_port, handle
        );
        handles.push(handle);
        socket_ctxs.insert(handle, ctx);
    }

    (
        ListenerPool { handles },
        socket_ctxs,
    )
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
    let payload = {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        extract_socket_payload(tcp_socket)
    };

    if let Some(payload) = payload {
        handle_local_payload(handle, payload, socket_ctxs, ctrl, global_tx).await?;
    }

    Ok(())
}

async fn handle_local_payload(
    handle: SocketHandle,
    payload: Vec<u8>,
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

    ctx.state = SocketState::OpeningRemote;
    println!("🔄 handle {:?} entering {:?}", handle, ctx.state);
    let request = RelayRequest::Tcp {
        target: ctx.target.clone(),
    };
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

    tcp_socket.send_slice(&payload).unwrap();
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

    #[test]
    fn build_listener_pool_creates_four_handles_and_contexts() {
        let spec = ListenerSpec {
            local_port: 80,
            pool_size: 4,
        };
        let default_target = TargetAddr::parse("httpbin.org:80").expect("target should parse");
        let mut sockets = SocketSet::new(vec![]);

        let (pool, socket_ctxs) = build_listener_pool(&mut sockets, &spec, &default_target);

        assert_eq!(pool.handles.len(), 4);
        assert_eq!(socket_ctxs.len(), 4);
        assert!(pool
            .handles
            .iter()
            .all(|handle| socket_ctxs.contains_key(handle)));
    }

    #[test]
    fn rearm_socket_restores_listening_state_and_clears_sender() {
        let spec = ListenerSpec {
            local_port: 80,
            pool_size: 1,
        };
        let default_target = TargetAddr::parse("httpbin.org:80").expect("target should parse");
        let mut socket = build_listener_socket(&spec);
        let (tx, _rx) = mpsc::channel(1);
        let mut ctx = SocketCtx {
            local_port: 80,
            state: SocketState::Relaying,
            target: default_target,
            uplink_tx: Some(tx),
        };

        rearm_socket(&mut socket, &mut ctx);

        assert_eq!(ctx.state, SocketState::Listening);
        assert!(ctx.uplink_tx.is_none());
    }

    #[test]
    fn tun_runtime_config_defaults_match_stage4_behavior() {
        let config = TunRuntimeConfig::from_sources(None, None, None, None, None, None)
            .expect("config should load");

        assert_eq!(config.listener.local_port, 80);
        assert_eq!(config.listener.pool_size, 4);
        assert_eq!(config.listener.target_addr.to_wire_string(), "httpbin.org:80");
    }

    #[test]
    fn tun_runtime_config_derives_listener_spec_and_target() {
        let config = TunRuntimeConfig::from_sources(
            Some("8080"),
            Some("127.0.0.1:7897"),
            Some("2"),
            None,
            None,
            None,
        )
        .expect("config should load");

        let listener_spec = config.listener.listener_spec();

        assert_eq!(listener_spec.local_port, 8080);
        assert_eq!(listener_spec.pool_size, 2);
        assert_eq!(config.listener.target_addr.to_wire_string(), "127.0.0.1:7897");
    }

    #[test]
    fn tun_runtime_config_rejects_invalid_local_port() {
        let err = TunRuntimeConfig::from_sources(Some("abc"), None, None, None, None, None)
            .expect_err("invalid port should fail");
        assert!(err.to_string().contains("invalid local port"));
    }

    #[test]
    fn tun_runtime_config_rejects_invalid_target_addr() {
        let err =
            TunRuntimeConfig::from_sources(None, Some("bad-target"), None, None, None, None)
            .expect_err("invalid target should fail");
        assert!(err.to_string().contains("invalid target"));
    }

    #[test]
    fn tun_runtime_config_rejects_zero_pool_size() {
        let err = TunRuntimeConfig::from_sources(None, None, Some("0"), None, None, None)
            .expect_err("zero pool size should fail");
        assert!(err.to_string().contains("at least 1"));
    }

    #[test]
    fn tun_runtime_config_accepts_valid_override_values() {
        let config = TunRuntimeConfig::from_sources(
            Some("8081"),
            Some("www.figma.com:443"),
            Some("3"),
            None,
            None,
            None,
        )
        .expect("valid config should load");

        assert_eq!(config.listener.local_port, 8081);
        assert_eq!(config.listener.pool_size, 3);
        assert_eq!(config.listener.target_addr.to_wire_string(), "www.figma.com:443");
    }

    #[test]
    fn tun_runtime_config_defaults_include_upstream_values() {
        let config = TunRuntimeConfig::from_sources(None, None, None, None, None, None)
            .expect("config should load");

        assert_eq!(config.listener.local_port, 80);
        assert_eq!(config.listener.pool_size, 4);
        assert_eq!(config.listener.target_addr.to_wire_string(), "httpbin.org:80");
        assert_eq!(config.upstream.server_addr, "127.0.0.1:8081");
        assert_eq!(config.upstream.tls_sni, "localhost");
        assert_eq!(config.tls.ca_path, "cert.pem");
    }

    #[test]
    fn tun_runtime_config_accepts_listener_and_upstream_overrides() {
        let config = TunRuntimeConfig::from_sources(
            Some("8080"),
            Some("127.0.0.1:7897"),
            Some("2"),
            Some("127.0.0.1:9000"),
            Some("example.com"),
            Some("certs/dev/ca-cert.pem"),
        )
        .expect("config should load");

        let listener_spec = config.listener.listener_spec();

        assert_eq!(listener_spec.local_port, 8080);
        assert_eq!(listener_spec.pool_size, 2);
        assert_eq!(config.listener.target_addr.to_wire_string(), "127.0.0.1:7897");
        assert_eq!(config.upstream.server_addr, "127.0.0.1:9000");
        assert_eq!(config.upstream.tls_sni, "example.com");
        assert_eq!(config.tls.ca_path, "certs/dev/ca-cert.pem");
    }

    #[test]
    fn tun_runtime_config_rejects_invalid_upstream_server_addr() {
        let err =
            TunRuntimeConfig::from_sources(None, None, None, Some("bad-addr"), None, None)
                .expect_err("invalid upstream server addr should fail");
        assert!(err
            .to_string()
            .contains("invalid upstream server addr"));
    }

    #[test]
    fn tun_runtime_config_rejects_invalid_upstream_tls_sni() {
        let err = TunRuntimeConfig::from_sources(None, None, None, None, Some("bad sni"), None)
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
