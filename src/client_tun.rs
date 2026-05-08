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
    spec: ListenerSpec,
    handles: Vec<SocketHandle>,
}

pub async fn start_tun_proxy() {
    let listener_spec = ListenerSpec {
        local_port: DEFAULT_TUN_LISTEN_PORT,
        pool_size: DEFAULT_TUN_POOL_SIZE,
    };
    let default_target =
        TargetAddr::parse(DEFAULT_TUN_TARGET).expect("默认 TUN 目标地址必须合法");

    let mut root_cert_store = RootCertStore::empty();
    let cert_file = &mut BufReader::new(File::open("cert.pem").unwrap());
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

    // 1. 初始化 TUN 设备(化底层网卡和通道) / 创建操作系统的原生异步虚拟网卡
    let raw_tun = create_tun_device().await;
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
    println!(
        "🚀 TUN 虚拟网卡主循环启动！监听端口 {}，当前槽位数 {}",
        listener_pool.spec.local_port,
        listener_pool.spec.pool_size
    );

    let domain = match ServerName::try_from("localhost") {
        Ok(domain) => domain,
        Err(e) => {
            println!("解析 SNI 域名失败: {e:?}");
            return;
        }
    };

    let server_stream = TcpStream::connect("127.0.0.1:8081")
        .await
        .expect("TcpStream 连接错误");

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
        ListenerPool {
            spec: *spec,
            handles,
        },
        socket_ctxs,
    )
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

pub async fn create_tun_device() -> tun::AsyncDevice {
    let mut config = tun::Configuration::default();

    config
        .address((10, 0, 0, 1)) // 网卡的 IP 地址
        .destination((10, 0, 0, 2)) // 🌟 新增：告诉 OS 水管另一头是谁！
        .netmask((255, 255, 255, 0)) // 子网掩码
        .up(); // 启动网卡

    #[cfg(target_os = "macos")]
    config.layer(tun::Layer::L3); // macOS 通常需要显式指定三层（IP层）

    // 创建异步读取的 TUN 设备
    tun::create_as_async(&config).expect("无法创建 TUN 设备")
}

#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::iface::SocketSet;

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
}
