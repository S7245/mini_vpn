//! Stage 12 QUIC datagram data plane — shared server/client config + endpoint builders.
//!
//! 中文要点：QUIC 用 TLS 1.3，复用现有 rustls 0.21 证书材料（quinn 0.10 依赖 rustls ^0.21，
//! 单一 rustls 版本，见 docs/adr/0003）。ALPN 必须设且两端一致，否则握手不成。

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use quinn::congestion::{BbrConfig, CubicConfig};
use quinn::{ClientConfig, Endpoint, IdleTimeout, MtuDiscoveryConfig, TransportConfig};
use rustls::{Certificate, RootCertStore};

/// QUIC ALPN：握手必须协商；client/server 一致。
pub const QUIC_ALPN: &[u8] = b"mvpn";

/// 数据面空闲超时：15s（刀9 从 30s 降——见下）。两端取 min 生效。
/// 中文要点（刀9，grill 裁决 2026-06-25）：此值 = failover 黑洞**检测下限**——QUIC `open_tcp` 在黑洞连接上
/// 乐观返回 Ok（开流是本地操作、不等服务端），failover 看不到失败，只能等 quinn 在 idle 超时把连接判死
/// （`close_reason`），下次 open 重连失败（5s）才切 REALITY，即「切换 ≈ idle + 5s」。30s 太慢（~35s 黑洞）；
/// 降到 15s → ~20s 切。**不会增加误切**：healthy 连接靠 keepalive=5s（3 PING/窗口）永不 idle 到阈值；
/// 弱网下 15–30s 瞬时中断只会自愈成一次廉价重连（~1-RTT），failover 还要求「重连也失败」才触发。
const QUIC_MAX_IDLE_SECS: u64 = 15;
/// keep-alive 间隔：5s。中文要点：必须明显小于本端 idle(15s) 与「对端可能的空闲超时」才能续命。
/// 取 5s（3 PING/15s 窗口，丢 1-2 个也续得上）：即便对端跑**旧二进制**（quinn 默认 idle=10s、无 keep-alive，
/// 协商后 idle=min=10s），每 5s 的 PING 也能在 10s 触发前重置对端 idle 计时器 → 连接不闪断（抗版本错配，稳优先）。
const QUIC_KEEPALIVE_SECS: u64 = 5;

/// 数据面起步 MTU：1280（IPv6 最小 MTU，任何真实路径都支持）。中文要点：quinn 默认 1200，
/// 此时 max_datagram_size ~1162，装不下「1200B 内层包(典型 QUIC initial) + ~20B 头 ≈ 1224」——
/// 冷连接(刚连上、PLPMTUD 没探完)发大包会被丢。起步设 1280 → max_datagram ~1242 → 立刻装得下，
/// 消除冷窗口；PLPMTUD 仍会继续往上探（~1414）拿更多余量。1280 普适安全，不会黑洞。
pub const QUIC_INITIAL_MTU: u16 = 1280;

/// 接收侧 `max_udp_payload_size` 传输参数（刀3）：告诉对端「我方单个 UDP 载荷最大能收多大」。
/// 中文要点（已核 quinn-proto-0.10.6）：**这是接收侧 headroom，不决定我方发送 datagram 上限**——
/// 发送上限 = `min(current_mtu 推导, peer.max_datagram_frame_size)`，由 MTU/PLPMTUD 决定（见 `send_udp`）。
/// 取 1472 = 1500 以太网 MTU − 28（IP+UDP 头），与默认一致、匹配普通互联网路径；显式设以**可见可调**：
/// 仅 jumbo-frame/回环等大 MTU 路径抬高才有收益（代价是接收缓冲线性增大），是否抬由真出口 probe 定档。
const QUIC_MAX_UDP_PAYLOAD_SIZE: u16 = 1472;

/// 拥塞控制器选择（刀3.5）。中文要点：quinn 默认 Cubic；高 RTT/丢包跨境路径 BBR 通常显著优于
/// Cubic（model-based，不因丢包腰斩 cwnd）——这是刀3 acceptance datagram ~5.3M 天花板的一大成因。
/// `quinn-proto 0.10.6` 已导出 `BbrConfig`（已查证），故可接。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CcChoice {
    Bbr,
    Cubic,
}

/// QUIC MTU policy. Default keeps the production path at IPv6-safe 1280 with PLPMTUD enabled;
/// Safe1200 is a bounded diagnostic/product profile for paths where post-handshake MTU probing may
/// be causing stream delivery stalls.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MtuPolicy {
    Default,
    Safe1200,
}

impl MtuPolicy {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Safe1200 => "safe1200",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct MtuPolicyProfile {
    initial_mtu: u16,
    min_mtu: u16,
    plpmtud_enabled: bool,
}

/// 纯解析：拥塞控制器名（大小写不敏感）→ `(选择, 是否回落)`。
/// 未知/空名回落 Cubic（quinn 默认），第二位 `true` 供调用方打一行告警（失败自愈不致命）。
pub fn parse_cc(name: &str) -> (CcChoice, bool) {
    match name.to_ascii_lowercase().as_str() {
        "bbr" => (CcChoice::Bbr, false),
        "cubic" => (CcChoice::Cubic, false),
        _ => (CcChoice::Cubic, true),
    }
}

/// 纯解析：MTU policy 名（大小写不敏感）→ `(策略, 是否回落)`。
/// 未知值回落 default；空/缺省不是错误，保持默认生产策略。
pub fn parse_mtu_policy(name: Option<&str>) -> (MtuPolicy, bool) {
    match name.map(str::trim).filter(|v| !v.is_empty()) {
        None => (MtuPolicy::Default, false),
        Some(v) => match v.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
            "default" => (MtuPolicy::Default, false),
            "safe1200" | "mtu1200" | "safe" => (MtuPolicy::Safe1200, false),
            _ => (MtuPolicy::Default, true),
        },
    }
}

/// 下行 uni-stream 并发配额（刀3.5）：quinn 默认仅 **100**，而 quic-relay-mode 每包一条 uni-stream，
/// 4K 下行 ~2600pps × ~1RTT(0.25s) ≈ 650 条在飞、多 flow(~33M) ≈ 850 条 → 默认 100 会让下行 stream
/// 一开就阻塞塌缩（TUIC issue #221）。抬到 4096：按需建流、空闲不预分配，上限不是预分配开销。
pub const QUIC_MAX_CONCURRENT_UNI_STREAMS: u32 = 4096;
/// 入站 bidi-stream 并发配额。TUIC TCP Connect 主要由客户端开 bidi stream，但保持一个高于 quinn
/// 默认 100 的显式上限，避免未来 server-initiated/control stream 误踩默认值；总接收内存仍由
/// `QUIC_RECEIVE_WINDOW_BYTES` 约束。
pub const QUIC_MAX_CONCURRENT_BIDI_STREAMS: u32 = 512;
/// 单 stream 接收窗口。quinn 默认按 100ms × 100Mbit/s 估算约 1.25MB；跨 VPS / 跨境链路 RTT
/// 更高时，reverse/downlink TCP 会被窗口周期性卡住。8MB 覆盖约 250Mbit/s × 250ms 的 BDP，
/// 同时仍足够小，避免单 stream 长时间吞掉所有接收缓冲。
pub const QUIC_STREAM_RECEIVE_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
/// 连接级接收窗口。多条 TCP stream 同时下行时需要高于单 stream 窗口；32MB 覆盖高并发测试，
/// 并为 `max_concurrent_bidi_streams * stream_receive_window` 提供实际内存上界。
pub const QUIC_RECEIVE_WINDOW_BYTES: u32 = 32 * 1024 * 1024;
/// 本端发送窗口上限。默认约 10MB，长 RTT 或多 stream forward 时容易在应用写入处表现成
/// `tcp-local-write-pressure`。32MB 给高吞吐 TCP 留出更接近真实 BDP 的在飞空间。
pub const QUIC_SEND_WINDOW_BYTES: u64 = 32 * 1024 * 1024;
/// QUIC UDP socket recv/send buffer 目标。Knife14ff/fg 证明本地 stream polling 已足够勤快，
/// 但仍有秒级 ordered-read gap；给 UDP ingress 留 8MB OS 缓冲，避免短调度抖动或 burst 重排把
/// QUIC stream 卡成 HOL。Linux root 下会尝试 `SO_*BUFFORCE` 突破较小的 net.core 上限。
pub const QUIC_UDP_SOCKET_BUFFER_BYTES: usize = 8 * 1024 * 1024;
pub const QUIC_MIN_UDP_SOCKET_BUFFER_BYTES: usize = 256 * 1024;
pub const QUIC_MAX_UDP_SOCKET_BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuicUdpSocketBufferSizes {
    pub requested_bytes: usize,
    pub recv_bytes: usize,
    pub send_bytes: usize,
}

fn parse_quic_udp_socket_buffer_bytes(raw: Option<&str>) -> (usize, bool) {
    let Some(value) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return (QUIC_UDP_SOCKET_BUFFER_BYTES, false);
    };
    match value.parse::<usize>() {
        Ok(bytes) => (
            bytes.clamp(
                QUIC_MIN_UDP_SOCKET_BUFFER_BYTES,
                QUIC_MAX_UDP_SOCKET_BUFFER_BYTES,
            ),
            false,
        ),
        Err(_) => (QUIC_UDP_SOCKET_BUFFER_BYTES, true),
    }
}

fn configure_quic_udp_socket_buffers(
    socket: &std::net::UdpSocket,
    requested_bytes: usize,
) -> Result<QuicUdpSocketBufferSizes, String> {
    let socket_ref = socket2::SockRef::from(socket);
    socket_ref
        .set_recv_buffer_size(requested_bytes)
        .map_err(|e| format!("set SO_RCVBUF={requested_bytes}: {e}"))?;
    socket_ref
        .set_send_buffer_size(requested_bytes)
        .map_err(|e| format!("set SO_SNDBUF={requested_bytes}: {e}"))?;

    #[cfg(target_os = "linux")]
    {
        let recv = socket_ref.recv_buffer_size().unwrap_or(0);
        if recv < requested_bytes {
            force_linux_socket_buffer(socket, libc::SO_RCVBUFFORCE, requested_bytes);
        }
        let send = socket_ref.send_buffer_size().unwrap_or(0);
        if send < requested_bytes {
            force_linux_socket_buffer(socket, libc::SO_SNDBUFFORCE, requested_bytes);
        }
    }

    Ok(QuicUdpSocketBufferSizes {
        requested_bytes,
        recv_bytes: socket_ref
            .recv_buffer_size()
            .map_err(|e| format!("get SO_RCVBUF: {e}"))?,
        send_bytes: socket_ref
            .send_buffer_size()
            .map_err(|e| format!("get SO_SNDBUF: {e}"))?,
    })
}

#[cfg(target_os = "linux")]
fn force_linux_socket_buffer(socket: &std::net::UdpSocket, opt: libc::c_int, bytes: usize) {
    let value = bytes as libc::c_int;
    unsafe {
        let _ = libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            opt,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        );
    }
}

/// 共享的 QUIC 传输参数：keep-alive + 拉长 idle + 起步 MTU + CC + uni-stream 配额（datagram 等其余默认）。
/// 中文要点（刀3.5）：装 `congestion_controller_factory`（quinn 默认 Cubic；高 RTT/丢包跨境 BBR 通常更优）
/// + 抬 `max_concurrent_uni_streams`（避 #221）。
fn quic_transport_config(cc: CcChoice, mtu_policy: MtuPolicy) -> Arc<TransportConfig> {
    let mut t = TransportConfig::default();
    let idle = IdleTimeout::try_from(Duration::from_secs(QUIC_MAX_IDLE_SECS))
        .expect("idle timeout fits VarInt");
    t.max_idle_timeout(Some(idle));
    t.keep_alive_interval(Some(Duration::from_secs(QUIC_KEEPALIVE_SECS)));
    apply_mtu_policy(&mut t, mtu_policy);
    t.max_concurrent_bidi_streams(QUIC_MAX_CONCURRENT_BIDI_STREAMS.into());
    t.max_concurrent_uni_streams(QUIC_MAX_CONCURRENT_UNI_STREAMS.into());
    t.stream_receive_window(quic_stream_receive_window_bytes(mtu_policy).into());
    t.receive_window(quic_receive_window_bytes(mtu_policy).into());
    t.send_window(QUIC_SEND_WINDOW_BYTES);
    match cc {
        CcChoice::Bbr => t.congestion_controller_factory(Arc::new(BbrConfig::default())),
        CcChoice::Cubic => t.congestion_controller_factory(Arc::new(CubicConfig::default())),
    };
    Arc::new(t)
}

pub fn quic_stream_receive_window_bytes(mtu_policy: MtuPolicy) -> u32 {
    match mtu_policy {
        MtuPolicy::Default | MtuPolicy::Safe1200 => QUIC_STREAM_RECEIVE_WINDOW_BYTES,
    }
}

pub fn quic_receive_window_bytes(mtu_policy: MtuPolicy) -> u32 {
    match mtu_policy {
        MtuPolicy::Default | MtuPolicy::Safe1200 => QUIC_RECEIVE_WINDOW_BYTES,
    }
}

fn apply_mtu_policy(t: &mut TransportConfig, mtu_policy: MtuPolicy) {
    let profile = mtu_policy_profile(mtu_policy);
    t.initial_mtu(profile.initial_mtu);
    t.min_mtu(profile.min_mtu);
    if profile.plpmtud_enabled {
        t.mtu_discovery_config(Some(MtuDiscoveryConfig::default()));
    } else {
        t.mtu_discovery_config(None);
    }
}

fn mtu_policy_profile(mtu_policy: MtuPolicy) -> MtuPolicyProfile {
    match mtu_policy {
        MtuPolicy::Default => MtuPolicyProfile {
            initial_mtu: QUIC_INITIAL_MTU,
            min_mtu: QUIC_INITIAL_MTU,
            plpmtud_enabled: true,
        },
        MtuPolicy::Safe1200 => MtuPolicyProfile {
            initial_mtu: 1200,
            min_mtu: 1200,
            plpmtud_enabled: false,
        },
    }
}

/// 构建 QUIC 客户端 config（信任给定 CA），ALPN 用本项目自有的 `mvpn`（Stage 12 数据面）。
/// 中文要点：legacy 数据面(`run_quic_pump` 不调 `into_0rtt`)**不需要也不开** 0-RTT——
/// `enable_0rtt=false` 严格保持 Stage-12 原行为（零回归），不把 0-RTT 能力泄漏到 legacy。
pub fn client_quic_config(ca_path: &str) -> Result<ClientConfig, String> {
    let crypto = client_crypto(ca_path, vec![QUIC_ALPN.to_vec()], false)?;
    // legacy 路径保持 quinn 默认 CC（Cubic），零回归——CC 选择只对 TUIC 数据面开放。
    Ok(finish_client_config(
        crypto,
        CcChoice::Cubic,
        MtuPolicy::Default,
    ))
}

/// 构建 QUIC 客户端 config，**ALPN + 拥塞控制器可指定**（TUIC 对接 sing-box 需用 `h3` 等，见 Stage 13a）。
/// 中文要点：TUIC(Stage 13)路径开 0-RTT early data（重连快速恢复，见 Stage 13c）；
/// 刀3.5 起 `cc` 由 config 决定（BBR/Cubic），贯穿到 transport config。
pub fn client_quic_config_alpn(
    ca_path: &str,
    alpn_protocols: Vec<Vec<u8>>,
    cc: CcChoice,
    mtu_policy: MtuPolicy,
) -> Result<ClientConfig, String> {
    let crypto = client_crypto(ca_path, alpn_protocols, true)?;
    Ok(finish_client_config(crypto, cc, mtu_policy))
}

/// 把 rustls 客户端配置包成 quinn `ClientConfig` 并装上共享传输参数（含选定 CC）。
fn finish_client_config(
    crypto: rustls::ClientConfig,
    cc: CcChoice,
    mtu_policy: MtuPolicy,
) -> ClientConfig {
    let mut cfg = ClientConfig::new(Arc::new(crypto));
    cfg.transport_config(quic_transport_config(cc, mtu_policy));
    cfg
}

/// 构建客户端 rustls 配置（信任 CA + ALPN + 可选 **0-RTT early data**）。
/// 中文要点：抽出为可测纯逻辑。`enable_early_data` 默认 false，仅 TUIC 0-RTT(Stage 13c)按需开；
/// rustls 默认已带内存 session cache(resumption)，重连即可复用 ticket 尝试 0-RTT，失败自动回落 1-RTT。
fn client_crypto(
    ca_path: &str,
    alpn_protocols: Vec<Vec<u8>>,
    enable_0rtt: bool,
) -> Result<rustls::ClientConfig, String> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_path)? {
        roots
            .add(&cert)
            .map_err(|e| format!("quic add ca {ca_path}: {e}"))?;
    }
    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = alpn_protocols;
    crypto.enable_early_data = enable_0rtt;
    Ok(crypto)
}

/// 绑定一个 QUIC 客户端 endpoint（本地 ephemeral UDP 端口）并装上 client config。
/// 中文要点（刀3）：经 `Endpoint::new` 注入自定义 `EndpointConfig`，显式设 `max_udp_payload_size`
/// （接收侧 headroom，见常量注释）；replaces `Endpoint::client`（其用默认 EndpointConfig，旋钮不可调）。
pub fn client_endpoint(cfg: ClientConfig) -> Result<Endpoint, String> {
    let bind: SocketAddr = "0.0.0.0:0".parse().expect("valid bind addr");
    let socket = std::net::UdpSocket::bind(bind).map_err(|e| format!("quic client bind: {e}"))?;
    let (udp_buffer_bytes, udp_buffer_fell_back) = parse_quic_udp_socket_buffer_bytes(
        std::env::var("MINI_VPN_QUIC_UDP_SOCKET_BUFFER_BYTES")
            .ok()
            .as_deref(),
    );
    if udp_buffer_fell_back {
        println!(
            "⚠️ MINI_VPN_QUIC_UDP_SOCKET_BUFFER_BYTES 无效，回落 {}B",
            QUIC_UDP_SOCKET_BUFFER_BYTES
        );
    }
    match configure_quic_udp_socket_buffers(&socket, udp_buffer_bytes) {
        Ok(sizes) => println!(
            "🧺 QUIC UDP socket buffers: requested={}B recv={}B send={}B",
            sizes.requested_bytes, sizes.recv_bytes, sizes.send_bytes
        ),
        Err(e) => println!("⚠️ QUIC UDP socket buffer 配置失败，继续使用系统默认: {e}"),
    }
    let runtime =
        quinn::default_runtime().ok_or_else(|| "no async runtime for quic endpoint".to_string())?;
    let mut ep_cfg = quinn::EndpointConfig::default();
    ep_cfg
        .max_udp_payload_size(QUIC_MAX_UDP_PAYLOAD_SIZE)
        .map_err(|e| format!("quic max_udp_payload_size: {e:?}"))?;
    let mut ep = Endpoint::new(ep_cfg, None, socket, runtime)
        .map_err(|e| format!("quic client endpoint: {e}"))?;
    ep.set_default_client_config(cfg);
    Ok(ep)
}

fn load_certs(path: &str) -> Result<Vec<Certificate>, String> {
    let f = File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let mut r = BufReader::new(f);
    let certs = rustls_pemfile::certs(&mut r).map_err(|e| format!("read certs {path}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("no certificates in {path}"));
    }
    Ok(certs.into_iter().map(Certificate).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cc_maps_and_falls_back() {
        // 已知名（大小写不敏感）→ 对应控制器，无回落。
        assert_eq!(parse_cc("bbr"), (CcChoice::Bbr, false));
        assert_eq!(parse_cc("BBR"), (CcChoice::Bbr, false));
        assert_eq!(parse_cc("cubic"), (CcChoice::Cubic, false));
        // 未知/空 → 回落 Cubic（quinn 默认），flag=true 供调用方告警。
        assert_eq!(parse_cc(""), (CcChoice::Cubic, true));
        assert_eq!(parse_cc("reno"), (CcChoice::Cubic, true));
    }

    #[test]
    fn parse_mtu_policy_maps_safe_profile_and_falls_back() {
        assert_eq!(parse_mtu_policy(None), (MtuPolicy::Default, false));
        assert_eq!(parse_mtu_policy(Some("")), (MtuPolicy::Default, false));
        assert_eq!(
            parse_mtu_policy(Some("default")),
            (MtuPolicy::Default, false)
        );
        assert_eq!(
            parse_mtu_policy(Some("safe1200")),
            (MtuPolicy::Safe1200, false)
        );
        assert_eq!(
            parse_mtu_policy(Some("safe-1200")),
            (MtuPolicy::Safe1200, false)
        );
        assert_eq!(
            parse_mtu_policy(Some("mtu1200")),
            (MtuPolicy::Safe1200, false)
        );
        assert_eq!(parse_mtu_policy(Some("jumbo")), (MtuPolicy::Default, true));
    }

    #[test]
    fn client_config_builds_with_dev_ca() {
        let cfg = client_quic_config("certs/dev/ca-cert.pem");
        assert!(cfg.is_ok(), "{:?}", cfg.err());
    }

    #[test]
    fn client_crypto_toggles_0rtt_early_data() {
        // TUIC(Stage 13c)显式开 early data;legacy 路径关(回到 Stage-12 原行为,零回归)。
        let on = client_crypto("certs/dev/ca-cert.pem", vec![b"h3".to_vec()], true).unwrap();
        assert!(on.enable_early_data, "TUIC 必须启用 0-RTT early data");
        assert_eq!(on.alpn_protocols, vec![b"h3".to_vec()]);
        let off = client_crypto("certs/dev/ca-cert.pem", vec![QUIC_ALPN.to_vec()], false).unwrap();
        assert!(!off.enable_early_data, "legacy 不应开 0-RTT early data");
    }

    // Endpoint::client 需要 tokio 运行时上下文（真实运行在 #[tokio::main] 下）。
    #[tokio::test]
    async fn client_endpoint_binds() {
        let cfg = client_quic_config("certs/dev/ca-cert.pem").unwrap();
        assert!(client_endpoint(cfg).is_ok());
    }

    // 刀3.5：BBR/Cubic 两种 CC 都能装进 transport config 并 bind（真 CC 生效靠 acceptance 验，
    // 此处只锁"装得上、bind 绿"，factory 本身 opaque 不可断言类型）。
    #[tokio::test]
    async fn client_endpoint_binds_with_each_cc() {
        for cc in [CcChoice::Bbr, CcChoice::Cubic] {
            let cfg = client_quic_config_alpn(
                "certs/dev/ca-cert.pem",
                vec![b"h3".to_vec()],
                cc,
                MtuPolicy::Default,
            )
            .unwrap();
            assert!(client_endpoint(cfg).is_ok(), "bind failed for {cc:?}");
        }
    }

    #[test]
    fn transport_config_sets_vpn_flow_control_windows() {
        let cfg = quic_transport_config(CcChoice::Cubic, MtuPolicy::Default);
        let dbg = format!("{cfg:?}");
        assert_eq!(
            quic_stream_receive_window_bytes(MtuPolicy::Default),
            QUIC_STREAM_RECEIVE_WINDOW_BYTES
        );
        assert_eq!(
            quic_receive_window_bytes(MtuPolicy::Default),
            QUIC_RECEIVE_WINDOW_BYTES
        );
        assert!(
            dbg.contains(&format!(
                "max_concurrent_bidi_streams: {}",
                QUIC_MAX_CONCURRENT_BIDI_STREAMS
            )),
            "{dbg}"
        );
        assert!(
            dbg.contains(&format!(
                "max_concurrent_uni_streams: {}",
                QUIC_MAX_CONCURRENT_UNI_STREAMS
            )),
            "{dbg}"
        );
        assert!(
            dbg.contains(&format!(
                "stream_receive_window: {}",
                QUIC_STREAM_RECEIVE_WINDOW_BYTES
            )),
            "{dbg}"
        );
        assert!(
            dbg.contains(&format!("receive_window: {}", QUIC_RECEIVE_WINDOW_BYTES)),
            "{dbg}"
        );
        assert!(
            dbg.contains(&format!("send_window: {}", QUIC_SEND_WINDOW_BYTES)),
            "{dbg}"
        );
    }

    #[test]
    fn safe1200_mtu_policy_disables_plpmtud_and_keeps_floor() {
        assert_eq!(
            mtu_policy_profile(MtuPolicy::Default),
            MtuPolicyProfile {
                initial_mtu: QUIC_INITIAL_MTU,
                min_mtu: QUIC_INITIAL_MTU,
                plpmtud_enabled: true,
            }
        );
        assert_eq!(
            mtu_policy_profile(MtuPolicy::Safe1200),
            MtuPolicyProfile {
                initial_mtu: 1200,
                min_mtu: 1200,
                plpmtud_enabled: false,
            }
        );

        let cfg = quic_transport_config(CcChoice::Cubic, MtuPolicy::Safe1200);
        let dbg = format!("{cfg:?}");
        assert_eq!(
            quic_stream_receive_window_bytes(MtuPolicy::Safe1200),
            QUIC_STREAM_RECEIVE_WINDOW_BYTES
        );
        assert_eq!(
            quic_receive_window_bytes(MtuPolicy::Safe1200),
            QUIC_RECEIVE_WINDOW_BYTES
        );
        assert!(
            dbg.contains(&format!(
                "stream_receive_window: {}",
                QUIC_STREAM_RECEIVE_WINDOW_BYTES
            )),
            "{dbg}"
        );
        assert!(
            dbg.contains(&format!("receive_window: {}", QUIC_RECEIVE_WINDOW_BYTES)),
            "{dbg}"
        );
        assert!(
            dbg.contains(&format!("send_window: {}", QUIC_SEND_WINDOW_BYTES)),
            "safe1200 policy must still keep the shared VPN send window"
        );
    }

    #[test]
    fn parse_quic_udp_socket_buffer_bytes_defaults_and_clamps() {
        assert_eq!(
            parse_quic_udp_socket_buffer_bytes(None),
            (QUIC_UDP_SOCKET_BUFFER_BYTES, false)
        );
        assert_eq!(
            parse_quic_udp_socket_buffer_bytes(Some("")),
            (QUIC_UDP_SOCKET_BUFFER_BYTES, false)
        );
        assert_eq!(
            parse_quic_udp_socket_buffer_bytes(Some("1024")),
            (QUIC_MIN_UDP_SOCKET_BUFFER_BYTES, false)
        );
        assert_eq!(
            parse_quic_udp_socket_buffer_bytes(Some("999999999999")),
            (QUIC_MAX_UDP_SOCKET_BUFFER_BYTES, false)
        );
        assert_eq!(
            parse_quic_udp_socket_buffer_bytes(Some("nope")),
            (QUIC_UDP_SOCKET_BUFFER_BYTES, true)
        );
    }

    #[test]
    fn quic_udp_socket_buffer_config_sets_observable_socket_buffers() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let sizes =
            configure_quic_udp_socket_buffers(&socket, QUIC_MIN_UDP_SOCKET_BUFFER_BYTES).unwrap();

        assert_eq!(sizes.requested_bytes, QUIC_MIN_UDP_SOCKET_BUFFER_BYTES);
        assert!(sizes.recv_bytes > 0, "{sizes:?}");
        assert!(sizes.send_bytes > 0, "{sizes:?}");
    }
}
