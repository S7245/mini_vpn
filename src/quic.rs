//! Stage 12 QUIC datagram data plane — shared server/client config + endpoint builders.
//!
//! 中文要点：QUIC 用 TLS 1.3，复用现有 rustls 0.21 证书材料（quinn 0.10 依赖 rustls ^0.21，
//! 单一 rustls 版本，见 docs/adr/0003）。ALPN 必须设且两端一致，否则握手不成。

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::{ClientConfig, Endpoint, IdleTimeout, TransportConfig};
use rustls::{Certificate, RootCertStore};

/// QUIC ALPN：握手必须协商；client/server 一致。
pub const QUIC_ALPN: &[u8] = b"mvpn";

/// 数据面空闲超时：30s（quinn 默认仅 10s，太短）。两端取 min 生效。
const QUIC_MAX_IDLE_SECS: u64 = 30;
/// keep-alive 间隔：5s。中文要点：必须明显小于「对端可能的空闲超时」才能续命。
/// 取 5s 是为了即便对端跑的是**旧二进制**（quinn 默认 idle=10s、无 keep-alive，协商后 idle=min=10s），
/// 客户端每 5s 的 PING 也能在 10s 触发前重置对端 idle 计时器 → 连接不闪断（抗版本错配，系统稳定优先）。
const QUIC_KEEPALIVE_SECS: u64 = 5;

/// 数据面起步 MTU：1280（IPv6 最小 MTU，任何真实路径都支持）。中文要点：quinn 默认 1200，
/// 此时 max_datagram_size ~1162，装不下「1200B 内层包(典型 QUIC initial) + ~20B 头 ≈ 1224」——
/// 冷连接(刚连上、PLPMTUD 没探完)发大包会被丢。起步设 1280 → max_datagram ~1242 → 立刻装得下，
/// 消除冷窗口；PLPMTUD 仍会继续往上探（~1414）拿更多余量。1280 普适安全，不会黑洞。
const QUIC_INITIAL_MTU: u16 = 1280;

/// 接收侧 `max_udp_payload_size` 传输参数（刀3）：告诉对端「我方单个 UDP 载荷最大能收多大」。
/// 中文要点（已核 quinn-proto-0.10.6）：**这是接收侧 headroom，不决定我方发送 datagram 上限**——
/// 发送上限 = `min(current_mtu 推导, peer.max_datagram_frame_size)`，由 MTU/PLPMTUD 决定（见 `send_udp`）。
/// 取 1472 = 1500 以太网 MTU − 28（IP+UDP 头），与默认一致、匹配普通互联网路径；显式设以**可见可调**：
/// 仅 jumbo-frame/回环等大 MTU 路径抬高才有收益（代价是接收缓冲线性增大），是否抬由真出口 probe 定档。
const QUIC_MAX_UDP_PAYLOAD_SIZE: u16 = 1472;

/// 共享的 QUIC 传输参数：keep-alive + 拉长 idle + 起步 MTU（datagram 等其余保持默认）。
fn quic_transport_config() -> Arc<TransportConfig> {
    let mut t = TransportConfig::default();
    let idle = IdleTimeout::try_from(Duration::from_secs(QUIC_MAX_IDLE_SECS))
        .expect("idle timeout fits VarInt");
    t.max_idle_timeout(Some(idle));
    t.keep_alive_interval(Some(Duration::from_secs(QUIC_KEEPALIVE_SECS)));
    t.initial_mtu(QUIC_INITIAL_MTU);
    t.min_mtu(QUIC_INITIAL_MTU);
    Arc::new(t)
}

/// 构建 QUIC 客户端 config（信任给定 CA），ALPN 用本项目自有的 `mvpn`（Stage 12 数据面）。
/// 中文要点：legacy 数据面(`run_quic_pump` 不调 `into_0rtt`)**不需要也不开** 0-RTT——
/// `enable_0rtt=false` 严格保持 Stage-12 原行为（零回归），不把 0-RTT 能力泄漏到 legacy。
pub fn client_quic_config(ca_path: &str) -> Result<ClientConfig, String> {
    let crypto = client_crypto(ca_path, vec![QUIC_ALPN.to_vec()], false)?;
    Ok(finish_client_config(crypto))
}

/// 构建 QUIC 客户端 config，**ALPN 可指定**（TUIC 对接 sing-box 需用 `h3` 等，见 Stage 13a）。
/// 中文要点：TUIC(Stage 13)路径开 0-RTT early data（重连快速恢复，见 Stage 13c）。
pub fn client_quic_config_alpn(
    ca_path: &str,
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<ClientConfig, String> {
    let crypto = client_crypto(ca_path, alpn_protocols, true)?;
    Ok(finish_client_config(crypto))
}

/// 把 rustls 客户端配置包成 quinn `ClientConfig` 并装上共享传输参数。
fn finish_client_config(crypto: rustls::ClientConfig) -> ClientConfig {
    let mut cfg = ClientConfig::new(Arc::new(crypto));
    cfg.transport_config(quic_transport_config());
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
}
