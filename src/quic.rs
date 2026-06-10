//! Stage 12 QUIC datagram data plane — shared server/client config + endpoint builders.
//!
//! 中文要点：QUIC 用 TLS 1.3，复用现有 rustls 0.21 证书材料（quinn 0.10 依赖 rustls ^0.21，
//! 单一 rustls 版本，见 docs/adr/0003）。ALPN 必须设且两端一致，否则握手不成。

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::{ClientConfig, Endpoint, IdleTimeout, ServerConfig, TransportConfig};
use rustls::{Certificate, PrivateKey, RootCertStore};

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

/// 构建 QUIC 服务端 config（PEM 证书链 + PKCS8 私钥 + ALPN）。
/// 中文要点：复用 server.rs 的证书加载方式，只是包成 quinn 的 crypto。
pub fn server_quic_config(cert_path: &str, key_path: &str) -> Result<ServerConfig, String> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    let mut crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("quic tls server config: {e}"))?;
    crypto.alpn_protocols = vec![QUIC_ALPN.to_vec()];
    let mut cfg = ServerConfig::with_crypto(Arc::new(crypto));
    cfg.transport_config(quic_transport_config());
    Ok(cfg)
}

/// 构建 QUIC 客户端 config（信任给定 CA），ALPN 用本项目自有的 `mvpn`（Stage 12 数据面）。
pub fn client_quic_config(ca_path: &str) -> Result<ClientConfig, String> {
    client_quic_config_alpn(ca_path, vec![QUIC_ALPN.to_vec()])
}

/// 构建 QUIC 客户端 config，**ALPN 可指定**（TUIC 对接 sing-box 需用 `h3` 等，见 Stage 13a）。
pub fn client_quic_config_alpn(
    ca_path: &str,
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<ClientConfig, String> {
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
    let mut cfg = ClientConfig::new(Arc::new(crypto));
    cfg.transport_config(quic_transport_config());
    Ok(cfg)
}

/// 绑定一个 QUIC 服务端 endpoint（监听 UDP `addr`）。
pub fn server_endpoint(cfg: ServerConfig, addr: SocketAddr) -> Result<Endpoint, String> {
    Endpoint::server(cfg, addr).map_err(|e| format!("quic server bind {addr}: {e}"))
}

/// 绑定一个 QUIC 客户端 endpoint（本地 ephemeral UDP 端口）并装上默认 client config。
pub fn client_endpoint(cfg: ClientConfig) -> Result<Endpoint, String> {
    let bind: SocketAddr = "0.0.0.0:0".parse().expect("valid bind addr");
    let mut ep = Endpoint::client(bind).map_err(|e| format!("quic client bind: {e}"))?;
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

fn load_key(path: &str) -> Result<PrivateKey, String> {
    let f = File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let mut r = BufReader::new(f);
    let mut keys =
        rustls_pemfile::pkcs8_private_keys(&mut r).map_err(|e| format!("read key {path}: {e}"))?;
    if keys.is_empty() {
        return Err(format!("no pkcs8 private key in {path}"));
    }
    Ok(PrivateKey(keys.remove(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_builds_with_dev_cert_and_alpn() {
        let cfg = server_quic_config("certs/dev/server-cert.pem", "certs/dev/server-key.pem");
        assert!(cfg.is_ok(), "{:?}", cfg.err());
    }

    #[test]
    fn client_config_builds_with_dev_ca() {
        let cfg = client_quic_config("certs/dev/ca-cert.pem");
        assert!(cfg.is_ok(), "{:?}", cfg.err());
    }

    #[test]
    fn server_config_missing_file_errs() {
        assert!(server_quic_config("does/not/exist.pem", "nope.pem").is_err());
    }

    // Endpoint::client 需要 tokio 运行时上下文（真实运行在 #[tokio::main] 下）。
    #[tokio::test]
    async fn client_endpoint_binds() {
        let cfg = client_quic_config("certs/dev/ca-cert.pem").unwrap();
        assert!(client_endpoint(cfg).is_ok());
    }
}
