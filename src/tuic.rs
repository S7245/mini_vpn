//! TUIC v5 client (Stage 13a) — command codec + client upstream.
//!
//! 中文要点：实现成熟的 TUIC v5 协议(出口对接 sing-box,client-only,见 ADR-0004)。
//! 本文件先落「命令编码」纯函数(TDD 主战场),字节布局**严格按 TUIC v5 规范**,与 sing-box 字节级互通。
//! 线格式参考见 docs/tech/2026-06-08-stage-13a-tuic-tcp-connect-plan.md。

use crate::quic;
use crate::shared::{ClientError, TargetAddr};
use crate::upstream::{ProxyUpstream, RelayStream};
use quinn::{Connection, Endpoint};
use std::net::SocketAddr;
use tokio::sync::Mutex;

/// 默认 ALPN：TUIC over QUIC 常用 `h3`，必须与 sing-box `tls.alpn` 一致。
const DEFAULT_TUIC_ALPN: &str = "h3";
const DEFAULT_TUIC_SNI: &str = "localhost";
const DEFAULT_TUIC_CA_PATH: &str = "cert.pem";
const DEFAULT_TUIC_CC: &str = "bbr";
const DEFAULT_TUIC_UDP_MODE: &str = "native";

/// TUIC 客户端配置（单一事实源；桌面从 env 加载，移动端将来从 file/FFI 注入）。
/// 中文要点：凭据(uuid/password)经自定义 Debug **脱敏**，绝不随日志泄漏。
#[derive(Clone)]
pub struct TuicClientConfig {
    pub server: SocketAddr,
    pub uuid: [u8; 16],
    pub password: String,
    pub sni: String,
    pub ca_path: String,
    pub alpn: String,
    pub congestion_control: String,
    pub udp_relay_mode: String,
}

impl std::fmt::Debug for TuicClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TuicClientConfig")
            .field("server", &self.server)
            .field("uuid", &"<redacted>")
            .field("password", &"<redacted>")
            .field("sni", &self.sni)
            .field("ca_path", &self.ca_path)
            .field("alpn", &self.alpn)
            .field("congestion_control", &self.congestion_control)
            .field("udp_relay_mode", &self.udp_relay_mode)
            .finish()
    }
}

/// 解析带连字符的 UUID 字符串 → 16 字节。非法返回 None。
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

impl TuicClientConfig {
    /// 从可选字符串源构建（server/uuid/password 必填）。
    pub fn from_sources(
        server: Option<&str>,
        uuid: Option<&str>,
        password: Option<&str>,
        sni: Option<&str>,
        ca_path: Option<&str>,
        alpn: Option<&str>,
    ) -> Result<Self, ClientError> {
        let server = server
            .ok_or_else(|| ClientError::InvalidTarget("tuic server addr required".into()))?
            .parse::<SocketAddr>()
            .map_err(|_| ClientError::InvalidTarget("invalid tuic server addr".into()))?;
        let uuid = parse_uuid(
            uuid.ok_or_else(|| ClientError::InvalidTarget("tuic uuid required".into()))?,
        )
        .ok_or_else(|| ClientError::InvalidTarget("invalid tuic uuid".into()))?;
        let password = password
            .filter(|p| !p.is_empty())
            .ok_or_else(|| ClientError::InvalidTarget("tuic password required".into()))?
            .to_string();
        Ok(Self {
            server,
            uuid,
            password,
            sni: sni.unwrap_or(DEFAULT_TUIC_SNI).to_string(),
            ca_path: ca_path.unwrap_or(DEFAULT_TUIC_CA_PATH).to_string(),
            alpn: alpn.unwrap_or(DEFAULT_TUIC_ALPN).to_string(),
            congestion_control: DEFAULT_TUIC_CC.to_string(),
            udp_relay_mode: DEFAULT_TUIC_UDP_MODE.to_string(),
        })
    }

    /// 从进程环境读取（`MINI_VPN_TUIC_*`）。
    pub fn from_env() -> Result<Self, ClientError> {
        let g = |k: &str| std::env::var(k).ok();
        Self::from_sources(
            g("MINI_VPN_TUIC_SERVER").as_deref(),
            g("MINI_VPN_TUIC_UUID").as_deref(),
            g("MINI_VPN_TUIC_PASSWORD").as_deref(),
            g("MINI_VPN_TUIC_SNI").as_deref(),
            g("MINI_VPN_TUIC_CA_PATH").as_deref(),
            g("MINI_VPN_TUIC_ALPN").as_deref(),
        )
    }
}

/// TUIC 协议版本字节。
const TUIC_VER: u8 = 0x05;
/// 命令类型。
const CMD_AUTHENTICATE: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
const CMD_PACKET: u8 = 0x02;
const CMD_HEARTBEAT: u8 = 0x04;
/// 地址 None 类型(回程可能省略地址)。
const ATYP_NONE: u8 = 0xff;
/// 地址类型(注意：TUIC 的 ATYP 取值与我们 Stage-12 自定义的不同)。
const ATYP_DOMAIN: u8 = 0x00;
const ATYP_IPV4: u8 = 0x01;
const ATYP_IPV6: u8 = 0x02;

/// 编码 TUIC 地址：`[ATYP][ADDR][PORT:u16 BE]`。
/// 中文要点：域名 `[len:u8][bytes]`，IPv4 4B，IPv6 16B；域名超 255 字节按 255 截断(不 panic)。
pub fn encode_address(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::new();
    match target {
        TargetAddr::IpPort(SocketAddr::V4(a)) => {
            v.push(ATYP_IPV4);
            v.extend_from_slice(&a.ip().octets());
            v.extend_from_slice(&a.port().to_be_bytes());
        }
        TargetAddr::IpPort(SocketAddr::V6(a)) => {
            v.push(ATYP_IPV6);
            v.extend_from_slice(&a.ip().octets());
            v.extend_from_slice(&a.port().to_be_bytes());
        }
        TargetAddr::DomainPort { host, port } => {
            let bytes = host.as_bytes();
            let len = bytes.len().min(u8::MAX as usize);
            v.push(ATYP_DOMAIN);
            v.push(len as u8);
            v.extend_from_slice(&bytes[..len]);
            v.extend_from_slice(&port.to_be_bytes());
        }
    }
    v
}

/// 编码 Authenticate 命令(走单向流)：`[0x05][0x00][UUID:16][TOKEN:32]`。
pub fn encode_authenticate(uuid: &[u8; 16], token: &[u8; 32]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 16 + 32);
    v.push(TUIC_VER);
    v.push(CMD_AUTHENTICATE);
    v.extend_from_slice(uuid);
    v.extend_from_slice(token);
    v
}

/// 编码 Connect 命令(走双向流，随后直接搬字节)：`[0x05][0x01][ADDR]`。
pub fn encode_connect(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 19);
    v.push(TUIC_VER);
    v.push(CMD_CONNECT);
    v.extend_from_slice(&encode_address(target));
    v
}

/// 编码 TUIC `Packet`(native datagram)：
/// `[0x05][0x02][ASSOC:u16][PKT_ID:u16=0][FRAG_TOTAL=1][FRAG_ID=0][SIZE:u16][ADDR][data]`。
pub fn encode_packet(assoc_id: u16, target: &TargetAddr, data: &[u8]) -> Vec<u8> {
    let addr = encode_address(target);
    let mut v = Vec::with_capacity(10 + addr.len() + data.len());
    v.push(TUIC_VER);
    v.push(CMD_PACKET);
    v.extend_from_slice(&assoc_id.to_be_bytes());
    v.extend_from_slice(&0u16.to_be_bytes()); // PKT_ID(native 不重组,固定 0)
    v.push(1); // FRAG_TOTAL
    v.push(0); // FRAG_ID
    v.extend_from_slice(&(data.len() as u16).to_be_bytes()); // SIZE
    v.extend_from_slice(&addr);
    v.extend_from_slice(data);
    v
}

/// 解码下行 `Packet`,只取 `(assoc_id, data)`(跳过 ADDR)。越界/地址类型未知返回 None。
pub fn decode_packet(buf: &[u8]) -> Option<(u16, &[u8])> {
    // 固定前缀 10 字节:ver type assoc(2) pkt(2) ftot fid size(2)。
    if buf.len() < 10 {
        return None;
    }
    let assoc = u16::from_be_bytes([buf[2], buf[3]]);
    let size = u16::from_be_bytes([buf[8], buf[9]]) as usize;
    let addr_len = address_len(buf, 10)?;
    let data_start = 10 + addr_len;
    let data_end = data_start.checked_add(size)?;
    if buf.len() < data_end {
        return None;
    }
    Some((assoc, &buf[data_start..data_end]))
}

/// 编码 Heartbeat：`[0x05][0x04]`。
pub fn encode_heartbeat() -> Vec<u8> {
    vec![TUIC_VER, CMD_HEARTBEAT]
}

/// ADDR 段的字节长度(用于解码时跳过地址)。
fn address_len(buf: &[u8], pos: usize) -> Option<usize> {
    match *buf.get(pos)? {
        ATYP_IPV4 => Some(1 + 4 + 2),
        ATYP_IPV6 => Some(1 + 16 + 2),
        ATYP_DOMAIN => {
            let l = *buf.get(pos + 1)? as usize;
            Some(1 + 1 + l + 2)
        }
        ATYP_NONE => Some(1),
        _ => None,
    }
}

/// 把任意可显示错误包成 ClientError（统一错误面）。
fn io_err<E: std::fmt::Display>(ctx: &str, e: E) -> ClientError {
    ClientError::from(std::io::Error::other(format!("{ctx}: {e}")))
}

/// TUIC 客户端上游：持有一条到 sing-box 的 QUIC 连接，每条 TCP 开一条 `Connect` 双向流。
/// 中文要点：连接断了按需重连+重认证(13a 最小实现;迁移/0-RTT 调优在 13c)。
pub struct TuicUpstream {
    endpoint: Endpoint,
    server: SocketAddr,
    sni: String,
    uuid: [u8; 16],
    password: String,
    conn: Mutex<Connection>,
}

impl TuicUpstream {
    /// 建连 + 发 Authenticate（token 经 keying-material 导出，字节级对齐 sing-box）。
    pub async fn connect(cfg: &TuicClientConfig) -> Result<Self, ClientError> {
        let qcfg = quic::client_quic_config_alpn(&cfg.ca_path, vec![cfg.alpn.as_bytes().to_vec()])
            .map_err(ClientError::InvalidTarget)?;
        let endpoint = quic::client_endpoint(qcfg).map_err(ClientError::InvalidTarget)?;
        let conn =
            Self::handshake(&endpoint, cfg.server, &cfg.sni, &cfg.uuid, &cfg.password).await?;
        Ok(Self {
            endpoint,
            server: cfg.server,
            sni: cfg.sni.clone(),
            uuid: cfg.uuid,
            password: cfg.password.clone(),
            conn: Mutex::new(conn),
        })
    }

    async fn handshake(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<Connection, ClientError> {
        let conn = endpoint
            .connect(server, sni)
            .map_err(|e| io_err("tuic connect", e))?
            .await
            .map_err(|e| io_err("tuic handshake", e))?;
        // TUIC token = export_keying_material(out=32, label=UUID(16), context=password)。
        let mut token = [0u8; 32];
        conn.export_keying_material(&mut token, uuid, password.as_bytes())
            .map_err(|_| ClientError::InvalidTarget("tuic keying-material export failed".into()))?;
        let mut uni = conn.open_uni().await.map_err(|e| io_err("tuic open_uni", e))?;
        uni.write_all(&encode_authenticate(uuid, &token))
            .await
            .map_err(|e| io_err("tuic auth write", e))?;
        uni.finish().await.map_err(|e| io_err("tuic auth finish", e))?;
        Ok(conn)
    }
}

#[async_trait::async_trait]
impl ProxyUpstream for TuicUpstream {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        // 取活连接，断了就地重连+重认证。
        let conn = {
            let mut guard = self.conn.lock().await;
            if guard.close_reason().is_some() {
                *guard =
                    Self::handshake(&self.endpoint, self.server, &self.sni, &self.uuid, &self.password)
                        .await?;
            }
            guard.clone()
        };
        let (mut send, recv) = conn.open_bi().await.map_err(|e| io_err("tuic open_bi", e))?;
        send.write_all(&encode_connect(target))
            .await
            .map_err(|e| io_err("tuic connect write", e))?;
        // 把双向流的收/发两半合成一条 AsyncRead+AsyncWrite，喂给现有双向泵。
        Ok(Box::new(tokio::io::join(recv, send)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::TargetAddr;

    #[test]
    fn address_ipv4() {
        let a = encode_address(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(a, vec![0x01, 1, 2, 3, 4, 0x01, 0xBB]); // ATYP=IPv4, port 443
    }

    #[test]
    fn address_domain() {
        let a = encode_address(&TargetAddr::DomainPort {
            host: "ab.com".into(),
            port: 443,
        });
        assert_eq!(
            a,
            vec![0x00, 6, b'a', b'b', b'.', b'c', b'o', b'm', 0x01, 0xBB]
        );
    }

    #[test]
    fn address_ipv6() {
        let a = encode_address(&TargetAddr::IpPort("[::1]:53".parse().unwrap()));
        assert_eq!(a[0], 0x02);
        assert_eq!(a.len(), 1 + 16 + 2);
        assert_eq!(&a[17..19], &[0x00, 0x35]); // port 53
    }

    #[test]
    fn authenticate_layout() {
        let uuid = [0xABu8; 16];
        let token = [0xCDu8; 32];
        let c = encode_authenticate(&uuid, &token);
        assert_eq!(c.len(), 2 + 16 + 32);
        assert_eq!(&c[..2], &[0x05, 0x00]);
        assert_eq!(&c[2..18], &uuid);
        assert_eq!(&c[18..50], &token);
    }

    #[test]
    fn connect_prefixes_header() {
        let c = encode_connect(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(&c[..2], &[0x05, 0x01]);
        assert_eq!(&c[2..], &[0x01, 1, 2, 3, 4, 0x01, 0xBB]);
    }

    #[test]
    fn domain_over_255_truncated_safely() {
        let host = "a".repeat(300);
        let a = encode_address(&TargetAddr::DomainPort { host, port: 80 });
        assert_eq!(a[0], 0x00);
        assert_eq!(a[1], 255); // length byte capped
        assert_eq!(a.len(), 1 + 1 + 255 + 2);
    }

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn config_valid_with_defaults() {
        let c = TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some(UUID),
            Some("secret123"),
            None,
            None,
            None,
        )
        .expect("valid");
        assert_eq!(c.server, "1.2.3.4:8443".parse().unwrap());
        assert_eq!(c.uuid[0], 0x55);
        assert_eq!(c.alpn, "h3"); // default
        assert_eq!(c.congestion_control, "bbr");
        assert_eq!(c.udp_relay_mode, "native");
    }

    #[test]
    fn config_requires_core_fields() {
        let bad = |s, u, p| TuicClientConfig::from_sources(s, u, p, None, None, None).is_err();
        assert!(bad(None, Some(UUID), Some("p"))); // no server
        assert!(bad(Some("1.2.3.4:8443"), None, Some("p"))); // no uuid
        assert!(bad(Some("1.2.3.4:8443"), Some(UUID), None)); // no password
        assert!(bad(Some("1.2.3.4:8443"), Some(UUID), Some(""))); // empty password
    }

    #[test]
    fn config_rejects_bad_server_and_uuid() {
        assert!(
            TuicClientConfig::from_sources(Some("nope"), Some(UUID), Some("p"), None, None, None)
                .is_err()
        );
        assert!(
            TuicClientConfig::from_sources(
                Some("1.2.3.4:8443"),
                Some("not-a-uuid"),
                Some("p"),
                None,
                None,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn packet_ipv4_layout() {
        let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
        assert_eq!(&p[..2], &[0x05, 0x02]); // ver + Packet
        assert_eq!(&p[2..4], &[0x00, 0x07]); // assoc-id
        assert_eq!(&p[4..6], &[0x00, 0x00]); // pkt-id
        assert_eq!(p[6], 1); // frag total
        assert_eq!(p[7], 0); // frag id
        assert_eq!(&p[8..10], &[0x00, 0x02]); // size = 2
        assert_eq!(&p[10..15], &[0x01, 1, 2, 3, 4]); // atyp ipv4 + ip
        assert_eq!(&p[15..17], &[0x00, 0x35]); // port 53
        assert_eq!(&p[17..], b"hi");
    }

    #[test]
    fn packet_domain_roundtrips_assoc_and_data() {
        let p = encode_packet(
            9,
            &TargetAddr::DomainPort {
                host: "a.com".into(),
                port: 443,
            },
            b"q",
        );
        let (assoc, data) = decode_packet(&p).unwrap();
        assert_eq!(assoc, 9);
        assert_eq!(data, b"q");
    }

    #[test]
    fn packet_decode_rejects_truncated() {
        assert!(decode_packet(&[0u8; 5]).is_none());
        // size says 0, atyp domain len 200 overruns:
        assert!(decode_packet(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 0, 0x00, 200]).is_none());
    }

    #[test]
    fn heartbeat_layout() {
        assert_eq!(encode_heartbeat(), vec![0x05, 0x04]);
    }

    #[test]
    fn quic_config_builds_with_h3_alpn() {
        // TUIC 上游的 TLS 配置(自定义 ALPN)能构建 —— connect 的真验证在互通 e2e(Task 6)。
        assert!(
            crate::quic::client_quic_config_alpn("certs/dev/ca-cert.pem", vec![b"h3".to_vec()])
                .is_ok()
        );
    }

    #[test]
    fn config_debug_redacts_credentials() {
        let c = TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some(UUID),
            Some("secret123"),
            None,
            None,
            None,
        )
        .unwrap();
        let s = format!("{c:?}");
        assert!(!s.contains("secret123"), "password leaked: {s}");
        assert!(!s.contains("550e8400"), "uuid leaked: {s}");
        assert!(s.contains("redacted"));
    }
}
