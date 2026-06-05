//! UDP relay over the QUIC datagram data plane (Stage 12).
//!
//! 中文要点：本模块是 UDP relay 的纯逻辑收口——QUIC datagram 的线格式编解码、flow 表、
//! 以及裸 IP/UDP 包的造/解。放在 lib crate 里，让 server/client 两个 binary 模块和
//! `tests/` 集成测试都能复用。承重决策见 docs/adr/0003 与 stage-12 spec。

use crate::shared::TargetAddr;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

/// ATYP=1：4 字节 IPv4 地址。
const ATYP_IPV4: u8 = 1;
/// ATYP=3：`[len:u8][域名 len 字节]`。
const ATYP_DOMAIN: u8 = 3;
/// ATYP=4：16 字节 IPv6 地址（TUN 路径目前只产 IPv4，这里为健壮性兜底支持）。
const ATYP_IPV6: u8 = 4;

/// 编码一个上行 datagram：`[flow-id:u32][ATYP][ADDR][PORT:u16][payload...]`（大端）。
/// 中文要点：target 每包内联，服务端逐包无状态解析，无建流握手。
pub fn encode_uplink(flow_id: u32, target: &TargetAddr, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(11 + payload.len());
    v.extend_from_slice(&flow_id.to_be_bytes());
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
            // 域名长度用 u8 承载；DNS 域名上限 253，必然装得下，这里 saturating 兜底。
            let host_bytes = host.as_bytes();
            let len = host_bytes.len().min(u8::MAX as usize);
            v.push(ATYP_DOMAIN);
            v.push(len as u8);
            v.extend_from_slice(&host_bytes[..len]);
            v.extend_from_slice(&port.to_be_bytes());
        }
    }
    v.extend_from_slice(payload);
    v
}

/// 解码上行 datagram，返回 `(flow-id, target, payload)`。
/// 中文要点：任何越界 / 非法 UTF-8 / 未知 ATYP 一律返回 None，绝不 panic。
pub fn decode_uplink(buf: &[u8]) -> Option<(u32, TargetAddr, &[u8])> {
    // 至少 4 字节 flow-id + 1 字节 ATYP。
    if buf.len() < 5 {
        return None;
    }
    let flow_id = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    let atyp = buf[4];
    let mut pos = 5usize;
    let target = match atyp {
        ATYP_IPV4 => {
            // 4 addr + 2 port
            if buf.len() < pos + 6 {
                return None;
            }
            let ip = Ipv4Addr::new(buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]);
            let port = u16::from_be_bytes([buf[pos + 4], buf[pos + 5]]);
            pos += 6;
            TargetAddr::IpPort(SocketAddr::from((ip, port)))
        }
        ATYP_DOMAIN => {
            if buf.len() < pos + 1 {
                return None;
            }
            let dlen = buf[pos] as usize;
            pos += 1;
            // 域名 dlen 字节 + 2 字节端口
            if buf.len() < pos + dlen + 2 {
                return None;
            }
            let host = std::str::from_utf8(&buf[pos..pos + dlen]).ok()?.to_string();
            let port = u16::from_be_bytes([buf[pos + dlen], buf[pos + dlen + 1]]);
            pos += dlen + 2;
            TargetAddr::DomainPort { host, port }
        }
        ATYP_IPV6 => {
            // 16 addr + 2 port
            if buf.len() < pos + 18 {
                return None;
            }
            let octets: [u8; 16] = buf[pos..pos + 16].try_into().ok()?;
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([buf[pos + 16], buf[pos + 17]]);
            pos += 18;
            TargetAddr::IpPort(SocketAddr::from((ip, port)))
        }
        _ => return None,
    };
    Some((flow_id, target, &buf[pos..]))
}

/// 编码一个下行 datagram：`[flow-id:u32][payload...]`。
pub fn encode_downlink(flow_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.extend_from_slice(&flow_id.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

/// 解码下行 datagram，返回 `(flow-id, payload)`。越界返回 None。
pub fn decode_downlink(buf: &[u8]) -> Option<(u32, &[u8])> {
    if buf.len() < 4 {
        return None;
    }
    let flow_id = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    Some((flow_id, &buf[4..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::TargetAddr;

    #[test]
    fn uplink_roundtrips_ipv4_target() {
        let t = TargetAddr::IpPort("1.2.3.4:443".parse().unwrap());
        let buf = encode_uplink(7, &t, b"hello");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 7);
        assert_eq!(target, t);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn uplink_roundtrips_domain_target() {
        let t = TargetAddr::DomainPort {
            host: "facebook.com".into(),
            port: 443,
        };
        let buf = encode_uplink(9, &t, b"q");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 9);
        assert_eq!(target, t);
        assert_eq!(payload, &b"q"[..]);
    }

    #[test]
    fn uplink_roundtrips_ipv6_target() {
        let t = TargetAddr::IpPort("[2001:db8::1]:53".parse().unwrap());
        let buf = encode_uplink(1, &t, b"x");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 1);
        assert_eq!(target, t);
        assert_eq!(payload, &b"x"[..]);
    }

    #[test]
    fn uplink_empty_payload_is_ok() {
        let t = TargetAddr::IpPort("9.9.9.9:443".parse().unwrap());
        let buf = encode_uplink(3, &t, b"");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 3);
        assert_eq!(target, t);
        assert!(payload.is_empty());
    }

    #[test]
    fn downlink_roundtrips() {
        let buf = encode_downlink(42, b"resp");
        assert_eq!(decode_downlink(&buf).unwrap(), (42, &b"resp"[..]));
    }

    #[test]
    fn decode_rejects_truncated_without_panic() {
        assert!(decode_uplink(&[0u8; 3]).is_none()); // < flow-id+atyp
        assert!(decode_uplink(&[0, 0, 0, 1, ATYP_IPV4, 1, 2]).is_none()); // ipv4 addr/port short
        assert!(decode_uplink(&[0, 0, 0, 1, ATYP_DOMAIN, 200, b'a']).is_none()); // domain len overruns
        assert!(decode_uplink(&[0, 0, 0, 1, 99]).is_none()); // unknown ATYP
        assert!(decode_downlink(&[0u8; 2]).is_none());
    }
}
