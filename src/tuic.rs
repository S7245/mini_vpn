//! TUIC v5 client (Stage 13a) — command codec + client upstream.
//!
//! 中文要点：实现成熟的 TUIC v5 协议(出口对接 sing-box,client-only,见 ADR-0004)。
//! 本文件先落「命令编码」纯函数(TDD 主战场),字节布局**严格按 TUIC v5 规范**,与 sing-box 字节级互通。
//! 线格式参考见 docs/tech/2026-06-08-stage-13a-tuic-tcp-connect-plan.md。

use crate::shared::TargetAddr;
use std::net::SocketAddr;

/// TUIC 协议版本字节。
const TUIC_VER: u8 = 0x05;
/// 命令类型。
const CMD_AUTHENTICATE: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
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
}
