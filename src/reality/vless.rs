//! VLESS 帧（空 flow）——请求头编码 + 响应头 strip（刀8 T1/T2，见 spec §3 不变量 8/9 + brief §1.4）。
//!
//! 中文要点：VLESS 是无状态代理协议，把一条中继请求（UUID auth + command + Target）框在已加密的流上，
//! 自身不带传输安全（靠下面的 REALITY）。**地址 = PortThenAddress**（port 2B BE 在前、atyp 在后），
//! ATYP v4=0x01/domain=0x02/v6=0x03——与 tuic.rs::encode_address（port-last + ATYP 错位）**完全不同**，
//! 故**新写专用编码器，绝不复用 tuic**。空 flow → addon_length=0。

use crate::shared::TargetAddr;
use std::net::SocketAddr;

/// VLESS 协议版本（恒 0）。
const VLESS_VERSION: u8 = 0x00;
/// VLESS command：TCP 中继。
pub const VLESS_CMD_TCP: u8 = 0x01;
/// VLESS ATYP（**注意与 tuic.rs 的 ATYP 数值不同**）：IPv4 / 域名 / IPv6。
const VLESS_ATYP_IPV4: u8 = 0x01;
const VLESS_ATYP_DOMAIN: u8 = 0x02;
const VLESS_ATYP_IPV6: u8 = 0x03;

/// 编码 VLESS 请求头（空 flow）：
/// `version(0) | UUID[16] | addon_len(0) | command | port(u16 BE) | atyp | address`。
/// 中文要点（**互通-critical**，brief §1.4）：地址段 = **PortThenAddress**（port 在 atyp 之前）；
/// ATYP v4=0x01/domain=0x02/v6=0x03；域名 = `0x02 + 1B 长度 + bytes`（超 255 截断不 panic）。
/// 空 flow → `addon_len=0`（无 addons）。**绝不复用 tuic::encode_address**（port-last + ATYP 错位）。
pub fn encode_vless_request(uuid: &[u8; 16], command: u8, target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 16 + 1 + 1 + 2 + 1 + 16);
    v.push(VLESS_VERSION);
    v.extend_from_slice(uuid);
    v.push(0); // addon_length（空 flow）
    v.push(command);
    match target {
        TargetAddr::IpPort(SocketAddr::V4(a)) => {
            v.extend_from_slice(&a.port().to_be_bytes());
            v.push(VLESS_ATYP_IPV4);
            v.extend_from_slice(&a.ip().octets());
        }
        TargetAddr::IpPort(SocketAddr::V6(a)) => {
            v.extend_from_slice(&a.port().to_be_bytes());
            v.push(VLESS_ATYP_IPV6);
            v.extend_from_slice(&a.ip().octets());
        }
        TargetAddr::DomainPort { host, port } => {
            v.extend_from_slice(&port.to_be_bytes());
            v.push(VLESS_ATYP_DOMAIN);
            let bytes = host.as_bytes();
            let len = bytes.len().min(u8::MAX as usize);
            v.push(len as u8);
            v.extend_from_slice(&bytes[..len]);
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::hex;

    /// golden KAT：UUID 全 0x11、TCP、1.2.3.4:443 → 26B（brief §1.4）。
    /// 钉死 PortThenAddress（port 在 atyp 前）+ ATYP IPv4=0x01。
    #[test]
    fn ipv4_request_golden() {
        let req = encode_vless_request(&[0x11; 16], VLESS_CMD_TCP, &TargetAddr::parse("1.2.3.4:443").unwrap());
        assert_eq!(
            req,
            hex("00 11111111111111111111111111111111 00 01 01bb 01 01020304")
        );
        assert_eq!(req.len(), 26);
        // 精确字段定位：port(0x01bb) 在 atyp(0x01) 之前。
        assert_eq!(&req[19..21], &[0x01, 0xbb], "port 443 BE 在 atyp 之前");
        assert_eq!(req[21], 0x01, "ATYP IPv4=0x01（非 tuic 的 0x01... 注意 domain/v6 错位）");
    }

    /// 域名变体：example.com:443 → ATYP domain=0x02 + 1B 长度 + host。
    #[test]
    fn domain_request_golden() {
        let req = encode_vless_request(
            &[0x11; 16],
            VLESS_CMD_TCP,
            &TargetAddr::DomainPort { host: "example.com".into(), port: 443 },
        );
        assert_eq!(
            req,
            hex("00 11111111111111111111111111111111 00 01 01bb 02 0b 6578616d706c652e636f6d")
        );
        assert_eq!(req[21], VLESS_ATYP_DOMAIN);
        assert_eq!(req[22], 11, "域名长度前缀");
    }

    /// IPv6 变体：ATYP=0x03 + 16B；port 仍在 atyp 之前。
    #[test]
    fn ipv6_request() {
        let req = encode_vless_request(
            &[0x22; 16],
            VLESS_CMD_TCP,
            &TargetAddr::parse("[2001:db8::1]:8443").unwrap(),
        );
        // 1+16+1+1 = 19；port(2) atyp(1) addr(16)
        assert_eq!(&req[19..21], &0x20fbu16.to_be_bytes(), "port 8443 BE");
        assert_eq!(req[21], VLESS_ATYP_IPV6);
        assert_eq!(req.len(), 19 + 2 + 1 + 16);
        assert_eq!(&req[22..38], &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    /// 超 255 字节域名按 255 截断，不 panic（与 tuic::encode_address 同纪律）。
    #[test]
    fn overlong_domain_truncated() {
        let host = "a".repeat(300);
        let req = encode_vless_request(
            &[0; 16],
            VLESS_CMD_TCP,
            &TargetAddr::DomainPort { host, port: 80 },
        );
        assert_eq!(req[22], 255, "域名长度截断到 255");
        assert_eq!(req.len(), 1 + 16 + 1 + 1 + 2 + 1 + 1 + 255);
    }
}
