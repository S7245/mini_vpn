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

/// VLESS 响应头剥离器（**互通-critical**，brief §1.4/§5 风险 6）。
/// 中文要点：服务端回的第一段 application_data（record 解密后明文）前缀 = `version(1B) + addon_length(1B=N) + addons(N B)`，
/// **无 command/address/port**。客户端须**首读一次性 strip `2+N` 字节**才接真数据：
/// - **动态算 N**（读 `buf[1]`），**禁硬编 2**（非空 addons 会污染上游真 TLS → bad-decrypt 类）；
/// - 头部 `2+N` 可能跨多 record 到达 → **累积**：不足 `2+N` 返回 false（等更多），不消费；
/// - **仅首次剥一次**（`stripped` 门控），之后透传。
#[derive(Default)]
pub struct VlessResponseStripper {
    stripped: bool,
}

impl VlessResponseStripper {
    pub fn new() -> Self {
        Self { stripped: false }
    }

    /// 响应头是否已剥完（调用方据此短路：剥完后无需再过 staging）。
    pub fn is_stripped(&self) -> bool {
        self.stripped
    }

    /// 尝试剥离响应头。返回 `true`=已剥完（`buf` 现为真数据，可放行）；`false`=头部未集齐（不消费 `buf`，等更多）。
    pub fn strip(&mut self, buf: &mut bytes::BytesMut) -> bool {
        if self.stripped {
            return true; // 已剥过 → 透传
        }
        if buf.len() < 2 {
            return false; // 连 version+addon_len 都不够 → 等更多
        }
        let addons_len = buf[1] as usize;
        let header_len = 2 + addons_len;
        if buf.len() < header_len {
            return false; // 头部跨 record 未集齐 → 累积，不消费
        }
        let _ = buf.split_to(header_len); // 一次性剥 2+N
        self.stripped = true;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::hex;
    use bytes::BytesMut;

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

    /// 空 addons：strip 2B（`00 00`），剩真数据，返回 true。
    #[test]
    fn strip_empty_addons() {
        let mut s = VlessResponseStripper::new();
        let mut buf = BytesMut::from(&hex("00 00 deadbeef")[..]);
        assert!(s.strip(&mut buf), "头部集齐 → 剥完");
        assert_eq!(&buf[..], &hex("deadbeef")[..], "剩真数据");
    }

    /// 非空 addons（N=3）：strip 2+3=5B，**动态算 N**（禁硬编 2）。
    #[test]
    fn strip_nonempty_addons() {
        let mut s = VlessResponseStripper::new();
        let mut buf = BytesMut::from(&hex("00 03 aabbcc cafe")[..]);
        assert!(s.strip(&mut buf));
        assert_eq!(&buf[..], &hex("cafe")[..], "硬编 2 会留 aabbcc 污染上游");
    }

    /// 跨 record 累积：头部分多段到达，集齐前不消费、返回 false。
    #[test]
    fn strip_accumulates_across_segments() {
        let mut s = VlessResponseStripper::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&hex("00")); // 仅 version
        assert!(!s.strip(&mut buf), "len<2 → 等更多");
        assert_eq!(buf.len(), 1, "不消费");
        buf.extend_from_slice(&hex("03 aa")); // addon_len=3，但只来了 1B addons
        assert!(!s.strip(&mut buf), "len 3 < 2+3 → 等更多");
        assert_eq!(buf.len(), 3, "不消费");
        buf.extend_from_slice(&hex("bbcc 11223344")); // 余下 2B addons + 真数据
        assert!(s.strip(&mut buf), "集齐 → 剥完");
        assert_eq!(&buf[..], &hex("11223344")[..]);
    }

    /// 仅首次剥一次：stripped 后透传，不再消费。
    #[test]
    fn strip_only_once() {
        let mut s = VlessResponseStripper::new();
        let mut buf = BytesMut::from(&hex("00 00 aa")[..]);
        assert!(s.strip(&mut buf));
        assert_eq!(&buf[..], &hex("aa")[..]);
        // 第二段数据恰好以 0x00 0x00 开头——不能被当响应头再剥。
        let mut buf2 = BytesMut::from(&hex("0000bbcc")[..]);
        assert!(s.strip(&mut buf2), "已 stripped → 透传 true");
        assert_eq!(&buf2[..], &hex("0000bbcc")[..], "透传，不消费");
    }
}
