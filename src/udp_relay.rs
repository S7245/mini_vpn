//! 裸 IPv4/UDP 包的解析与构造 + UDP flow 身份类型（TUIC UDP 数据面复用）。
//!
//! 中文要点：Stage 13d 退役 legacy 自研 UDP relay（QUIC datagram codec + 服务端 + 客户端 FlowTable）后，
//! 本模块只保留 TUIC 路径复用的纯逻辑：`FourTuple`/`FlowEntry`（被 `tuic::AssocTable` 复用）、
//! `UdpInbound`/`parse_inbound_udp`/`build_udp_ip_packet`（裸包造/解，下行注入 TUN）。
//! 放在 lib crate 供 client binary 与 `tests/` 复用。

use std::net::Ipv4Addr;

/// 单客户端最多并发的 UDP flow 数；到顶 LRU 驱逐（`tuic::AssocTable` 用作容量上限）。
pub const MAX_UDP_FLOWS: usize = 1024;

/// UDP flow 空闲回收阈值（秒）。客户端按它周期 sweep `AssocTable`（见 Stage 13b）。
pub const UDP_FLOW_IDLE_SECS: u64 = 60;

/// 一条 UDP flow 的四元组身份：`(srcIP:srcPort, dstIP:dstPort)`（IPv4，TUN 路径）。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FourTuple {
    pub src_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_ip: Ipv4Addr,
    pub dst_port: u16,
}

/// flow 表里一条 flow 的运行态（`tuic::AssocTable` 复用）。
#[derive(Clone, Copy, Debug)]
pub struct FlowEntry {
    pub tuple: FourTuple,
    /// 最近活动时间（秒，由调用方注入，便于单测；不在内部读时钟）。
    pub last_activity: u64,
}

impl FlowEntry {
    /// 下行包的目的：app 当初的源端点。
    pub fn app_endpoint(&self) -> (Ipv4Addr, u16) {
        (self.tuple.src_ip, self.tuple.src_port)
    }
    /// 下行包的源：app 当初发往的目的（fake-IP/target），让 app 认得是对端回包。
    pub fn target_src(&self) -> (Ipv4Addr, u16) {
        (self.tuple.dst_ip, self.tuple.dst_port)
    }
}

/// 解析出来的入站 UDP 报文要点（裸包路径用）。
#[derive(Debug, PartialEq, Eq)]
pub struct UdpInbound<'a> {
    pub src_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_ip: Ipv4Addr,
    pub dst_port: u16,
    pub payload: &'a [u8],
}

/// 解析一个裸 IPv4/UDP 包。非 IPv4 / 非 UDP / 解析失败一律 None（绝不 panic）。
pub fn parse_inbound_udp(pkt: &[u8]) -> Option<UdpInbound<'_>> {
    let headers = etherparse::PacketHeaders::from_ip_slice(pkt).ok()?;
    let etherparse::IpHeader::Version4(ipv4, _) = headers.ip? else {
        return None;
    };
    let etherparse::TransportHeader::Udp(udp) = headers.transport? else {
        return None;
    };
    Some(UdpInbound {
        src_ip: Ipv4Addr::from(ipv4.source),
        src_port: udp.source_port,
        dst_ip: Ipv4Addr::from(ipv4.destination),
        dst_port: udp.destination_port,
        payload: headers.payload,
    })
}

/// 构造一个裸 IPv4/UDP 包（含正确的 IPv4 + UDP 校验和），用于下行注入 TUN。
pub fn build_udp_ip_packet(src: (Ipv4Addr, u16), dst: (Ipv4Addr, u16), payload: &[u8]) -> Vec<u8> {
    let builder =
        etherparse::PacketBuilder::ipv4(src.0.octets(), dst.0.octets(), 64).udp(src.1, dst.1);
    let mut buf = Vec::with_capacity(20 + 8 + payload.len());
    // etherparse 在 write 时计算 IPv4 + UDP 校验和；写入 Vec 不会失败。
    builder
        .write(&mut buf, payload)
        .expect("build_udp_ip_packet: write to Vec is infallible");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_entry_maps_downlink_endpoints() {
        let e = FlowEntry {
            tuple: FourTuple {
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                src_port: 51000,
                dst_ip: Ipv4Addr::new(198, 18, 0, 5),
                dst_port: 443,
            },
            last_activity: 0,
        };
        // 下行回程：src = app 当初发往的 target，dst = app 的源端点。
        assert_eq!(e.app_endpoint(), (Ipv4Addr::new(10, 0, 0, 1), 51000));
        assert_eq!(e.target_src(), (Ipv4Addr::new(198, 18, 0, 5), 443));
    }

    #[test]
    fn build_then_parse_roundtrips_with_checksums() {
        let src = (Ipv4Addr::new(198, 18, 0, 5), 443);
        let dst = (Ipv4Addr::new(10, 0, 0, 1), 51000);
        let pkt = build_udp_ip_packet(src, dst, b"payload");
        let got = parse_inbound_udp(&pkt).expect("parses back");
        assert_eq!(got.src_ip, src.0);
        assert_eq!(got.src_port, src.1);
        assert_eq!(got.dst_ip, dst.0);
        assert_eq!(got.dst_port, dst.1);
        assert_eq!(got.payload, b"payload");
    }

    #[test]
    fn build_empty_payload_roundtrips() {
        let src = (Ipv4Addr::new(1, 1, 1, 1), 53);
        let dst = (Ipv4Addr::new(10, 0, 0, 1), 40000);
        let pkt = build_udp_ip_packet(src, dst, b"");
        let got = parse_inbound_udp(&pkt).expect("parses back");
        assert!(got.payload.is_empty());
        assert_eq!(got.dst_port, 40000);
    }

    #[test]
    fn parse_rejects_garbage_and_tcp() {
        assert!(parse_inbound_udp(&[0u8; 4]).is_none());
        // 一个 IPv4+TCP 包能解析出 IP/Transport，但不是 UDP → None。
        let mut tcp = Vec::new();
        etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [1, 1, 1, 1], 64)
            .tcp(50000, 443, 0, 1024)
            .write(&mut tcp, &[])
            .unwrap();
        assert!(parse_inbound_udp(&tcp).is_none());
    }
}
