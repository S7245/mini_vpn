use super::*;

mod packet_limits;

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
    let pkt = build_udp_ip_packet(src, dst, b"payload").expect("valid UDP packet");
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
    let pkt = build_udp_ip_packet(src, dst, b"").expect("valid UDP packet");
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
