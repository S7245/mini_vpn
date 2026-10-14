//! Packet construction rejects impossible wire lengths before allocating output.
use super::*;

#[test]
fn oversized_udp_payload_does_not_unwind_the_caller() {
    let payload = vec![0; MAX_IPV4_UDP_PAYLOAD_BYTES + 1];
    let result = std::panic::catch_unwind(|| {
        build_udp_ip_packet(
            (Ipv4Addr::LOCALHOST, 5000),
            (Ipv4Addr::LOCALHOST, 6000),
            &payload,
        )
    });
    assert!(
        result.is_ok(),
        "invalid UDP length must be an ordinary rejection"
    );
    assert!(result.expect("no unwind").is_none(), "no truncated packet");
}

#[test]
fn exact_ipv4_payload_limit_roundtrips_after_rejection() {
    let src = (Ipv4Addr::new(198, 18, 0, 2), 443);
    let dst = (Ipv4Addr::new(10, 0, 0, 2), 50_000);
    let payload = vec![0x5a; MAX_IPV4_UDP_PAYLOAD_BYTES + 1];
    assert!(build_udp_ip_packet(src, dst, &payload).is_none());
    for length in [
        0,
        MAX_IPV4_UDP_PAYLOAD_BYTES - 1,
        MAX_IPV4_UDP_PAYLOAD_BYTES,
    ] {
        let packet = build_udp_ip_packet(src, dst, &payload[..length]).expect("fits IPv4");
        assert_eq!(packet.len(), 28 + length);
        let decoded = parse_inbound_udp(&packet).expect("valid packet");
        assert_eq!(decoded.payload, &payload[..length]);
        assert_eq!((decoded.dst_ip, decoded.dst_port), dst);
    }
}
