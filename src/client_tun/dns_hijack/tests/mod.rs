//! Checks the DNS wire response through the real fake-IP pool capacity seam.
use super::*;
use std::net::Ipv4Addr;

#[test]
fn exhausted_pool_returns_servfail_without_changing_an_active_target() {
    let mut pool = FakeIpPool::new();
    let active = pool.alloc("active.example", 0).expect("first address");
    pool.acquire(active, 0);
    // Exercise the public constructor and actual /15 range, with no capacity
    // setter or alternate production allocation path.
    for index in 1..131_070 {
        pool.alloc(&format!("{index}.example"), 0)
            .expect("remaining address");
    }
    let before = pool.usage();
    let query = [
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 3, b'n', b'e', b'w', 7, b'e', b'x',
        b'a', b'm', b'p', b'l', b'e', 0, 0, 1, 0, 1,
    ];
    let udp = UdpInbound {
        src_ip: Ipv4Addr::new(10, 0, 0, 2),
        src_port: 50_000,
        dst_ip: Ipv4Addr::new(8, 8, 8, 8),
        dst_port: 53,
        payload: &query,
    };
    let reply = forge_dns_reply(&udp, &mut pool, 1).expect("SERVFAIL reply");
    let parsed = parse_inbound_udp(&reply).expect("valid UDP reply");
    assert_eq!((parsed.src_ip, parsed.src_port), (udp.dst_ip, 53));
    assert_eq!((parsed.dst_ip, parsed.dst_port), (udp.src_ip, udp.src_port));
    assert_eq!(&parsed.payload[..2], &query[..2]);
    assert_eq!(parsed.payload[3] & 0x0f, 2, "SERVFAIL");
    assert_eq!(&parsed.payload[6..12], &[0; 6], "no advertised answers");
    assert_eq!(&parsed.payload[12..], &query[12..], "same question");
    assert_eq!(pool.usage(), before);
    assert_eq!(pool.resolve(active).as_deref(), Some("active.example"));
    assert_eq!(pool.alloc("active.example", 1), Ok(active));
}
