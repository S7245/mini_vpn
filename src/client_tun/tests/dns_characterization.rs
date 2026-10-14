//! Existing DNS response characterization, moved without behavior changes.
use super::*;

/// 刀5 T1：任意 resolver 的明文 A 查询 → 本地伪造 fake-IP 回包；
/// 回包 src=被查询的 resolver:53、dst=app、RDATA=fake-IP（不依赖 198.18.0.1）。
#[test]
fn forge_dns_reply_forges_fake_ip_for_any_resolver() {
    let mut pool = FakeIpPool::new();
    let app = (Ipv4Addr::new(10, 0, 0, 2), 50000);
    // app 把 A 查询发给 8.8.8.8:53（非我方 resolver）。
    let query = dns_query(0x1234, "example.com", dns::QTYPE_A);
    let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(8, 8, 8, 8), 53), &query)
        .expect("valid UDP packet");
    let udp = parse_inbound_udp(&pkt).unwrap();

    let reply = forge_dns_reply(&udp, &mut pool, 0).expect("A 查询应被伪造");
    let r = parse_inbound_udp(&reply).expect("回包应是合法 IPv4/UDP");
    // 源 = 被查询的 resolver（否则 app socket 丢弃），目的 = app 原端点。
    assert_eq!((r.src_ip, r.src_port), (Ipv4Addr::new(8, 8, 8, 8), 53));
    assert_eq!((r.dst_ip, r.dst_port), app);
    // RDATA = fake-IP，落 198.18/15 且能 resolve 回域名。
    let fake = reply_rdata_ip(&reply);
    assert!(pool.is_fake(fake), "RDATA 应是 fake-IP, got {fake}");
    assert_eq!(pool.resolve(fake).as_deref(), Some("example.com"));
}

/// 刀5 T1：dst 落 fake-IP 段（app 把 resolver 配成被 fake 的域名）也照样伪造，不 Refuse。
#[test]
fn forge_dns_reply_forges_even_for_fake_range_resolver() {
    let mut pool = FakeIpPool::new();
    let app = (Ipv4Addr::new(10, 0, 0, 2), 50001);
    let resolver = (Ipv4Addr::new(198, 18, 0, 9), 53); // fake 段内
    let query = dns_query(1, "foo.com", dns::QTYPE_A);
    let pkt = build_udp_ip_packet(app, resolver, &query).expect("valid UDP packet");
    let udp = parse_inbound_udp(&pkt).unwrap();
    let reply = forge_dns_reply(&udp, &mut pool, 0).expect("应伪造");
    let r = parse_inbound_udp(&reply).unwrap();
    assert_eq!((r.src_ip, r.src_port), resolver);
    assert!(pool.is_fake(reply_rdata_ip(&reply)));
}

/// 刀5 T1：AAAA 查询 → NODATA（ANCOUNT=0），不分配 fake-IP。
#[test]
fn forge_dns_reply_aaaa_is_nodata() {
    let mut pool = FakeIpPool::new();
    let app = (Ipv4Addr::new(10, 0, 0, 2), 50002);
    let query = dns_query(2, "example.com", dns::QTYPE_AAAA);
    let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(1, 1, 1, 1), 53), &query)
        .expect("valid UDP packet");
    let udp = parse_inbound_udp(&pkt).unwrap();
    let reply = forge_dns_reply(&udp, &mut pool, 0).expect("AAAA 应回 NODATA（非 None）");
    let r = parse_inbound_udp(&reply).unwrap();
    // 响应 payload 偏移 6..8 = ANCOUNT。
    assert_eq!(
        u16::from_be_bytes([r.payload[6], r.payload[7]]),
        0,
        "NODATA ANCOUNT=0"
    );
}

/// 刀5 T1：不可解析的 :53 payload → None（调用方丢弃，绝不转发真 DNS = 不泄漏）。
#[test]
fn forge_dns_reply_unparseable_is_none() {
    let mut pool = FakeIpPool::new();
    let app = (Ipv4Addr::new(10, 0, 0, 2), 50003);
    let pkt = build_udp_ip_packet(app, (Ipv4Addr::new(8, 8, 8, 8), 53), &[0u8; 4])
        .expect("valid UDP packet"); // 截断
    let udp = parse_inbound_udp(&pkt).unwrap();
    assert!(forge_dns_reply(&udp, &mut pool, 0).is_none());
}

/// 刀5 T1：同域名两次查询 → 同一 fake-IP（稳定复用，DNS 给的 IP 与 TCP 时查表一致）。
#[test]
fn forge_dns_reply_stable_fake_ip_per_domain() {
    let mut pool = FakeIpPool::new();
    let app = (Ipv4Addr::new(10, 0, 0, 2), 50004);
    let res = (Ipv4Addr::new(8, 8, 8, 8), 53);
    let p1 = build_udp_ip_packet(app, res, &dns_query(1, "stable.com", dns::QTYPE_A))
        .expect("valid UDP packet");
    let p2 = build_udp_ip_packet(app, res, &dns_query(2, "stable.com", dns::QTYPE_A))
        .expect("valid UDP packet");
    let r1 = forge_dns_reply(&parse_inbound_udp(&p1).unwrap(), &mut pool, 0).unwrap();
    let r2 = forge_dns_reply(&parse_inbound_udp(&p2).unwrap(), &mut pool, 1).unwrap();
    assert_eq!(reply_rdata_ip(&r1), reply_rdata_ip(&r2));
}
