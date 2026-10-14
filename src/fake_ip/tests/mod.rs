//! Allocation and flow-reference lifecycle tests for the parent FakeIpPool authority.

use super::*;

#[test]
fn exhausted_pool_preserves_routes_until_unreferenced_mapping_expires() {
    let mut pool = FakeIpPool::new();
    pool.range_end = pool.range_start + 3;
    let active = pool.alloc("active.com", 0).unwrap();
    let idle = pool.alloc("idle.com", 0).unwrap();
    pool.acquire(active, 1);
    pool.acquire(active, 2);
    let cursor = pool.next;
    let forward = pool.domain_to_ip.clone();
    let mappings = |pool: &FakeIpPool| {
        let mut entries: Vec<_> = pool
            .ip_to_mapping
            .iter()
            .map(|(&ip, m)| (ip, m.domain.clone(), m.refcount, m.last_used))
            .collect();
        entries.sort();
        entries
    };
    let reverse = mappings(&pool);

    assert_eq!(pool.alloc("overflow.com", 10), Err(FakeIpPoolExhausted));
    assert_eq!(pool.resolve(active).as_deref(), Some("active.com"));
    assert_eq!(pool.resolve(idle).as_deref(), Some("idle.com"));
    assert_eq!(pool.domain_to_ip, forward);
    assert_eq!(mappings(&pool), reverse);
    assert_eq!(pool.next, cursor);
    assert_eq!(pool.alloc("active.com", 11).unwrap(), active);
    assert_eq!(pool.usage(), (2, 1));
    assert_eq!(pool.sweep(300, 300), 0);

    assert_eq!(pool.sweep(301, 300), 1);
    assert_eq!(pool.resolve(idle), None);
    assert_eq!(pool.alloc("replacement.com", 301).unwrap(), idle);
    assert_eq!(pool.resolve(active).as_deref(), Some("active.com"));
    pool.release(active, 302);
    assert_eq!(pool.usage(), (2, 1), "one active reference remains");
    assert_eq!(pool.sweep(603, 300), 1, "only replacement may expire");
    assert_eq!(pool.resolve(active).as_deref(), Some("active.com"));
    pool.release(active, 604);
    assert_eq!(pool.sweep(905, 300), 1);
    assert_eq!(pool.alloc("last.com", 906).unwrap(), active);
    assert_eq!(pool.resolve(active).as_deref(), Some("last.com"));
}

#[test]
fn alloc_is_stable_per_domain() {
    let mut p = FakeIpPool::new();
    let a = p.alloc("facebook.com", 0).unwrap();
    let b = p.alloc("facebook.com", 1).unwrap();
    assert_eq!(a, b, "same domain must reuse the same fake-IP");
    let c = p.alloc("google.com", 0).unwrap();
    assert_ne!(a, c, "different domains get different fake-IPs");
}

#[test]
fn alloc_starts_at_dot_two_skipping_resolver() {
    let mut p = FakeIpPool::new();
    assert_eq!(p.alloc("a.com", 0).unwrap(), Ipv4Addr::new(198, 18, 0, 2));
    assert_eq!(p.alloc("b.com", 0).unwrap(), Ipv4Addr::new(198, 18, 0, 3));
}

#[test]
fn resolve_round_trips_and_misses() {
    let mut p = FakeIpPool::new();
    let ip = p.alloc("x.com", 0).unwrap();
    assert_eq!(p.resolve(ip).as_deref(), Some("x.com"));
    assert_eq!(p.resolve(Ipv4Addr::new(198, 18, 0, 250)), None);
}

#[test]
fn is_fake_range_boundaries() {
    let p = FakeIpPool::new();
    assert!(p.is_fake(Ipv4Addr::new(198, 18, 0, 0)));
    assert!(p.is_fake(Ipv4Addr::new(198, 19, 255, 255)));
    assert!(!p.is_fake(Ipv4Addr::new(198, 17, 255, 255)));
    assert!(!p.is_fake(Ipv4Addr::new(198, 20, 0, 0)));
    assert!(!p.is_fake(Ipv4Addr::new(1, 1, 1, 1)));
}

/// 引用计数回收：活跃 flow（refcount>0）即使 idle 超 TTL 也**绝不回收**。
#[test]
fn sweep_never_reclaims_active_flow() {
    let mut p = FakeIpPool::new();
    let ip = p.alloc("live.com", 0).unwrap();
    p.acquire(ip, 0); // 一条活跃 flow
    // 远超 TTL，但 refcount>0 → 不回收。
    assert_eq!(p.sweep(10_000, 300), 0);
    assert_eq!(p.resolve(ip).as_deref(), Some("live.com"));
}

/// 引用计数回收：refcount 归零 + idle 超 TTL → 回收；回收后 resolve miss。
#[test]
fn sweep_reclaims_idle_zero_refcount() {
    let mut p = FakeIpPool::new();
    let ip = p.alloc("idle.com", 0).unwrap();
    p.acquire(ip, 0);
    p.release(ip, 5); // flow 结束，last_used=5，refcount=0
    // idle 未到 TTL（now=100, ttl=300）→ 不回收。
    assert_eq!(p.sweep(100, 300), 0);
    assert!(p.resolve(ip).is_some());
    // idle 超 TTL（now=5+301）→ 回收。
    assert_eq!(p.sweep(306, 300), 1);
    assert_eq!(p.resolve(ip), None);
}

/// release 饱和减不下溢（多 release 不 panic、不回绕成巨值）。
#[test]
fn release_saturates_at_zero() {
    let mut p = FakeIpPool::new();
    let ip = p.alloc("x.com", 0).unwrap();
    p.release(ip, 1); // refcount 本就是 0
    p.release(ip, 2);
    // 归零状态，idle 超 TTL 即可回收（证明没被 release 弄成巨值卡住）。
    assert_eq!(p.sweep(1000, 300), 1);
}

/// 池用量 usage()（刀11）：alloc 计 total 不计 active；acquire/release 动 active；sweep 降 total。
#[test]
fn usage_tracks_total_and_active() {
    let mut p = FakeIpPool::new();
    assert_eq!(p.usage(), (0, 0), "空池");
    let ip = p.alloc("a.com", 0).unwrap();
    assert_eq!(
        p.usage(),
        (1, 0),
        "alloc 计 total、不改 refcount → active=0"
    );
    p.acquire(ip, 0);
    assert_eq!(p.usage(), (1, 1), "acquire → active=1");
    let _ = p.alloc("b.com", 0).unwrap();
    assert_eq!(p.usage(), (2, 1), "第二域名进 total、未 acquire");
    p.release(ip, 5);
    assert_eq!(p.usage(), (2, 0), "release 归零 → active=0、total 不变");
    // a.com、b.com 均 refcount==0 且 idle 超 TTL → 回收。
    assert_eq!(p.sweep(1000, 300), 2);
    assert_eq!(p.usage(), (0, 0), "sweep 后池空");
}

/// 回收腾出的 IP 可被新域名复用（next_free_ip 跳过在册、回收后空出）。
#[test]
fn reclaimed_ip_is_reusable() {
    let mut p = FakeIpPool::new();
    let ip1 = p.alloc("a.com", 0).unwrap();
    let _ip2 = p.alloc("b.com", 0).unwrap();
    // a.com idle 回收。
    assert_eq!(p.sweep(1000, 300), 2); // a 和 b 都 idle 无 flow
    assert_eq!(p.resolve(ip1), None);
    // 新分配应能再拿到地址（池未泄漏）。
    let ip3 = p.alloc("c.com", 2000).unwrap();
    assert!(p.is_fake(ip3));
    assert_eq!(p.resolve(ip3).as_deref(), Some("c.com"));
}

mod checked;
