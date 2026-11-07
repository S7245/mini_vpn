use super::super::*;
use crate::fake_ip::FakeIpPool;

#[test]
fn retirement_returns_pending_eviction_and_all_live_refs_exactly_once() {
    let clock = std::time::Instant::now();
    let mut pool = FakeIpPool::new();
    let now = clock.elapsed().as_secs();
    let ip = pool.alloc("retained.example", now).unwrap();
    let mut table = AssocTable::with_cap(1);
    for port in [1000, 1001] {
        let id = table.intern(FourTuple {
            src_ip: Ipv4Addr::new(10, 0, 0, 1),
            src_port: port,
            dst_ip: ip,
            dst_port: 443,
        });
        table.touch(id, now);
        table.set_fake_ip(id, ip);
        pool.acquire(ip, now);
    }
    assert_eq!(pool.usage(), (1, 1));
    let retired = table.retire_all();
    assert_eq!(retired, [ip, ip]);
    for ip in retired {
        pool.release(ip, clock.elapsed().as_secs());
    }
    assert_eq!(pool.usage(), (1, 0));
    assert_eq!(pool.resolve(ip).as_deref(), Some("retained.example"));
    assert!(table.retire_all().is_empty());
    assert_eq!(table.len(), 0);
    assert!(table.tuple_to_id.is_empty() && table.id_to_fake_ip.is_empty());
}
