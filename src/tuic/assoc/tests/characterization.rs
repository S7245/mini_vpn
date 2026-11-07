use super::super::*;

fn tuple(p: u16) -> FourTuple {
    FourTuple {
        src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
        src_port: p,
        dst_ip: std::net::Ipv4Addr::new(198, 18, 0, 5),
        dst_port: 443,
    }
}

#[test]
fn assoc_intern_stable_and_unique() {
    let mut t = AssocTable::new();
    let a = t.intern(tuple(1000));
    assert_eq!(a, t.intern(tuple(1000)));
    assert_ne!(a, t.intern(tuple(1001)));
}

#[test]
fn assoc_intern_skips_live_id_on_wraparound() {
    // u16 回绕后 next_id 可能落到仍在册的 id 上：intern 必须跳过，绝不覆盖活跃 flow。
    let mut t = AssocTable::with_cap(8);
    let keep = t.intern(tuple(1)); // 一条长寿命 flow 占住 id=keep
    t.next_id = keep; // 把分配游标强行推回 keep（模拟回绕撞上活跃 id）
    let other = t.intern(tuple(2));
    assert_ne!(other, keep, "intern 覆盖了仍在册的 flow");
    assert!(t.resolve(keep).is_some(), "长寿命 flow 被覆盖丢失");
    assert!(t.resolve(other).is_some());
    // 两条 flow 各自映射独立，未发生 tuple_to_id 泄漏/串号。
    assert_eq!(t.intern(tuple(1)), keep);
    assert_eq!(t.intern(tuple(2)), other);
}

#[test]
fn assoc_contains_tracks_membership() {
    let mut t = AssocTable::new();
    assert!(!t.contains(&tuple(1000)));
    t.intern(tuple(1000));
    assert!(t.contains(&tuple(1000)));
    assert!(!t.contains(&tuple(1001)));
}

#[test]
fn assoc_resolve_endpoints_and_sweep() {
    let mut t = AssocTable::new();
    let id = t.intern(tuple(1000));
    let e = t.resolve(id).expect("entry");
    assert_eq!(
        e.app_endpoint(),
        (std::net::Ipv4Addr::new(10, 0, 0, 1), 1000)
    );
    assert_eq!(
        e.target_src(),
        (std::net::Ipv4Addr::new(198, 18, 0, 5), 443)
    );
    t.sweep(61, 60);
    assert!(t.resolve(id).is_none());
}

/// 刀2：sweep 回收带 fake-IP 的 assoc 时，直接返回该 fake-IP（review #8：自包含返回）。
#[test]
fn assoc_sweep_reclaims_fake_ip() {
    let mut t = AssocTable::new();
    let id = t.intern(tuple(1000));
    t.set_fake_ip(id, Ipv4Addr::new(198, 18, 0, 5));
    t.touch(id, 0);
    // 未过期：不回收，返回空。
    assert!(t.sweep(30, 60).is_empty());
    // 过期：回收，返回含该 fake-IP。
    assert_eq!(t.sweep(61, 60), vec![Ipv4Addr::new(198, 18, 0, 5)]);
}

/// 刀2：LRU 驱逐带 fake-IP 的 assoc 时也累积 reclaimed（intern 到 cap 触发 evict）。
#[test]
fn assoc_evict_reclaims_fake_ip() {
    let mut t = AssocTable::with_cap(1);
    let id1 = t.intern(tuple(1));
    t.set_fake_ip(id1, Ipv4Addr::new(198, 18, 0, 9));
    // 再 intern 一条 → cap=1 触发 evict id1。
    let _id2 = t.intern(tuple(2));
    assert_eq!(
        t.take_reclaimed_fake_ips(),
        vec![Ipv4Addr::new(198, 18, 0, 9)]
    );
}

#[test]
fn assoc_touch_keeps_alive() {
    let mut t = AssocTable::new();
    let id = t.intern(tuple(1000));
    t.touch(id, 50);
    t.sweep(100, 60);
    assert!(t.resolve(id).is_some());
}

#[test]
fn assoc_lru_evicts_at_cap() {
    let mut t = AssocTable::with_cap(2);
    let a = t.intern(tuple(1));
    let _b = t.intern(tuple(2));
    let _c = t.intern(tuple(3));
    assert!(t.resolve(a).is_none());
    assert_eq!(t.len(), 2);
}
