//! Original Target address preparation, refusal and single-owning transfer.
//! Exercises real OPEN/join/retire; not map-node or aggregate peak acceptance.

use super::*;

fn backing(target: &TargetAddr) -> (*const u8, usize, usize) {
    let TargetAddr::DomainPort { host, .. } = target else {
        panic!("this observation requires a domain");
    };
    (host.as_ptr(), host.len(), host.capacity())
}

#[test]
fn address_refusal_preserves_original_duplicate_stale_full_and_preflight_priority() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap());
    let address = target(443);
    let id = flow(2);
    io.open(id, &address).unwrap();
    memory_io::fail_next_address_allocation_for_test();
    assert_eq!(io.open(id, &address), Ok(TargetOpenCompletion::AlreadyOpen));
    assert_eq!(
        io.open(id, &target(80)),
        Err(TargetIoError::ConflictingTarget { flow_id: id })
    );
    assert_eq!(
        io.open(flow(1), &address),
        Err(TargetIoError::StaleFlowId {
            flow_id: flow(1),
            highest_seen: id
        })
    );
    io.snapshot.opened_flows = u64::MAX;
    assert_eq!(
        io.open(flow(3), &address),
        Ok(TargetOpenCompletion::Failed(
            TargetOpenFailure::ResourceExhausted
        ))
    );
    assert_eq!(io.highest_flow_id, Some(flow(3)));
    assert_eq!(
        io.open(flow(3), &address),
        Err(TargetIoError::StaleFlowId {
            flow_id: flow(3),
            highest_seen: flow(3)
        })
    );
    io.cancel(id).unwrap();
    io.join(id).unwrap();
    assert_eq!(
        io.open(id, &address),
        Ok(TargetOpenCompletion::AlreadyTerminal(
            TargetTerminalKind::Cancelled
        ))
    );
    assert_eq!(
        io.open(id, &target(80)),
        Err(TargetIoError::ConflictingTarget { flow_id: id })
    );
    assert_eq!(
        io.open(flow(4), &address),
        Err(TargetIoError::CounterOverflow {
            counter: "opened_flows"
        })
    );
    io.snapshot.opened_flows = 1;

    // Defensive inconsistent registration fixture: prove its existing error
    // precedes allocation, not that a valid free-position chain can disappear.
    let mut occupied = MemoryReadiness::new(1);
    let slot = occupied.prepare_open().unwrap();
    occupied.bind(flow(99), slot);
    std::mem::swap(&mut io.ready, &mut occupied);
    assert_eq!(
        io.open(flow(4), &address),
        Err(TargetIoError::ReadinessInvariant(
            "live capacity without free position"
        ))
    );
    assert_eq!(io.highest_flow_id, Some(flow(3)));
    std::mem::swap(&mut io.ready, &mut occupied);

    let before = io.cold_inventory().unwrap();
    assert_eq!(
        io.open(flow(4), &address),
        Err(TargetIoError::StorageReserve)
    );
    assert_eq!(io.cold_inventory().unwrap(), before);
    assert_eq!(io.highest_flow_id, Some(flow(3)));
    assert_eq!(io.open(flow(4), &address), Ok(TargetOpenCompletion::Opened));
    assert_eq!(io.highest_flow_id, Some(flow(4)));
    io.retire_terminal_tombstone(id).unwrap();
    io.cancel(flow(4)).unwrap();
    io.join(flow(4)).unwrap();
    io.retire_terminal_tombstone(flow(4)).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn raw_ip_and_empty_address_do_not_allocate_or_consume_domain_refusal() {
    let mut io = MemoryTarget::new(config());
    let ip = TargetAddr::IpPort(std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
        std::net::Ipv6Addr::LOCALHOST,
        0,
        7,
        9,
    )));
    let empty = TargetAddr::DomainPort {
        host: String::with_capacity(4096),
        port: 0,
    };
    let original = backing(&empty);
    memory_io::fail_next_address_allocation_for_test();
    assert_eq!(io.open(flow(1), &ip), Ok(TargetOpenCompletion::Opened));
    assert_eq!(io.storage.get(&flow(1)).unwrap().target, ip);
    assert_eq!(io.open(flow(2), &empty), Ok(TargetOpenCompletion::Opened));
    assert_eq!(io.storage.get(&flow(2)).unwrap().target, empty);
    assert_eq!(backing(&io.storage.get(&flow(2)).unwrap().target).2, 0);
    assert_eq!(backing(&empty), original);

    // Raw TargetIo has never imposed the wire's 253-byte domain limit.
    let long = TargetAddr::DomainPort {
        host: "界".repeat(300),
        port: 0,
    };
    assert_eq!(io.open(flow(3), &long), Err(TargetIoError::StorageReserve));
    assert_eq!(io.highest_flow_id, Some(flow(2)));
    assert_eq!(io.open(flow(3), &long), Ok(TargetOpenCompletion::Opened));
    assert_eq!(io.storage.get(&flow(3)).unwrap().target, long);
    assert_eq!(
        (
            backing(&long).1,
            backing(&io.storage.get(&flow(3)).unwrap().target).2
        ),
        (900, 900)
    );
    for id in [flow(1), flow(2), flow(3)] {
        io.cancel(id).unwrap();
        io.join(id).unwrap();
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn address_reserve_preserves_open_admission_and_retry_transfers_exact_backing() {
    let mut io = MemoryTarget::new(config());
    let other = flow(1);
    io.open(other, &target(80)).unwrap();
    io.feed_read(other, Bytes::from_static(b"other")).unwrap();
    let mut host = String::with_capacity(4096);
    host.push_str("準備.example.invalid");
    let address = TargetAddr::DomainPort { host, port: 443 };
    let original = backing(&address);
    assert!(original.2 > original.1);
    let before = io.cold_inventory().unwrap();
    let snapshot = io.snapshot();
    let id = flow(2);

    memory_io::fail_next_address_allocation_for_test();
    assert_eq!(io.open(id, &address), Err(TargetIoError::StorageReserve));
    assert_eq!(backing(&address), original);
    assert_eq!(io.cold_inventory().unwrap(), before);
    let mut expected = snapshot;
    expected.open_attempts += 1;
    assert_eq!(io.snapshot(), expected);
    assert!(io.storage.get(&id).is_none());

    assert_eq!(io.open(id, &address), Ok(TargetOpenCompletion::Opened));
    let retained = &io.storage.get(&id).unwrap().target;
    assert_eq!(retained, &address);
    let admitted = backing(retained);
    assert_eq!((admitted.1, admitted.2), (original.1, original.1));
    assert_ne!(admitted.0, original.0);
    assert_eq!(backing(&address), original);
    assert_eq!(io.cold_inventory().unwrap().live_flows(), 2);
    assert_eq!(io.write(id, b"ok").unwrap().accepted_bytes(), 2);
    assert_eq!(io.take_written(id, 8).unwrap(), Bytes::from_static(b"ok"));
    assert!(
        matches!(io.read(other, 8).unwrap(), TargetReadCompletion::Data(bytes) if bytes == b"other"[..])
    );

    io.cancel(id).unwrap();
    assert_eq!(
        io.join(id),
        Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Cancelled))
    );
    assert!(io.storage.get(&id).is_none());
    assert_eq!(
        backing(&io.storage.get_joined(&id).unwrap().target),
        admitted
    );
    assert_eq!(io.storage.get_joined(&id).unwrap().target, address);
    assert_eq!(
        io.open(id, &address),
        Ok(TargetOpenCompletion::AlreadyTerminal(
            TargetTerminalKind::Cancelled
        ))
    );
    assert_eq!(
        backing(&io.storage.get_joined(&id).unwrap().target),
        admitted
    );
    assert_eq!(
        io.retire_terminal_tombstone(id),
        Ok(TargetTombstoneRetireCompletion::Retired(
            TargetTerminalKind::Cancelled
        ))
    );
    assert!(io.storage.get_joined(&id).is_none());
    assert_eq!(backing(&address), original);
    io.cancel(other).unwrap();
    io.join(other).unwrap();
    io.retire_terminal_tombstone(other).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}
