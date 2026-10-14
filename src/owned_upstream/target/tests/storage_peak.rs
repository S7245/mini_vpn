//! Checked requested backing of the original MemoryTarget and its preparations.
//! Not opaque caller pointees, escaped outputs or the whole Root allocation cap.

use super::*;

struct CountWake(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl std::task::Wake for CountWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn configuration_rejects_combined_requested_backing_before_allocation() {
    let m = isize::MAX as usize / 4 + 1;
    // Every old individual check succeeds; no MemoryTarget or large buffer is
    // constructed. The conservative combined envelope alone must be rejected.
    assert!(std::alloc::Layout::array::<u8>(m).is_ok());
    assert!(MemoryReadiness::check_layout(1).is_ok());
    assert!(MemoryTargetStorage::check_layout(1, 1).is_ok());
    assert_eq!(
        MemoryTargetConfig::new(1, m, m, 1, 1, 1),
        Err(TargetIoConfigError::StorageLayoutOverflow)
    );
}

#[test]
fn real_near_limit_open_checks_address_overlap_before_node_allocation() {
    let (mut low, mut high) = (1usize, isize::MAX as usize / 4);
    while low < high {
        let mid = low + (high - low + 1) / 2;
        if MemoryTargetConfig::new(1, mid, mid, 1, 1, 1).is_ok() {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let config = MemoryTargetConfig::new(1, low, low, 1, 1, 1).unwrap();
    assert!(isize::MAX as usize - config.requested_storage_peak.size() < 64);
    // Only the tiny fixed readiness/node are allocated; enormous byte buffers
    // remain declared capacity. This is a valid config, not corrupted state.
    let mut io = MemoryTarget::new(config);
    let address = TargetAddr::DomainPort {
        host: "bounded.test.invalid".repeat(8),
        port: 443,
    };
    let before = io.cold_inventory().unwrap();
    let mut snapshot = io.snapshot();
    MemoryTarget::fail_next_node_allocation_for_test();
    assert_eq!(
        io.open(flow(1), &address),
        Err(TargetIoError::StorageLayout)
    );
    snapshot.open_attempts += 1;
    assert_eq!(io.snapshot(), snapshot);
    assert_eq!(io.cold_inventory().unwrap(), before);
    let ip = TargetAddr::IpPort("127.0.0.1:443".parse().unwrap());
    assert_eq!(io.open(flow(1), &ip), Err(TargetIoError::StorageReserve));
    assert_eq!(io.open(flow(1), &ip), Ok(TargetOpenCompletion::Opened));
    io.cancel(flow(1)).unwrap();
    io.join(flow(1)).unwrap();
    io.retire_terminal_tombstone(flow(1)).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn real_backing_and_long_raw_addresses_stay_charged_until_original_retirement() {
    use crate::resumable::effect_storage::{
        EffectStorageError, exact_vec, fail_next_effect_allocation_for_test,
    };
    for f in [1, 64, 65, 257] {
        let config = MemoryTargetConfig::new(f, 4, 4, 4, 4, 3).unwrap();
        fail_next_effect_allocation_for_test();
        let mut io = MemoryTarget::new(config);
        assert!(matches!(
            exact_vec::<u8>(1),
            Err(EffectStorageError::Reserve)
        ));
        let mut host = String::with_capacity(4096);
        host.push_str(&"界".repeat(300));
        let address = TargetAddr::DomainPort { host, port: 0 };
        let mut returned = Vec::new();
        for value in 1..=f as u64 {
            let id = flow(value);
            io.open(id, &address).unwrap();
            assert_eq!(io.write(id, b"ABCD").unwrap().accepted_bytes(), 4);
            returned.push(io.take_written(id, 4).unwrap());
            io.feed_read(id, Bytes::from_static(b"EFGH")).unwrap();
            let TargetReadCompletion::Data(bytes) = io.read(id, 4).unwrap() else {
                panic!("real read")
            };
            returned.push(bytes);
            io.push_write_directive(id, MemoryWriteDirective::WouldBlock)
                .unwrap();
            io.cold_inventory().unwrap();
        }
        let actual_bytes = io
            .storage
            .values()
            .map(|flow| flow.written.capacity() + flow.readable.capacity())
            .sum::<usize>();
        let actual_directives = io
            .storage
            .values()
            .map(|flow| flow.write_directives.capacity())
            .sum::<usize>();
        let actual_addresses = io
            .storage
            .values()
            .map(|flow| match &flow.target {
                TargetAddr::DomainPort { host, .. } => host.capacity(),
                _ => 0,
            })
            .sum::<usize>();
        assert_eq!(
            (actual_bytes, actual_directives, actual_addresses),
            (8 * f, 3 * f, 900 * f)
        );
        assert_eq!(io.owned_address_capacity, actual_addresses);
        assert_eq!(io.cold_inventory().unwrap().summed_buffered_bytes(), 0);
        // These transferred Bytes are now caller-owned, not Target pending.
        assert_eq!(returned.iter().map(Bytes::len).sum::<usize>(), 8 * f);
        let resident = std::mem::size_of::<MemoryTarget>()
            + f * std::mem::size_of::<TargetNode>()
            + MemoryReadiness::check_layout(f).unwrap().size()
            + actual_bytes
            + actual_directives * std::mem::size_of::<MemoryWriteDirective>();
        assert!(resident <= config.requested_storage_peak.size());
        for value in 1..=f as u64 {
            io.cancel(flow(value)).unwrap();
            io.join(flow(value)).unwrap();
            assert_eq!(io.owned_address_capacity, 900 * f);
            io.cold_inventory().unwrap();
        }
        for value in 1..=f as u64 {
            io.retire_terminal_tombstone(flow(value)).unwrap();
            assert_eq!(io.owned_address_capacity, 900 * (f - value as usize));
            io.cold_inventory().unwrap();
        }
        assert!(io.cold_inventory().unwrap().is_quiescent());
        assert_eq!(io.storage.free_len, f);
        assert_eq!(io.owned_address_capacity, 0);
    }
}

#[test]
fn cold_audit_rejects_backing_and_address_mirror_corruption_without_normalizing() {
    // Defensive private-state mutations, not claims about legal API reachability.
    for case in 0..5 {
        let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 4, 16, 4, 4, 1).unwrap());
        let id = flow(1);
        io.open(id, &target(443)).unwrap();
        let original = io.owned_address_capacity;
        match case {
            0 => io.owned_address_capacity -= 1,
            1 => {
                let TargetAddr::DomainPort { host, .. } =
                    &mut io.storage.get_mut(&id).unwrap().target
                else {
                    panic!("domain")
                };
                host.reserve_exact(256);
            }
            2 => io.storage.get_mut(&id).unwrap().written.reserve_exact(5),
            3 => io
                .storage
                .get_mut(&id)
                .unwrap()
                .write_directives
                .reserve_exact(2),
            4 => {
                io.storage.get_mut(&id).unwrap().output = MemoryTargetOutput::Publishing {
                    direction: MemoryTargetOutputDirection::Read,
                    bytes: Bytes::from_static(b"12345"),
                };
                io.buffered_bytes = 5;
            }
            _ => unreachable!(),
        }
        let expected = if case < 2 {
            "Target address backing"
        } else {
            "Target requested backing"
        };
        assert!(
            matches!(io.cold_inventory(), Err(TargetIoColdInventoryError::StoredMismatch { domain, .. }) if domain == expected)
        );
        assert_eq!(
            io.owned_address_capacity,
            if case == 0 { original - 1 } else { original }
        );
    }
}

#[test]
fn retire_address_preflight_rejects_without_consuming_joined_custody_or_waking() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 4, 4, 4, 4, 1).unwrap());
    let id = flow(1);
    io.open(id, &target(443)).unwrap();
    io.cancel(id).unwrap();
    io.join(id).unwrap();
    let original = io.owned_address_capacity;
    let TargetAddr::DomainPort { host, .. } = &io.storage.get_joined(&id).unwrap().target else {
        panic!("domain")
    };
    let pointer = host.as_ptr();
    let wakes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker = std::task::Waker::from(std::sync::Arc::new(CountWake(wakes.clone())));
    let mut cx = Context::from_waker(&waker);
    assert_eq!(io.poll_ready_notifications(&mut cx), Poll::Pending);
    // Defensive mirror underflow; original owning node/address remain intact.
    io.owned_address_capacity = original - 1;
    assert_eq!(
        io.retire_terminal_tombstone(id),
        Err(TargetIoError::StorageLayout)
    );
    assert_eq!(io.owned_address_capacity, original - 1);
    assert_eq!(io.storage.free_len, 0);
    assert_eq!(io.storage.joined_len, 1);
    let TargetAddr::DomainPort { host, .. } = &io.storage.get_joined(&id).unwrap().target else {
        panic!("domain")
    };
    assert_eq!(host.as_ptr(), pointer);
    assert_eq!(io.poll_ready_notifications(&mut cx), Poll::Pending);
    assert_eq!(wakes.load(std::sync::atomic::Ordering::SeqCst), 0);
    io.owned_address_capacity = original;
    io.cold_inventory().unwrap();
    assert_eq!(
        io.retire_terminal_tombstone(id),
        Ok(TargetTombstoneRetireCompletion::Retired(
            TargetTerminalKind::Cancelled
        ))
    );
    assert_eq!(io.owned_address_capacity, 0);
    assert_eq!(wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(io.cold_inventory().unwrap().is_quiescent());
}
