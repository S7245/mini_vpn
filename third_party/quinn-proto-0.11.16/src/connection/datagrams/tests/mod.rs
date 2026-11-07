//! Original DatagramState physical descriptors, values, pressure and retirement.
use super::*;
use bytes::{BackingBudget, BackingMeter, BytesMut};
#[test]
fn native_incoming_empty_frames_cannot_grow_unpaid_descriptors_or_change_old_data() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut state = DatagramState::new(Some(meter.clone()));
    let original = Bytes::copy_from_slice_metered(b"x", &meter).unwrap();
    let identity = original.allocation_identity();
    assert!(
        state
            .received(Datagram { data: original }, &Some(4096))
            .unwrap()
    );
    for _ in 0..3 {
        assert!(
            !state
                .received(Datagram { data: Bytes::new() }, &Some(4096))
                .unwrap()
        );
    }
    let cold = budget.used();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = BytesMut::try_zeroed_metered(budget.limit() - cold - header, &meter).unwrap();
    for _ in 0..1024 {
        assert!(
            !state
                .received(Datagram { data: Bytes::new() }, &Some(4096))
                .unwrap()
        );
        assert_eq!((state.incoming.len(), state.recv_buffered), (4, 4)); // D16: empty costs 1
        assert_eq!(budget.used(), budget.limit());
    }
    drop(pressure);
    assert_eq!(budget.used(), cold);
    let first = state.recv().unwrap();
    assert_eq!(first.allocation_identity(), identity);
    assert_eq!(first.as_ref(), b"x");
    for _ in 0..3 {
        assert!(state.recv().unwrap().is_empty());
    }
    assert!(state.recv().is_none());
    drop(first);
    assert_eq!(
        budget.used(),
        4 * std::mem::size_of::<Datagram>(),
        "dequeue returns slots; owning physical descriptor storage remains"
    );
    drop(state);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn native_prepared_growth_cancellation_keeps_actual_old_queue_and_layout() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut state = DatagramState::new(Some(meter.clone()));
    for _ in 0..4 {
        state
            .received(Datagram { data: Bytes::new() }, &Some(64))
            .unwrap();
    }
    let before = budget.used();
    let prepared = state.incoming.prepare(5, Some(&meter)).unwrap();
    assert_eq!(budget.used(), before + 8 * std::mem::size_of::<Datagram>());
    assert_eq!(state.incoming.len(), 4);
    drop(prepared);
    assert_eq!(budget.used(), before);
    assert_eq!(state.incoming.len(), 4);
    state
        .received(Datagram { data: Bytes::new() }, &Some(64))
        .unwrap();
    assert_eq!(state.incoming.len(), 5);
    assert_eq!(budget.used(), 8 * std::mem::size_of::<Datagram>());
    drop(state);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn native_empty_alias_from_foreign_budget_is_rejected_without_erasing_either_owner() {
    let a = BackingBudget::new(4096).unwrap();
    let b = BackingBudget::new(4096).unwrap();
    let ma = BackingMeter::with_budget(&a);
    let mb = BackingMeter::with_budget(&b);
    let state = DatagramState::new(Some(ma));
    let original = Bytes::copy_from_slice_metered(b"x", &mb).unwrap();
    let mut empty = original.clone();
    empty.truncate(0);
    assert_eq!(empty.allocation_identity(), original.allocation_identity());
    assert_eq!(state.admit(&empty), Err(MeteredCopyError::Quarantined));
    assert!(a.quarantined() && b.quarantined());
    assert_eq!(original.as_ref(), b"x");
    drop((state, original, empty));
    assert!(a.used() > 0 && b.used() > 0);
}
#[test]
fn original_byte_pressure_evicts_only_after_successful_native_metadata_admission() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut state = DatagramState::new(Some(meter.clone()));
    for byte in [b'a', b'b', b'c'] {
        state
            .received(
                Datagram {
                    data: Bytes::copy_from_slice_metered(&[byte], &meter).unwrap(),
                },
                &Some(2),
            )
            .unwrap();
    }
    assert_eq!((state.incoming.len(), state.recv_buffered), (2, 2));
    assert_eq!(state.recv().unwrap().as_ref(), b"b");
    assert_eq!(state.recv().unwrap().as_ref(), b"c");
    drop(state);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn native_unknown_inventory_drop_retains_actual_payload_and_descriptor_charge() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut state = DatagramState::new(Some(meter.clone()));
    let original = Bytes::copy_from_slice_metered(b"original", &meter).unwrap();
    let alias = original.clone();
    let identity = original.allocation_identity();
    state
        .received(Datagram { data: original }, &Some(4096))
        .unwrap();
    let before = budget.used();
    state.recv_buffered += 1;
    drop(state);
    assert!(budget.quarantined());
    assert_eq!(alias.allocation_identity(), identity);
    drop(alias);
    eprintln!(
        "[N1-native-datagram-unknown-drop] before={before} after={} actual_backings={}",
        budget.used(),
        meter.snapshot().allocations
    );
    assert_eq!(
        budget.used(),
        before,
        "unknown byte inventory must retain both physical owners, not Q1"
    );
    assert_eq!(
        meter.snapshot().allocations,
        1,
        "actual final payload reference still exists"
    );
}

#[test]
fn native_known_final_inventory_drop_releases_real_storage_even_after_q() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut state = DatagramState::new(Some(meter.clone()));
    state
        .received(
            Datagram {
                data: Bytes::copy_from_slice_metered(b"known", &meter).unwrap(),
            },
            &Some(4096),
        )
        .unwrap();
    meter.quarantine();
    drop(state);
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}

/// A2 (D16): zero-length DATAGRAM frames used to cost 0 against the receive
/// window, so an unread queue grew without bound in count. Each entry now
/// costs at least one byte: the queue holds at most `window` entries.
#[test]
fn zero_length_datagrams_are_bounded_by_the_receive_window() {
    let budget = BackingBudget::new(64 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut state = DatagramState::new(Some(meter.clone()));
    for _ in 0..10_000 {
        state
            .received(Datagram { data: Bytes::new() }, &Some(4096))
            .unwrap();
    }
    assert_eq!((state.incoming.len(), state.recv_buffered), (4096, 4096));
    assert!(state.recv().unwrap().is_empty());
    assert_eq!(state.recv_buffered, 4095);
    drop(state);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
