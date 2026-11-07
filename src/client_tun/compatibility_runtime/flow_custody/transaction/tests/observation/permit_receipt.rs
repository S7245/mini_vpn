//! Deterministic oracle checks for retained original Permit reset receipts.
//! No runtime owner, queue, publication or production receipt is constructed.
//! Real production merge/reset and lifecycle acceptance stay in Service tests.
use super::*;
use crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow::PermitResetObservation;
use crate::client_tun::downlink_custody::{
    EgressLedgerEvent as Event, EgressLedgerEventKind as Kind, EgressLedgerSnapshot as Snapshot,
    EgressRefundSource,
};

fn receipt() -> PermitResetObservation {
    let mut events = [None; 32];
    events[0] = Some(Event {
        kind: Kind::Admit,
        source: None,
        epoch: 1,
        start: 0,
        end: 4,
        cap_ordinal: 0,
        cap_before: 4,
        cap_after: 4,
    });
    events[1] = Some(Event {
        kind: Kind::Refund,
        source: Some(EgressRefundSource::PacketFlush),
        epoch: 1,
        start: 0,
        end: 2,
        cap_ordinal: 0,
        cap_before: 4,
        cap_after: 2,
    });
    events[2] = Some(Event {
        kind: Kind::Admit,
        source: None,
        epoch: 1,
        start: 4,
        end: 7,
        cap_ordinal: 0,
        cap_before: 2,
        cap_after: 5,
    });
    let pre = Snapshot {
        epoch: 1,
        origin: 40000,
        admitted: 7,
        proved: 2,
        refunded: 2,
        first_cap_remaining: 5,
        first_cap_ordinal: 0,
        cap_count: 1,
        quarantined: false,
        events,
        event_count: 3,
        event_overflow: false,
    };
    let post = Snapshot {
        refunded: 7,
        first_cap_remaining: 0,
        first_cap_ordinal: 1,
        cap_count: 0,
        ..pre
    };
    PermitResetObservation {
        pre,
        post,
        released_before: 2,
        released_after: 7,
        pending: 0,
        discarded_inflight: 5,
    }
}

#[test]
fn permit_receipt_replay_accepts_live_cap_extension_after_partial_refund() {
    FlowObservation::replay_permit_reset_receipt(receipt());
    let mut legacy = receipt();
    let event = legacy.pre.events[2].as_mut().unwrap();
    event.cap_ordinal = 1;
    event.cap_before = 3;
    event.cap_after = 3;
    legacy.pre.cap_count = 2;
    legacy.pre.first_cap_remaining = 2;
    legacy.post.events = legacy.pre.events;
    FlowObservation::replay_permit_reset_receipt(legacy);
}

#[test]
fn permit_receipt_replay_rejects_corrupt_cap_extension() {
    for fault in 0..6 {
        let mut value = receipt();
        let event = value.pre.events[2].as_mut().unwrap();
        match fault {
            0 => event.cap_before += 1,
            1 => event.cap_after += 1,
            2 => event.start += 1,
            3 => event.cap_ordinal += 1,
            4 => event.source = Some(EgressRefundSource::AckDrain),
            5 => {
                // Every byte/range total is consistent. Only reviving the
                // already fully refunded original cap is illegal.
                event.cap_before = 0;
                event.cap_after = 3;
                let refund = value.pre.events[1].as_mut().unwrap();
                refund.end = 4;
                refund.cap_after = 0;
                value.pre.refunded = 4;
                value.pre.proved = 4;
                value.pre.first_cap_remaining = 3;
                value.post.proved = 4;
                value.released_before = 4;
                value.discarded_inflight = 3;
            }
            _ => unreachable!(),
        }
        value.post.events = value.pre.events;
        assert!(
            std::panic::catch_unwind(|| FlowObservation::replay_permit_reset_receipt(value))
                .is_err(),
            "corrupt extension {fault} was accepted"
        );
    }
}
