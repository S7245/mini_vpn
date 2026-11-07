//! Closed current-thread diagnostic of this real hard partial-flush case.
//! Borrows original owners synchronously, without await, clone, Drop or publish.
//! Owns only Copy observations; grants no receipt, refund, clean or absent right.
//! All tasks in this case use the current Tokio runtime; no external Sender,
//! blocking publisher or custom callback exists. Other callers get no such proof.
use super::*;
use crate::client_tun::downlink_custody::EgressLedgerSnapshot;
use crate::client_tun::legacy_event_loop::relay_events::RelayEventTestBridge;
use crate::client_tun::{RelayEvent, SocketHandle};
use crate::tcp_downlink_pump::{
    AsyncLeasedByteFlowQueue, DownstreamPermitReleaseMode, PermitQueueIdentity,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Inventory {
    pub main_pending: usize,
    pub main_inflight: usize,
    pub ring: usize,
    pub core_held: usize,
    pub selected: usize,
    pub dispatcher_held: usize,
    pub dispatcher_segment: usize,
}
impl Inventory {
    pub(super) fn total(self) -> usize {
        [
            self.main_pending,
            self.main_inflight,
            self.ring,
            self.core_held,
            self.selected,
            self.dispatcher_held,
            self.dispatcher_segment,
        ]
        .into_iter()
        .try_fold(0usize, |sum, n| sum.checked_add(n))
        .unwrap()
    }
    fn published_outside_main(self) -> usize {
        self.ring
            .checked_add(self.core_held)
            .unwrap()
            .checked_add(self.selected)
            .unwrap()
    }
}

pub(super) fn inspect(
    owner: &SessionControl,
    bridge: &RelayEventTestBridge,
    queue: &AsyncLeasedByteFlowQueue,
    target: (SocketHandle, u64),
    first: EgressLedgerSnapshot,
    dispatcher: (usize, usize, usize),
) -> Inventory {
    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    );
    assert_eq!(owner.flow_observation().unwrap().joined, 5);
    let actual = owner.flow_contracts()[0].id;
    assert_eq!((actual.handle, actual.epoch), target);
    let identity = PermitQueueIdentity::from_queue(queue);
    let (held, segment, published) = dispatcher;
    let domains = std::cell::Cell::new([0usize; 2]);
    // SAFETY: this concrete current-thread test owns the only running task now.
    // Every actual Node/Source/pump/driver task belongs to this runtime. There
    // are no external publishers, and this entire visitor has no await, Service
    // poll, arbitrary callback, disposal, or retained reference to an original T.
    let raw = unsafe {
        owner.inspect_relay_lane_for_test(|value, core_held| {
            if let (
                handle,
                RelayEvent::Data {
                    epoch,
                    bytes,
                    downstream_permit,
                    ..
                },
            ) = value
            {
                assert_eq!((*handle, *epoch), target);
                let permit = downstream_permit.as_ref().unwrap();
                assert!(identity.matches(permit, DownstreamPermitReleaseMode::OnEgressDrain));
                assert_eq!(permit.remaining_bytes(), bytes.len());
                let mut sums = domains.get();
                let index = usize::from(core_held);
                sums[index] = sums[index].checked_add(permit.remaining_bytes()).unwrap();
                domains.set(sums);
            }
            crate::shared::bounded_channel::Value::lane_bytes(value)
        })
    }
    .expect("actual ring/held stamps and stored inventory must close");
    let selected =
        bridge
            .selected_probe_custody()
            .map_or(0, |(handle, epoch, remaining, same_queue)| {
                assert_eq!((handle, epoch), target);
                assert!(same_queue);
                remaining
            });
    let (terminal, census, pending, visits, ambiguous) = bridge.egress_shutdown_snapshot();
    assert!(visits > 0 && !ambiguous);
    let terminal = terminal.unwrap();
    assert_eq!(terminal.handle, target.0);
    assert_eq!(terminal.ledger.epoch, target.1);
    assert_eq!(terminal.mirror_receipt, Some(false));
    let a = first;
    let b = terminal.ledger;
    assert_eq!(
        (
            a.epoch,
            a.origin,
            a.admitted,
            a.proved,
            a.refunded,
            a.first_cap_remaining,
            a.first_cap_ordinal,
            a.cap_count
        ),
        (
            b.epoch,
            b.origin,
            b.admitted,
            b.proved,
            b.refunded,
            b.first_cap_remaining,
            b.first_cap_ordinal,
            b.cap_count
        )
    );
    assert_eq!(
        (a.quarantined, a.event_count, a.event_overflow, a.events),
        (b.quarantined, b.event_count, b.event_overflow, b.events)
    );
    let pending = pending.unwrap();
    assert!(pending.same_queue);
    assert_eq!(pending.bytes, pending.permit_bytes);
    assert_eq!(pending.count == 0, pending.bytes == 0);
    let actual = queue.permit_release_audit_for_test().unwrap();
    let census = census.unwrap();
    let tuple = |a: crate::tcp_downlink_pump::PermitQueueReleaseAudit| {
        [
            a.emitted,
            a.consumed,
            a.discarded,
            a.queued,
            a.leased,
            a.released,
            a.packet_flush,
            a.ack_drain,
            a.explicit_other,
            a.dropped,
        ]
    };
    assert_eq!(tuple(actual), tuple(census));
    assert!(!actual.interrupted && !census.interrupted);
    let sums = domains.get();
    let (cap_count, cap_bytes) = terminal
        .physical_caps
        .expect("every actual Main cap must match original queue/mode");
    assert_eq!(cap_count, b.cap_count);
    assert_eq!(Some(cap_bytes as u64), b.admitted.checked_sub(b.refunded));
    let result = Inventory {
        main_pending: pending.permit_bytes,
        main_inflight: cap_bytes,
        ring: sums[0],
        core_held: sums[1],
        selected,
        dispatcher_held: held,
        dispatcher_segment: segment,
    };
    assert_eq!(result.total(), actual.leased);
    assert_eq!(
        actual.consumed,
        published
            .checked_add(held)
            .unwrap()
            .checked_add(segment)
            .unwrap()
    );
    assert_eq!(
        published,
        usize::try_from(b.admitted)
            .unwrap()
            .checked_add(pending.bytes)
            .unwrap()
            .checked_add(result.published_outside_main())
            .unwrap()
    );
    eprintln!(
        "partial physical actual domains: {result:?} raw={raw:?} published={published} audit={actual:?}"
    );
    result
}

pub(super) fn wire_prefix(
    outbound: &PacketLink, cut: crate::device::TunFlushedTcpPacket, origin: u32, cut_index: usize,
) -> usize {
    check_wire(outbound.packets_for_test().iter().map(|p| p.as_ref()), cut, origin, cut_index, &super::REPLY)
}

fn check_wire<'a>(
    packets: impl Iterator<Item = &'a [u8]>, cut: crate::device::TunFlushedTcpPacket,
    origin: u32, cut_index: usize, expected: &[u8],
) -> usize {
    let mut prefix = 0usize;
    let mut saw_cut = false;
    for (index, packet) in packets.enumerate() {
        let summary = crate::device::summarize_flushed_ip_tcp_packet(packet);
        if index == cut_index { assert_eq!(summary, Some(cut)); saw_cut = true; }
        let Some(actual) = summary else { continue; };
        if (actual.src_addr, actual.dst_addr, actual.src_port, actual.dst_port)
            != (cut.src_addr, cut.dst_addr, cut.src_port, cut.dst_port) { continue; }
        assert!(index <= cut_index, "DATA after actual partial cut, including retransmission");
        let start = actual.payload_start_seq.wrapping_sub(origin) as usize;
        let end = start.checked_add(actual.payload_bytes).unwrap();
        assert!(start <= prefix && end <= expected.len() && end < (1usize << 31), "gap or foreign sequence");
        let parsed = etherparse::PacketHeaders::from_ip_slice(packet).unwrap();
        assert_eq!(parsed.payload, &expected[start..end], "every original/retransmitted payload byte");
        prefix = prefix.max(end);
    }
    assert!(saw_cut, "actual cut must be present at its original queue index");
    prefix
}

/// Each original Refund is one proved contiguous PacketFlush prefix; a real
/// partial cut may follow earlier real flushes. No synthetic terminal receipt.
pub(super) fn refund_prefix(ledger: EgressLedgerSnapshot, wire: usize) {
    assert!(!ledger.event_overflow);
    let mut end = 0u64;
    for event in &ledger.events[..ledger.event_count] {
        let event = event.as_ref().expect("original event prefix has no holes");
        assert_eq!(event.epoch, ledger.epoch);
        match event.kind {
            EgressLedgerEventKind::Admit => assert_eq!(event.source, None),
            EgressLedgerEventKind::Refund => {
                assert_eq!(event.source, Some(EgressRefundSource::PacketFlush));
                assert_eq!(event.start, end);
                assert!(event.end > event.start);
                let delta = usize::try_from(event.end.checked_sub(event.start).unwrap()).unwrap();
                assert_eq!(event.cap_before.checked_sub(event.cap_after), Some(delta));
                end = event.end;
            }
        }
    }
    assert_eq!(end, wire as u64);
    assert_eq!((ledger.proved, ledger.refunded), (end, end));
}

// Oracle fixture copied from the actual two-PacketFlush diagnostic sequence.
// It does not stand in for the real hard Service test above this child seam.
fn two_flush_snapshot() -> EgressLedgerSnapshot {
    use crate::client_tun::downlink_custody::EgressLedgerEvent as Event;
    let mut events = [None; 32];
    events[0] = Some(Event {
        kind: EgressLedgerEventKind::Admit,
        source: None,
        epoch: 1,
        start: 0,
        end: 1424,
        cap_ordinal: 0,
        cap_before: 1424,
        cap_after: 1424,
    });
    events[1] = Some(Event {
        kind: EgressLedgerEventKind::Refund,
        source: Some(EgressRefundSource::PacketFlush),
        epoch: 1,
        start: 0,
        end: 1424,
        cap_ordinal: 0,
        cap_before: 1424,
        cap_after: 0,
    });
    events[2] = Some(Event {
        kind: EgressLedgerEventKind::Admit,
        source: None,
        epoch: 1,
        start: 1424,
        end: 4268,
        cap_ordinal: 1,
        cap_before: 2844,
        cap_after: 2844,
    });
    events[3] = Some(Event {
        kind: EgressLedgerEventKind::Refund,
        source: Some(EgressRefundSource::PacketFlush),
        epoch: 1,
        start: 1424,
        end: 2884,
        cap_ordinal: 1,
        cap_before: 2844,
        cap_after: 1384,
    });
    EgressLedgerSnapshot {
        epoch: 1,
        origin: 4260165795,
        admitted: 4268,
        proved: 2884,
        refunded: 2884,
        first_cap_remaining: 1384,
        first_cap_ordinal: 1,
        cap_count: 1,
        quarantined: true,
        events,
        event_count: 4,
        event_overflow: false,
    }
}

#[test]
fn partial_prefix_oracle_accepts_actual_two_flush_contiguous_history() {
    refund_prefix(two_flush_snapshot(), 2884);
}
#[test]
fn partial_prefix_oracle_rejects_overlap_gap_wrong_source_and_false_bytes() {
    for fault in 0..5 {
        let mut ledger = two_flush_snapshot();
        let mut wire = 2884;
        let event = ledger.events[3].as_mut().unwrap();
        match fault {
            0 => event.start = 0,
            1 => event.start += 1,
            2 => event.source = Some(EgressRefundSource::AckDrain),
            3 => event.cap_after += 1,
            4 => wire += 1,
            _ => unreachable!(),
        }
        assert!(
            std::panic::catch_unwind(|| refund_prefix(ledger, wire)).is_err(),
            "invalid original refund prefix {fault} was accepted"
        );
    }
}

mod wire;
