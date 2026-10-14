//! Exact DATA dispositions and actual pre-receive admission.
use super::*;

fn flow_mirror() -> FlowMirror {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    FlowMirror {
        id: FlowIdentity {
            index: 0,
            handle,
            epoch: 7,
            generation: 1,
        },
        lease: None,
        published_bytes: Some(3),
        producer_stopped: false,
        data_messages: 0,
        data_bytes: 0,
        transferred: (0, 0),
        discarded: (0, 0),
        abandoned_pending: 0,
        hints: 0,
        event_in_flight: false,
        quarantined: false,
        rearm_started: false,
        reset: None,
        receipt: None,
        window: None,
    }
}

#[test]
fn popped_data_without_a_physical_disposition_cannot_finish_clean() {
    let mut mirror = flow_mirror();
    assert!(mirror.begin_event(7, Some(3)));
    mirror.finish_event();
    assert!(
        mirror.quarantined,
        "pop totals alone do not prove the DATA disposition"
    );
}

#[test]
fn each_data_event_requires_one_exact_transfer_or_discard() {
    let mut ctx = SocketCtx::new(12345);
    ctx.flow_mirror = Some(flow_mirror());
    assert!(ctx.flow_mirror.as_mut().unwrap().begin_event(7, Some(3)));
    FlowMirror::accept_data(&mut ctx, &[1, 2, 3], None);
    let mirror = ctx.flow_mirror.as_mut().unwrap();
    mirror.finish_event();
    assert!(!mirror.quarantined);
    assert!(mirror.begin_event(7, Some(4)));
    mirror.discard_data(4);
    mirror.finish_event();
    assert!(!mirror.quarantined);
    assert_eq!((mirror.transferred, mirror.discarded), ((1, 3), (1, 4)));
    assert_eq!((mirror.data_messages, mirror.data_bytes), (2, 7));
    assert_eq!(ctx.downlink_pending.len(), 3);
}

#[test]
fn duplicate_or_short_disposition_remains_unknown() {
    for bytes in [2, 3] {
        let mut mirror = flow_mirror();
        assert!(mirror.begin_event(7, Some(3)));
        mirror.discard_data(bytes);
        if bytes == 3 {
            mirror.discard_data(3);
        }
        mirror.finish_event();
        assert!(mirror.quarantined);
        assert!(!mirror.begin_event(7, Some(1)));
    }
}

#[cfg(feature = "harness")]
pub(in crate::client_tun::compatibility_runtime::flow_custody) mod admission;

// Fault only after the real physical commit. Other identities cannot consume it.
pub(super) fn defer_receipt(
    owner: &crate::client_tun::compatibility_runtime::SessionControl,
    id: FlowIdentity,
) -> bool {
    let mut opens = owner.opens();
    let Some(accounting) = &mut opens.flow else {
        return false;
    };
    if accounting.defer_receipt != Some(id) {
        return false;
    }
    accounting.defer_receipt = None;
    true
}

impl FlowMirror {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn replace_actual_lease(
        &mut self,
        lease: Option<crate::fake_ip::CheckedFakeIpLease>,
    ) -> Option<crate::fake_ip::CheckedFakeIpLease> {
        std::mem::replace(&mut self.lease, lease)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn commit_probe(
        &self,
    ) -> (bool, bool, bool) {
        (
            self.rearm_started,
            self.reset.is_some(),
            self.receipt.is_some(),
        )
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn replace_identity(
        &mut self,
        id: FlowIdentity,
    ) -> FlowIdentity {
        std::mem::replace(&mut self.id, id)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_receipt(
        &self,
    ) -> Option<FlowPhysicalReceipt> {
        self.receipt
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn replace_receipt(
        &mut self,
        receipt: Option<FlowPhysicalReceipt>,
    ) {
        self.receipt = receipt;
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) enum ReceiptFault {
    Index,
    Epoch,
    NextEpoch,
    Messages,
    TransferredMessages,
    DiscardedMessages,
    TransferredBytes,
    DiscardedBytes,
    Escrow,
    Uplink,
    Downlink,
    FakeMissing,
    FakeIp,
    FakeIncarnation,
    FakeAcquisition,
    FakeBefore,
    FakeAfter,
    SumOverflow,
}

impl FlowPhysicalReceipt {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn corrupt_actual(
        mut self,
        fault: ReceiptFault,
    ) -> Self {
        match fault {
            ReceiptFault::Index => self.id.index += 1,
            ReceiptFault::Epoch => self.id.epoch += 1,
            ReceiptFault::NextEpoch => self.next_epoch += 1,
            ReceiptFault::Messages => self.data_messages += 1,
            ReceiptFault::TransferredMessages => self.transferred.0 += 1,
            ReceiptFault::DiscardedMessages => self.discarded.0 += 1,
            ReceiptFault::TransferredBytes => self.transferred.1 += 1,
            ReceiptFault::DiscardedBytes => self.discarded.1 += 1,
            ReceiptFault::Escrow => self.discarded_escrow += 1,
            ReceiptFault::Uplink => self.discarded_uplink += 1,
            ReceiptFault::Downlink => self.discarded_downlink += 1,
            ReceiptFault::FakeMissing => self.fake_release = None,
            ReceiptFault::FakeIp => {
                self.fake_release.as_mut().unwrap().ip = std::net::Ipv4Addr::new(198, 18, 0, 3)
            }
            ReceiptFault::FakeIncarnation => self.fake_release.as_mut().unwrap().incarnation += 1,
            ReceiptFault::FakeAcquisition => self.fake_release.as_mut().unwrap().acquisition += 1,
            ReceiptFault::FakeBefore => self.fake_release.as_mut().unwrap().before += 1,
            ReceiptFault::FakeAfter => self.fake_release.as_mut().unwrap().after += 1,
            ReceiptFault::SumOverflow => {
                self.discarded_socket_rx = usize::MAX;
                self.discarded_socket_tx = 1;
            }
        }
        self
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn wrong_generation(
        mut self,
    ) -> Self {
        self.id.generation = self.id.generation.checked_add(1).unwrap();
        self
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn wrong_data_bytes(
        mut self,
    ) -> Self {
        self.data_bytes = self.data_bytes.checked_add(1).unwrap();
        self
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn actual_fake_release(
        self,
    ) -> Option<crate::fake_ip::FakeIpLeaseReceipt> {
        self.fake_release
    }
}

pub(super) mod window;
