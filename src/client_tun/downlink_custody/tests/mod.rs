//! Scalar primitive access for the original unit characterizers only.
//! Real socket proofs go through the production helper in ordered_flush/wire.
use super::*;
impl SocketCtx {
    pub(in crate::client_tun) fn admit_downlink_pending_prefix(&mut self, bytes: usize) -> usize {
        self.admit_pending(bytes)
    }
    pub(in crate::client_tun) fn release_downlink_inflight_permits(
        &mut self,
        bytes: usize,
    ) -> usize {
        self.release_inflight_prefix(bytes)
    }
}

#[tokio::test]
async fn foreign_inflight_capability_keeps_original_source_fault_and_charge() {
    let owner = AsyncLeasedByteFlowQueue::new_with_release_mode(
        4,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let foreign = AsyncLeasedByteFlowQueue::new_with_release_mode(
        4,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    foreign
        .push(bytes::Bytes::from_static(b"abcd"))
        .await
        .unwrap();
    let (_, cap) = foreign.recv_up_to(4).await.unwrap().into_parts();
    let mut ledger = EgressPermitLedger::default();
    assert!(ledger.permits.insert(&mut Some(cap)));
    assert!(ledger.validate_inflight(7));
    assert!(!ledger.validate_source(Some(&owner)));
    assert_eq!(
        ledger.fault(),
        Some("capability does not belong to original flow queue")
    );
    assert!(ledger.quarantined());
    assert_eq!(ledger.permits.front().unwrap().remaining_bytes(), 4);
    assert_eq!(foreign.snapshot_now().leased_bytes, 4);
}

/// Passive original-custody facts; no capability, byte buffer or queue is cloned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) struct OrderedCustodySnapshot {
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) pending: (usize, usize, usize),
    pub(in crate::client_tun) pending_caps: (usize, Option<(usize, DownstreamPermitReleaseMode)>),
    pub(in crate::client_tun) inflight_caps: (usize, Option<(usize, DownstreamPermitReleaseMode)>),
    pub(in crate::client_tun) inflight_bytes: usize,
    pub(in crate::client_tun) busy: bool,
    pub(in crate::client_tun) quarantined: bool,
    pub(in crate::client_tun) fault: Option<&'static str>,
    pub(in crate::client_tun) mirror_quarantined: bool,
}
impl SocketCtx {
    pub(in crate::client_tun) fn test_ordered_custody(&self) -> OrderedCustodySnapshot {
        let pending = &self.downlink_pending_permits;
        let inflight = &self.downlink_inflight_permits.permits;
        let front = |caps: &crate::client_tun::downlink_custody::PermitStock| {
            (
                caps.len(),
                caps.front()
                    .map(|cap| (cap.remaining_bytes(), cap.release_mode())),
            )
        };
        OrderedCustodySnapshot {
            epoch: self.conn_epoch,
            pending: (
                self.downlink_pending.len(),
                self.downlink_pending.as_ptr() as usize,
                self.downlink_pending.capacity(),
            ),
            pending_caps: front(pending),
            inflight_caps: front(inflight),
            inflight_bytes: self.downlink_inflight_permit_bytes(),
            busy: self.downlink_inflight_permits.busy,
            quarantined: self.downlink_inflight_permits.quarantined(),
            fault: self.downlink_inflight_permits.fault(),
            mirror_quarantined: self
                .flow_mirror
                .as_ref()
                .is_some_and(|mirror| mirror.quarantined),
        }
    }
}
