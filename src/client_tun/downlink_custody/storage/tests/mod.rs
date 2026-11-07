use super::*;
// Legacy fixture mutation stays outside the production storage interface.
impl PendingBytes {
    pub(in crate::client_tun) fn resize(&mut self, n: usize, value: u8) {
        assert!(!self.native());
        self.values.resize(n, value);
    }
    pub(in crate::client_tun) fn truncate(&mut self, n: usize) {
        self.values.truncate(n);
    }
    pub(in crate::client_tun) fn extend_from_slice(&mut self, bytes: &[u8]) {
        assert!(!self.native());
        self.values.extend_from_slice(bytes);
    }
}

#[test]
fn cold_main_exact_layout_full_is_atomic_and_empty_storage_stays_charged() {
    let total =
        PENDING_BYTES + 2 * Layout::new::<DownstreamBytePermit>().size() + capture_layout_bytes();
    for limit in [1, PENDING_BYTES, total - 1, total] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (mut pending, mut caps, mut inflight) = (
            PendingBytes::default(),
            PermitStock::default(),
            EgressPermitLedger::default(),
        );
        let result = prepare(&mut pending, &mut caps, &mut inflight, &meter);
        if limit < total {
            assert_eq!(
                result,
                Err(if limit < PENDING_BYTES {
                    MeteredCopyError::TooLarge
                } else {
                    MeteredCopyError::BudgetFull
                })
            );
            assert_eq!(
                (
                    pending.capacity(),
                    caps.capacity(),
                    inflight.permits.capacity()
                ),
                (0, 0, 0)
            );
            assert_eq!(budget.used(), 0);
        } else {
            result.unwrap();
            assert_eq!(
                (
                    pending.capacity(),
                    caps.capacity(),
                    inflight.permits.capacity()
                ),
                (PENDING_BYTES, 1, 1)
            );
            assert_eq!(budget.used(), total);
            let pointer = pending.as_ptr();
            append(&mut pending, &mut caps, b"abcd", &mut None).unwrap();
            assert_eq!(consume(&mut pending, &mut caps, 4).consumed_bytes, 4);
            assert_eq!(pending.as_ptr(), pointer);
            assert_eq!(budget.used(), total);
        }
        drop((pending, caps, inflight));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test]
async fn native_capacity_rejection_keeps_borrowed_bytes_and_original_permit() {
    let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (mut pending, mut caps, mut ledger) = (
        PendingBytes::default(),
        PermitStock::default(),
        EgressPermitLedger::default(),
    );
    prepare(&mut pending, &mut caps, &mut ledger, &meter).unwrap();
    let q = AsyncLeasedByteFlowQueue::new_with_release_mode(
        65537,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    q.push(bytes::Bytes::from(vec![1; 65537])).await.unwrap();
    let (bytes, cap) = q.recv_up_to(65537).await.unwrap().into_parts();
    let mut permit = Some(cap);
    assert!(append(&mut pending, &mut caps, &bytes, &mut permit).is_none());
    assert_eq!(permit.as_ref().unwrap().remaining_bytes(), 65537);
    assert_eq!(q.snapshot_now().leased_bytes, 65537);
    assert!(pending.is_empty() && caps.is_empty());
    assert!(
        !budget.quarantined(),
        "borrowed preflight itself does not dispose or publish"
    );
    drop(permit);
    assert_eq!(q.snapshot_now().leased_bytes, 0);
    drop((pending, caps, ledger));
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn native_d16_main_pressure_leaves_unselected_suffix_in_original_queue() {
    let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut ctx = SocketCtx::new(443);
    prepare(
        &mut ctx.downlink_pending,
        &mut ctx.downlink_pending_permits,
        &mut ctx.downlink_inflight_permits,
        &meter,
    )
    .unwrap();
    let q = AsyncLeasedByteFlowQueue::new_with_release_mode(
        2 * PENDING_BYTES,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    q.push(bytes::Bytes::from(vec![7; 2 * PENDING_BYTES]))
        .await
        .unwrap();
    ctx.d16_downlink_queue = Some(q.clone());
    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 2 * PENDING_BYTES);
    let actual = (
        drained.bytes,
        ctx.downlink_pending.len(),
        q.snapshot_now().queued_bytes,
        q.snapshot_now().leased_bytes,
        budget.quarantined(),
    );
    // Clear actual capabilities before asserting; the RED remains a failed
    // pressure admission, not a second panic during diagnostic cleanup.
    drop(ctx);
    drop(q);
    assert_eq!(
        actual,
        (
            PENDING_BYTES,
            PENDING_BYTES,
            PENDING_BYTES,
            PENDING_BYTES,
            false
        )
    );
    assert_eq!(budget.used(), 0);
}

// Explicit owner-boundary fixture only. No production call can prepare a
// Native capture without the original Flow Root's cold identity qualification.
pub(in crate::client_tun) fn prepare(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
    ledger: &mut EgressPermitLedger,
    meter: &BackingMeter,
) -> Result<(), MeteredCopyError> {
    prepare_for_owner(pending, caps, ledger, meter, capture::Owner::Standalone)
}
