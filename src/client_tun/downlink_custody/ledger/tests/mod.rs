//! Controlled physical corruption keeps both real original capabilities live.
use super::*;
impl EgressPermitLedger {
    pub(in crate::client_tun) fn test_swap_complete_inflight(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.permits, &mut other.permits);
        std::mem::swap(&mut self.queue, &mut other.queue);
    }
}

impl EgressPermitLedger {
    /// Borrow every original capability; a queue binding marker is insufficient.
    /// This Copy observation neither releases permits nor grants a receipt.
    pub(in crate::client_tun) fn actual_inflight_caps_for_test(
        &self,
        queue: &crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue,
    ) -> Option<(usize, usize)> {
        self.permits
            .iter()
            .try_fold((0usize, 0usize), |(count, sum), cap| {
                let bytes = cap.remaining_bytes();
                if !queue.owns_permit(cap, bytes)
                    || cap.release_mode()
                        != crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain
                {
                    return None;
                }
                Some((count.checked_add(1)?, sum.checked_add(bytes)?))
            })
    }
}

#[test]
fn empty_clear_keeps_existing_source_mode_history_and_native_stock_semantics() {
    for case in 0..8 {
        let mut ctx = SocketCtx::new(443);
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        match case {
            0 => ctx.downlink_delay_permit_release_until_egress = true,
            1 => ctx.d16_downlink_queue = Some(AsyncLeasedByteFlowQueue::new_with_release_mode(
                16, DownstreamPermitReleaseMode::OnEgressDrain).unwrap()),
            2 => ctx.downlink_inflight_permits.terminal = true,
            3 => {
                ctx.downlink_inflight_permits.admitted = 4;
                ctx.downlink_inflight_permits.refunded = 4;
            }
            4 => { ctx.downlink_inflight_permits.fail("original unknown clear"); }
            5 => storage::prepare(&mut ctx.downlink_pending, &mut ctx.downlink_pending_permits,
                &mut ctx.downlink_inflight_permits, &meter).unwrap(),
            6 => ctx.downlink_inflight_permits.origin = 9,
            7 => ctx.flow_mirror = Some(crate::client_tun::compatibility_runtime::FlowMirror::test_wire_bound_queue(
                SocketHandle::default(), 0, AsyncLeasedByteFlowQueue::new_with_release_mode(
                    16, DownstreamPermitReleaseMode::OnEgressDrain).unwrap())),
            _ => unreachable!(),
        }
        let cold = budget.used();
        ctx.clear_downlink_pending();
        if case == 4 {
            assert_eq!(ctx.downlink_inflight_permits.fault(), Some("original unknown clear"));
            assert!(ctx.downlink_inflight_permits.quarantined());
        } else {
            assert!(ctx.downlink_inflight_permits.terminal, "case={case} must perform original clear");
            assert!(!ctx.downlink_delay_permit_release_until_egress);
        }
        if case == 1 { assert!(ctx.d16_downlink_queue.is_some()); }
        if case == 7 { assert!(ctx.flow_mirror.is_some()); }
        if case == 3 { assert_eq!((ctx.downlink_inflight_permits.admitted, ctx.downlink_inflight_permits.refunded), (4, 4)); }
        if case == 6 { assert_eq!(ctx.downlink_inflight_permits.origin, 9); }
        assert_eq!(budget.used(), cold);
        drop((ctx, meter));
        assert_eq!(budget.used(), 0);
    }
}

mod event_runs;
