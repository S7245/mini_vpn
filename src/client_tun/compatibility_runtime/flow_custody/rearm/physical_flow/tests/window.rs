//! One-identity test scheduling and observations at the real owner.
//! The active-close tracer invokes the original reasoned-close transaction;
//! it does not synthesize DATA, receipts or physical byte dispositions.
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;

use crate::client_tun::compatibility_runtime::flow_custody::transaction::{OpenCustody, OpenPhase};

fn identity(owner: &SessionControl, handle: SocketHandle) -> FlowIdentity {
    owner
        .opens()
        .slots
        .iter()
        .flatten()
        .find_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) if flow.id.handle == handle => Some(flow.id),
            _ => None,
        })
        .expect("actual flow identity")
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn test_flow_identity(
        &self,
        handle: SocketHandle,
    ) -> FlowIdentity {
        identity(self, handle)
    }

    pub(in crate::client_tun::compatibility_runtime) fn request_active_reasoned_close_probe(
        &self,
        handle: SocketHandle,
    ) {
        let id = identity(self, handle);
        let mut opens = self.opens();
        let accounting = opens.flow.as_mut().unwrap();
        assert!(accounting.active_close_probe.is_none());
        accounting.active_close_observation = None;
        accounting.active_close_probe = Some(id);
    }

    pub(in crate::client_tun::compatibility_runtime) fn active_reasoned_close_observation(
        &self,
    ) -> Option<(bool, usize, usize, usize, usize, Option<usize>)> {
        self.opens().flow.as_ref()?.active_close_observation
    }

    pub(in crate::client_tun::compatibility_runtime) fn watch_ordered_physical(
        &self,
        id: FlowIdentity,
    ) {
        let mut opens = self.opens();
        let slot = opens.slots[id.index]
            .as_ref()
            .expect("actual physical slot");
        assert!(slot.matches_physical(id));
        assert_eq!(
            opens.flow_mut(id).expect("actual original flow").stage,
            FlowStage::Published
        );
        let a = opens.flow.as_mut().unwrap();
        assert!(a.ordered_watch.is_none());
        a.ordered_watch = Some(id);
        a.ordered_physical = None;
    }
    pub(in crate::client_tun::compatibility_runtime) fn ordered_physical(
        &self,
    ) -> Option<(FlowIdentity, u64, usize, usize, usize, usize, bool, bool)> {
        self.opens()
            .flow
            .as_ref()?
            .ordered_physical
            .map(|(physical, _)| physical)
    }
    // The one sampled slot is overwritten by execution or fatal observation.
    // Reading this Copy does not borrow the physical ctx afresh.
    pub(in crate::client_tun::compatibility_runtime) fn ordered_custody(
        &self,
    ) -> Option<crate::client_tun::downlink_custody::OrderedCustodySnapshot> {
        self.opens()
            .flow
            .as_ref()?
            .ordered_physical
            .map(|(_, custody)| custody)
    }
    pub(in crate::client_tun::compatibility_runtime) fn test_queued_data(
        &self,
        handle: SocketHandle,
    ) -> Option<usize> {
        let id = identity(self, handle);
        self.opens()
            .flow_mut(id)?
            .roles
            .as_ref()?
            .test_queued_data()
    }

    pub(in crate::client_tun::compatibility_runtime) fn hold_test_lane(
        &self,
        handle: SocketHandle,
    ) {
        let id = identity(self, handle);
        let mut opens = self.opens();
        assert!(
            opens
                .flow_mut(id)
                .and_then(|flow| flow.roles.as_ref())
                .is_some_and(|roles| {
                    roles.test_queued_data().is_some_and(|n| n > 0)
                        || roles
                            .d16_queue()
                            .is_some_and(|queue| queue.snapshot_now().queued_bytes > 0)
                })
        );
        let accounting = opens.flow.as_mut().unwrap();
        assert!(accounting.held_lane.is_none());
        accounting.held_lane = Some(id);
        accounting.lane_observation = None;
    }

    pub(in crate::client_tun::compatibility_runtime) fn release_test_lane(&self) {
        if let Some(accounting) = &mut self.opens().flow {
            accounting.held_lane = None;
        }
    }

    pub(in crate::client_tun::compatibility_runtime) fn held_lane_inactive(&self) -> bool {
        self.opens().flow.as_ref().is_some_and(|a| {
            a.lane_observation
                .is_some_and(|(id, inactive, _)| a.held_lane == Some(id) && inactive)
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn fail_test_window(
        &self,
        handle: SocketHandle,
    ) {
        let id = identity(self, handle);
        let mut opens = self.opens();
        let accounting = opens.flow.as_mut().unwrap();
        assert!(accounting.window_fault.is_none());
        accounting.window_fault = Some(id);
    }
}

impl OpenDispatch {
    pub(in crate::client_tun) fn run_active_reasoned_close_probe(
        &self,
        sockets: &mut SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
        fake_pool: &mut FakeIpPool,
        now_secs: u64,
    ) {
        let Some(owner) = &self.owner else {
            return;
        };
        let id = {
            let mut opens = owner.opens();
            opens
                .flow
                .as_mut()
                .and_then(|flow| flow.active_close_probe.take())
        };
        let Some(id) = id else {
            return;
        };
        let Some(ctx) = contexts.get_mut(&id.handle) else {
            return;
        };
        let socket = sockets.get_mut::<TcpSocket>(id.handle);
        let snapshot = SocketCloseSnapshot::from_socket(socket);
        let before_inflight = ctx.downlink_inflight_permit_bytes();
        let before_budget = crate::client_tun::d16_process_global_budget()
            .snapshot()
            .total_bytes();
        legacy_reasoned_rearm_with_snapshot_and_gate(
            RearmGate::original_loop(self),
            id.handle,
            socket,
            ctx,
            fake_pool,
            now_secs,
            "local",
            "test_active_reasoned",
            Some(snapshot),
        );
        let after_inflight = ctx.downlink_inflight_permit_bytes();
        let after_budget = crate::client_tun::d16_process_global_budget()
            .snapshot()
            .total_bytes();
        let preclear = ctx
            .flow_mirror
            .as_ref()
            .and_then(FlowMirror::d16_terminal_preclear_inflight);
        owner
            .opens()
            .flow
            .as_mut()
            .unwrap()
            .active_close_observation = Some((
            snapshot.active,
            before_inflight,
            after_inflight,
            before_budget,
            after_budget,
            preclear,
        ));
    }

    pub(in crate::client_tun) fn observe_ordered_physical(
        &self,
        contexts: &HashMap<SocketHandle, SocketCtx>,
        sockets: &SocketSet<'_>,
    ) {
        let Some(owner) = &self.owner else {
            return;
        };
        let mut opens = owner.opens();
        let Some(id) = opens.flow.as_ref().and_then(|a| a.ordered_watch) else {
            return;
        };
        let snapshot = contexts
            .get(&id.handle)
            .filter(|ctx| {
                ctx.conn_epoch == id.epoch
                    && ctx.flow_mirror.as_ref().is_some_and(|m| m.id == id)
                    && opens
                        .slots
                        .get(id.index)
                        .and_then(Option::as_ref)
                        .is_some_and(|s| s.matches_physical(id))
            })
            .map(|ctx| {
                (
                    (
                        id,
                        ctx.conn_epoch,
                        ctx.downlink_pending.len(),
                        ctx.downlink_pending_permits
                            .iter()
                            .map(DownstreamBytePermit::remaining_bytes)
                            .sum(),
                        ctx.downlink_inflight_permit_bytes(),
                        sockets.get::<TcpSocket>(id.handle).send_queue(),
                        ctx.d16_downlink_queue.is_none(),
                        ctx.flow_mirror.as_ref().is_some_and(|m| m.reset.is_some()),
                    ),
                    ctx.test_ordered_custody(),
                )
            });
        opens.flow.as_mut().unwrap().ordered_physical = snapshot;
    }
    /// D72 tracer receive hold, checked on every poll of the selected receive.
    pub(in crate::client_tun) fn test_receive_held(&self) -> bool {
        #[cfg(feature = "harness")]
        { self.test_egress_bridge().is_some_and(|bridge| bridge.relay_receive_held()) }
        #[cfg(not(feature = "harness"))]
        { false }
    }

    pub(in crate::client_tun) fn test_lane_held(&self) -> bool {
        self.owner.as_ref().is_some_and(|owner| {
            owner
                .opens()
                .flow
                .as_ref()
                .is_some_and(|a| a.held_lane.is_some())
        })
    }
}

pub(in crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow) fn control_probe(
    opens: &mut OpenCustody,
    id: FlowIdentity,
    completion: &mut HandshakeCompletion<'_, '_>,
) -> bool {
    super::cold::receipt_source_fault(opens, id, completion);
    let published = opens
        .flow_mut(id)
        .is_some_and(|flow| flow.stage == FlowStage::Published);
    let Some(accounting) = &mut opens.flow else {
        return false;
    };
    let Some(ctx) = completion.socket_ctxs.get_mut(&id.handle) else {
        return false;
    };
    if accounting.held_lane == Some(id) {
        accounting.lane_observation = Some((
            id,
            !completion.sockets.get::<TcpSocket>(id.handle).is_active(),
            ctx.downlink_pending.len(),
        ));
    }
    if accounting.window_fault != Some(id) {
        return false;
    }
    accounting.window_fault = None;
    let mirror = ctx.flow_mirror.as_mut().expect("original physical owner");
    assert_eq!(mirror.id, id);
    let pending = mirror.receipt.map_or(
        ctx.downlink_pending.len() + mirror.abandoned_pending,
        |receipt| receipt.discarded_downlink,
    );
    mirror
        .window
        .as_mut()
        .expect("original cumulative window")
        .audit_storage(mirror.transferred.1, pending.checked_add(1).unwrap(), 0);
    // Published must close via actual reader->monitor notification/role join.
    // The injected control turn itself must not call flow.close.
    published
}
