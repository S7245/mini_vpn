//! One-identity test scheduling and bad-observation injection at the real owner.
//! Never changes DATA, roles, receipt, producer state or the shared control budget.
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
        assert!(self.test_queued_data(handle).is_some_and(|n| n > 0));
        let id = identity(self, handle);
        let mut opens = self.opens();
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
