//! Passive test observations of the single transaction owner.
//! Owns no lifecycle or control state; methods only inspect retained root values.
//! Used by transaction and saved-loop compatibility tests.
use super::super::*;

#[derive(Debug, PartialEq, Eq)]
pub(in crate::client_tun::compatibility_runtime) struct PartialConnectObservation {
    pub(in crate::client_tun::compatibility_runtime) id: FlowIdentity,
    pub(in crate::client_tun::compatibility_runtime) provider: usize,
    pub(in crate::client_tun::compatibility_runtime) deadline: tokio::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) open_deadline: std::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) view: crate::upstream::TcpOpenView,
    pub(in crate::client_tun::compatibility_runtime) escrow: Vec<u8>,
    pub(in crate::client_tun::compatibility_runtime) reserved: usize,
    pub(in crate::client_tun::compatibility_runtime) physical: bool,
    pub(in crate::client_tun::compatibility_runtime) w: bool,
    pub(in crate::client_tun::compatibility_runtime) f: bool,
    pub(in crate::client_tun::compatibility_runtime) roles: bool,
    pub(in crate::client_tun::compatibility_runtime) carrier: bool,
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn retained_provider_io_kind(
        &self,
    ) -> Option<std::io::ErrorKind> {
        let opens = self.opens();
        match opens
            .flow
            .as_ref()?
            .first_provider_failure
            .as_ref()?
            .failure()?
        {
            crate::shared::ClientError::Io(source) => Some(source.kind()),
            _ => None,
        }
    }

    pub(in crate::client_tun::compatibility_runtime) fn partial_connect_observations(
        &self,
    ) -> Vec<PartialConnectObservation> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| {
                let OpenPhase::Flow(flow) = &slot.phase else {
                    return None;
                };
                let attempt = flow.attempt.as_ref()?;
                let view = attempt.view();
                (view.phase == TcpOpenPhase::Connecting).then(|| PartialConnectObservation {
                    id: flow.id,
                    provider: attempt.as_ref() as *const _ as *const () as usize,
                    deadline: slot.deadline,
                    open_deadline: flow.open_deadline,
                    view,
                    escrow: flow.escrow.clone(),
                    reserved: flow.reserved,
                    physical: flow.physical,
                    w: flow.w_held,
                    f: flow.f_held,
                    roles: flow.roles.is_some(),
                    carrier: flow.carrier.is_some(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(in crate::client_tun::compatibility_runtime) struct FlowObservation {
    pub(in crate::client_tun::compatibility_runtime) factory_started: u64,
    pub(in crate::client_tun::compatibility_runtime) last_fake_release: Option<(
        FlowIdentity,
        crate::fake_ip::FakeIpLeaseReceipt,
        (usize, usize),
    )>,
    pub(in crate::client_tun::compatibility_runtime) live: usize,
    pub(in crate::client_tun::compatibility_runtime) retained_identity_faults: usize,
    pub(in crate::client_tun::compatibility_runtime) w: usize,
    pub(in crate::client_tun::compatibility_runtime) f: usize,
    pub(in crate::client_tun::compatibility_runtime) reserved: usize,
    pub(in crate::client_tun::compatibility_runtime) retained_capacity: usize,
    pub(in crate::client_tun::compatibility_runtime) escrow_bytes: usize,
    pub(in crate::client_tun::compatibility_runtime) running: usize,
    pub(in crate::client_tun::compatibility_runtime) attempts: usize,
    pub(in crate::client_tun::compatibility_runtime) opening_providers: usize,
    pub(in crate::client_tun::compatibility_runtime) waiting: usize,
    pub(in crate::client_tun::compatibility_runtime) retry: usize,
    pub(in crate::client_tun::compatibility_runtime) stopped_producers: usize,
    pub(in crate::client_tun::compatibility_runtime) settled_providers: usize,
    pub(in crate::client_tun::compatibility_runtime) published: u64,
    pub(in crate::client_tun::compatibility_runtime) released: u64,
    pub(in crate::client_tun::compatibility_runtime) joined: u64,
    pub(in crate::client_tun::compatibility_runtime) audit: bool,
    pub(in crate::client_tun::compatibility_runtime) last_data_disposition:
        Option<(usize, usize, usize, usize, usize)>,
}
#[cfg(test)]
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn retained_provider_failure(
        &self,
    ) -> Option<(usize, crate::upstream::TcpOpenView, String)> {
        let opens = self.opens();
        let attempt = opens.flow.as_ref()?.first_provider_failure.as_ref()?;
        let source = attempt.failure()?;
        Some((
            source as *const crate::shared::ClientError as usize,
            attempt.view(),
            source.to_string(),
        ))
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_debug(&self) -> Vec<String> {
        self.opens().slots.iter().flatten().filter_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) => Some(format!(
                "stage={:?} stop={} joins={:?} settled={:?} provider={:?} steps={} published={:?} hints={:?}",
                flow.stage, flow.producer_stopped,
                flow.roles.as_ref().map(FlowRoles::join_count),
                flow.roles.as_ref().map(FlowRoles::settled),
                flow.attempt.as_ref().map(|a| a.view()), flow.open_steps,
                flow.roles.as_ref().and_then(FlowRoles::published),
                flow.roles.as_ref().and_then(FlowRoles::published_hints),
            )),
            _ => None,
        }).collect()
    }

    // Progress polling reads the original counter; boundary audits stay complete.
    pub(in crate::client_tun::compatibility_runtime) fn published_flow_count(&self) -> Option<u64> {
        Some(self.opens().flow.as_ref()?.published)
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_observation(
        &self,
    ) -> Option<FlowObservation> {
        let opens = self.opens();
        let accounting = opens.flow.as_ref()?;
        let (mut running, mut retained_capacity, mut joined) = (0, 0, accounting.joined);
        for slot in opens.slots.iter().flatten() {
            if let OpenPhase::Flow(flow) = &slot.phase {
                retained_capacity += flow.escrow.capacity();
                running += flow.roles.as_ref().map_or(0, FlowRoles::running_count);
                joined += flow.roles.as_ref().map_or(0, FlowRoles::join_count) as u64;
            }
        }
        Some(FlowObservation {
            factory_started: accounting.factory_started,
            last_fake_release: accounting.last_fake_release,
            live: opens.live,
            retained_identity_faults: usize::from(accounting.identity_fault.is_some()),
            w: accounting.w,
            f: accounting.f,
            reserved: accounting.bytes,
            retained_capacity,
            escrow_bytes: accounting.escrow_bytes,
            attempts: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.attempt.is_some())).count(),
            opening_providers: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.attempt.as_ref()
                    .is_some_and(|a| a.view().phase == TcpOpenPhase::Opening))).count(),
            waiting: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.stage == FlowStage::Waiting
                    && flow.attempt.is_none() && !flow.f_held && flow.w_held)).count(),
            retry: accounting.retry,
            running,
            stopped_producers: opens
                .slots
                .iter()
                .flatten()
                .filter(
                    |slot| matches!(&slot.phase, OpenPhase::Flow(flow) if flow.producer_stopped),
                )
                .count(),
            settled_providers: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.attempt.as_ref()
                    .is_some_and(|a| {
                        let view = a.view();
                        view.phase == TcpOpenPhase::Settled && !view.lease_held && !view.retirement_pending
                    }))).count(),
            published: accounting.published,
            released: accounting.released,
            joined,
            audit: opens.audit_flows(),
            last_data_disposition: accounting.last_data_disposition,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::client_tun::compatibility_runtime) struct FlowContract {
    pub(in crate::client_tun::compatibility_runtime) id: FlowIdentity,
    pub(in crate::client_tun::compatibility_runtime) slot_deadline: tokio::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) open_deadline: std::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) active: bool,
    pub(in crate::client_tun::compatibility_runtime) outstanding: Option<usize>,
    pub(in crate::client_tun::compatibility_runtime) waiting: bool,
    pub(in crate::client_tun::compatibility_runtime) published: bool,
    pub(in crate::client_tun::compatibility_runtime) attempt: bool,
    pub(in crate::client_tun::compatibility_runtime) w: bool,
    pub(in crate::client_tun::compatibility_runtime) f: bool,
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_contracts(&self) -> Vec<FlowContract> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| {
                let OpenPhase::Flow(flow) = &slot.phase else {
                    return None;
                };
                Some(FlowContract {
                    id: flow.id,
                    slot_deadline: slot.deadline,
                    open_deadline: flow.open_deadline,
                    active: flow.stage == FlowStage::Published,
                    outstanding: flow
                        .roles
                        .as_ref()
                        .and_then(FlowRoles::test_outstanding_data),
                    waiting: flow.stage == FlowStage::Waiting,
                    published: flow.published,
                    attempt: flow.attempt.is_some(),
                    w: flow.w_held,
                    f: flow.f_held,
                })
            })
            .collect()
    }
}
