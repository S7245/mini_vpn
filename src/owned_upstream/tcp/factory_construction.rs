//! Bounded source-factory construction at the existing TCP port authority.
//!
//! Owns validated capacity layout, original ledger creation and bound admission.
//! Factory storage, source transfer and terminal settlement remain in the parent.
//! No factory is published before the original checked construction succeeds.
//! Entry: new_bound; unbound test construction uses the same internal operation.
//! Tests remain in parent tests, including source capacity and admission close.

use super::{
    Arc, Direction, FactoryFlowGate, FactoryProgress, FlowPortConfig, FlowPortConfigError, Mutex,
    ResumableTcpPortFactory, SessionId, TcpFactoryAdmission, TcpFactoryOrigin,
    TcpPortFactoryCapacityLayout, UplinkByteLedger, UplinkSegmentLedger, validate_segment_capacity,
};
use crate::owned_upstream::owner_target::OwnerTargetColdPreparationGate;
use crate::owned_upstream::supervisor::OwnerSourceIdentityError;

impl ResumableTcpPortFactory {
    pub(in crate::owned_upstream) fn validate_owner_binding(
        &self,
        _gate: &OwnerTargetColdPreparationGate,
        origin: &TcpFactoryOrigin,
        admission: &TcpFactoryAdmission,
        session_id: SessionId,
    ) -> Result<(), OwnerSourceIdentityError> {
        use OwnerSourceIdentityError as Error;
        if !self
            .origin
            .as_ref()
            .is_some_and(|actual| actual.matches(origin))
        {
            return Err(Error::FactoryOriginMismatch);
        }
        if !Arc::ptr_eq(&self.flow_gate, &admission.flow_gate) {
            return Err(Error::FactoryAdmissionMismatch);
        }
        let gate = self
            .flow_gate
            .lock()
            .map_err(|_| Error::FactoryGatePoisoned)?;
        if gate.closed {
            return Err(Error::FactoryGateClosed);
        }
        match gate.binding {
            super::FactoryFlowBinding::Bound {
                session_id: actual,
                direction: Direction::TargetToClient,
                ..
            } if actual == session_id => Ok(()),
            _ => Err(Error::FactoryGateBindingMismatch),
        }
    }

    pub(in crate::owned_upstream) fn new_bound(
        config: FlowPortConfig,
        session_id: SessionId,
        direction: Direction,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
        origin: TcpFactoryOrigin,
    ) -> Result<(Self, TcpFactoryAdmission), FlowPortConfigError> {
        let factory = Self::new_internal(
            config,
            session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity,
            FactoryFlowGate::bound(session_id, direction),
            Some(origin),
        )?;
        let admission = TcpFactoryAdmission {
            flow_gate: Arc::clone(&factory.flow_gate),
        };
        Ok((factory, admission))
    }

    pub(super) fn new_internal(
        config: FlowPortConfig,
        session_uplink_byte_capacity: usize,
        per_flow_uplink_segment_capacity: usize,
        session_uplink_segment_capacity: usize,
        flow_gate: FactoryFlowGate,
        origin: Option<TcpFactoryOrigin>,
    ) -> Result<Self, FlowPortConfigError> {
        validate_segment_capacity(
            "per-flow",
            per_flow_uplink_segment_capacity,
            FlowPortConfigError::ZeroUplinkSegmentCapacity,
        )?;
        let capacity = TcpPortFactoryCapacityLayout::checked(
            config,
            session_uplink_byte_capacity,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_capacity,
        )?;
        Ok(Self {
            config,
            capacity,
            session_uplink_ledger: UplinkByteLedger::new(session_uplink_byte_capacity)?,
            per_flow_uplink_segment_capacity,
            session_uplink_segment_ledger: UplinkSegmentLedger::new(
                session_uplink_segment_capacity,
            )?,
            flow_gate: Arc::new(Mutex::new(flow_gate)),
            progress: FactoryProgress::new(),
            origin,
        })
    }
}
