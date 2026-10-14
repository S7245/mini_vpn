//! Sole TCP factory mint at the existing session Supervisor authority.
//!
//! Owns pristine/phase/capacity checks and the original one-shot claim commit.
//! Factory storage, admission close and reducer/lifecycle operations stay parent.
//! Rejection does not consume the claim or publish a second factory.
//! Entries: mint_tcp_port_factory and its initial-legless counterpart.
//! Tests remain in the parent's factory mint/admission characterizations.

use super::{
    Direction, FlowPortConfig, OwnerSourceIdentityError, ResumableTcpPortFactory, SessionPhase,
    SessionRole, SessionSupervisor, TcpFactoryAdmissionState, TcpFactoryOrigin,
    TcpPortFactoryMintError,
};
use crate::owned_upstream::owner_ready::{OwnerReadySessionBinding, OwnerReadySessionReservation};
use crate::owned_upstream::owner_target::OwnerTargetColdPreparationGate;

impl SessionSupervisor {
    pub(in crate::owned_upstream) fn validate_owner_source_factory(
        &self,
        gate: &OwnerTargetColdPreparationGate,
        ready: &OwnerReadySessionReservation<'_>,
        factory: &ResumableTcpPortFactory,
    ) -> Result<(), OwnerSourceIdentityError> {
        let (origin, admission) = self.owner_factory_identity()?;
        ready
            .compare_declared_session(self.model.session_id())
            .map_err(|_| OwnerSourceIdentityError::ReservationSessionMismatch)?;
        factory.validate_owner_binding(gate, origin, admission, self.model.session_id())
    }

    pub(in crate::owned_upstream) fn validate_owner_ready_activation(
        &self,
        gate: &OwnerTargetColdPreparationGate,
        ready: &OwnerReadySessionBinding,
        factory: &ResumableTcpPortFactory,
        attached: &crate::owned_upstream::leg::AttachedLeg,
    ) -> Result<(), OwnerSourceIdentityError> {
        use OwnerSourceIdentityError as Error;
        let (origin, admission) = self.owner_factory_identity()?;
        let current = self.model.snapshot();
        if current.phase() != SessionPhase::Active {
            return Err(Error::ActiveSessionRequired);
        }
        // The original owner attach transaction installs model and exact
        // physical contract together, as in standby authentication.
        if current.generation() != attached.generation()
            || !self
                .owner_active_contract
                .as_ref()
                .is_some_and(|contract| contract.matches(attached))
        {
            return Err(Error::ActiveAttachmentMismatch);
        }
        ready
            .compare_declared_session(self.model.session_id())
            .map_err(|_| Error::ReservationSessionMismatch)?;
        factory.validate_owner_binding(gate, origin, admission, self.model.session_id())
    }

    fn owner_factory_identity(
        &self,
    ) -> Result<(&TcpFactoryOrigin, &super::TcpFactoryAdmission), OwnerSourceIdentityError> {
        use OwnerSourceIdentityError as Error;
        if self.model.role() != SessionRole::Owner {
            return Err(Error::OwnerRoleRequired);
        }
        if self.poisoned {
            return Err(Error::PoisonedSupervisor);
        }
        if self.model.snapshot().phase() == SessionPhase::Expired {
            return Err(Error::SessionExpired);
        }
        let origin = self
            .tcp_factory_origin
            .as_ref()
            .ok_or(Error::FactoryNotMinted)?;
        let admission = match &self.tcp_factory_admission {
            TcpFactoryAdmissionState::Minted(admission) => admission,
            TcpFactoryAdmissionState::Unminted => return Err(Error::FactoryNotMinted),
            TcpFactoryAdmissionState::Closed => return Err(Error::FactoryAdmissionClosed),
        };
        Ok((origin, admission))
    }

    /// Mints the only production TCP source factory for this session. The
    /// session-global byte/segment bounds and per-flow segment bound are
    /// derived from the reducer itself; callers may select only a smaller
    /// per-flow byte/channel shape. A rejected construction leaves the one-shot
    /// claim available for a corrected configuration.
    pub(crate) fn mint_tcp_port_factory(
        &mut self,
        config: FlowPortConfig,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        self.mint_tcp_port_factory_in_phase(config, SessionPhase::Active)
    }

    /// Initial acceptance loss constructs OwnerTarget only after the exact
    /// leg has already moved this pristine session to Legless. This narrow
    /// entry preserves every ordinary factory invariant while avoiding a
    /// global relaxation of Active-only construction.
    pub(in crate::owned_upstream) fn mint_tcp_port_factory_for_legless_bootstrap(
        &mut self,
        config: FlowPortConfig,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        self.mint_tcp_port_factory_in_phase(config, SessionPhase::Legless)
    }

    pub(super) fn mint_tcp_port_factory_in_phase(
        &mut self,
        config: FlowPortConfig,
        required_phase: SessionPhase,
    ) -> Result<ResumableTcpPortFactory, TcpPortFactoryMintError> {
        if matches!(self.tcp_factory_admission, TcpFactoryAdmissionState::Closed) {
            return Err(TcpPortFactoryMintError::FactoryAdmissionClosed);
        }
        if self.tcp_factory_origin.is_some() {
            return Err(TcpPortFactoryMintError::AlreadyMinted);
        }
        if !matches!(
            self.tcp_factory_admission,
            TcpFactoryAdmissionState::Unminted
        ) {
            return Err(TcpPortFactoryMintError::AlreadyMinted);
        }
        if self.poisoned {
            return Err(TcpPortFactoryMintError::PoisonedSupervisor);
        }
        let snapshot = self.model.snapshot();
        let client_replay = snapshot.replay_usage(Direction::ClientToTarget);
        let target_replay = snapshot.replay_usage(Direction::TargetToClient);
        let replay_bytes = client_replay.bytes().saturating_add(target_replay.bytes());
        let replay_segments = client_replay
            .segments()
            .saturating_add(target_replay.segments());
        if snapshot.phase() != required_phase
            || snapshot.flow_count() != 0
            || replay_bytes != 0
            || replay_segments != 0
            || snapshot.terminal_tombstones() != 0
            || snapshot.next_local_flow_id() != Some(1)
            || snapshot.highest_peer_flow_id() != 0
        {
            return Err(TcpPortFactoryMintError::SessionNotPristine {
                phase: snapshot.phase(),
                flow_count: snapshot.flow_count(),
                replay_bytes,
                replay_segments,
                terminal_tombstones: snapshot.terminal_tombstones(),
                next_local_flow_id: snapshot.next_local_flow_id(),
                highest_peer_flow_id: snapshot.highest_peer_flow_id(),
            });
        }
        let direction = self.model.role().local_send_direction();
        let per_flow = self.model.config().send_window();
        if config.uplink_byte_capacity() > per_flow.max_bytes() {
            return Err(TcpPortFactoryMintError::PerFlowBytesExceedReducer {
                requested: config.uplink_byte_capacity(),
                available: per_flow.max_bytes(),
            });
        }
        let session = self.model.config().replay_limit(direction);
        let origin = TcpFactoryOrigin::mint();
        let (factory, admission) = ResumableTcpPortFactory::new_bound(
            config,
            self.model.session_id(),
            direction,
            session.max_bytes(),
            per_flow.max_segments(),
            session.max_segments(),
            origin.clone(),
        )?;
        self.tcp_factory_origin = Some(origin);
        self.tcp_factory_admission = TcpFactoryAdmissionState::Minted(admission);
        Ok(factory)
    }
}
