//! TCP pool generation slot installation and predecessor handoff.
//! Owns the complete slot validation/mutation transaction and original retirement.
//! Node lifecycle, admission, networking and recovery policy stay with their owners.
//! Entry: install_successor_with, take_drained and current-service handoff.
//! Existing characterizers and service-proof tests live in tuic/tests/mod.rs.
use super::*;

impl<T: TcpPoolTransport> TcpPoolGenerationSlot<T> {
    pub(super) fn new(
        index: usize,
        transport: T,
        generation: u64,
        auth_attempts: u64,
        clock: Instant,
    ) -> Self {
        Self {
            index,
            state: Mutex::new(TcpPoolGenerationSlotState {
                current: TcpPoolGeneration::new(transport, generation, auth_attempts, clock),
                draining: None,
            }),
        }
    }

    pub(super) fn replacement_available(&self) -> bool {
        if self.index == 0 {
            return false;
        }
        self.state
            .try_lock()
            .map(|state| state.replacement_available(self.index))
            .unwrap_or(false)
    }

    pub(super) async fn current_activity(&self) -> Arc<TcpPoolGenerationActivity> {
        self.state.lock().await.current.activity.clone()
    }

    #[cfg(test)]
    pub(super) async fn replacement_forward_service_floor(
        &self,
        expected_identity: TcpPoolTransportIdentity,
    ) -> Result<Option<u64>, TcpPoolGenerationInstallError> {
        let state = self.state.lock().await;
        if state.current.identity() != expected_identity {
            return Err(TcpPoolGenerationInstallError::StalePredecessor);
        }
        Ok(state.current.replacement_forward_service_floor)
    }

    #[cfg(test)]
    pub(super) async fn current_transport(&self) -> T {
        self.state.lock().await.current.transport.clone()
    }

    #[cfg(test)]
    pub(super) async fn draining_transport(&self) -> Option<T> {
        self.state
            .lock()
            .await
            .draining
            .as_ref()
            .map(|draining| draining.generation.transport.clone())
    }

    #[cfg(test)]
    pub(super) async fn install_successor(
        &self,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: TcpPoolGeneration<T>,
        started_at: Instant,
    ) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
        let proved_forward_service = TcpPoolSuccessorInstallServiceProof::for_successor(
            &successor,
            successor.replacement_forward_service_floor.unwrap_or(0),
        )
        .ok_or(TcpPoolGenerationInstallError::InvalidSuccessor)?;
        self.install_successor_with(
            expected_identity,
            expected_activity,
            successor,
            proved_forward_service,
            started_at,
            |_, _| true,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn install_successor_with_proved_service(
        &self,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: TcpPoolGeneration<T>,
        final_cwnd: u64,
        started_at: Instant,
    ) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
        let proved_forward_service =
            TcpPoolSuccessorInstallServiceProof::for_successor(&successor, final_cwnd)
                .ok_or(TcpPoolGenerationInstallError::InvalidSuccessor)?;
        self.install_successor_with(
            expected_identity,
            expected_activity,
            successor,
            proved_forward_service,
            started_at,
            |_, _| true,
        )
        .await
    }

    pub(super) async fn install_successor_with(
        &self,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: TcpPoolGeneration<T>,
        proved_forward_service: TcpPoolSuccessorInstallServiceProof,
        started_at: Instant,
        install_activity: impl FnOnce(
            &Arc<TcpPoolGenerationActivity>,
            &Arc<TcpPoolGenerationActivity>,
        ) -> bool,
    ) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
        let mut state = self.state.lock().await;
        state.validate_successor(
            self.index,
            expected_identity,
            expected_activity,
            &successor,
            proved_forward_service,
        )?;
        let successor_identity = successor.identity();
        if !install_activity(&state.current.activity, &successor.activity) {
            return Err(TcpPoolGenerationInstallError::StalePredecessor);
        }
        let predecessor = std::mem::replace(&mut state.current, successor);
        let predecessor_identity = predecessor.identity();
        let predecessor_activity = predecessor.activity.clone();
        state.draining = Some(TcpPoolDrainingGeneration {
            generation: predecessor,
            started_at,
        });
        Ok(TcpPoolGenerationInstall {
            predecessor_identity,
            successor_identity,
            predecessor_activity,
        })
    }

    pub(super) async fn take_drained(
        &self,
        expected_identity: TcpPoolTransportIdentity,
    ) -> Option<TcpPoolDrainingGeneration<T>> {
        let mut state = self.state.lock().await;
        let draining = state.draining.as_ref()?;
        if draining.generation.identity() != expected_identity
            || draining.generation.activity.active() != 0
        {
            return None;
        }
        state.draining.take()
    }
}

impl TcpPoolGenerationSlot<Connection> {
    pub(super) async fn prepare_replacement_current_service_handoff(
        &self,
        expected_identity: TcpPoolTransportIdentity,
    ) -> Result<TcpPoolReplacementCurrentServiceHandoff, TcpPoolReplacementCurrentServiceHandoffError>
    {
        let mut state = self.state.lock().await;
        if state.current.identity() != expected_identity {
            return Err(TcpPoolReplacementCurrentServiceHandoffError::StalePredecessor);
        }
        let observed_cwnd = state.current.transport.stats().path.cwnd;
        let required_cwnd_floor = state
            .current
            .record_forward_service_floor(observed_cwnd)
            .ok_or(TcpPoolReplacementCurrentServiceHandoffError::ZeroWindow)?;
        Ok(TcpPoolReplacementCurrentServiceHandoff {
            observed_cwnd,
            required_cwnd_floor,
        })
    }
}

impl<T: TcpPoolTransport> TcpPoolGenerationSlotState<T> {
    pub(super) fn replacement_available(&self, index: usize) -> bool {
        index != 0 && self.draining.is_none()
    }

    pub(super) fn validate_successor(
        &self,
        index: usize,
        expected_identity: TcpPoolTransportIdentity,
        expected_activity: &Arc<TcpPoolGenerationActivity>,
        successor: &TcpPoolGeneration<T>,
        proved_forward_service: TcpPoolSuccessorInstallServiceProof,
    ) -> Result<(), TcpPoolGenerationInstallError> {
        if index == 0 {
            return Err(TcpPoolGenerationInstallError::Primary);
        }
        if self.draining.is_some() {
            return Err(TcpPoolGenerationInstallError::PredecessorDraining);
        }
        if self.current.identity() != expected_identity
            || !Arc::ptr_eq(&self.current.activity, expected_activity)
        {
            return Err(TcpPoolGenerationInstallError::StalePredecessor);
        }
        let successor_identity = successor.identity();
        let successor_current_service = successor.transport.tcp_pool_install_service_state();
        if successor_identity.stable_id == expected_identity.stable_id
            || expected_identity.generation.checked_add(1) != Some(successor_identity.generation)
            || !proved_forward_service.matches_successor(successor)
            || successor_current_service.is_some_and(|current| {
                current.closed || current.path_generation != proved_forward_service.path_generation
            })
        {
            return Err(TcpPoolGenerationInstallError::InvalidSuccessor);
        }
        if successor_current_service
            .is_some_and(|current| current.cwnd < proved_forward_service.final_cwnd)
        {
            return Err(TcpPoolGenerationInstallError::InsufficientForwardService);
        }
        if self
            .current
            .replacement_forward_service_floor
            .is_some_and(|required| proved_forward_service.final_cwnd < required)
        {
            return Err(TcpPoolGenerationInstallError::InsufficientForwardService);
        }
        Ok(())
    }
}
