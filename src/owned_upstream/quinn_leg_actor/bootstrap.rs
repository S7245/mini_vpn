//! Admitted-server bootstrap implementation and unclaimed-actor cleanup.
//!
//! Owns Bootstrap start/Drop, handoff, input, and termination-guard bodies;
//! preserves deadline, identity, poison-before-release, and cleanup ordering.
//! Does not own listener admission, client cleanup, or owner-session custody.
//! Entry points are the parent QuinnLegActorBootstrap start methods.
//! Characterization lives in actor tests/bootstrap_custody.rs.

use super::*;

pub(super) struct ServerBootstrapHandoff {
    runtime: tokio::runtime::Handle,
    actor: Option<StartedQuinnLegActor>,
}

impl ServerBootstrapHandoff {
    pub(super) fn new(runtime: tokio::runtime::Handle, actor: StartedQuinnLegActor) -> Self {
        Self {
            runtime,
            actor: Some(actor),
        }
    }

    fn claim(mut self) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        self.actor
            .take()
            .ok_or(QuinnLegActorError::BootstrapConsumed)
    }
}

impl Drop for ServerBootstrapHandoff {
    fn drop(&mut self) {
        let Some(mut actor) = self.actor.take() else {
            return;
        };
        let cleanup = self.runtime.spawn(async move {
            actor.request_cancel();
            let _cleanup_verdict = actor.join().await;
        });
        drop(cleanup);
    }
}

impl QuinnLegActorBootstrap {
    pub(crate) async fn start(self) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        self.start_with_clock(SessionMonotonicClock::start()).await
    }

    pub(crate) async fn start_with_clock(
        mut self,
        session_clock: SessionMonotonicClock,
    ) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        use QuinnLegActorError::BootstrapConsumed;
        let input = ServerBootstrapInput {
            connecting: self.connecting.take().ok_or(BootstrapConsumed)?,
            reservation: self.reservation.take().ok_or(BootstrapConsumed)?,
            leg: self.leg,
            limits: self.limits,
            peer_policy: self.peer_policy,
            deadline: self.deadline,
            #[cfg(test)]
            fail_start_before_publication: self.fail_start_before_publication,
            session_clock,
        };
        let task_guard = BootstrapTaskTerminationGuard::new(&input.reservation);
        let (result_tx, result_rx) = oneshot::channel();
        let handoff_runtime = self.runtime.clone();
        let task = self.runtime.spawn(async move {
            let mut task_guard = task_guard;
            let result = run_server_bootstrap(input).await;
            if !matches!(result, Err(QuinnLegActorError::EndpointReleaseLost)) {
                task_guard.mark_clean();
            }
            drop(task_guard);
            let result = result.map(|actor| ServerBootstrapHandoff::new(handoff_runtime, actor));
            let _unclaimed = result_tx.send(result);
        });
        match result_rx.await {
            Ok(Ok(handoff)) => {
                drop(task);
                handoff.claim()
            }
            Ok(Err(error)) => {
                drop(task);
                Err(error)
            }
            Err(_sender_lost) => match task.await {
                Ok(()) => Err(QuinnLegActorError::BootstrapConsumed),
                Err(error) => Err(QuinnLegActorError::TaskJoin(error.to_string())),
            },
        }
    }
}

impl Drop for QuinnLegActorBootstrap {
    fn drop(&mut self) {
        let (Some(mut connecting), Some(reservation)) =
            (self.connecting.take(), self.reservation.take())
        else {
            return;
        };
        let task_guard = BootstrapTaskTerminationGuard::new(&reservation);
        let cleanup = self.runtime.spawn(async move {
            let mut task_guard = task_guard;
            let Some(drain) = connecting.take_drain_handle() else {
                drop((connecting, reservation));
                return;
            };
            drain.abandon();
            drop(connecting);
            if !drain.drained().await {
                drop(reservation);
                return;
            }
            drop(reservation);
            task_guard.mark_clean();
        });
        drop(cleanup);
    }
}

struct ServerBootstrapInput {
    connecting: quinn::Connecting,
    reservation: AcceptReservation,
    leg: LegId,
    limits: QuinnLegActorLimits,
    peer_policy: AttachPeerPolicy,
    session_clock: SessionMonotonicClock,
    deadline: tokio::time::Instant,
    #[cfg(test)]
    fail_start_before_publication: bool,
}
async fn run_server_bootstrap(
    ServerBootstrapInput {
        mut connecting,
        reservation,
        leg,
        limits,
        peer_policy,
        session_clock,
        deadline,
        #[cfg(test)]
        fail_start_before_publication,
    }: ServerBootstrapInput,
) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
    let drain = connecting
        .take_drain_handle()
        .ok_or(QuinnLegActorError::EndpointReleaseLost)?;
    let mut drain = Some(drain);
    let mut endpoint_driver_drain = None;
    let mut reservation = Some(reservation);
    let cleanup_slot = Arc::clone(
        &reservation
            .as_ref()
            .ok_or(QuinnLegActorError::BootstrapConsumed)?
            .slot,
    );
    let result = tokio::time::timeout_at(deadline, async {
        ensure_bootstrap_deadline(deadline)?;
        let connection = connecting
            .await
            .map_err(|error| QuinnLegActorError::Handshake(error.to_string()))?;
        validate_alpn(&connection)?;
        let exporter = export_binding(&connection, peer_policy)?;
        binding_confirmation(&connection, ActorRole::Owner, leg, peer_policy, &exporter).await?;
        let streams = bootstrap_streams(&connection, ActorRole::Owner, leg).await?;
        let binding = attach_binding(peer_policy, exporter)?;
        start_actor(
            connection,
            ActorBootstrapEndpointOwnership::Server,
            &mut drain,
            &mut endpoint_driver_drain,
            ActorRole::Owner,
            leg,
            limits,
            binding,
            streams,
            session_clock,
            deadline,
            #[cfg(test)]
            fail_start_before_publication,
            reservation.take(),
        )
    })
    .await
    .map_err(|_| QuinnLegActorError::BootstrapTimedOut)
    .and_then(std::convert::identity);
    if result.is_err() {
        let drain = drain
            .take()
            .ok_or(QuinnLegActorError::EndpointReleaseLost)?;
        drain.abandon();
        if !drain.drained().await {
            drop(reservation);
            drop(cleanup_slot);
            return Err(QuinnLegActorError::EndpointReleaseLost);
        }
    }
    drop(reservation);
    drop(cleanup_slot);
    result
}

pub(super) struct BootstrapTaskTerminationGuard {
    registry: Arc<ListenerRegistry>,
    slot: Option<Arc<ActorSlotLease>>,
    active: bool,
}

impl BootstrapTaskTerminationGuard {
    pub(super) fn new(reservation: &AcceptReservation) -> Self {
        let registry = Arc::clone(&reservation.slot.registry);
        registry.terminating_tasks.fetch_add(1, Ordering::AcqRel);
        Self {
            registry,
            slot: Some(Arc::clone(&reservation.slot)),
            active: true,
        }
    }

    fn mark_clean(&mut self) {
        self.active = false;
    }
}

impl Drop for BootstrapTaskTerminationGuard {
    fn drop(&mut self) {
        if self.active {
            self.registry.mark_unclean();
        }
        let previous = self
            .registry
            .terminating_tasks
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous >= 1);
        self.registry.task_termination.notify_waiters();
        // Keep the last live-slot reference behind both sticky poison and the
        // termination-counter release. Otherwise another admission could
        // reuse this slot while the old guard is still counted and exceed the
        // checked max_live * PEAK_TERMINATION_GUARDS_PER_ACTOR peak.
        drop(self.slot.take());
    }
}
