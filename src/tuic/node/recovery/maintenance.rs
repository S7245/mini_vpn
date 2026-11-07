//! Healthy auxiliary replacement inside NodeHost's saved recovery executor.
//! Owns original preparation transfer, one-time proof receipt and actual service
//! proof; root recovery owns publication, candidate witness and predecessor.
//! No task or DATA authority. Entry: request/await_healthy_replacement and recover.
//! Real peer lifecycle tests: node/tests/healthy_recovery.rs.
use super::*;

#[derive(Debug)]
pub(in crate::tuic) struct SubmissionFailure {
    pub(in crate::tuic) error: ClientError,
    pub(in crate::tuic) settled: bool,
}
impl From<ClientError> for SubmissionFailure {
    fn from(error: ClientError) -> Self {
        Self {
            error,
            settled: false,
        }
    }
}

pub(in crate::tuic) struct Receipt {
    index: usize,
    identity: TcpPoolTransportIdentity,
    proof: Proof,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::tuic) enum Proof {
    Replacement,
    Probed,
}
impl Receipt {
    pub(in crate::tuic) fn consume(self, reservation: &TcpPoolSlotReservation) -> bool {
        self.consume_proof(reservation) == Some(Proof::Replacement)
    }
    pub(in crate::tuic) fn consume_proof(
        self,
        reservation: &TcpPoolSlotReservation,
    ) -> Option<Proof> {
        (reservation.index == self.index
            && reservation
                .candidates
                .iter()
                .find(|c| c.index == self.index)
                .and_then(|c| c.identity)
                == Some(self.identity))
        .then_some(self.proof)
    }
}

impl TuicUpstream {
    pub(in crate::tuic) async fn request_healthy_replacement(
        &self,
        replacement: TcpPoolAuxiliaryReplacementPreparation,
    ) -> Result<Ticket, SubmissionFailure> {
        // Fix the budget before waiting for the original slot, not at handshake.
        let deadline = tokio::time::Instant::now() + TUIC_RECONNECT_TIMEOUT;
        let slot = self
            .tcp_pool_slots
            .get(replacement.index)
            .ok_or_else(|| io_err("tuic healthy recovery", "slot missing"))?;
        let slot = slot.state.lock().await;
        self.submit_healthy_replacement(replacement, slot, deadline)
    }

    pub(in crate::tuic) fn submit_healthy_replacement(
        &self,
        replacement: TcpPoolAuxiliaryReplacementPreparation,
        slot: tokio::sync::MutexGuard<'_, TcpPoolGenerationSlotState<Connection>>,
        deadline: tokio::time::Instant,
    ) -> Result<Ticket, SubmissionFailure> {
        let index = replacement.index;
        if !self.tcp_pool_slots.get(index).is_some_and(|expected| {
            std::ptr::eq(&expected.state, tokio::sync::MutexGuard::mutex(&slot))
        }) {
            drop(slot);
            drop(replacement);
            return Err(io_err("tuic healthy recovery", "guard belongs to another slot").into());
        }
        let Some(owner) = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            drop(slot);
            drop(replacement);
            return Err(io_err("tuic healthy recovery", "node owner unavailable").into());
        };
        let identity = replacement.identity;
        let mut preparation = Some(replacement.preparation);
        let mut root = owner.state();
        let mut previous = None;
        let mut safely_rejected = false;
        let result = (|| {
            if index == 0
                || root.stopping
                || root.quarantined
                || root.config_rejected
                || slot.current.identity() != identity
                || slot.draining.is_some()
                || !preparation.as_ref().is_some_and(|p| {
                    Arc::ptr_eq(
                        &p.preparing,
                        &self.tcp_pool_admission.preparing_slots[index],
                    ) && p.preparing.load(Ordering::Acquire)
                        && Arc::ptr_eq(&p.available, &self.tcp_pool_admission.available)
                })
            {
                return Err(io_err(
                    "tuic healthy recovery",
                    "original request inventory invalid",
                ));
            }
            if !probe::slot_available(&root.recovery, index)
                || root.recovery.demands[index].as_ref().is_some_and(|d| {
                    d.outcome == Outcome::Pending || (d.outcome == Outcome::Failed && !d.settled)
                })
            {
                return Err(io_err(
                    "tuic healthy recovery",
                    "original demand still owns slot",
                ));
            }
            if slot.current.transport.close_reason().is_some()
                || tokio::time::Instant::now() >= deadline
            {
                safely_rejected = true;
                return Err(io_err(
                    "tuic healthy recovery",
                    "request closed or expired before transfer",
                ));
            }
            let sequence = root
                .recovery
                .sequence
                .checked_add(1)
                .ok_or_else(|| io_err("tuic healthy recovery", "ticket sequence exhausted"))?;
            let ticket = Ticket {
                index,
                identity,
                deadline,
                sequence,
                kind: Kind::Healthy,
            };
            root.recovery.sequence = sequence;
            previous = root.recovery.demands[index].replace(Demand {
                ticket,
                outcome: Outcome::Pending,
                attempts: 0,
                preparation: preparation.take(),
                settled: false,
                ready_identity: None,
                receipt_issued: false,
            });
            Ok(ticket)
        })();
        drop(root);
        drop(slot);
        drop((previous, preparation));
        owner.wake_recovery();
        result.map_err(|error| SubmissionFailure {
            error,
            settled: safely_rejected,
        })
    }

    pub(in crate::tuic) async fn await_healthy_replacement(
        &self,
        ticket: Ticket,
    ) -> Result<Receipt, ClientError> {
        self.await_aux_recovery(ticket).await?;
        self.poll_maintenance_receipt(ticket)?
            .ok_or_else(|| io_err("tuic healthy recovery", "ready receipt missing"))
    }

    pub(in crate::tuic) fn poll_maintenance_receipt(
        &self,
        ticket: Ticket,
    ) -> Result<Option<Receipt>, ClientError> {
        if !self.observe_aux_recovery(ticket)? {
            return Ok(None);
        }
        let owner = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| io_err("tuic healthy recovery", "node owner unavailable"))?;
        let mut root = owner.state();
        if root.stopping || root.quarantined || root.config_rejected {
            return Err(recorded_failure());
        }
        let demand = root.recovery.demands[ticket.index]
            .as_mut()
            .ok_or_else(|| io_err("tuic healthy recovery", "demand missing"))?;
        if demand.ticket != ticket
            || ticket.kind == Kind::Closed
            || demand.outcome != Outcome::Ready
            || demand.receipt_issued
        {
            return Err(io_err(
                "tuic healthy recovery",
                "receipt unavailable or already issued",
            ));
        }
        let identity = demand
            .ready_identity
            .ok_or_else(|| io_err("tuic healthy recovery", "successor identity missing"))?;
        demand.receipt_issued = true;
        Ok(Some(Receipt {
            index: ticket.index,
            identity,
            proof: if ticket.kind == Kind::Healthy {
                Proof::Replacement
            } else {
                Proof::Probed
            },
        }))
    }

    pub(in crate::tuic) fn healthy_fallback_allowed(&self, ticket: Ticket) -> bool {
        let Some(owner) = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            return false;
        };
        let root = owner.state();
        !root.stopping
            && !root.quarantined
            && !root.config_rejected
            && root
                .recovery
                .demands
                .get(ticket.index)
                .and_then(Option::as_ref)
                .is_some_and(|d| {
                    d.ticket == ticket
                        && ticket.kind == Kind::Healthy
                        && d.outcome == Outcome::Failed
                        && d.settled
                })
    }
}

pub(super) async fn recover(
    owner: &StartupResources,
    data: &TuicUpstream,
    config: &TuicClientConfig,
    ticket: Ticket,
) -> Result<(), ClientError> {
    let slot = &data.tcp_pool_slots[ticket.index];
    {
        let state = slot.state.lock().await;
        let mut root = owner.state();
        if root.stopping
            || root.quarantined
            || root.config_rejected
            || state.current.identity() != ticket.identity
            || state.draining.is_some()
            || root.recovery.preparation.is_some()
            || root.recovery.permit.is_some()
        {
            return Err(io_err(
                "tuic healthy recovery",
                "original custody unavailable",
            ));
        }
        let demand = root.recovery.demands[ticket.index]
            .as_mut()
            .ok_or_else(recorded_failure)?;
        if demand.ticket != ticket || demand.outcome != Outcome::Pending {
            return Err(recorded_failure());
        }
        demand.attempts = 0;
        let preparation = demand.preparation.take().ok_or_else(recorded_failure)?;
        root.recovery.preparation = Some(preparation);
        root.recovery.permit =
            TcpPoolDrainingPermit::try_acquire(data.tcp_pool_draining_predecessor.clone());
        if root.recovery.permit.is_none() {
            return Err(io_err(
                "tuic healthy recovery",
                "predecessor capacity unavailable",
            ));
        }
    }
    let handoff = slot
        .prepare_replacement_current_service_handoff(ticket.identity)
        .await
        .map_err(|error| io_err("tuic healthy recovery", format!("handoff: {error:?}")))?;
    let mut attempts = 0;
    let candidate = loop {
        if tokio::time::Instant::now() >= ticket.deadline {
            return Err(io_err("tuic healthy recovery", "deadline before attempt"));
        }
        let attempt = attempts;
        attempts += 1;
        {
            let mut root = owner.state();
            if root.stopping || root.quarantined || root.config_rejected {
                return Err(recorded_failure());
            }
            let demand = root.recovery.demands[ticket.index]
                .as_mut()
                .ok_or_else(recorded_failure)?;
            if demand.ticket != ticket || demand.outcome != Outcome::Pending {
                return Err(recorded_failure());
            }
            demand.attempts = attempts as u8;
        }
        match owner.handshake(&data.endpoint, config).await {
            Ok(candidate) => break candidate,
            Err(error) => {
                owner.record_startup_result(error);
                let stop_retry = {
                    let root = owner.state();
                    root.quarantined || root.config_rejected
                };
                if stop_retry {
                    return Err(recorded_failure());
                }
                let delay = tcp_pool_aux_retry_delay(attempt).ok_or_else(recorded_failure)?;
                // handshake returned only after the original failed candidate drained.
                // The outer saved ticket still bounds all attempts and proof to 5s.
                tokio::time::sleep(delay).await;
            }
        }
    };
    let proof = prove_tcp_pool_successor_forward_service(
        &candidate,
        Some(handoff.required_cwnd_floor),
        ticket.deadline,
    )
    .await?;
    owner.reap_stats().await?;
    if let (Some(secs), Some(stop)) = (data.quic_stats_secs, &data.quic_stats_stop) {
        owner.spawn_stats_identified(Some(candidate.stable_id()), || {
            spawn_quic_stats_logger(candidate.clone(), ticket.index, secs, stop.subscribe())
        })?;
    }
    commit(owner, data, ticket, candidate, attempts as u64, Some(proof)).await
}

pub(super) async fn join_retired_stats(owner: &StartupResources) -> Result<(), ClientError> {
    loop {
        let task = {
            let mut root = owner.state();
            let index = root.tasks.iter().position(|task| {
                task.stable_id.is_some_and(|id| {
                    !root
                        .connections
                        .iter()
                        .flatten()
                        .any(|entry| entry.stable_id == Some(id))
                })
            });
            index.map(|index| root.tasks.swap_remove(index))
        };
        let Some(task) = task else {
            break;
        };
        owner.join_task(task.handle).await;
        owner.state().tasks_joined += 1;
    }
    if owner.state().quarantined {
        Err(recorded_failure())
    } else {
        Ok(())
    }
}
