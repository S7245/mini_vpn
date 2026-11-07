//! Idle maintenance owned by the saved NodeHost executor. Transfers the original
//! reservation, settles its lease outside guards, and retains partial debit debt.
//! Owns probe/one-handshake phase budgets, not DATA or another executor. Entries:
//! submit_probe, settle, recover; real-peer tests: node/tests/probe_recovery.rs.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::tuic) enum ProbeKind {
    Idle,
    PreviousFailure,
}
impl ProbeKind {
    pub(in crate::tuic) fn deadline(self) -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(if self == Self::Idle { 7 } else { 6 })
    }
}
pub(in crate::tuic) struct ProbeSubmissionFailure {
    pub(in crate::tuic) error: ClientError,
    pub(in crate::tuic) reservation: TcpPoolSlotReservation,
}
pub(super) struct ProbeSlot {
    pub(super) ticket: Ticket,
    state: CustodyState,
}
impl ProbeSlot {
    pub(super) fn complete(&self) -> bool {
        self.state.complete()
    }
}
pub(super) fn slot_available(state: &State, index: usize) -> bool {
    state
        .probe_custody
        .get(index)
        .is_some_and(|slot| slot.as_ref().is_none_or(|slot| slot.state.complete()))
}
pub(super) enum CustodyState {
    Held(Custody),
    Settling,
}
impl CustodyState {
    pub(super) fn complete(&self) -> bool {
        matches!(self, Self::Held(c) if c.complete() && c.preparation.is_none())
    }
}
pub(super) struct Custody {
    lease: TcpPoolSlotLease,
    preparation: Option<TcpPoolSlotPreparation>,
    interrupted: bool,
}
impl Custody {
    pub(super) fn complete(&self) -> bool {
        !self.interrupted && self.lease.release.is_some_and(|r| r.complete())
    }
}

impl TuicUpstream {
    pub(in crate::tuic) fn node_probe_kind(
        &self,
        reservation: &TcpPoolSlotReservation,
        current: &TcpPoolGeneration<Connection>,
        proved: bool,
    ) -> Option<ProbeKind> {
        if reservation.index == 0 || reservation.active_before != 0 {
            return None;
        }
        if current.open_state.needs_reconnect(reservation.index) {
            Some(ProbeKind::PreviousFailure)
        } else if should_probe_tcp_pool_generation(
            reservation.index,
            self.clock.elapsed().as_secs(),
            current.open_state.last_success_secs().unwrap_or(0),
            true,
            proved,
        ) {
            Some(ProbeKind::Idle)
        } else {
            None
        }
    }

    pub(in crate::tuic) fn submit_probe(
        &self,
        mut reservation: TcpPoolSlotReservation,
        slot: tokio::sync::MutexGuard<'_, TcpPoolGenerationSlotState<Connection>>,
        kind: ProbeKind,
        deadline: tokio::time::Instant,
    ) -> Result<Ticket, ProbeSubmissionFailure> {
        let index = reservation.index;
        let Some(owner) = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            drop(slot);
            return Err(ProbeSubmissionFailure {
                error: recorded_failure(),
                reservation,
            });
        };
        let mut root = owner.state();
        let result = (|| {
            let current = &slot.current;
            let valid = index != 0
                && self
                    .tcp_pool_slots
                    .get(index)
                    .is_some_and(|s| std::ptr::eq(&s.state, tokio::sync::MutexGuard::mutex(&slot)))
                && reservation
                    .candidates
                    .iter()
                    .find(|c| c.index == index)
                    .and_then(|c| c.identity)
                    == Some(current.identity())
                && reservation.active_before == 0
                && current.activity.active() == 1
                && reservation.lease.release.is_none()
                && reservation
                    .lease
                    .generation_active
                    .as_ref()
                    .is_some_and(|a| Arc::ptr_eq(a, &current.activity))
                && self
                    .tcp_pool_admission
                    .active_slots
                    .get(index)
                    .is_some_and(|a| Arc::ptr_eq(a, &reservation.lease.slot_active))
                && reservation
                    .lease
                    .pool_active_total
                    .as_ref()
                    .is_some_and(|a| Arc::ptr_eq(a, &self.tcp_pool_admission.pool_active_total))
                && self
                    .tcp_pool_admission
                    .preparing_slots
                    .get(index)
                    .is_some_and(|a| {
                        Arc::ptr_eq(a, &reservation.preparation.preparing)
                            && a.load(Ordering::Acquire)
                    })
                && Arc::ptr_eq(
                    &reservation.preparation.available,
                    &self.tcp_pool_admission.available,
                )
                && slot.draining.is_none();
            if !valid
                || !slot_available(&root.recovery, index)
                || root.stopping
                || root.quarantined
                || root.config_rejected
                || tokio::time::Instant::now() >= deadline
                || root
                    .recovery
                    .demands
                    .get(index)
                    .and_then(Option::as_ref)
                    .is_some_and(|d| {
                        d.outcome == Outcome::Pending
                            || (d.outcome == Outcome::Failed && !d.settled)
                    })
            {
                return Err(io_err(
                    "tuic probe",
                    "original idle reservation unavailable",
                ));
            }
            let sequence = root
                .recovery
                .sequence
                .checked_add(1)
                .ok_or_else(recorded_failure)?;
            Ok(Ticket {
                index,
                identity: current.identity(),
                deadline,
                sequence,
                kind: Kind::Probe(kind),
            })
        })();
        let ticket = match result {
            Ok(ticket) => ticket,
            Err(error) => {
                drop(root);
                drop(slot);
                return Err(ProbeSubmissionFailure { error, reservation });
            }
        };
        reservation.lease.retain_until_release();
        let old = root.recovery.demands[index].replace(Demand {
            ticket,
            outcome: Outcome::Pending,
            attempts: 0,
            preparation: None,
            settled: false,
            ready_identity: None,
            receipt_issued: false,
        });
        let previous_custody = root.recovery.probe_custody[index].replace(ProbeSlot {
            ticket,
            state: CustodyState::Held(Custody {
                lease: reservation.lease,
                preparation: Some(reservation.preparation),
                interrupted: false,
            }),
        });
        root.recovery.sequence = ticket.sequence;
        drop(root);
        drop(slot);
        drop((old, previous_custody));
        owner.wake_recovery();
        Ok(ticket)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Settlement {
    Complete,
    Busy,
    Quarantined,
}

/// All callers (run, expiry, stop) use this one debit. Even a panic in the actual
/// zero waker retains the mutated original report; no retry or Drop fallback.
pub(super) fn settle(owner: &StartupResources, ticket: Ticket, activate: bool) -> Settlement {
    if !matches!(ticket.kind, Kind::Probe(_)) {
        return Settlement::Complete;
    }
    let mut custody = {
        let mut root = owner.state();
        // A fully returned old physical record can outlive its control demand.
        // It carries no lease/preparation debt and must not poison a later open.
        if root.recovery.probe_custody[ticket.index]
            .as_ref()
            .is_some_and(|slot| slot.ticket == ticket && slot.state.complete())
        {
            return Settlement::Complete;
        }
        if !root.recovery.demands[ticket.index]
            .as_ref()
            .is_some_and(|d| d.ticket == ticket)
        {
            root.quarantined = true;
        }
        let Some(slot) = root.recovery.probe_custody[ticket.index]
            .as_mut()
            .filter(|s| s.ticket == ticket)
        else {
            root.quarantined = true;
            return Settlement::Quarantined;
        };
        match std::mem::replace(&mut slot.state, CustodyState::Settling) {
            CustodyState::Held(custody) => custody,
            CustodyState::Settling => return Settlement::Busy,
        }
    };
    if custody.lease.release.is_none() {
        custody.interrupted =
            catch_unwind(AssertUnwindSafe(|| custody.lease.release_once())).is_err();
    }
    let complete = custody.complete();
    let mut root = owner.state();
    if !complete {
        root.quarantined = true;
        root.recovery.release_failure.get_or_insert((
            ticket.index,
            ticket.identity,
            custody.lease.release,
        ));
    }
    let valid_ticket = root.recovery.demands[ticket.index]
        .as_ref()
        .is_some_and(|d| d.ticket == ticket);
    if !valid_ticket {
        root.quarantined = true;
    }
    // Settling reserves this physical slot until this original debit returns.
    // No submit can replace it; a damaged control demand never receives custody.
    let preparation = if complete && valid_ticket && !root.quarantined {
        custody.preparation.take()
    } else {
        None
    };
    root.recovery.probe_custody[ticket.index] = Some(ProbeSlot {
        ticket,
        state: CustodyState::Held(custody),
    });
    let preparation = if activate && complete && !root.stopping && !root.quarantined {
        if root.recovery.preparation.is_some() {
            root.quarantined = true;
            preparation
        } else {
            root.recovery.preparation = preparation;
            None
        }
    } else {
        preparation
    };
    let valid = complete && !root.quarantined;
    drop(root);
    drop(preparation);
    if !valid {
        owner.record(io_err(
            "tuic probe lease",
            "original release incomplete or interrupted",
        ));
    }
    owner.wake_recovery();
    if valid {
        Settlement::Complete
    } else {
        Settlement::Quarantined
    }
}

pub(super) async fn recover(
    owner: &StartupResources,
    data: &TuicUpstream,
    config: &TuicClientConfig,
    ticket: Ticket,
    kind: ProbeKind,
) -> Result<(), ClientError> {
    if settle(owner, ticket, true) != Settlement::Complete {
        return Err(recorded_failure());
    }
    let conn = {
        let slot = data.tcp_pool_slots[ticket.index].state.lock().await;
        let root = owner.state();
        if root.stopping
            || root.quarantined
            || root.config_rejected
            || tokio::time::Instant::now() >= ticket.deadline
            || slot.current.identity() != ticket.identity
            || slot.current.activity.active() != 0
            || root.recovery.preparation.is_none()
            || slot.draining.is_some()
        {
            return Err(io_err("tuic probe", "original idle custody changed"));
        }
        slot.current.transport.clone()
    };
    if tokio::time::Instant::now() >= ticket.deadline {
        return Err(io_err("tuic probe", "deadline before initial phase"));
    }
    if kind == ProbeKind::Idle {
        let deadline = (tokio::time::Instant::now() + TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT)
            .min(ticket.deadline);
        let outcome = tokio::time::timeout_at(
            deadline,
            data.probe_tcp_pool_connection(&conn, TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT),
        )
        .await;
        if matches!(outcome, Ok(TcpPoolProbeOutcome::Alive { .. })) {
            // Return preparation before Ready, then verify the original physical slot.
            let preparation = owner.state().recovery.preparation.take();
            drop(preparation);
            let slot = data.tcp_pool_slots[ticket.index].state.lock().await;
            let mut root = owner.state();
            if root.stopping
                || root.quarantined
                || root.config_rejected
                || slot.current.identity() != ticket.identity
                || conn.close_reason().is_some()
                || tokio::time::Instant::now() >= ticket.deadline
            {
                return Err(recorded_failure());
            }
            let demand = root.recovery.demands[ticket.index]
                .as_mut()
                .ok_or_else(recorded_failure)?;
            if demand.ticket != ticket || demand.outcome != Outcome::Pending {
                return Err(recorded_failure());
            }
            demand.ready_identity = Some(ticket.identity);
            demand.settled = true;
            demand.outcome = Outcome::Ready;
            root.recovery.active_ticket = None;
            drop(root);
            drop(slot);
            drop(conn);
            owner.wake_recovery();
            return Ok(());
        }
    }
    {
        let mut root = owner.state();
        if tokio::time::Instant::now() >= ticket.deadline {
            return Err(io_err("tuic probe", "deadline before handshake"));
        }
        if root.stopping || root.quarantined || root.config_rejected {
            return Err(recorded_failure());
        }
        root.recovery.permit =
            TcpPoolDrainingPermit::try_acquire(data.tcp_pool_draining_predecessor.clone());
        if root.recovery.permit.is_none() {
            return Err(io_err("tuic probe", "predecessor capacity unavailable"));
        }
        if let Some(d) = root.recovery.demands[ticket.index].as_mut() {
            d.attempts = 1;
        }
    }
    if tokio::time::Instant::now() >= ticket.deadline {
        return Err(io_err("tuic probe", "deadline before close"));
    }
    conn.close(VarInt::from_u32(0), b"tcp_pool_probe_reconnect");
    drop(conn);
    let deadline = (tokio::time::Instant::now() + TUIC_RECONNECT_TIMEOUT).min(ticket.deadline);
    if tokio::time::Instant::now() >= deadline {
        return Err(io_err("tuic probe", "deadline at handshake entry"));
    }
    let candidate = tokio::time::timeout_at(deadline, owner.handshake(&data.endpoint, config))
        .await
        .map_err(|_| io_err("tuic probe", "original handshake phase expired"))??;
    let deadline =
        (tokio::time::Instant::now() + TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT).min(ticket.deadline);
    if tokio::time::Instant::now() >= deadline {
        return Err(io_err("tuic probe", "deadline before ready probe"));
    }
    let ready = tokio::time::timeout_at(
        deadline,
        data.probe_tcp_pool_connection(&candidate, TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT),
    )
    .await;
    if !matches!(ready, Ok(TcpPoolProbeOutcome::Alive { .. })) {
        return Err(io_err("tuic probe", "actual successor ready probe failed"));
    }
    owner.reap_stats().await?;
    if let (Some(secs), Some(stop)) = (data.quic_stats_secs, &data.quic_stats_stop) {
        owner.spawn_stats_identified(Some(candidate.stable_id()), || {
            spawn_quic_stats_logger(candidate.clone(), ticket.index, secs, stop.subscribe())
        })?;
    }
    commit(owner, data, ticket, candidate, 1, None).await
}

#[cfg(test)]
mod tests;
