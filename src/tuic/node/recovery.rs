//! NodeHost's saved closed-auxiliary recovery transaction. Owns fixed demands,
//! original preparation/draining permits and predecessor retirement. Only run
//! polls physical work; callers submit/observe tickets and re-admit after commit.
//! Primary uses original UDP seals; healthy predecessors retain their live carriers.
//! No new tasks or DATA-path coordination. Real-peer tests: node/tests/recovery.rs.
use super::*;
mod maintenance;
mod probe;
mod waiting;
#[cfg(test)]
mod tests;
pub(in crate::tuic) use maintenance::{Proof, Receipt, SubmissionFailure};
pub(in crate::tuic) use probe::{ProbeKind, ProbeSubmissionFailure};
#[cfg(test)]
pub(super) use tests::ClearGate;

pub(super) struct StatsTask {
    pub(super) stable_id: Option<usize>,
    pub(super) handle: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::tuic) struct Ticket {
    index: usize,
    identity: TcpPoolTransportIdentity,
    deadline: tokio::time::Instant,
    sequence: u64,
    kind: Kind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Closed,
    Healthy,
    Probe(ProbeKind),
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Pending,
    Ready,
    Failed,
}
struct Demand {
    ticket: Ticket,
    outcome: Outcome,
    attempts: u8,
    preparation: Option<TcpPoolSlotPreparation>,
    settled: bool,
    ready_identity: Option<TcpPoolTransportIdentity>,
    receipt_issued: bool,
}
pub(super) struct Predecessor {
    pub(super) generation: TerminalGeneration,
    pub(super) permit: TcpPoolDrainingPermit,
    /// `None` only when Waiting already retired this physical entry (D152 A2).
    entry: Option<usize>,
    healthy: bool,
}
pub(super) struct State {
    #[cfg(test)]
    pub(super) clear_gate: Option<Arc<ClearGate>>,
    demands: Vec<Option<Demand>>,
    sequence: u64,
    active_ticket: Option<Ticket>,
    changed: Arc<Notify>,
    pub(super) preparation: Option<TcpPoolSlotPreparation>,
    pub(super) permit: Option<TcpPoolDrainingPermit>,
    pub(super) predecessor: Option<Predecessor>,
    pub(super) primary_predecessor: Option<Predecessor>,
    pub(super) primary_permit: Arc<AtomicBool>,
    probe_custody: Vec<Option<probe::ProbeSlot>>,
    /// N2 Waiting side table (`waiting`), one row per slot.
    retries: Vec<Option<waiting::Retry>>,
    /// D152 (A2): identity of a slot's current connection whose storage and
    /// physical entry were released while its closed demand settled. Its
    /// address-based stable id may be reused by a later connection.
    released: Vec<Option<TcpPoolTransportIdentity>>,
    /// N2 counters: Waiting entries, re-mints, and retryable handshake
    /// failures (counted, never recorded as Node/session failures).
    pub(super) waits: u64,
    pub(super) reprobes: u64,
    pub(super) waiting_failures: u64,
    pub(super) release_failure: Option<(
        usize,
        TcpPoolTransportIdentity,
        Option<tcp_pool_admission::LeaseReleaseReport>,
    )>,
}
/// N5-OBS (D241): value-only recovery census for the observer and the
/// transition line; never mutates the ledger.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Census {
    pub(super) pending: usize,
    pub(super) waiting: usize,
    pub(super) active: Option<(usize, u64, u64)>,
    pub(super) waits: u64,
    pub(super) reprobes: u64,
    pub(super) waiting_failures: u64,
}
impl State {
    pub(super) fn census(&self) -> Census {
        Census {
            pending: self
                .demands
                .iter()
                .flatten()
                .filter(|demand| demand.outcome == Outcome::Pending)
                .count(),
            waiting: self.retries.iter().flatten().filter(|retry| retry.parked()).count(),
            active: self
                .active_ticket
                .map(|t| (t.index, t.identity.generation, t.sequence)),
            waits: self.waits,
            reprobes: self.reprobes,
            waiting_failures: self.waiting_failures,
        }
    }
    #[cfg(test)]
    pub(super) fn retries_held(&self) -> (usize, usize) {
        let held = self.retries.iter().flatten();
        (held.clone().count(), held.filter(|retry| retry.parked()).count())
    }
    #[cfg(test)]
    pub(super) fn completed_probe_custodies(&self) -> usize {
        self.probe_custody
            .iter()
            .flatten()
            .filter(|slot| slot.complete())
            .count()
    }

    /// D152 (A2): this exact current connection's physical entry is retired.
    pub(super) fn released(&self, index: usize, identity: TcpPoolTransportIdentity) -> bool {
        self.released.get(index).copied().flatten() == Some(identity)
    }

    pub(super) fn has_rebuild_debt(&self) -> bool {
        self.fallback_pending()
            || self.predecessor.is_some()
            || self.primary_predecessor.is_some()
            || self.primary_permit.load(Ordering::Acquire)
            || self.release_failure.is_some()
            || self
                .probe_custody
                .iter()
                .flatten()
                .any(|slot| !slot.complete())
            || self
                .demands
                .iter()
                .flatten()
                .any(|d| d.preparation.is_some())
    }

    pub(super) fn fallback_pending(&self) -> bool {
        self.active_ticket.is_some()
            || self.preparation.is_some()
            || self.permit.is_some()
            || self
                .demands
                .iter()
                .flatten()
                .any(|demand| demand.outcome == Outcome::Failed && !demand.settled)
    }
    fn predecessor(&self, primary: bool) -> &Option<Predecessor> {
        if primary {
            &self.primary_predecessor
        } else {
            &self.predecessor
        }
    }
    fn predecessor_mut(&mut self, primary: bool) -> &mut Option<Predecessor> {
        if primary {
            &mut self.primary_predecessor
        } else {
            &mut self.predecessor
        }
    }
    pub(super) fn new(count: usize) -> Self {
        Self {
            #[cfg(test)]
            clear_gate: None,
            demands: (0..count).map(|_| None).collect(),
            sequence: 0,
            active_ticket: None,
            changed: Arc::new(Notify::new()),
            preparation: None,
            permit: None,
            predecessor: None,
            primary_predecessor: None,
            primary_permit: Arc::new(AtomicBool::new(false)),
            probe_custody: (0..count).map(|_| None).collect(),
            retries: (0..count).map(|_| None).collect(),
            released: vec![None; count],
            waits: 0,
            reprobes: 0,
            waiting_failures: 0,
            release_failure: None,
        }
    }
}

pub(super) fn configuration_connect_rejection(error: &quinn::ConnectError) -> bool {
    matches!(
        error,
        quinn::ConnectError::InvalidServerName(_)
            | quinn::ConnectError::InvalidRemoteAddress(_)
            | quinn::ConnectError::NoDefaultClientConfig
            | quinn::ConnectError::UnsupportedVersion
    )
}

fn configuration_crypto_rejection(code: u64) -> bool {
    let Some(alert) = code
        .checked_sub(0x100)
        .and_then(|code| u8::try_from(code).ok())
    else {
        return false;
    };
    use rustls::AlertDescription::*;
    matches!(
        rustls::AlertDescription::from(alert),
        HandshakeFailure
            | NoCertificate
            | BadCertificate
            | UnsupportedCertificate
            | CertificateRevoked
            | CertificateExpired
            | CertificateUnknown
            | IllegalParameter
            | UnknownCA
            | AccessDenied
            | DecryptError
            | ProtocolVersion
            | InsufficientSecurity
            | MissingExtension
            | UnsupportedExtension
            | CertificateUnobtainable
            | UnrecognisedName
            | BadCertificateStatusResponse
            | BadCertificateHashValue
            | UnknownPSKIdentity
            | CertificateRequired
            | NoApplicationProtocol
    )
}

impl StartupResources {
    pub(super) fn handshake_failure(&self, error: quinn::ConnectionError) -> ClientError {
        // Classify the typed QUIC/TLS evidence before bounded error conversion.
        // Arbitrary TUIC ApplicationClosed reasons never imply authentication ACK.
        let rejected = match &error {
            quinn::ConnectionError::VersionMismatch => true,
            quinn::ConnectionError::TransportError(error) => {
                configuration_crypto_rejection(error.code.into())
            }
            quinn::ConnectionError::ConnectionClosed(error) => {
                configuration_crypto_rejection(error.error_code.into())
            }
            _ => false,
        };
        self.state().config_rejected |= rejected;
        io_err("tuic handshake", error)
    }
    pub(in crate::tuic) fn wake_recovery(&self) {
        let changed = self.state().recovery.changed.clone();
        changed.notify_waiters();
    }
    fn expire_demands(&self) {
        let expired = {
            let mut state = self.state();
            let active = state.recovery.active_ticket;
            let mut expired = Vec::new();
            for demand in state.recovery.demands.iter_mut().flatten() {
                if demand.outcome == Outcome::Pending
                    && tokio::time::Instant::now() >= demand.ticket.deadline
                {
                    demand.outcome = Outcome::Failed;
                    expired.push((
                        demand.ticket,
                        demand.preparation.take(),
                        active == Some(demand.ticket),
                    ));
                }
            }
            // Physical custody remains authoritative if its control demand is
            // damaged. Use its original deadline and avoid the active slot.
            for slot in state.recovery.probe_custody.iter().flatten() {
                if !slot.complete()
                    && tokio::time::Instant::now() >= slot.ticket.deadline
                    && !expired.iter().any(|(ticket, _, _)| *ticket == slot.ticket)
                {
                    expired.push((
                        slot.ticket,
                        None,
                        active.is_some_and(|t| t.index == slot.ticket.index),
                    ));
                }
            }
            // N2 (D127, F-N2b): every clean expired closed demand stays
            // retryable (Waiting). The active one is classified and parked by
            // its run loop; a queued one is parked below once its custody settles.
            let retry: Vec<bool> = expired
                .iter()
                .map(|(ticket, _, _)| waiting::hold(&mut state, *ticket))
                .collect();
            expired.into_iter().zip(retry).collect::<Vec<_>>()
        };
        let changed = !expired.is_empty();
        for ((ticket, preparation, active), retry) in expired {
            drop(preparation); // Wake only after releasing the root mutex.
            let mut retry = retry;
            if !active {
                let settled = probe::settle(self, ticket, false) == probe::Settlement::Complete;
                if settled {
                    if let Some(demand) = self.state().recovery.demands[ticket.index].as_mut() {
                        if demand.ticket == ticket {
                            demand.settled = true;
                        }
                    }
                }
                if retry && settled {
                    waiting::park(self, ticket); // Arms the backoff, or drops an invalid row.
                    retry = self.state().recovery.retries[ticket.index].is_some();
                } else if retry {
                    // Unsettled custody stays terminal (D127), never parked Waiting.
                    waiting::clear(&mut self.state().recovery, ticket.index);
                    retry = false;
                }
            }
            if !retry {
                self.record(io_err("tuic recovery", "original demand deadline expired"));
            }
        }
        if changed {
            self.wake_recovery();
        }
    }
    pub(super) async fn retire_unpublished(&self) -> bool {
        let changed = self.state().recovery.changed.clone();
        let mut settled = true;
        let count = self.state().connections.len();
        for index in 0..count {
            if self.state().connections[index]
                .as_ref()
                .is_some_and(|entry| !entry.published)
            {
                let drain = self.retire(index);
                tokio::pin!(drain);
                loop {
                    let notified = changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    self.expire_demands();
                    let deadline = self
                        .state()
                        .recovery
                        .demands
                        .iter()
                        .flatten()
                        .filter(|d| d.outcome == Outcome::Pending)
                        .map(|d| d.ticket.deadline)
                        .min();
                    tokio::select! {
                        result = &mut drain => { settled &= result.is_ok(); break; },
                        _ = &mut notified => {},
                        _ = async { match deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } } => {},
                    }
                }
            }
        }
        settled
    }
    async fn reap_stats(&self) -> Result<(), ClientError> {
        loop {
            let task = {
                let mut state = self.state();
                let Some(index) = state
                    .tasks
                    .iter()
                    .position(|task| task.handle.is_finished())
                else {
                    break;
                };
                state.tasks.swap_remove(index)
            };
            // is_finished guarantees the saved handle's next poll cannot suspend.
            // The original stable ID remains on the handle until its actual join.
            let _stable_id = task.stable_id;
            self.join_task(task.handle).await;
            self.state().tasks_joined += 1;
        }
        if self.state().quarantined {
            Err(recorded_failure())
        } else {
            Ok(())
        }
    }
}

impl TuicUpstream {
    /// The original lease shares the retained admission/generation counters.
    /// Preserve the first exact report, count later failures, and never retry a debit.
    pub(in crate::tuic) fn release_recovery_lease(
        &self,
        index: usize,
        identity: TcpPoolTransportIdentity,
        activity: &Arc<TcpPoolGenerationActivity>,
        lease: &mut TcpPoolSlotLease,
    ) -> Result<(), ClientError> {
        let valid = lease
            .generation_active
            .as_ref()
            .is_some_and(|a| Arc::ptr_eq(a, activity))
            && self
                .tcp_pool_admission
                .active_slots
                .get(index)
                .is_some_and(|a| Arc::ptr_eq(a, &lease.slot_active))
            && lease
                .pool_active_total
                .as_ref()
                .is_some_and(|a| Arc::ptr_eq(a, &self.tcp_pool_admission.pool_active_total));
        lease.retain_until_release();
        let report = valid.then(|| lease.release_once());
        if report.is_some_and(|report| report.complete()) {
            return Ok(());
        }
        if let Some(owner) = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
        {
            {
                let mut root = owner.state();
                root.quarantined = true;
                root.recovery
                    .release_failure
                    .get_or_insert((index, identity, report));
            }
            owner.record(io_err(
                "tuic recovery lease",
                format!("identity_valid={valid}, report={report:?}"),
            ));
            owner.wake_recovery();
        }
        self.udp_runtime.stop_node();
        self.tcp_pool_admission.available.notify_waiters();
        Err(recorded_failure())
    }
    pub(in crate::tuic) fn request_aux_recovery(
        &self,
        index: usize,
        slot: tokio::sync::MutexGuard<
            '_,
            TcpPoolGenerationSlotState<crate::tuic::generation_slot::SlotConnection>,
        >,
    ) -> Result<Ticket, ClientError> {
        if !self.tcp_pool_slots.get(index).is_some_and(|expected| {
            std::ptr::eq(&expected.state, tokio::sync::MutexGuard::mutex(&slot))
        }) {
            return Err(io_err("tuic recovery", "guard belongs to another slot"));
        }
        let identity = slot.current.identity();
        let owner = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| io_err("tuic recovery", "node owner unavailable"))?;
        // The caller supplies its original closed-slot guard. This fixes the
        // first budget before any mutex/preparation wait, without a second lookup.
        let state = owner.state();
        if index >= state.recovery.demands.len()
            || state.stopping
            || state.quarantined
            || state.config_rejected
            || !probe::slot_available(&state.recovery, index)
        {
            return Err(io_err(
                "tuic recovery",
                "only running closed-slot recovery is enabled",
            ));
        }
        if let Some(demand) = &state.recovery.demands[index] {
            if demand.ticket.identity == identity && demand.ticket.kind == Kind::Closed {
                return Ok(demand.ticket);
            }
            if demand.outcome == Outcome::Pending
                || (demand.ticket.kind != Kind::Closed
                    && !demand.settled
                    && demand.outcome == Outcome::Failed)
            {
                return Err(io_err(
                    "tuic recovery",
                    "another generation still owns the slot demand",
                ));
            }
        }
        if slot.current.transport.close_reason().is_none() {
            return Err(io_err("tuic recovery", "stale or live demand rejected"));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        drop(state);
        if index == 0 {
            self.udp_runtime.freeze_primary(identity.generation)?;
        }
        let mut state = owner.state();
        if state.stopping || state.quarantined || state.config_rejected {
            drop(state);
            drop(slot);
            self.udp_runtime.generation.changed.notify_waiters();
            return Err(recorded_failure());
        }
        let sequence = state
            .recovery
            .sequence
            .checked_add(1)
            .ok_or_else(|| io_err("tuic recovery", "ticket sequence exhausted"))?;
        state.recovery.sequence = sequence;
        let ticket = Ticket {
            index,
            identity,
            deadline,
            sequence,
            kind: Kind::Closed,
        };
        state.recovery.demands[index] = Some(Demand {
            ticket,
            outcome: Outcome::Pending,
            attempts: 0,
            preparation: None,
            settled: false,
            ready_identity: None,
            receipt_issued: false,
        });
        waiting::join_parked(&mut state, ticket); // D141: known-dead path ⇒ Waiting at mint.
        let changed = state.recovery.changed.clone();
        drop(state);
        drop(slot);
        if index == 0 {
            self.udp_runtime.generation.changed.notify_waiters();
        }
        changed.notify_one();
        Ok(ticket)
    }
    pub(in crate::tuic) fn observe_aux_recovery(
        &self,
        ticket: Ticket,
    ) -> Result<bool, ClientError> {
        let owner = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| io_err("tuic recovery", "original node owner lost"))?;
        owner.expire_demands();
        let state = owner.state();
        if state.stopping || state.quarantined || state.config_rejected {
            return Err(recorded_failure());
        }
        let Some(demand) = state
            .recovery
            .demands
            .get(ticket.index)
            .and_then(Option::as_ref)
        else {
            return Err(io_err("tuic recovery", "original demand missing"));
        };
        if demand.ticket != ticket {
            return Err(io_err("tuic recovery", "original ticket superseded"));
        }
        match demand.outcome {
            Outcome::Pending => Ok(false),
            Outcome::Ready => Ok(true),
            Outcome::Failed if ticket.kind != Kind::Closed && !demand.settled => Ok(false),
            Outcome::Failed if waiting::holds(&state.recovery, ticket) => Err(waiting_failure()),
            Outcome::Failed => Err(recorded_failure()),
        }
    }
    pub(in crate::tuic) async fn await_aux_recovery(
        &self,
        ticket: Ticket,
    ) -> Result<(), ClientError> {
        let owner = self
            .udp_runtime
            .owner
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| io_err("tuic recovery", "original node owner lost"))?;
        let changed = owner.state().recovery.changed.clone();
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.observe_aux_recovery(ticket)? {
                return Ok(());
            }
            notified.await;
        }
    }
}

pub(super) async fn settle_uncommitted(owner: &StartupResources) {
    let changed = owner.state().recovery.changed.clone();
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if clear_uncommitted(owner) {
            return;
        }
        notified.await;
    }
}

pub(super) fn clear_uncommitted(owner: &StartupResources) -> bool {
    let tickets: Vec<_> = {
        let state = owner.state();
        let mut tickets: Vec<_> = state
            .recovery
            .demands
            .iter()
            .flatten()
            .filter(|d| state.stopping || state.recovery.active_ticket == Some(d.ticket))
            .map(|d| d.ticket)
            .collect();
        for slot in state.recovery.probe_custody.iter().flatten() {
            if (state.stopping
                || state
                    .recovery
                    .active_ticket
                    .is_some_and(|t| t.index == slot.ticket.index))
                && !tickets.contains(&slot.ticket)
            {
                tickets.push(slot.ticket);
            }
        }
        tickets
    };
    let mut busy = false;
    for ticket in tickets {
        busy |= probe::settle(owner, ticket, false) == probe::Settlement::Busy;
    }
    if busy {
        return false;
    }
    let (preparation, permit, active, pending) = {
        let mut state = owner.state();
        let stopping = state.stopping;
        let active = state.recovery.active_ticket;
        let pending: Vec<_> = state
            .recovery
            .demands
            .iter_mut()
            .flatten()
            .filter(|d| stopping || active == Some(d.ticket))
            .filter_map(|d| d.preparation.take())
            .collect();
        (
            state.recovery.preparation.take(),
            state.recovery.permit.take(),
            state.recovery.active_ticket.take(),
            pending,
        )
    };
    #[cfg(test)]
    let gate = owner.state().recovery.clear_gate.clone();
    #[cfg(test)]
    if let Some(gate) = &gate {
        gate.observe(1);
    }
    drop((preparation, permit, pending));
    #[cfg(test)]
    if let Some(gate) = &gate {
        gate.observe(2);
    }
    if let Some(ticket) = active {
        let mut root = owner.state();
        let complete = !root.quarantined && probe::slot_available(&root.recovery, ticket.index);
        if let Some(demand) = root.recovery.demands[ticket.index].as_mut() {
            if demand.ticket == ticket && complete {
                demand.settled = true;
            }
        }
    }
    owner.wake_recovery();
    true
}

pub(super) async fn run(
    owner: Arc<StartupResources>,
    data: Arc<TuicUpstream>,
    config: TuicClientConfig,
) -> Result<(), ClientError> {
    run_inner(&owner, &data, &config, None).await
}
pub(in crate::tuic) async fn run_native(
    owner: &Arc<StartupResources>,
    data: &Arc<TuicUpstream>,
    config: &TuicClientConfig,
    guard: &crate::owned_upstream::generation::callbacks::Guard,
) -> Result<(), ClientError> {
    run_inner(owner, data, config, Some(guard)).await
}
async fn run_inner(
    owner: &Arc<StartupResources>,
    data: &Arc<TuicUpstream>,
    config: &TuicClientConfig,
    guard: Option<&crate::owned_upstream::generation::callbacks::Guard>,
) -> Result<(), ClientError> {
    let changed = owner.state().recovery.changed.clone();
    let mut transitions = super::observe::Transitions::default();
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        owner.expire_demands();
        owner.reap_stats().await?;
        waiting::release_settled(owner, data).await?;
        let (old, primary_old, next, due, census) = {
            let mut state = owner.state();
            if state.stopping || state.quarantined {
                return Err(recorded_failure());
            }
            waiting::rearm_due(&mut state);
            let next = state
                .recovery
                .demands
                .iter()
                .flatten()
                .filter(|d| {
                    d.outcome == Outcome::Pending
                        && state.recovery.predecessor(d.ticket.index == 0).is_none()
                })
                .min_by_key(|d| d.ticket.deadline)
                .map(|d| d.ticket);
            state.recovery.active_ticket = next;
            (
                state.recovery.predecessor.is_some(),
                state.recovery.primary_predecessor.is_some(),
                next,
                waiting::next_due(&state.recovery),
                state.recovery.census(),
            )
        };
        // N5-OBS (D241): one line per observable transition, outside the lock.
        if let Some(line) = transitions.observe(census) {
            eprintln!("{line}");
        }
        let wake = async {
            match due {
                Some(due) => tokio::time::sleep_until(due).await,
                None => std::future::pending().await,
            }
        };
        if (old || primary_old) && next.is_none() {
            let deadline = owner
                .state()
                .recovery
                .demands
                .iter()
                .flatten()
                .filter(|d| d.outcome == Outcome::Pending)
                .map(|d| d.ticket.deadline)
                .min();
            tokio::select! {
                result = finish_predecessor(&owner, false), if old => result?,
                result = finish_predecessor(&owner, true), if primary_old => result?,
                _ = &mut notified => {},
                _ = async { match deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } } => {},
                _ = wake => {},
            }
            continue;
        }
        let Some(ticket) = next else {
            tokio::select! {
                _ = &mut notified => {},
                _ = wake => {},
            }
            continue;
        };
        let work = async {
            match ticket.kind {
                Kind::Healthy => maintenance::recover(&owner, &data, &config, ticket).await,
                Kind::Closed => recover(&owner, &data, &config, ticket, guard).await,
                Kind::Probe(kind) => probe::recover(&owner, &data, &config, ticket, kind).await,
            }
        };
        let (timed_out, result) = match tokio::time::timeout_at(ticket.deadline, work).await {
            Ok(result) => (false, result),
            Err(_) => (true, Err(io_err("tuic recovery", "original recovery budget expired"))),
        };
        if let Err(error) = result {
            let retry = {
                let mut state = owner.state();
                let retry = waiting::retryable(&mut state, ticket, timed_out, &error);
                if retry {
                    state.recovery.waits = state.recovery.waits.saturating_add(1);
                    waiting::join_queued(&mut state, ticket.index); // D141
                } else {
                    // A terminal re-probe drops its row (D143 F1).
                    waiting::clear(&mut state.recovery, ticket.index);
                }
                retry
            };
            if retry {
                owner.dispose(error); // N2: a clean expiry is Waiting, not a failure.
            } else {
                owner.record_startup_result(error);
            }
            if let Some(demand) = owner.state().recovery.demands[ticket.index].as_mut() {
                demand.outcome = Outcome::Failed;
            }
            owner.wake_recovery();
            // Publish failure before waiting for the original physical witness.
            // The same saved retire future continues while queued deadlines expire.
            let settled = owner.retire_unpublished().await;
            if settled {
                if ticket.kind != Kind::Closed {
                    maintenance::join_retired_stats(&owner).await?;
                }
                settle_uncommitted(&owner).await;
                if retry {
                    waiting::park(&owner, ticket);
                }
            } else {
                if retry {
                    waiting::clear(&mut owner.state().recovery, ticket.index);
                    owner.record(io_err("tuic recovery", "waiting custody unsettled"));
                }
                return Err(recorded_failure());
            }
        }
    }
}

async fn recover(
    owner: &StartupResources,
    data: &TuicUpstream,
    config: &TuicClientConfig,
    ticket: Ticket,
    guard: Option<&crate::owned_upstream::generation::callbacks::Guard>,
) -> Result<(), ClientError> {
    if owner.state().config_rejected {
        return Err(recorded_failure());
    }
    let slot = data
        .tcp_pool_slots
        .get(ticket.index)
        .ok_or_else(|| io_err("tuic recovery", "slot missing"))?;
    loop {
        let available = data.tcp_pool_admission.available.notified();
        let state = slot.state.lock().await;
        if state.current.identity() != ticket.identity {
            drop(state);
            if let Some(demand) = owner.state().recovery.demands[ticket.index].as_mut() {
                demand.outcome = Outcome::Ready;
            }
            owner.wake_recovery();
            return Ok(());
        }
        if state.current.transport.close_reason().is_none() || state.draining.is_some() {
            return Err(io_err(
                "tuic recovery",
                "requires an explicitly closed slot without predecessor",
            ));
        }
        ticket
            .identity
            .generation
            .checked_add(1)
            .ok_or_else(|| io_err("tuic recovery", "generation exhausted"))?;
        let preparing = data.tcp_pool_admission.preparing_slots[ticket.index].clone();
        if preparing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            drop(state);
            available.await;
            continue;
        }
        let mut root = owner.state();
        root.recovery.preparation = Some(TcpPoolSlotPreparation {
            preparing,
            available: data.tcp_pool_admission.available.clone(),
        });
        let permit = if ticket.index == 0 {
            root.recovery.primary_permit.clone()
        } else {
            data.tcp_pool_draining_predecessor.clone()
        };
        root.recovery.permit = TcpPoolDrainingPermit::try_acquire(permit);
        if root.recovery.permit.is_none() {
            return Err(io_err("tuic recovery", "predecessor capacity unavailable"));
        }
        break;
    }
    if ticket.index == 0 {
        let valid = if guard.is_some() {
            // The original saved recovery actor already owns this exact role.
            data.udp_runtime
                .generation
                .wait_sealed(ticket.identity.generation)
                .await;
            data.udp_runtime.generation.valid()
        } else {
            data.udp_runtime
                .wait_generation_sealed(
                    ticket.identity.generation,
                    crate::owned_upstream::generation::callbacks::Role::Recovery,
                )
                .await
        };
        if !valid {
            owner.state().quarantined = true;
            return Err(io_err("UDP generation", "invalid physical retirement"));
        }
    }
    let budget = waiting::budget(&owner.state().recovery, ticket);
    let mut attempt = 0;
    let mut recorded = false;
    let candidate = loop {
        if tokio::time::Instant::now() >= ticket.deadline {
            return Err(waiting::exhausted(recorded));
        }
        attempt += 1;
        {
            let mut state = owner.state();
            let demand = state.recovery.demands[ticket.index]
                .as_mut()
                .ok_or_else(|| io_err("tuic recovery", "demand missing"))?;
            if demand.outcome != Outcome::Pending || demand.attempts >= budget {
                return Err(recorded_failure());
            }
            demand.attempts += 1;
        }
        match owner.handshake_as(&data.endpoint, config, true).await {
            Ok(conn) => break conn,
            Err(error) => {
                // A local error before the handshake work is recorded here.
                recorded |= !is_recorded(&error);
                owner.record_startup_result(error);
                let stop_retry = {
                    let state = owner.state();
                    state.quarantined || state.config_rejected
                };
                if stop_retry {
                    return Err(recorded_failure());
                }
                if attempt >= u64::from(budget) {
                    return Err(waiting::exhausted(recorded));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    owner.reap_stats().await?;
    if let (Some(secs), Some(stop)) = (data.quic_stats_secs, &data.quic_stats_stop) {
        owner.spawn_stats_identified(Some(candidate.stable_id()), || {
            spawn_quic_stats_logger(candidate.clone(), ticket.index, secs, stop.subscribe())
        })?;
    }
    commit(owner, data, ticket, candidate, attempt, None).await
}

async fn commit(
    owner: &StartupResources,
    data: &TuicUpstream,
    ticket: Ticket,
    candidate: Connection,
    attempts: u64,
    proof: Option<TcpPoolSuccessorForwardServiceProof>,
) -> Result<(), ClientError> {
    let mut slot = data.tcp_pool_slots[ticket.index].state.lock().await;
    let mut activity = data.tcp_pool_admission.current_generation_activity[ticket.index]
        .lock()
        .map_err(|_| {
            owner.state().quarantined = true;
            io_err("tuic recovery", "activity poisoned")
        })?;
    let mut root = owner.state();
    let released = root.recovery.released(ticket.index, ticket.identity);
    // A released id may already name a newer connection (address reuse).
    let old_entry = (!released)
        .then(|| {
            root.connections.iter().position(|entry| {
                entry
                    .as_ref()
                    .is_some_and(|e| e.stable_id == Some(ticket.identity.stable_id) && e.published)
            })
        })
        .flatten();
    let new_entry = root.connections.iter().position(|entry| {
        entry.as_ref().is_some_and(|e| {
            e.stable_id == Some(candidate.stable_id())
                && !e.published
                && matches!(e.retirement, Retirement::Ready(_))
        })
    });
    if root.stopping
        || root.quarantined
        || root.recovery.predecessor(ticket.index == 0).is_some()
        || root.recovery.preparation.is_none()
        || root.recovery.permit.is_none()
        || slot.current.identity() != ticket.identity
        || !Arc::ptr_eq(&activity, &slot.current.activity)
        || (ticket.kind != Kind::Healthy && slot.current.transport.close_reason().is_none())
        || slot.draining.is_some()
        || old_entry.is_none() != released
        || new_entry.is_none()
        || root.connections.iter().flatten().count()
            + root.recovery.released.iter().flatten().count()
            != data.tcp_pool_slots.len()
                + 1
                + [
                    &root.recovery.predecessor,
                    &root.recovery.primary_predecessor,
                ]
                .into_iter()
                .filter_map(Option::as_ref)
                .filter(|p| !p.generation.physical_drained)
                .count()
        || root
            .connections
            .iter()
            .flatten()
            .filter(|e| !e.published)
            .count()
            != 1
    {
        root.quarantined = true;
        return Err(io_err("tuic recovery", "publication inventory mismatch"));
    }
    let Some(new_entry) = new_entry else {
        return Err(recorded_failure());
    };
    // Every tagged task names one real physical entry; no duplicate connection
    // logger is allowed. Finished handles count until their actual join.
    if root
        .tasks
        .iter()
        .filter_map(|task| task.stable_id)
        .any(|id| {
            root.connections
                .iter()
                .flatten()
                .filter(|entry| entry.stable_id == Some(id))
                .count()
                != 1
                || root
                    .tasks
                    .iter()
                    .filter(|task| task.stable_id == Some(id))
                    .count()
                    != 1
        })
        || (data.quic_stats_secs.is_some()
            && root
                .tasks
                .iter()
                .filter(|task| task.stable_id == Some(candidate.stable_id()))
                .count()
                != 1)
    {
        root.quarantined = true;
        return Err(io_err("tuic recovery", "stats physical inventory mismatch"));
    }
    if candidate.close_reason().is_some() || tokio::time::Instant::now() >= ticket.deadline {
        return Err(io_err(
            "tuic recovery",
            "candidate unavailable before publication",
        ));
    }
    let generation = ticket
        .identity
        .generation
        .checked_add(1)
        .ok_or_else(|| io_err("tuic recovery", "generation exhausted"))?;
    let udp_publication = if ticket.index == 0 {
        Some(
            data.udp_runtime
                .generation
                .prepare_publication(ticket.identity.generation, generation)
                .ok_or_else(|| {
                    if !data.udp_runtime.generation.valid() {
                        root.quarantined = true;
                    }
                    io_err("UDP generation", "publication seal mismatch")
                })?,
        )
    } else {
        None
    };
    let successor = if let Some(proof) = proof {
        let successor = TcpPoolGeneration::new_successor(
            candidate.into(),
            generation,
            attempts,
            data.clock,
            proof.path_generation,
            proof.readiness_cwnd_floor,
        )
        .ok_or_else(|| io_err("tuic recovery", "invalid service certificate"))?;
        let install = TcpPoolSuccessorInstallServiceProof::from_forward_service(&successor, &proof)
            .ok_or_else(|| io_err("tuic recovery", "service proof mismatch"))?;
        slot.validate_successor(
            ticket.index,
            ticket.identity,
            &activity,
            &successor,
            install,
        )
        .map_err(|error| io_err("tuic recovery", format!("service install: {error:?}")))?;
        successor
    } else {
        if ticket.kind == Kind::Healthy {
            return Err(io_err(
                "tuic recovery",
                "healthy publication lacks service proof",
            ));
        }
        TcpPoolGeneration::new(candidate.into(), generation, attempts, data.clock)
    };
    let successor_identity = successor.identity();
    let predecessor = TerminalGeneration {
        slot: ticket.index,
        current: false,
        identity: ticket.identity,
        physical_drained: released,
        activity: slot.current.activity.clone(),
        writes: slot.current.write_pressure.clone(),
        reads: slot.current.read_pressure.clone(),
    };
    let Some(permit) = root.recovery.permit.take() else {
        return Err(recorded_failure());
    };
    // All fallible checks precede this synchronous commit. Root retains the old
    // logical authority before Connection storage is released. No ACK/reservation
    // allocation or background task is created after publication.
    *root.recovery.predecessor_mut(ticket.index == 0) = Some(Predecessor {
        generation: predecessor,
        permit,
        entry: old_entry,
        healthy: ticket.kind == Kind::Healthy,
    });
    *activity = successor.activity.clone();
    let old = std::mem::replace(&mut slot.current, successor);
    if let Some(entry) = root.connections[new_entry].as_mut() {
        entry.published = true;
    }
    let preparation = root.recovery.preparation.take();
    root.recovery.active_ticket = None;
    if let Some(demand) = root.recovery.demands[ticket.index].as_mut() {
        demand.outcome = Outcome::Ready;
        demand.ready_identity = Some(successor_identity);
    }
    waiting::clear(&mut root.recovery, ticket.index);
    root.recovery.released[ticket.index] = None;
    drop(root);
    drop(activity);
    drop(slot);
    drop(old);
    if let Some(publication) = udp_publication {
        publication.publish();
    }
    drop(preparation); // Original admission wake follows the completed publication.
    owner.wake_recovery();
    Ok(())
}

async fn finish_predecessor(owner: &StartupResources, primary: bool) -> Result<(), ClientError> {
    let (entry, mut drained, activity, healthy) = {
        let state = owner.state();
        let Some(old) = state.recovery.predecessor(primary) else {
            return Ok(());
        };
        (
            old.entry,
            old.generation.physical_drained,
            old.generation.activity.clone(),
            old.healthy,
        )
    };
    if healthy {
        activity.wait_for_zero().await;
        wait_registries(owner, primary).await?;
        if let Some(entry) = entry.filter(|_| !drained) {
            owner.retire(entry).await?;
            if let Some(old) = owner.state().recovery.predecessor_mut(primary).as_mut() {
                old.generation.physical_drained = true;
            }
            drained = true;
        }
    }
    owner.reap_stats().await?;
    // A Waiting-released predecessor (no entry) joined its logger before
    // release; a newer connection may reuse its address-based stable id.
    let old_logger_pending = entry.is_some() && {
        let state = owner.state();
        let id = state
            .recovery
            .predecessor(primary)
            .as_ref()
            .map(|old| old.generation.identity.stable_id);
        state
            .tasks
            .iter()
            .any(|task| task.stable_id.is_some() && task.stable_id == id)
    };
    if old_logger_pending {
        std::future::pending::<()>().await;
    }
    if let Some(entry) = entry.filter(|_| !drained) {
        owner.retire(entry).await?;
        if let Some(old) = owner.state().recovery.predecessor_mut(primary).as_mut() {
            old.generation.physical_drained = true;
        }
    }
    activity.wait_for_zero().await;
    wait_registries(owner, primary).await?;
    let old = owner.state().recovery.predecessor_mut(primary).take();
    drop(old);
    Ok(())
}

async fn wait_registries(owner: &StartupResources, primary: bool) -> Result<(), ClientError> {
    let clear = {
        let state = owner.state();
        let Some(old) = state.recovery.predecessor(primary) else {
            return Ok(());
        };
        let writes = old.generation.writes.writers.try_lock();
        let reads = old.generation.reads.readers.try_lock();
        match (writes, reads) {
            (Ok(writes), Ok(reads)) => Ok(writes.is_empty() && reads.is_empty()),
            (Err(std::sync::TryLockError::Poisoned(_)), _)
            | (_, Err(std::sync::TryLockError::Poisoned(_))) => {
                Err(io_err("tuic recovery", "predecessor registry poisoned"))
            }
            _ => Ok(false),
        }
    };
    let clear = clear.map_err(|error| {
        owner.state().quarantined = true;
        error
    })?;
    if !clear {
        std::future::pending::<()>().await;
    }
    Ok(())
}
