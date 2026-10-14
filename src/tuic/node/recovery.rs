//! NodeHost's saved closed-auxiliary recovery transaction. Owns fixed demands,
//! original preparation/draining permits and predecessor retirement. Only run
//! polls physical work; callers submit/observe tickets and re-admit after commit.
//! Primary uses the original UDP producer/consumer seals; healthy replacement remains closed.
//! No new tasks or DATA-path coordination. Real-peer tests: node/tests/recovery.rs.
use super::*;

pub(super) struct StatsTask {
    pub(super) stable_id: Option<usize>,
    pub(super) handle: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::tuic) struct Ticket {
    index: usize,
    identity: TcpPoolTransportIdentity,
    deadline: tokio::time::Instant,
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
}
pub(super) struct Predecessor {
    pub(super) generation: TerminalGeneration,
    pub(super) permit: TcpPoolDrainingPermit,
    entry: usize,
}
pub(super) struct State {
    demands: Vec<Option<Demand>>,
    changed: Arc<Notify>,
    pub(super) preparation: Option<TcpPoolSlotPreparation>,
    pub(super) permit: Option<TcpPoolDrainingPermit>,
    pub(super) predecessor: Option<Predecessor>,
    pub(super) primary_predecessor: Option<Predecessor>,
    pub(super) primary_permit: Arc<AtomicBool>,
    pub(super) release_failure: Option<(
        usize,
        TcpPoolTransportIdentity,
        Option<tcp_pool_admission::LeaseReleaseReport>,
    )>,
}
impl State {
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
            demands: (0..count).map(|_| None).collect(),
            changed: Arc::new(Notify::new()),
            preparation: None,
            permit: None,
            predecessor: None,
            primary_predecessor: None,
            primary_permit: Arc::new(AtomicBool::new(false)),
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
            let mut expired = 0;
            for demand in state.recovery.demands.iter_mut().flatten() {
                if demand.outcome == Outcome::Pending
                    && tokio::time::Instant::now() >= demand.ticket.deadline
                {
                    demand.outcome = Outcome::Failed;
                    expired += 1;
                }
            }
            expired
        };
        for _ in 0..expired {
            self.record(io_err("tuic recovery", "original 15s deadline expired"));
        }
        if expired != 0 {
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
        slot: tokio::sync::MutexGuard<'_, TcpPoolGenerationSlotState<Connection>>,
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
        {
            return Err(io_err(
                "tuic recovery",
                "only running closed-slot recovery is enabled",
            ));
        }
        if let Some(demand) = &state.recovery.demands[index] {
            if demand.ticket.identity == identity {
                return Ok(demand.ticket);
            }
            if demand.outcome == Outcome::Pending {
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
        let ticket = Ticket {
            index,
            identity,
            deadline,
        };
        state.recovery.demands[index] = Some(Demand {
            ticket,
            outcome: Outcome::Pending,
            attempts: 0,
        });
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
        if state.stopping || state.quarantined {
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
        if demand.ticket.identity != ticket.identity {
            // A newer generation has its own demand. This old observer may only
            // re-admit; it has no authority to restart its old transaction.
            return Ok(true);
        }
        match demand.outcome {
            Outcome::Pending => Ok(false),
            Outcome::Ready => Ok(true),
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

pub(super) fn clear_uncommitted(owner: &StartupResources) {
    let custody = {
        let mut state = owner.state();
        (
            state.recovery.preparation.take(),
            state.recovery.permit.take(),
        )
    };
    drop(custody);
}

pub(super) async fn run(
    owner: Arc<StartupResources>,
    data: Arc<TuicUpstream>,
    config: TuicClientConfig,
) -> Result<(), ClientError> {
    let changed = owner.state().recovery.changed.clone();
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        owner.expire_demands();
        owner.reap_stats().await?;
        let (old, primary_old, next) = {
            let state = owner.state();
            if state.stopping || state.quarantined {
                return Err(recorded_failure());
            }
            (
                state.recovery.predecessor.is_some(),
                state.recovery.primary_predecessor.is_some(),
                state
                    .recovery
                    .demands
                    .iter()
                    .flatten()
                    .filter(|d| {
                        d.outcome == Outcome::Pending
                            && state.recovery.predecessor(d.ticket.index == 0).is_none()
                    })
                    .min_by_key(|d| d.ticket.deadline)
                    .map(|d| d.ticket),
            )
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
            }
            continue;
        }
        let Some(ticket) = next else {
            notified.await;
            continue;
        };
        let result =
            tokio::time::timeout_at(ticket.deadline, recover(&owner, &data, &config, ticket))
                .await
                .unwrap_or_else(|_| {
                    Err(io_err("tuic recovery", "original recovery budget expired"))
                });
        if let Err(error) = result {
            owner.record_startup_result(error);
            if let Some(demand) = owner.state().recovery.demands[ticket.index].as_mut() {
                demand.outcome = Outcome::Failed;
            }
            owner.wake_recovery();
            // Publish failure before waiting for the original physical witness.
            // The same saved retire future continues while queued deadlines expire.
            let settled = owner.retire_unpublished().await;
            if settled {
                clear_uncommitted(&owner);
            } else {
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
        data.udp_runtime
            .generation
            .wait_sealed(ticket.identity.generation)
            .await;
        if !data.udp_runtime.generation.valid() {
            owner.state().quarantined = true;
            return Err(io_err("UDP generation", "invalid physical retirement"));
        }
    }
    let mut attempt = 0;
    let candidate = loop {
        if tokio::time::Instant::now() >= ticket.deadline {
            return Err(io_err("tuic recovery", "deadline before attempt"));
        }
        attempt += 1;
        {
            let mut state = owner.state();
            let demand = state.recovery.demands[ticket.index]
                .as_mut()
                .ok_or_else(|| io_err("tuic recovery", "demand missing"))?;
            if demand.outcome != Outcome::Pending || demand.attempts >= 2 {
                return Err(recorded_failure());
            }
            demand.attempts += 1;
        }
        match owner.handshake(&data.endpoint, config).await {
            Ok(conn) => break conn,
            Err(error) => {
                owner.record_startup_result(error);
                let stop_retry = {
                    let state = owner.state();
                    state.quarantined || state.config_rejected
                };
                if attempt >= 2 || stop_retry {
                    return Err(recorded_failure());
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
    commit(owner, data, ticket, candidate, attempt).await
}

async fn commit(
    owner: &StartupResources,
    data: &TuicUpstream,
    ticket: Ticket,
    candidate: Connection,
    attempts: u64,
) -> Result<(), ClientError> {
    let mut slot = data.tcp_pool_slots[ticket.index].state.lock().await;
    let mut activity = data.tcp_pool_admission.current_generation_activity[ticket.index]
        .lock()
        .map_err(|_| {
            owner.state().quarantined = true;
            io_err("tuic recovery", "activity poisoned")
        })?;
    let mut root = owner.state();
    let old_entry = root.connections.iter().position(|entry| {
        entry
            .as_ref()
            .is_some_and(|e| e.stable_id == Some(ticket.identity.stable_id) && e.published)
    });
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
        || slot.current.transport.close_reason().is_none()
        || slot.draining.is_some()
        || old_entry.is_none()
        || new_entry.is_none()
        || root.connections.iter().flatten().count()
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
    let (Some(old_entry), Some(new_entry)) = (old_entry, new_entry) else {
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
    let successor = TcpPoolGeneration::new(candidate, generation, attempts, data.clock);
    let predecessor = TerminalGeneration {
        slot: ticket.index,
        current: false,
        identity: ticket.identity,
        physical_drained: false,
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
    });
    *activity = successor.activity.clone();
    let old = std::mem::replace(&mut slot.current, successor);
    if let Some(entry) = root.connections[new_entry].as_mut() {
        entry.published = true;
    }
    let preparation = root.recovery.preparation.take();
    if let Some(demand) = root.recovery.demands[ticket.index].as_mut() {
        demand.outcome = Outcome::Ready;
    }
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
    let (entry, drained, activity) = {
        let state = owner.state();
        let Some(old) = state.recovery.predecessor(primary) else {
            return Ok(());
        };
        (
            old.entry,
            old.generation.physical_drained,
            old.generation.activity.clone(),
        )
    };
    owner.reap_stats().await?;
    let old_logger_pending = {
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
    if !drained {
        owner.retire(entry).await?;
        if let Some(old) = owner.state().recovery.predecessor_mut(primary).as_mut() {
            old.generation.physical_drained = true;
        }
    }
    activity.wait_for_zero().await;
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
    owner.state().recovery.predecessor_mut(primary).take();
    Ok(())
}
