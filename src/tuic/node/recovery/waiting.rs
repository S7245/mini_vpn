//! N2 Waiting (D126/D127): Node-owned dormant re-probe of a cleanly expired
//! closed-slot demand on the same Node, same slot and same session.
//!
//! Owns: the per-slot `Retry` side table (identity, backoff step, due time),
//! the deterministic 1,2,4,8,16,30…s schedule, the Failed→Pending re-mint of the
//! original Demand, and the UDP-primary observer that follows that re-mint.
//! Does not own: handshake, publication (`commit`), custody settlement or Service
//! phase; those stay in the parent `recovery` run loop.
//!
//! Invariants: a `Retry` exists only beside a `Kind::Closed` demand of the same
//! identity that is Failed (Waiting) or Pending with a one-handshake budget
//! (re-probing); any stop/Q/config rejection, or an unsealed primary UDP
//! generation, makes every `Retry` terminal (as in N1): `rearm_due` drops the
//! rows, or `run` ends first on Q/stop and the remaining rows are inert because
//! every reader checks `node_clean` before them; a Failed demand without
//! `Retry` is terminal. A non-clean re-probe result drops its row (D144). One in-flight handshake per
//! slot, one timer folded into the existing `run` wait, no task, no queue, and the
//! count of attempts is independent of how many opens/UDP sends observe it.
//! D152 (A2): once a closed demand settles Failed (parked Waiting or terminal),
//! `release_settled` drops the dead connection's storage from the slot (a
//! `SlotConnection` tombstone keeps identity and close evidence) and retires
//! its physical entry, so stream witnesses on it retire and leases drain
//! instead of holding the storage for the whole Waiting period. It runs in the
//! same `run` loop turn, on the bounded control path, with no task or queue.
//! Entry: `hold`, `join_parked`, `join_queued`, `park`, `rearm_due`, `next_due`, `budget`, `clean_expiry`,
//! `release_settled`, `TuicUpstream::await_closed_recovery`. Tests:
//! tuic_service/tests/native_waiting*, tuic/node/tests/waiting_release.rs.
use super::*;

/// One re-probe = one handshake (≤5s) plus publication margin.
const REPROBE_BUDGET: Duration = Duration::from_secs(6);
const BACKOFF_SECS: [u64; 6] = [1, 2, 4, 8, 16, 30];

#[derive(Clone, Copy, Debug)]
pub(super) struct Retry {
    identity: TcpPoolTransportIdentity,
    step: u8,
    due: Option<tokio::time::Instant>,
}

impl Retry {
    pub(super) fn parked(&self) -> bool {
        self.due.is_some()
    }
}

fn delay(step: u8) -> Duration {
    let index = usize::from(step).min(BACKOFF_SECS.len() - 1);
    Duration::from_secs(BACKOFF_SECS[index])
}

/// The closed-slot failure carried only handshake failure/timeout evidence.
#[derive(Debug)]
struct CleanExpiry;
impl std::fmt::Display for CleanExpiry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("closed-slot demand expired cleanly")
    }
}
impl std::error::Error for CleanExpiry {}

pub(super) fn clean_expiry() -> ClientError {
    ClientError::Io(io::Error::other(CleanExpiry))
}

fn is_clean_expiry(error: &ClientError) -> bool {
    matches!(error, ClientError::Io(io) if io.get_ref().is_some_and(|s| s.is::<CleanExpiry>()))
}

pub(super) fn node_clean(root: &StartupState) -> bool {
    !root.stopping && !root.quarantined && !root.config_rejected
}

/// D136: a primary expiry is clean only once its old UDP generation is sealed;
/// a missing consumer seal is unsettled custody and stays terminal (D127).
fn udp_settled(root: &StartupState, ticket: Ticket) -> bool {
    ticket.index != 0
        || root
            .udp
            .as_ref()
            .is_none_or(|udp| udp.generation_sealed(ticket.identity.generation))
}

/// Keep (or start) the Waiting record for this exact closed demand before it is
/// published Failed, so the UDP primary never observes a retryable expiry as
/// terminal. Returns whether the failure is retryable.
pub(super) fn hold(root: &mut StartupState, ticket: Ticket) -> bool {
    if ticket.kind != Kind::Closed || !node_clean(root) || !udp_settled(root, ticket) {
        return false;
    }
    let Some(entry) = root.recovery.retries.get_mut(ticket.index) else {
        return false;
    };
    let step = entry
        .filter(|retry| retry.identity == ticket.identity)
        .map_or(0, |retry| retry.step);
    *entry = Some(Retry {
        identity: ticket.identity,
        step,
        due: None,
    });
    true
}

/// D141: a clean Waiting row of another slot (parked or re-probing) means the
/// path is known black-holed, so a NEW closed demand joins Waiting at mint
/// (published Failed, nothing prepared) on the shared schedule (earliest parked
/// due, or that slot's next backoff while it re-probes, and its step)
/// instead of a fresh 15s/two-handshake budget. `hold` keeps every terminal
/// rule (stop/Q/config, unsealed primary UDP). Returns whether it joined.
pub(super) fn join_parked(root: &mut StartupState, ticket: Ticket) -> bool {
    let now = tokio::time::Instant::now();
    let shared = root
        .recovery
        .retries
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != ticket.index)
        .filter_map(|(_, retry)| *retry)
        .map(|r| (r.due.unwrap_or(now + delay(r.step)), r.step))
        .min_by_key(|(due, _)| *due);
    let Some((due, step)) = shared else {
        return false;
    };
    let fresh = root.recovery.demands[ticket.index]
        .as_ref()
        .is_some_and(|d| {
            d.ticket == ticket
                && d.outcome == Outcome::Pending
                && d.attempts == 0
                && d.preparation.is_none()
        });
    if !fresh || !hold(root, ticket) {
        return false;
    }
    if let Some(demand) = root.recovery.demands[ticket.index].as_mut() {
        demand.outcome = Outcome::Failed;
        demand.settled = true;
    }
    root.recovery.retries[ticket.index] = Some(Retry {
        identity: ticket.identity,
        step,
        due: Some(due),
    });
    root.recovery.waits = root.recovery.waits.saturating_add(1);
    true
}

/// D141: when slot `held` enters Waiting, every other fresh queued closed
/// demand (Pending, 0 attempts, nothing prepared) joins it at once instead of
/// spending its own original budget on the same dead path.
pub(super) fn join_queued(root: &mut StartupState, held: usize) {
    let queued: Vec<Ticket> = root
        .recovery
        .demands
        .iter()
        .flatten()
        .filter(|d| {
            d.ticket.index != held
                && d.ticket.kind == Kind::Closed
                && d.outcome == Outcome::Pending
                && d.attempts == 0
                && d.preparation.is_none()
        })
        .map(|d| d.ticket)
        .collect();
    for ticket in queued {
        join_parked(root, ticket);
    }
}

/// Classify one failed closed-demand result before settlement. Only a clean
/// expiry (handshake failure/timeout evidence) may wait; every other error of
/// an original demand or a re-probe keeps its recorded terminal disposition
/// (D127; D143 F1).
pub(super) fn retryable(
    root: &mut StartupState,
    ticket: Ticket,
    timed_out: bool,
    error: &ClientError,
) -> bool {
    (timed_out || is_clean_expiry(error)) && hold(root, ticket)
}

/// Budget exhausted: clean only if no attempt recorded a local pre-handshake
/// error; otherwise the recorded failure stays terminal (D143 F2).
pub(super) fn exhausted(recorded: bool) -> ClientError {
    if recorded {
        recorded_failure()
    } else {
        clean_expiry()
    }
}

/// After actual custody settlement: arm the next re-probe, or make it terminal.
pub(super) fn park(owner: &StartupResources, ticket: Ticket) {
    let mut root = owner.state();
    let settled = root.recovery.demands[ticket.index]
        .as_ref()
        .is_some_and(|d| d.ticket == ticket && d.settled && d.outcome == Outcome::Failed);
    let clean = node_clean(&root);
    let entry = &mut root.recovery.retries[ticket.index];
    match entry.as_mut() {
        Some(retry) if settled && clean && retry.identity == ticket.identity => {
            retry.due = Some(tokio::time::Instant::now() + delay(retry.step));
            retry.step = retry.step.saturating_add(1);
        }
        _ => *entry = None,
    }
    drop(root);
    owner.wake_recovery();
}

/// D152 (A2): release every settled Failed closed slot's dead storage. Its
/// stats logger is joined first (one physical entry per tagged task); a slot
/// that is live, draining or of another identity is left untouched. The
/// physical entry is retired with the original witness; a failure is Q.
pub(super) async fn release_settled(
    owner: &StartupResources,
    data: &TuicUpstream,
) -> Result<(), ClientError> {
    for index in 0..data.tcp_pool_slots.len() {
        let identity = {
            let root = owner.state();
            let Some(demand) = root.recovery.demands[index].as_ref() else {
                continue;
            };
            let identity = demand.ticket.identity;
            if demand.ticket.kind != Kind::Closed
                || demand.outcome != Outcome::Failed
                || !demand.settled
                || root.recovery.released(index, identity)
                || root
                    .tasks
                    .iter()
                    .any(|task| task.stable_id == Some(identity.stable_id))
            {
                continue;
            }
            identity
        };
        let storage = {
            let mut slot = data.tcp_pool_slots[index].state.lock().await;
            if slot.current.identity() != identity || slot.draining.is_some() {
                continue;
            }
            let storage = slot.current.transport.release();
            if !slot.current.transport.released() {
                continue;
            }
            storage
        };
        drop(storage); // Outside the slot lock.
        let entry = owner.state().connections.iter().position(|entry| {
            entry
                .as_ref()
                .is_some_and(|e| e.published && e.stable_id == Some(identity.stable_id))
        });
        if let Some(entry) = entry {
            owner.retire(entry).await?;
        }
        owner.state().recovery.released[index] = Some(identity);
    }
    Ok(())
}

/// Re-mint every due Waiting demand as a one-handshake Pending demand on the
/// same identity. Invalid side-table rows are dropped (terminal), never repaired.
pub(super) fn rearm_due(root: &mut StartupState) {
    if !node_clean(root) {
        root.recovery
            .retries
            .iter_mut()
            .for_each(|entry| *entry = None);
        return;
    }
    let now = tokio::time::Instant::now();
    let recovery = &mut root.recovery;
    for index in 0..recovery.retries.len() {
        let Some(retry) = recovery.retries[index] else {
            continue;
        };
        let Some(due) = retry.due else { continue };
        if now < due {
            continue;
        }
        let sequence = recovery.sequence.checked_add(1);
        let demand = recovery.demands[index].as_mut().filter(|d| {
            d.ticket.kind == Kind::Closed
                && d.ticket.identity == retry.identity
                && d.outcome == Outcome::Failed
                && d.settled
                && d.preparation.is_none()
        });
        let (Some(demand), Some(sequence)) = (demand, sequence) else {
            recovery.retries[index] = None;
            continue;
        };
        recovery.sequence = sequence;
        demand.ticket = Ticket {
            index,
            identity: retry.identity,
            deadline: now + REPROBE_BUDGET,
            sequence,
            kind: Kind::Closed,
        };
        demand.outcome = Outcome::Pending;
        demand.attempts = 0;
        demand.settled = false;
        demand.ready_identity = None;
        demand.receipt_issued = false;
        recovery.retries[index] = Some(Retry { due: None, ..retry });
        recovery.reprobes = recovery.reprobes.saturating_add(1);
    }
}

pub(super) fn next_due(recovery: &State) -> Option<tokio::time::Instant> {
    recovery
        .retries
        .iter()
        .flatten()
        .filter_map(|retry| retry.due)
        .min()
}

/// Whether this exact closed identity holds a Waiting/re-probe row.
pub(super) fn holds(recovery: &State, ticket: Ticket) -> bool {
    recovery
        .retries
        .get(ticket.index)
        .copied()
        .flatten()
        .is_some_and(|retry| retry.identity == ticket.identity)
}

/// Handshakes allowed for this demand: 2 for the original, 1 per re-probe.
pub(super) fn budget(recovery: &State, ticket: Ticket) -> u8 {
    if holds(recovery, ticket) { 1 } else { 2 }
}

pub(super) fn clear(recovery: &mut State, index: usize) {
    if let Some(entry) = recovery.retries.get_mut(index) {
        *entry = None;
    }
}

impl TuicUpstream {
    /// UDP primary only: wait for the closed slot through any number of
    /// Waiting/re-probe rounds of the same identity. Terminal Failed, stop, Q
    /// and config rejection return the original recorded error.
    pub(in crate::tuic) async fn await_closed_recovery(
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
            owner.expire_demands();
            {
                let state = owner.state();
                if !node_clean(&state) {
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
                if demand.ticket.kind != Kind::Closed || demand.ticket.identity != ticket.identity {
                    return Ok(()); // The slot moved on; the caller re-reads it.
                }
                let waiting = state.recovery.retries[ticket.index]
                    .is_some_and(|retry| retry.identity == ticket.identity);
                match demand.outcome {
                    Outcome::Ready => return Ok(()),
                    Outcome::Pending => {}
                    Outcome::Failed if waiting => {}
                    Outcome::Failed => return Err(recorded_failure()),
                }
            }
            notified.await;
        }
    }
}
