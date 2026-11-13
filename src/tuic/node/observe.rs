//! N5-OBS (D241) Native Node observer: read-only facts for the CLI metrics
//! tick and one line per recovery state transition.
//!
//! Owns: the weak observer handle (promoted from the N4 test-only
//! `NativeNodeWeakObserver`; its test `sample`/`waiting` impls stay in
//! `node/tests`), the value-only [`NodeFacts`] and the [`Transitions`] diff.
//! Does not own: any startup, recovery, DATA or drain authority. A sample takes
//! the Node state lock once, copies counters and drops every handle before
//! returning; the transition diff runs on the recovery loop's own turn with no
//! new task, timer, queue or allocation per packet.
//! [`OpenClasses`] counts each TUIC open once, when its attempt is dropped
//! (control path, one relaxed add per open, never per packet).
//! Entries: [`NodeHost::native_observer`], [`NativeNodeWeakObserver::facts`],
//! [`Transitions::observe`], [`OpenClasses::settle`]. Tests: `node/tests/observe.rs`.
use super::*;

/// Bounded weak read window onto one Node; holds no startup, DATA or drain authority.
#[derive(Clone)]
pub(crate) struct NativeNodeWeakObserver(
    pub(in crate::tuic::node) std::sync::Weak<StartupResources>,
);

/// Node-level recovery phase, derived from the original recovery ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodePhase {
    Running,
    /// A closed/healthy/probe demand is pending or active.
    Recovering,
    /// At least one slot is parked in the N2 Waiting side table.
    Waiting,
    Stopping,
    Quarantined,
}

/// One value-only Node sample. No handle survives the call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NodeFacts {
    pub(crate) phase: NodePhase,
    /// Connections ever started on this Node (startup + recovery handshakes).
    pub(crate) connections_started: u64,
    pub(crate) connections_live: usize,
    pub(crate) waits: u64,
    pub(crate) reprobes: u64,
    pub(crate) waiting_failures: u64,
    pub(crate) waiting_rows: usize,
    pub(crate) pending_demands: usize,
    /// Active ticket `(slot, replaced generation, sequence)`.
    pub(crate) active: Option<(usize, u64, u64)>,
    pub(crate) backing_physical: usize,
    pub(crate) backing_allocations: usize,
    pub(crate) first_failure: bool,
    pub(crate) opens: OpenCounts,
}

/// Terminal class of one TUIC open attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::tuic) enum OpenClass {
    /// The carrier was transferred to the flow.
    Success,
    /// Failed with a typed error other than a timeout or interruption.
    Typed,
    /// Failed with `TimedOut` (open bound or deadline cancel).
    Timeout,
    /// Cancelled/shut down, or dropped before any outcome.
    Cancelled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OpenCounts {
    pub(crate) success: u64,
    pub(crate) typed: u64,
    pub(crate) timeout: u64,
    pub(crate) cancelled: u64,
}

/// Node-owned cumulative open settlement counters (in `UdpRuntime`).
#[derive(Default)]
pub(in crate::tuic) struct OpenClasses([std::sync::atomic::AtomicU64; 4]);

impl OpenClasses {
    pub(in crate::tuic) fn settle(&self, class: OpenClass) {
        self.0[class as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub(in crate::tuic) fn counts(&self) -> OpenCounts {
        let get = |class: OpenClass| self.0[class as usize].load(Ordering::Relaxed);
        OpenCounts {
            success: get(OpenClass::Success),
            typed: get(OpenClass::Typed),
            timeout: get(OpenClass::Timeout),
            cancelled: get(OpenClass::Cancelled),
        }
    }
}

impl OpenClass {
    /// Classify from the attempt's retained facts at drop.
    pub(in crate::tuic) fn of(transferred: bool, failure: Option<&ClientError>) -> Self {
        match failure {
            _ if transferred => Self::Success,
            Some(ClientError::Io(error)) if error.kind() == io::ErrorKind::TimedOut => {
                Self::Timeout
            }
            Some(ClientError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {
                Self::Cancelled
            }
            Some(_) => Self::Typed,
            None => Self::Cancelled,
        }
    }
}

impl NodeHost {
    /// N5-OBS: production weak observer for the CLI metrics tick.
    pub(crate) fn native_observer(&self) -> NativeNodeWeakObserver {
        NativeNodeWeakObserver(Arc::downgrade(&self.resources))
    }
}

impl NativeNodeWeakObserver {
    pub(crate) fn facts(&self) -> Option<NodeFacts> {
        let original = self.0.upgrade()?;
        let (facts, meter) = {
            let state = original.state();
            let census = state.recovery.census();
            let facts = NodeFacts {
                phase: phase(&state, &census),
                connections_started: state.attempts_started,
                connections_live: state
                    .connections
                    .iter()
                    .flatten()
                    .filter(|attempt| {
                        attempt
                            .connection
                            .as_ref()
                            .is_some_and(|c| c.close_reason().is_none())
                    })
                    .count(),
                waits: census.waits,
                reprobes: census.reprobes,
                waiting_failures: census.waiting_failures,
                waiting_rows: census.waiting,
                pending_demands: census.pending,
                active: census.active,
                backing_physical: 0,
                backing_allocations: 0,
                first_failure: state.first.is_some(),
                opens: state
                    .udp
                    .as_ref()
                    .map_or_else(OpenCounts::default, |udp| udp.runtime().opens.counts()),
            };
            (facts, state.backing_meter.clone())
        };
        // Meter atomics are read and its handle dropped outside the Node lock.
        let backing = meter.snapshot();
        drop(meter);
        drop(original);
        Some(NodeFacts {
            backing_physical: backing.physical_bytes,
            backing_allocations: backing.allocations,
            ..facts
        })
    }
}

fn phase(state: &StartupState, census: &recovery::Census) -> NodePhase {
    if state.quarantined {
        NodePhase::Quarantined
    } else if state.stopping {
        NodePhase::Stopping
    } else if census.waiting != 0 {
        NodePhase::Waiting
    } else if census.pending != 0 || census.active.is_some() {
        NodePhase::Recovering
    } else {
        NodePhase::Running
    }
}

/// Recovery-loop transition diff: the loop owns one value and calls
/// [`Transitions::observe`] once per turn, outside the Node lock.
#[derive(Default)]
pub(super) struct Transitions {
    seen: Option<recovery::Census>,
}

impl Transitions {
    /// One line when the observable recovery state changed since the last turn.
    pub(super) fn observe(&mut self, census: recovery::Census) -> Option<String> {
        if self.seen == Some(census) {
            return None;
        }
        let before = self.seen.replace(census);
        let label = |c: &recovery::Census| {
            if c.waiting != 0 {
                "Waiting"
            } else if c.pending != 0 || c.active.is_some() {
                "Recovering"
            } else {
                "Running"
            }
        };
        let from = before.as_ref().map_or("Start", label);
        let mut events = String::new();
        if let Some(before) = before {
            if census.waits > before.waits {
                events.push_str(" waiting-enter");
            }
            if census.waiting < before.waiting {
                events.push_str(" waiting-exit");
            }
            if census.reprobes > before.reprobes {
                events.push_str(" reprobe");
            }
        }
        let active = census.active.map_or_else(
            || "-".to_owned(),
            |(slot, generation, sequence)| format!("slot{slot}/gen{generation}/seq{sequence}"),
        );
        Some(format!(
            "🧭 Native Node: {from}→{} active={active} pending={} waiting={} waits={} reprobes={} waiting_failures={}{}",
            label(&census),
            census.pending,
            census.waiting,
            census.waits,
            census.reprobes,
            census.waiting_failures,
            if events.is_empty() {
                String::new()
            } else {
                format!(" events:{events}")
            },
        ))
    }
}
