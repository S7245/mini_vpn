//! N5-OBS (D241) hard-Native CLI observer: one periodic `📊 Native` line on
//! the `MINI_VPN_METRICS_SECS` cadence.
//!
//! Owns: the [`Watch`] of weak observers (current Node, current session) that
//! the saved driver replaces at each Node construction, and the observer-local
//! lateness/peak [`Window`]. Does not own: any Node, session, budget, flow or
//! DATA authority; it never drives or settles the Service, upgrades weak
//! handles only for one bounded sample and drops them before awaiting.
//! Lateness is measured on this CLI task's own 10 ms timer (the Service run
//! future is polled in the same task), into fixed buckets; budget peak is the
//! max `used()` seen at that cadence (sampled, not an allocator high-water).
//! Entries: [`report`], [`Watch::replace`], [`Window`]. Tests:
//! `tuic_service/tests/n5_observe.rs`.
use super::*;
use crate::tuic::{NativeNodeWeakObserver, NodeFacts};

/// Weak read windows onto the current generation; replaced, never chained.
#[derive(Default)]
pub(super) struct Watch {
    node: Option<NativeNodeWeakObserver>,
    session: Option<std::sync::Weak<SessionControl>>,
}

impl Watch {
    pub(super) fn replace(
        slot: &Mutex<Watch>,
        node: Option<NativeNodeWeakObserver>,
        session: &Arc<SessionControl>,
    ) {
        let old = std::mem::replace(
            &mut *slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Watch {
                node,
                session: Some(Arc::downgrade(session)),
            },
        );
        drop(old);
    }
}

pub(super) const PROBE: std::time::Duration = std::time::Duration::from_millis(10);
/// 1 ms buckets up to 250 ms; the last bucket holds everything later.
const BUCKETS: usize = 251;

/// Observer-local lateness histogram and sampled budget peak for one period.
pub(super) struct Window {
    late: [u32; BUCKETS],
    samples: u32,
    max: std::time::Duration,
    peak: usize,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            late: [0; BUCKETS],
            samples: 0,
            max: std::time::Duration::ZERO,
            peak: 0,
        }
    }
}

impl Window {
    pub(super) fn record(&mut self, late: std::time::Duration, used: usize) {
        let bucket = usize::try_from(late.as_millis())
            .unwrap_or(usize::MAX)
            .min(BUCKETS - 1);
        self.late[bucket] = self.late[bucket].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
        self.max = self.max.max(late);
        self.peak = self.peak.max(used);
    }

    /// Upper edge (ms) of the bucket holding the 99th percentile; `None` if empty.
    pub(super) fn p99_ms(&self) -> Option<usize> {
        let rank = (u64::from(self.samples) * 99).div_ceil(100);
        let mut seen = 0u64;
        for (ms, count) in self.late.iter().enumerate() {
            seen += u64::from(*count);
            if seen >= rank && rank != 0 {
                return Some(ms + 1);
            }
        }
        None
    }

    pub(super) fn max(&self) -> std::time::Duration {
        self.max
    }

    pub(super) fn peak(&self) -> usize {
        self.peak
    }
}

/// The value-only inputs of one line.
pub(super) struct Line<'a> {
    pub(super) node: Option<NodeFacts>,
    pub(super) budget: Option<&'a bytes::BackingBudget>,
    pub(super) flows: Option<(
        crate::client_tun::compatibility_runtime::flow_custody::FlowCensus,
        bool,
        u64,
    )>,
    pub(super) tasks: usize,
    pub(super) window: &'a Window,
}

pub(super) fn format_line(line: &Line<'_>) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("📊 Native:");
    match &line.node {
        Some(n) => {
            let active = n.active.map_or_else(
                || "-".to_owned(),
                |(slot, generation, sequence)| format!("slot{slot}/gen{generation}/seq{sequence}"),
            );
            let _ = write!(
                out,
                " node={:?} conns started={} live={} active={active} pending={} waiting={} waits={} reprobes={} waiting_failures={} first_failure={} backing={}B/{}allocs |",
                n.phase,
                n.connections_started,
                n.connections_live,
                n.pending_demands,
                n.waiting_rows,
                n.waits,
                n.reprobes,
                n.waiting_failures,
                n.first_failure,
                n.backing_physical,
                n.backing_allocations,
            );
            let o = n.opens;
            let _ = write!(
                out,
                " opens ok={} typed={} timeout={} cancelled={} |",
                o.success, o.typed, o.timeout, o.cancelled
            );
        }
        None => out.push_str(" node=- |"),
    }
    match line.budget {
        Some(b) => {
            let _ = write!(
                out,
                " budget used={} peak={} limit={} reserve={} q={} |",
                b.used(),
                line.window.peak().max(b.used()),
                b.limit(),
                b.protocol_reserve(),
                b.quarantined()
            );
        }
        None => out.push_str(" budget=- |"),
    }
    match &line.flows {
        Some((f, session_q, flow_q_raised)) => {
            let _ = write!(
                out,
                " flows live={} closing={} rearming={} quarantined={} flow_q_rows={} flow_q_raised={flow_q_raised} session_q={session_q} tail_abort={} tail_extend={} published={} released={} |",
                f.live,
                f.closing,
                f.rearming,
                f.quarantined,
                f.flow_q_rows,
                f.tail_aborted,
                f.tail_extended,
                f.published,
                f.released,
            );
        }
        None => out.push_str(" flows=- |"),
    }
    let p99 = line
        .window
        .p99_ms()
        .map_or_else(|| "-".to_owned(), |ms| format!("≤{ms}ms"));
    let _ = write!(
        out,
        " tasks={} late p99={p99} max={}ms",
        line.tasks,
        line.window.max().as_millis()
    );
    out
}

/// One bounded sample of the current generation; no handle survives it.
pub(super) fn sample(control: &Control, window: &Window) -> String {
    let (node, session) = {
        let watch = control
            .observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (watch.node.clone(), watch.session.clone())
    };
    let node = node.as_ref().and_then(NativeNodeWeakObserver::facts);
    let flows = session.and_then(|weak| weak.upgrade()).map(|session| {
        let census = session.opens().census();
        let ledger = session.ledger();
        let q = (ledger.session_quarantined(), ledger.flow_q_epoch());
        drop(ledger);
        (census, q.0, q.1)
    });
    format_line(&Line {
        node,
        budget: control.backing_budget.as_ref(),
        flows,
        tasks: tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks(),
        window,
    })
}

/// Observer only: never returns, never drives or settles the Service.
pub(super) async fn report(
    handle: &Handle,
    period: std::time::Duration,
    mut sink: impl FnMut(String),
) -> std::convert::Infallible {
    let control = &handle.0;
    let mut probe = tokio::time::interval(PROBE);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut window = Window::default();
    let mut emitted = tokio::time::Instant::now();
    loop {
        // `tick` yields its deadline; the wake delay past it is the lateness.
        let deadline = probe.tick().await;
        let used = control
            .backing_budget
            .as_ref()
            .map_or(0, bytes::BackingBudget::used);
        window.record(
            tokio::time::Instant::now().saturating_duration_since(deadline),
            used,
        );
        if emitted.elapsed() >= period {
            sink(sample(control, &window));
            window = Window::default();
            emitted = tokio::time::Instant::now();
        }
    }
}
