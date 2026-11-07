//! Starvation-compensated observation window (O4, D28) for real loopback
//! Service tests. Owns only the bound of one test observation; no Service,
//! budget or settlement authority.
//!
//! Invariant: the original fixed bound is kept in *available* time. The window
//! extends only by measured OS starvation of the observing thread: timer
//! lateness not covered by that thread's own CPU time. Idle stalls and busy
//! self-work therefore still fail at exactly the original bound, and a paused
//! Tokio clock (timers never late) behaves like `tokio::time::timeout`. A hang
//! cap of `HANG_FACTOR` × bound fails closed if starvation never ends. A
//! thread migration between samples credits nothing.
//!
//! [`progress_window`] (D47 R2, D33 seam) keeps the same compensated `stall`
//! bound but restarts it on every strict increase of a monotone progress
//! counter the work publishes; the total stays capped at `HANG_FACTOR` × stall.
//! It replaces a fixed completion bound where machine throughput (e.g. E-core
//! placement, where own CPU is not a work unit) decides the duration.
//!
//! Entries: [`window`], [`progress_window`]. Shared by the lib Service tests (via
//! `tuic_service/tests/fixture.rs`) and the dual-compiled fixtures here.
//! Tracers: `tuic_service/tests/observe_window.rs`.
#![allow(dead_code)]

use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;

const TICK: Duration = Duration::from_millis(20);
const HANG_FACTOR: u32 = 12;

/// The window expired; `starved` is the total compensation that was granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowElapsed {
    pub(crate) elapsed: Duration,
    pub(crate) starved: Duration,
}

impl std::fmt::Display for WindowElapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deadline has elapsed after {:?} ({:?} starvation compensated)", self.elapsed, self.starved)
    }
}

fn thread_cpu()-> (std::thread::ThreadId, Duration) {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: valid out-pointer; CLOCK_THREAD_CPUTIME_ID is supported on the
    // macOS/Linux hosts that run this suite. On failure ts stays zero, which
    // credits no starvation (the window degrades to the fixed bound).
    let ok = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0;
    let cpu = if ok {
        Duration::new(ts.tv_sec.max(0) as u64, ts.tv_nsec.clamp(0, 999_999_999) as u32)
    } else {
        Duration::MAX
    };
    (std::thread::current().id(), cpu)
}

/// Await `work` within `bound` of available (non-starved) time.
pub(crate) async fn window<F: Future>(bound: Duration, work: F) -> Result<F::Output, WindowElapsed> {
    let start = Instant::now();
    let cap = start + bound * HANG_FACTOR;
    let mut deadline = start + bound;
    let mut starved = Duration::ZERO;
    let mut sample = (start, thread_cpu());
    let mut work = std::pin::pin!(work);
    loop {
        let due = (sample.0 + TICK).min(deadline);
        tokio::select! {
            biased;
            value = &mut work => return Ok(value),
            () = tokio::time::sleep_until(due) => {}
        }
        let now = Instant::now();
        let cpu = thread_cpu();
        if cpu.0 == (sample.1).0 && cpu.1 != Duration::MAX && (sample.1).1 != Duration::MAX {
            let late = now.saturating_duration_since(due);
            let own = cpu.1.saturating_sub((sample.1).1);
            let credit = late.saturating_sub(own);
            starved += credit;
            deadline += credit;
        }
        if now >= deadline || now >= cap {
            return Err(WindowElapsed { elapsed: now - start, starved });
        }
        sample = (now, cpu);
    }
}

/// No strict progress within the compensated `stall`, or the total cap hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressElapsed {
    Stalled { progress: u64, since_progress: Duration, starved: Duration },
    Cap { progress: u64, elapsed: Duration },
}

/// Await `work`; fail only when `progress` (published by `work`) stops
/// strictly increasing for `stall` of available time, or after the hang cap.
pub(crate) async fn progress_window<F: Future>(
    stall: Duration,
    progress: &std::cell::Cell<u64>,
    work: F,
) -> Result<F::Output, ProgressElapsed> {
    let start = Instant::now();
    let cap = start + stall * HANG_FACTOR;
    let mut last = (progress.get(), start);
    let mut deadline = start + stall;
    let mut starved = Duration::ZERO;
    let mut sample = (start, thread_cpu());
    let mut work = std::pin::pin!(work);
    loop {
        let due = (sample.0 + TICK).min(deadline).min(cap);
        tokio::select! {
            biased;
            value = &mut work => return Ok(value),
            () = tokio::time::sleep_until(due) => {}
        }
        let now = Instant::now();
        let cpu = thread_cpu();
        if cpu.0 == (sample.1).0 && cpu.1 != Duration::MAX && (sample.1).1 != Duration::MAX {
            let late = now.saturating_duration_since(due);
            let credit = late.saturating_sub(cpu.1.saturating_sub((sample.1).1));
            starved += credit;
            deadline += credit;
        }
        let current = progress.get();
        if current > last.0 {
            last = (current, now);
            deadline = now + stall;
            starved = Duration::ZERO;
        }
        if now >= cap {
            return Err(ProgressElapsed::Cap { progress: current, elapsed: now - start });
        }
        if now >= deadline {
            return Err(ProgressElapsed::Stalled { progress: current, since_progress: now - last.1, starved });
        }
        sample = (now, cpu);
    }
}
