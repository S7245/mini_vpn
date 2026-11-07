//! N1 stage C3 F5: explicit controlled rebuild while a real first-generation
//! Native flow (Chunk or Permit engine) owns DATA, and the original peer never
//! retires the stream. Disposition (D61, measured; same as the accepted
//! single-generation `StopHeld` contract in `native_chunk_loaded_stop.rs`):
//! the original business never settles; at the original limit (request+15s)
//! the session is `Incomplete`, Phase sticky `Quarantined`, the held flow stays
//! (live1,pub1,join5,rel0,F1), no cut, no successor accept/DATA, no completion,
//! budget Q non-zero; forced destruction keeps Q.
//!
//! Also the tracer for the D61 self-wake finding: while the held flow
//! drains and after sticky Q, the session loop must park between its 5ms
//! ticks. D65 criterion: gated Service wake-ups (polled only on its own waker,
//! `native_park_wakeup::gated`) ≤ 2 per 5ms tick; debug CPU share is printed
//! with a coarse ≤ 0.25 guard only (debug CPU on P/E cores measures the build).
//! Measured before repair: ≈78% of one (debug) core for the whole wait
//! and indefinitely after Q (`OpenDispatch::poll_flows` 32-slot chunked rescan
//! of the 4096-slot table, `take_control` budget exhaustion → `OpenEvent::Yield`).
//! Real clock (≈20s per engine; the CPU guard needs wall time), joint gate
//! since D65. Reuses the B7 loading rig; owns no
//! production authority.
use super::*;
use fixture::{capture, window};
use native_rebuild_fatal::{close, rig_with, Rig};
use native_rebuild_loaded_cut::{load_original, Loaded};
use native_rebuild_loaded_stop::Engine;
use native_park_wakeup::{gated, GateWake};
use std::sync::atomic::Ordering;
use tokio::time::Instant;

/// Driving tick of the test loop.
const TICK: Duration = Duration::from_millis(10);
/// D65 primary park criterion: gated Service wake-ups per 5ms loop tick
/// (pre-D63 ≈140; parked ≈1.0–1.64).
const MAX_WAKES_PER_TICK: f64 = 2.0;
/// D65 coarse guard on the debug session thread's CPU share (pre-D63 0.78).
const MAX_IDLE_CPU_SHARE: f64 = 0.25;

#[tokio::test(flavor = "current_thread")]
async fn hard_native_rebuild_chunk_loaded_unretired_original_quarantines_without_successor() {
    unretired_original_rebuild(Engine::Chunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_rebuild_permit_loaded_unretired_original_quarantines_without_successor() {
    unretired_original_rebuild(Engine::Permit).await;
}

fn thread_cpu() -> Duration {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: valid out-pointer; on failure ts stays zero (share reads 0).
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    Duration::new(ts.tv_sec.max(0) as u64, ts.tv_nsec.clamp(0, 999_999_999) as u32)
}

/// Drive the Service until `until`, failing on completion or any successor
/// accept; returns (gated Service wake-ups per 5ms tick, this thread's CPU
/// share) over the interval.
async fn hold<F: std::future::Future<Output = Completion>>(rig: &mut Rig<F>, until: Instant) -> (f64, f64) {
    let Rig { server, service, .. } = rig;
    let (wall, cpu) = (Instant::now(), thread_cpu());
    let accept = async { server.accept().await.map(|_| ()) };
    tokio::pin!(accept);
    let gate = Arc::new(GateWake::default());
    let mut run = std::pin::pin!(gated(service.run(), &gate));
    tokio::select! {
        report = &mut run => panic!("held original must not complete: {report:?}"),
        incoming = &mut accept => panic!("successor reached the peer: {incoming:?}"),
        () = tokio::time::sleep_until(until) => {}
    }
    let wall = wall.elapsed().as_secs_f64().max(f64::EPSILON);
    let wakes = gate.polls.load(Ordering::Relaxed) as f64 / (wall / 0.005);
    (wakes, (thread_cpu() - cpu).as_secs_f64() / wall)
}

async fn unretired_original_rebuild(engine: Engine) {
    let mut rig = rig_with(engine.policy(), |_| {});
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let Loaded { peer, send, recv, old, meter } = load_original(&mut rig, engine).await;
        drop(meter);
        assert!(rig.handle.request_rebuild());
        // The saved driver adopts the original business limit (request+15s).
        let limit = window(Duration::from_secs(5), async {
            loop {
                let limit = old.ledger().shutdown_deadline;
                if let Some(limit) = limit {
                    break limit;
                }
                let Rig { service, .. } = &mut rig;
                let _ = tokio::time::timeout(TICK, service.run()).await;
            }
        })
        .await
        .expect("rebuild request adopted the original limit");
        // Held drain: business pending, no cut, no successor until the limit.
        let draining = hold(&mut rig, limit - Duration::from_secs(1)).await;
        assert!(!old.business_complete.load(Ordering::Acquire), "held business settled early");
        assert_eq!(rig.handle.phase(), Phase::Rebuilding);
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 0, "no cut while held");
        // Past the limit: sticky Q, still no completion/successor.
        let quarantined = hold(&mut rig, limit + Duration::from_secs(1)).await;
        let after_q = hold(&mut rig, limit + Duration::from_secs(3)).await;
        eprintln!("F5 {engine:?}: (wakes/tick, cpu share) draining={draining:.2?} at_limit={quarantined:.2?} after_q={after_q:.2?}");
        drop((send, recv));
        peers.push(peer);
        (old, (draining, after_q))
    })
    .await;
    let disposition = capture(async {
        let (old, _) = scenario.as_ref().ok().cloned().expect("scenario");
        assert_eq!(old.state(), SessionDeviceState::Incomplete);
        assert_eq!(rig.handle.phase(), Phase::Quarantined);
        assert!(rig.service.completion.is_none(), "no completion under sticky Q");
        assert!(!rig.handle.request_rebuild(), "no further generation");
        let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&current, &old), "successor session never installed");
        assert_eq!(peers.len(), 1, "no successor connection reached the peer");
        for (count, want) in [(&rig.gate.cuts, 0), (&rig.gate.resumes, 0), (&rig.gate.joins, 0)] {
            assert_eq!(count.load(Ordering::Acquire), want);
        }
        let flow = old.flow_observation().expect("original flow observation");
        assert_eq!((flow.live, flow.published, flow.joined, flow.released, flow.f), (1, 1, 5, 0, 1));
        assert!(old.ledger().quarantined, "held flow quarantines the original session");
        assert!(!old.business_complete.load(Ordering::Acquire));
        assert!(budget.quarantined() && budget.used() != 0, "sticky non-zero Q");
    })
    .await;
    // D61/D65: the loop must park while it only waits (draining and after Q).
    let parked = capture(async {
        let (_, (draining, after_q)) = scenario.as_ref().ok().cloned().expect("scenario");
        for (phase, (wakes, share)) in [("held drain", draining), ("sticky Q", after_q)] {
            assert!(wakes <= MAX_WAKES_PER_TICK, "{phase} spins: {wakes:.2} wakes per 5ms tick");
            assert!(share <= MAX_IDLE_CPU_SHARE, "{phase} burns cpu share {share:.3}");
        }
    })
    .await;
    // Forced shell destruction is explicitly not asynchronous cleanup.
    let Rig { server, service, gate, _profile, .. } = rig;
    drop(service);
    let drained = capture(close(server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    disposition.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
    assert!(budget.quarantined(), "Q stays sticky after forced destruction");
    parked.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}
