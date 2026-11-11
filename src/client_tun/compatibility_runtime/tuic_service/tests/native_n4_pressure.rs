//! N4 pressure cells M5 (Target reset + slow consumer + half-closed flow beside
//! L), M6 (16×4 MiB unread budget peak) and M7 (DNS flood fairness, b2 per-poll
//! RX bound) on the real hard Native Service (D145 §2–§4). Each cell measures
//! its own undisturbed baseline phase first ("same run" baseline). Owns no
//! production authority; shared helpers come from `native_n4_recovery`.
use super::*;
use native_n4_harness::{Census, HALF_CLOSE_PORT, Load, LoadSpec, Peer, RESET_PORT, census};
use native_n4_recovery::{assert_tasks_converge, run_until, standard};
use smoltcp::socket::tcp;
use smoltcp::wire::Ipv4Address;
use std::cell::Cell;
use std::time::Instant;

const MIB: usize = 1024 * 1024;

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Isolation bound: P99 ≤ baseline P99 × 2 + 50ms (D145 §3).
fn isolation_bound(baseline_p99: Duration) -> Duration {
    baseline_p99 * 2 + Duration::from_millis(50)
}

/// M5: L for 30s; after a 5s baseline (i) one flow's Target resets, (ii) one
/// slow consumer stops reading while the peer pushes 2 MiB (resumes at +20s),
/// (iii) one flow is half-closed by its peer. Healthy rounds keep the isolation
/// bound with 0 failures, (ii) is bounded by its window and then completes,
/// (iii) is reset alone (D147), and the session never quarantines.
#[tokio::test(flavor = "current_thread")]
async fn n4_m5_reset_slow_consumer_and_half_close_isolated_under_load() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let (stopped, mut samples, mut out) = (Cell::new(None), vec![], None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(90), async {
        let mut load = Load::new(&mut rig.apps, standard());
        let start = Instant::now();
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(5),
        )
        .await;
        let baseline = load.rounds_between(
            start + Duration::from_secs(1),
            start + Duration::from_secs(5),
        );
        let base_budget = samples.iter().map(|c| c.budget_used).max().unwrap_or(0);
        let target = Ipv4Address::from(rig.apps.target);
        let reset = load.connect(&mut rig.apps, target, RESET_PORT, 1024);
        let half = load.connect(&mut rig.apps, target, HALF_CLOSE_PORT, 1024);
        let slow = load.bulk(&mut rig.apps, 2 * MIB, 64 * 1024);
        load.bulks[slow].reading = false;
        let dist = Instant::now();
        let (mut sent, mut reset_at, mut fin_at, mut half_reset_at) =
            ([false; 2], None, None, None);
        let mut stall_budget = 0;
        while dist.elapsed() < Duration::from_secs(25) {
            run_until(
                &mut load,
                &mut rig,
                &peer,
                &mut samples,
                Instant::now() + Duration::from_millis(250),
            )
            .await;
            for (i, handle) in [reset, half].into_iter().enumerate() {
                let socket = rig.apps.sockets.get_mut::<tcp::Socket>(handle);
                if !sent[i] && socket.can_send() {
                    sent[i] = socket.send_slice(b"n4m5").is_ok();
                }
                while socket.can_recv() {
                    let mut b = [0u8; 64];
                    let _ = socket.recv_slice(&mut b);
                }
            }
            let state = |h| rig.apps.sockets.get::<tcp::Socket>(h).state();
            if reset_at.is_none() && sent[0] && state(reset) == tcp::State::Closed {
                reset_at = Some(dist.elapsed());
            }
            if fin_at.is_none() && state(half) == tcp::State::CloseWait {
                fin_at = Some(Instant::now());
            }
            if fin_at.is_some() && half_reset_at.is_none() && state(half) == tcp::State::Closed {
                half_reset_at = fin_at.map(|f| f.elapsed());
            }
            if dist.elapsed() < Duration::from_secs(20) {
                stall_budget = stall_budget.max(samples.last().map_or(0, |c| c.budget_used));
            } else {
                load.bulks[slow].reading = true;
            }
        }
        let window = load.rounds_between(dist, Instant::now());
        let limit = Instant::now() + Duration::from_secs(10);
        while load.bulks[slow].done.is_none() && Instant::now() < limit {
            run_until(
                &mut load,
                &mut rig,
                &peer,
                &mut samples,
                Instant::now() + Duration::from_millis(250),
            )
            .await;
        }
        let bulk = &load.bulks[slow];
        let bulk = (bulk.got, bulk.done.is_some(), bulk.corrupt);
        rig.apps.sockets.get_mut::<tcp::Socket>(half).close();
        load.run_for(&mut rig.apps, Duration::from_secs(1)).await;
        out = Some((
            baseline,
            window,
            base_budget,
            stall_budget,
            reset_at,
            half_reset_at,
            bulk,
            load.report(),
        ));
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let (baseline, window, base_budget, stall_budget, reset_at, half_reset, bulk, load) =
        out.unwrap();
    let resets = peer.log.lock().unwrap().resets;
    println!(
        "N4M5 baseline={baseline:?} window={window:?} bound={:?} base_budget={base_budget} stall_budget={stall_budget} delta={} reset_at={reset_at:?} resets={resets} half_reset_after_fin={half_reset:?} bulk={bulk:?} {load:?}",
        isolation_bound(baseline.1[2]),
        stall_budget.saturating_sub(base_budget)
    );
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
    assert!(samples.iter().all(|c: &Census| !c.q), "session Q");
    assert_eq!(load.round_failures, 0, "healthy flows failed");
    assert!(
        window.0 > 0 && window.1[2] <= isolation_bound(baseline.1[2]),
        "healthy P99 {window:?} vs {baseline:?}"
    );
    assert!(
        resets == 1 && reset_at.is_some_and(|r| r <= Duration::from_secs(5)),
        "(i) typed Target reset"
    );
    // (ii) D155: the unread flow's backing is bounded by its real window chain:
    // A2_MEASURED.rx (5/3, honest backing per received byte) × (min(pushed,
    // QUIC stream window 8 MiB) + flow-custody reader WINDOW_BYTES 64 KiB).
    use crate::quic::honest_bound::{MeasuredFactors, Ratio};
    assert_eq!(MeasuredFactors::A2_MEASURED.rx, Ratio::new(5, 3).unwrap());
    let stream = (2 * MIB).min(crate::quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize);
    let chain = (stream + 65_536) * 5 / 3;
    assert!(
        stall_budget.saturating_sub(base_budget) <= chain,
        "(ii) stalled flow budget > window chain {chain}"
    );
    // ... and is delivered intact once the App reads again.
    assert_eq!(bulk, (2 * MIB, true, false), "(ii) slow consumer completes");
    assert!(
        half_reset.is_some_and(|r| r >= Duration::from_secs(14) && r <= Duration::from_secs(17)),
        "(iii) {half_reset:?}"
    );
    assert_tasks_converge(rig, peer, tasks_before).await;
}

/// M6: 16 flows each pulling 4 MiB while the App reads nothing for 20s, then
/// everything is read. Budget peak ≤ hard limit without Q, the source is paused
/// (peer send stops below the total), and every byte arrives intact.
#[tokio::test(flavor = "current_thread")]
async fn n4_m6_unread_bulk_peak_is_bounded_then_drains() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let (stopped, mut samples, mut out) = (Cell::new(None), vec![], None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(120), async {
        let spec = LoadSpec::default();
        let mut load = Load::new(&mut rig.apps, spec);
        let bulks: Vec<_> = (0..16)
            .map(|_| load.bulk(&mut rig.apps, 4 * MIB, 64 * 1024))
            .collect();
        for &b in &bulks {
            load.bulks[b].reading = false;
        }
        let start = Instant::now();
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(15),
        )
        .await;
        let written_15 = peer.log.lock().unwrap().bulk_written;
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(20),
        )
        .await;
        let written_20 = peer.log.lock().unwrap().bulk_written;
        let stall_peak = samples.iter().map(|c| c.budget_used).max().unwrap_or(0);
        for &b in &bulks {
            load.bulks[b].reading = true;
        }
        let limit = Instant::now() + Duration::from_secs(60);
        while load.bulks.iter().any(|b| b.done.is_none()) && Instant::now() < limit {
            run_until(
                &mut load,
                &mut rig,
                &peer,
                &mut samples,
                Instant::now() + Duration::from_millis(500),
            )
            .await;
        }
        let drain = start.elapsed() - Duration::from_secs(20);
        let got: Vec<_> = load
            .bulks
            .iter()
            .map(|b| (b.got, b.done.is_some(), b.corrupt))
            .collect();
        // P1 (F1): App view of every unfinished flow at the drain deadline.
        let stalled: Vec<_> = load
            .bulks
            .iter()
            .filter(|b| b.done.is_none())
            .map(|b| {
                let socket = rig.apps.sockets.get::<tcp::Socket>(b.handle);
                let port = socket.local_endpoint().map(|e| e.port);
                (port, b.got, socket.recv_queue(), socket.state())
            })
            .collect();
        let drops = rig.links.each_ref().map(|l| l.dropped_packets());
        out = Some((written_15, written_20, stall_peak, drain, got, stalled, drops));
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let (written_15, written_20, stall_peak, drain, got, stalled, drops) = out.unwrap();
    let limit = rig.budget.limit();
    let peak = samples.iter().map(|c| c.budget_used).max().unwrap_or(0);
    println!(
        "N4M6 limit={limit} stall_peak={stall_peak} peak={peak} written_15={written_15} written_20={written_20} total={} drain={drain:?} got={got:?} dropped[app→svc,svc→app]={drops:?} stalled(port,got,app_recv_queue,state)={stalled:?}",
        16 * 4 * MIB
    );
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        false,
    );
    assert!(
        samples.iter().all(|c| !c.q && !c.first_failure),
        "session Q/first failure"
    );
    assert!(peak <= limit, "budget peak {peak} > hard {limit}");
    assert!(
        written_20 < 16 * 4 * MIB && written_20 == written_15,
        "source not paused: {written_15}→{written_20}"
    );
    assert!(
        got.iter().all(|g| *g == (4 * MIB, true, false)),
        "delivery {got:?}"
    );
    assert_tasks_converge(rig, peer, tasks_before).await;
}

/// M7: 16 long TCP flows; after a 5s baseline a DNS flood (200 queries every
/// 100ms, 4000 in all) beside them for the rest of 30s. Every query is
/// answered (ServFail counted apart), TCP P99 keeps the isolation bound, loop
/// lateness stays in the O-N2d bound, and one Service poll takes ≤128 RX on
/// the main receive path (b2) and ≤ Device slots in all (D159).
#[tokio::test(flavor = "current_thread")]
async fn n4_m7_dns_flood_is_fair_to_tcp_and_bounded_per_poll() {
    let (mut service, mut rig) = native_waiting::rig_links(Duration::from_millis(5), 1, 512).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let rx = rig.rx_installed.clone();
    let (stopped, max_rx, mut samples, mut out) =
        (Cell::new(None), Cell::new((0usize, 0usize)), vec![], None);
    let script = async {
        let spec = LoadSpec {
            long: 16,
            dns_burst: Some((Duration::from_millis(100), 200, 4000)),
            ..LoadSpec::default()
        };
        let mut load = Load::new(&mut rig.apps, spec);
        let start = Instant::now();
        load.hold_dns(start + Duration::from_secs(5));
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(5),
        )
        .await;
        let baseline = load.rounds_between(
            start + Duration::from_secs(1),
            start + Duration::from_secs(5),
        );
        let base_rx = max_rx.replace((0, 0));
        let flood = Instant::now();
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(30),
        )
        .await;
        let window = load.rounds_between(flood, Instant::now());
        out = Some((baseline, window, base_rx, load.report()));
        native_waiting::stop_polling(&mut rig, &stopped).await
    };
    let mut run = std::pin::pin!(service.run());
    let measured = std::future::poll_fn(|cx| {
        let load = || {
            rx.each_ref()
                .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        };
        let before = load();
        let poll = run.as_mut().poll(cx);
        let [main, drained] = load();
        let taken = (
            (main - before[0]) as usize,
            (main + drained - before[0] - before[1]) as usize,
        );
        let (most_main, most_total) = max_rx.get();
        max_rx.set((most_main.max(taken.0), most_total.max(taken.1)));
        poll
    });
    let report = fixture::window(Duration::from_secs(90), async {
        tokio::select! {
            _ = script => unreachable!(),
            report = measured => report,
        }
    })
    .await
    .expect("scenario or stop exceeded its window");
    let Some((baseline, window, base_rx, load)) = out else {
        panic!(
            "Service ended before the M7 script: max_rx={:?} samples={:?} report={report:?}",
            max_rx.get(),
            samples.last()
        );
    };
    let flood_rx = max_rx.get();
    println!(
        "N4M7 baseline={baseline:?} window={window:?} bound={:?} max_rx_per_poll(main,total) base={base_rx:?} flood={flood_rx:?} {load:?}",
        isolation_bound(baseline.1[2])
    );
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        false,
    );
    assert!(
        samples.iter().all(|c| !c.q && !c.first_failure),
        "session Q/first failure"
    );
    assert_eq!(load.dns_sent, 4000, "flood not fully sent");
    assert_eq!(
        (load.dns_answered, load.dns_unanswered),
        (4000, 0),
        "DNS answers"
    );
    assert!(load.dns_p[2] <= Duration::from_millis(100), "DNS P99");
    assert_eq!(load.round_failures, 0);
    assert!(
        window.0 > 0 && window.1[2] <= isolation_bound(baseline.1[2]),
        "TCP P99 {window:?} vs {baseline:?}"
    );
    assert!(
        load.late_p[2] <= Duration::from_millis(50) && load.late_p[3] <= Duration::from_millis(250),
        "loop lateness"
    );
    // D159 (amends D145): b2 bounds the main receive path; timer/deferred
    // drains (≤256 per call, O-N2a) stay inside the Device slots.
    assert!(
        flood_rx.0 > 0 && flood_rx.0 <= 128 && flood_rx.1 <= 512,
        "one Service poll took {flood_rx:?} RX packets (main b2 ≤128, total ≤512 slots)"
    );
    assert_tasks_converge(rig, peer, tasks_before).await;
}
