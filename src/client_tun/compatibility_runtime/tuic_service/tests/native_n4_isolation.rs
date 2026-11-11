//! N4 cells M1 (steady concurrency/long run) and M5(iii) (half-closed flow
//! isolation, F-N2a) on the real hard Native Service (D145). Frozen windows
//! live in `docs/current/n4-design-20261109.md`; `N4_SPAN_SECS` may only
//! shorten M1 for exploratory runs, gate runs use the frozen default.
use super::*;
use native_n4_harness::{
    Census, HALF_CLOSE_PORT, Load, LoadSpec, Peer, SLOW_TAIL_BYTES, SLOW_TAIL_PORT, census,
};
use smoltcp::socket::tcp;
use smoltcp::wire::Ipv4Address;
use std::time::Instant;

const M1_BULK: usize = 8 * 1024 * 1024;

fn span(frozen: u64) -> Duration {
    let secs = std::env::var("N4_SPAN_SECS")
        .ok()
        .and_then(|v| v.parse().ok());
    Duration::from_secs(secs.map_or(frozen, |s: u64| s.min(frozen)))
}

fn peak(samples: &[Census]) -> (usize, usize, usize) {
    samples.iter().fold((0, 0, 0), |(l, b, t), c| {
        (l.max(c.live), b.max(c.budget_used), t.max(c.tasks))
    })
}

/// M1: standard load L (16 long TCP, 2 UDP, DNS+short TCP every 500ms) for
/// 120s on one Node: quality, 0 reconnects, bounded peaks, exact convergence.
#[tokio::test(flavor = "current_thread")]
async fn n4_m1_steady_load_quality_peaks_and_convergence() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let span = span(120);
    let stopped = std::cell::Cell::new(None);
    let (mut samples, mut result, mut bulk_result) = (vec![], None, None);
    let tasks_before = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let report = native_waiting::drive(&mut service, span + Duration::from_secs(60), async {
        let spec = LoadSpec {
            long: 16,
            udp: 2,
            dns_every: Some(Duration::from_millis(500)),
            dns_burst: None,
        };
        let mut load = Load::new(&mut rig.apps, spec);
        let start = Instant::now();
        let mut bulk = None;
        while start.elapsed() < span {
            load.run_for(&mut rig.apps, Duration::from_secs(1)).await;
            samples.push(census(&rig.saved, &rig.budget, &peer));
            if bulk.is_none() && start.elapsed() >= span.min(Duration::from_secs(5)) {
                // D145 M1: one 8 MiB downlink bulk flow beside L (O-N2b record).
                bulk = Some(load.bulk(&mut rig.apps, M1_BULK, 256 * 1024));
            }
        }
        let bulk = &load.bulks[bulk.expect("bulk started")];
        bulk_result = Some((bulk.got, bulk.done, bulk.corrupt));
        result = Some(load.report());
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let elapsed = stopped.get().unwrap().elapsed();
    let load = result.unwrap();
    let (bulk_got, bulk_done, bulk_corrupt) = bulk_result.unwrap();
    let mbps = bulk_done.map(|d| M1_BULK as f64 * 8.0 / d.as_secs_f64() / 1e6);
    println!(
        "N4M1 bulk got={bulk_got}/{M1_BULK} done={bulk_done:?} mbit_s={mbps:?} corrupt={bulk_corrupt}"
    );
    let (live, budget, tasks) = peak(&samples);
    let connections = peer.log.lock().unwrap().connections;
    println!(
        "N4M1 span={span:?} {load:?} peaks live={live} budget={budget} tasks={tasks} tasks_before={tasks_before} connections={connections} last={:?}",
        samples.last()
    );
    native_waiting::assert_clean_stop(report, elapsed, &rig.budget, &rig.handle, false);
    assert!(
        samples.iter().all(|c| !c.q && !c.first_failure),
        "session Q/first failure"
    );
    assert_eq!(connections, 1, "0 new DCIDs on a healthy path");
    assert!(
        bulk_done.is_some() && bulk_got == M1_BULK && !bulk_corrupt,
        "8 MiB bulk flow"
    );
    assert_eq!(load.round_failures, 0);
    assert!(load.udp_lost * 200 <= load.udp_sent, "UDP loss > 0.5%");
    assert_eq!(load.udp_duplicates, 0);
    assert!(load.udp_p[2] <= Duration::from_millis(200), "UDP P99");
    assert!(
        load.dns_unanswered <= 1 && load.dns_answered + 1 >= load.dns_sent,
        "DNS answers"
    );
    assert!(load.dns_p[2] <= Duration::from_millis(100), "DNS P99");
    assert!(load.shorts_open <= 2, "short flows");
    assert!(
        load.late_p[2] <= Duration::from_millis(50) && load.late_p[3] <= Duration::from_millis(250),
        "loop lateness"
    );
    // 16 long + the fixture's default TCP + short flows still inside their
    // close tail (2/s x the 15s close tail of the flow custody contract).
    assert!(live <= 17 + 30, "live flow peak {live}");
    peer.stop();
    rig.server.close(0u32.into(), b"done");
    // Endpoint/connection drivers drain within their own 3xPTO close window.
    let limit = Instant::now() + Duration::from_secs(5);
    let mut after = usize::MAX;
    while Instant::now() < limit && after >= tasks_before + 2 * connections {
        tokio::time::sleep(Duration::from_millis(100)).await;
        after = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
    }
    // D147 attribution (d147/task-probe.log): no Service-owned task survives
    // stop (dropping the Service/Control changes nothing). The test RttRelay
    // keeps 2 forwarding tasks per client connection it has seen, created
    // after the baseline; the aborted peer acceptor leaves (-1).
    let rig_owned = 2 * connections;
    println!("N4M1 tasks_after_peer_close={after} tasks_before={tasks_before} relay={rig_owned}");
    assert!(
        after < tasks_before + rig_owned,
        "task census did not converge: {after} vs {tasks_before}+{rig_owned}-1"
    );
}

/// M5(iii) F-N2a: one flow is half-closed by its peer (FIN after the reply)
/// while the App keeps its socket open; 4 healthy long flows keep running.
/// Frozen: the session never quarantines and healthy flows keep progressing;
/// D147 (A'): after 15s without App progress only that flow is reset.
#[tokio::test(flavor = "current_thread")]
async fn n4_m5_half_closed_flow_is_isolated_from_session() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let stopped = std::cell::Cell::new(None);
    let mut lines = vec![];
    let report = native_waiting::drive(&mut service, Duration::from_secs(60), async {
        let spec = LoadSpec {
            long: 4,
            udp: 1,
            dns_every: None,
            dns_burst: None,
        };
        let mut load = Load::new(&mut rig.apps, spec);
        load.run_for(&mut rig.apps, Duration::from_secs(2)).await;
        let target = Ipv4Address::from(rig.apps.target);
        let half = load.connect(&mut rig.apps, target, HALF_CLOSE_PORT, 1024);
        let (mut sent, mut fin_at, mut reset_at) = (false, None, None);
        let (mut any_q, mut rounds_after_reset) = (false, 0);
        let start = Instant::now();
        let mut rounds_before = 0;
        while start.elapsed() < Duration::from_secs(25) {
            load.run_for(&mut rig.apps, Duration::from_millis(250))
                .await;
            let socket = rig.apps.sockets.get_mut::<tcp::Socket>(half);
            if !sent && socket.can_send() {
                sent = socket.send_slice(b"half").is_ok();
            }
            while socket.can_recv() {
                let mut b = [0u8; 64];
                let _ = socket.recv_slice(&mut b);
            }
            let state = socket.state();
            if fin_at.is_none() && state == tcp::State::CloseWait {
                fin_at = Some(Instant::now());
            }
            if fin_at.is_some() && reset_at.is_none() && state == tcp::State::Closed {
                reset_at = Some(Instant::now());
            }
            let c = census(&rig.saved, &rig.budget, &peer);
            let rounds = load.report().rounds;
            if reset_at.is_some() {
                rounds_after_reset += rounds - rounds_before;
            }
            lines.push(format!(
                "t={:.2} fin+{:?} half={state:?} rounds+{} {c:?}",
                start.elapsed().as_secs_f64(),
                fin_at.map(|f| f.elapsed().as_millis()),
                rounds - rounds_before
            ));
            rounds_before = rounds;
            any_q |= c.q;
            if c.q {
                break;
            }
        }
        for line in &lines {
            println!("N4M5iii {line}");
        }
        let fin_at = fin_at.expect("peer FIN reached the App socket");
        assert!(
            !any_q,
            "F-N2a: half-closed flow quarantined the whole session"
        );
        let reset = reset_at
            .expect("idle half-closed tail was not reset locally")
            .duration_since(fin_at);
        println!("N4M5iii reset_after_fin={reset:?} rounds_after_reset={rounds_after_reset}");
        assert!(
            reset >= Duration::from_secs(14) && reset <= Duration::from_secs(17),
            "tail reset at {reset:?}"
        );
        assert!(
            rounds_after_reset > 0,
            "healthy flows stalled after the local reset"
        );
        let report = load.report();
        assert_eq!(report.round_failures, 0, "healthy flows failed");
        assert_eq!(peer.log.lock().unwrap().half_closed, 1);
        rig.apps.sockets.get_mut::<tcp::Socket>(half).close();
        load.run_for(&mut rig.apps, Duration::from_secs(2)).await;
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let elapsed = stopped.get().unwrap().elapsed();
    native_waiting::assert_clean_stop(report, elapsed, &rig.budget, &rig.handle, false);
    peer.stop();
    rig.server.close(0u32.into(), b"done");
}

/// M5(iii) D147 (A'): a slow but progressing reader of a FINed tail (2 KiB/s,
/// 96 KiB) outlives the FIN-measured 15s and still receives every byte and the
/// graceful FIN; the close-tail deadline counts from the last App progress.
#[tokio::test(flavor = "current_thread")]
async fn n4_m5_slow_progressing_tail_reader_is_not_cut() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let stopped = std::cell::Cell::new(None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(110), async {
        let spec = LoadSpec {
            long: 2,
            udp: 0,
            dns_every: None,
            dns_burst: None,
        };
        let mut load = Load::new(&mut rig.apps, spec);
        load.run_for(&mut rig.apps, Duration::from_secs(1)).await;
        let target = Ipv4Address::from(rig.apps.target);
        let slow = load.connect(&mut rig.apps, target, SLOW_TAIL_PORT, 2048);
        let expected = 4 + SLOW_TAIL_BYTES;
        let (mut sent, mut got, mut first_at, mut any_q) = (false, 0usize, None, false);
        let start = Instant::now();
        let state = loop {
            load.run_for(&mut rig.apps, Duration::from_millis(250))
                .await;
            let socket = rig.apps.sockets.get_mut::<tcp::Socket>(slow);
            if !sent && socket.can_send() {
                sent = socket.send_slice(b"slow").is_ok();
            }
            if socket.can_recv() {
                let mut b = [0u8; 512];
                got += socket.recv_slice(&mut b).unwrap_or(0);
                first_at.get_or_insert_with(Instant::now);
            }
            let state = socket.state();
            any_q |= census(&rig.saved, &rig.budget, &peer).q;
            let eof = state == tcp::State::CloseWait && got == expected;
            if eof
                || any_q
                || state == tcp::State::Closed
                || start.elapsed() > Duration::from_secs(90)
            {
                break state;
            }
        };
        let drain = first_at.map(|f| f.elapsed());
        println!("N4M5slow got={got}/{expected} state={state:?} drain={drain:?} q={any_q}");
        assert!(!any_q, "slow tail reader quarantined the session");
        assert_eq!(
            got, expected,
            "slow but progressing reader was cut ({state:?})"
        );
        assert_eq!(
            state,
            tcp::State::CloseWait,
            "graceful FIN after the full tail"
        );
        assert_eq!(load.report().round_failures, 0, "healthy flows failed");
        rig.apps.sockets.get_mut::<tcp::Socket>(slow).close();
        load.run_for(&mut rig.apps, Duration::from_secs(2)).await;
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let elapsed = stopped.get().unwrap().elapsed();
    native_waiting::assert_clean_stop(report, elapsed, &rig.budget, &rig.handle, false);
    peer.stop();
    rig.server.close(0u32.into(), b"done");
}
