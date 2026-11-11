//! N4 recovery cells M2 (repeated explicit close + generation isolation), M3
//! (slow segment, black holes 8s×2 and 30s/Waiting) and M4 (storm, App RST and
//! stop during Waiting) on the real hard Native Service under standard load L
//! (D145, `docs/current/n4-design-20261109.md` §2–§4). Durations are local
//! scaling, never N5 evidence. Owns no production authority; the shared cell
//! helpers here are reused by `native_n4_pressure`.
use super::*;
use native_n4_harness::{Census, Load, LoadSpec, Peer, UDP_PORT, census};
use native_rebuild_high_rtt::Dir;
use native_waiting::{Rig, Storm, a2_bound};
use std::cell::Cell;
use std::time::Instant;

/// Standard load L (D145 §2).
pub(super) fn standard() -> LoadSpec {
    LoadSpec {
        long: 16,
        udp: 2,
        dns_every: Some(Duration::from_millis(500)),
        dns_burst: None,
    }
}

/// Run L until `at`, sampling the census every second.
pub(super) async fn run_until(
    load: &mut Load,
    rig: &mut Rig,
    peer: &Peer,
    samples: &mut Vec<Census>,
    at: Instant,
) {
    while Instant::now() < at {
        let step = at.saturating_duration_since(Instant::now());
        load.run_for(&mut rig.apps, step.min(Duration::from_secs(1)))
            .await;
        samples.push(census(&rig.saved, &rig.budget, peer));
    }
}

pub(super) fn session_id(rig: &Rig) -> Option<usize> {
    native_waiting::session(&rig.saved).map(|c| Arc::as_ptr(&c) as usize)
}

/// After stop and peer close, alive tasks return to the baseline plus the
/// attributed test-relay forwarding tasks: 2 per relay client socket address
/// (D148; D152: an endpoint socket rebind is a second address, not a second
/// peer connection).
pub(super) async fn assert_tasks_converge(rig: Rig, peer: Peer, tasks_before: usize) {
    let clients = rig.relay.first_seen.lock().unwrap().len();
    peer.stop();
    rig.server.close(0u32.into(), b"done");
    let limit = Instant::now() + Duration::from_secs(5);
    let rig_owned = 2 * clients;
    let mut after = usize::MAX;
    while Instant::now() < limit && after >= tasks_before + rig_owned {
        tokio::time::sleep(Duration::from_millis(100)).await;
        after = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
    }
    println!("N4 tasks_after_peer_close={after} tasks_before={tasks_before} relay={rig_owned}");
    assert!(
        after < tasks_before + rig_owned,
        "task census did not converge: {after} vs {tasks_before}+{rig_owned}-1"
    );
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// `encode_packet` with an explicit fragment header (D107 wire layout).
fn fragment(assoc: u16, target: &TargetAddr, packet: u16, id: u8, data: &[u8]) -> Vec<u8> {
    let mut wire = encode_packet(assoc, target, data);
    wire[4..6].copy_from_slice(&packet.to_be_bytes());
    (wire[6], wire[7]) = (2, id);
    wire
}

fn last_assoc(peer: &Peer) -> Option<u16> {
    peer.log.lock().unwrap().assoc.last().copied().flatten()
}

fn no_q(samples: &[Census]) -> bool {
    samples.iter().all(|c| !c.q)
}

/// M2: L with 5 explicit peer closes, one every 10s. Each cycle a new TCP
/// round and a same-4-tuple UDP round trip return within 2s of the fault; the
/// old generation's fragment 0, a late fragment 1 and a foreign association
/// sent after recovery never reach the App (M8 merged).
#[tokio::test(flavor = "current_thread")]
async fn n4_m2_repeated_explicit_close_recovers_and_isolates_generations() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let (stopped, mut samples, mut cycles, mut result) = (Cell::new(None), vec![], vec![], None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(120), async {
        let mut load = Load::new(&mut rig.apps, standard());
        let udp_target =
            TargetAddr::IpPort(std::net::SocketAddr::from((rig.apps.target, UDP_PORT)));
        let start = Instant::now();
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(10),
        )
        .await;
        let control = session_id(&rig);
        for cycle in 0..5u32 {
            let old = peer.latest().expect("live peer connection");
            let assoc = last_assoc(&peer).expect("UDP association on the old connection");
            // D155 F-N4a: a per-cycle packet id. Cycle k's NEW connection is cycle
            // k+1's OLD one, so a shared id would pair cycle k's fragment 1 with
            // cycle k+1's fragment 0 legally inside ONE generation (tracer
            // `n4_m2_single_cycle_fragment_generation_tracer`).
            let packet = 100 + u16::try_from(cycle).unwrap();
            let _ = old.send_datagram(fragment(assoc, &udp_target, packet, 0, b"late-old-").into());
            load.run_for(&mut rig.apps, Duration::from_millis(200))
                .await;
            let fault = Instant::now();
            old.close(0u32.into(), b"n4-m2");
            let (mut probed, end) = (false, fault + Duration::from_secs(10));
            while Instant::now() < end {
                let at = (Instant::now() + Duration::from_millis(250)).min(end);
                run_until(&mut load, &mut rig, &peer, &mut samples, at).await;
                let recovered = load
                    .first_round_after(fault)
                    .zip(load.first_udp_after(fault));
                let new = peer.latest().filter(|c| c.stable_id() != old.stable_id());
                if let (false, Some(_), Some(new), Some(assoc)) =
                    (probed, recovered, new, last_assoc(&peer))
                {
                    let _ = new.send_datagram(
                        encode_packet(assoc ^ 0x5555, &udp_target, b"late-foreign").into(),
                    );
                    let _ = new
                        .send_datagram(fragment(assoc, &udp_target, packet, 1, b"late-new").into());
                    probed = true;
                }
            }
            let (tcp, udp) = (load.first_round_after(fault), load.first_udp_after(fault));
            println!("N4M2 cycle={cycle} t_tcp={tcp:?} t_udp={udp:?} probed={probed}");
            cycles.push((tcp, udp, probed));
        }
        assert_eq!(session_id(&rig), control, "same SessionControl");
        result = Some(load.report());
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let load = result.unwrap();
    let connections = peer.log.lock().unwrap().connections;
    let live = samples.iter().map(|c| c.live).max().unwrap_or(0);
    println!(
        "N4M2 {load:?} connections={connections} live_peak={live} last={:?}",
        samples.last()
    );
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
    assert!(no_q(&samples), "session Q");
    assert_eq!(connections, 6, "one new connection per explicit close");
    let two = Duration::from_secs(2);
    for (cycle, (tcp, udp, probed)) in cycles.iter().enumerate() {
        assert!(
            tcp.is_some_and(|t| t <= two) && udp.is_some_and(|u| u <= two),
            "cycle {cycle}: {tcp:?} {udp:?}"
        );
        assert!(probed, "cycle {cycle}: isolation probe not sent");
    }
    assert_eq!(
        load.udp_foreign, 0,
        "old/foreign generation datagram reached the App"
    );
    assert_eq!(load.udp_duplicates, 0);
    assert!(live <= 17 + 30 + 16, "live flow peak {live}");
    assert_tasks_converge(rig, peer, tasks_before).await;
}

/// F-N4a single cycle (D155): old connection fragment 0 of packet 7, then an
/// explicit close, then fragment 1 of packet 7 on the NEW connection must not
/// reassemble (cross-generation); packet 9's two fragments on the new
/// connection are the same-generation positive control (exactly one delivery).
#[tokio::test(flavor = "current_thread")]
async fn n4_m2_single_cycle_fragment_generation_tracer() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let (stopped, mut samples, mut heads) = (Cell::new(None), vec![], None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(60), async {
        let mut load = Load::new(&mut rig.apps, standard());
        let udp_target =
            TargetAddr::IpPort(std::net::SocketAddr::from((rig.apps.target, UDP_PORT)));
        let start = Instant::now();
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            start + Duration::from_secs(5),
        )
        .await;
        let old = peer.latest().expect("live peer connection");
        let assoc = last_assoc(&peer).expect("UDP association on the old connection");
        let _ = old.send_datagram(fragment(assoc, &udp_target, 7, 0, b"late-old-").into());
        load.run_for(&mut rig.apps, Duration::from_millis(200))
            .await;
        let fault = Instant::now();
        old.close(0u32.into(), b"n4-m2-tracer");
        let end = fault + Duration::from_secs(10);
        let mut new_assoc = None;
        while Instant::now() < end && new_assoc.is_none() {
            run_until(
                &mut load,
                &mut rig,
                &peer,
                &mut samples,
                Instant::now() + Duration::from_millis(100),
            )
            .await;
            let new = peer.latest().filter(|c| c.stable_id() != old.stable_id());
            if let (Some(_), Some(new), Some(a)) =
                (load.first_udp_after(fault), new, last_assoc(&peer))
            {
                let _ = new.send_datagram(fragment(a, &udp_target, 7, 1, b"late-new").into());
                let _ = new.send_datagram(fragment(a, &udp_target, 9, 0, b"pos-a-").into());
                let _ = new.send_datagram(fragment(a, &udp_target, 9, 1, b"pos-b").into());
                new_assoc = Some(a);
            }
        }
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        println!(
            "N4M2T assoc old={assoc} new={new_assoc:?} heads={:?}",
            load.foreign_heads()
        );
        heads = Some((assoc, new_assoc, load.foreign_heads()));
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
    let (_, new_assoc, heads) = heads.unwrap();
    assert!(new_assoc.is_some(), "recovery/probe not reached");
    let count = |p: &str| heads.iter().filter(|h| h.as_str() == p).count();
    assert_eq!(
        count("pos-a-pos-b"),
        1,
        "same-generation control: {heads:?}"
    );
    assert_eq!(
        heads.iter().filter(|h| h.contains("late")).count(),
        0,
        "cross-generation leak: {heads:?}"
    );
}

/// Datagrams closer than this belong to one burst (D157).
const BURST_GAP: Duration = Duration::from_millis(50);

/// D157 loss-probe evidence for one hole, from the relay trace only.
#[derive(Debug)]
struct Probe {
    /// Side whose datagram is the first one observed at or after the lift.
    dir: Dir,
    after_lift: Duration,
    /// Its datagrams within `BURST_GAP` before the other side's first.
    burst: usize,
    /// (first post-lift − last hole burst) / (last − previous hole burst).
    backoff: f64,
}

/// D157: the first post-lift datagram opens a burst of ≤2 from one side,
/// sent before the other side's first, whose gap after that side's last
/// hole burst has doubled (RFC 9002 PTO backoff), i.e. a loss probe and
/// not lift-triggered or timer traffic. `None` = not enough hole bursts.
fn loss_probe(trace: &[(Instant, Dir, usize)], lift: Instant) -> Option<Probe> {
    let &(first, dir, _) = trace.iter().find(|(at, ..)| *at >= lift)?;
    let other = trace.iter().find(|(at, d, _)| *at >= first && *d != dir);
    let cut = other.map_or(first + BURST_GAP, |o| o.0.min(first + BURST_GAP));
    let burst = trace
        .iter()
        .filter(|(at, d, _)| *d == dir && *at >= first && *at < cut)
        .count();
    let (mut starts, mut last) = (vec![], None::<Instant>);
    for (at, ..) in trace.iter().filter(|(at, d, _)| *d == dir && *at < lift) {
        if last.is_none_or(|l| *at - l > BURST_GAP) {
            starts.push(*at);
        }
        last = Some(*at);
    }
    let [.., prev, last] = starts[..] else {
        return None;
    };
    let backoff = (first - last).as_secs_f64() / (last - prev).as_secs_f64();
    Some(Probe {
        dir,
        after_lift: first - lift,
        burst,
        backoff,
    })
}

/// One black hole of `outage` under L; returns (recovery after lift, new
/// DCIDs, D157 loss-probe evidence).
async fn black_hole(
    load: &mut Load,
    rig: &mut Rig,
    peer: &Peer,
    samples: &mut Vec<Census>,
    outage: Duration,
    limit: Duration,
) -> (Option<Duration>, usize, Option<Probe>) {
    let from = Instant::now();
    let lift = from + outage;
    {
        let mut tap = rig.relay.tap.lock().unwrap();
        tap.outage(from, lift);
        tap.trace_window(from, lift + limit);
    }
    run_until(load, rig, peer, samples, lift).await;
    let end = lift + limit;
    // A surviving long flow's stuck round completing is its recovery round.
    let probe = |load: &Load| {
        load.first_completion_after(lift)
            .zip(load.first_udp_after(lift))
    };
    while Instant::now() < end && probe(load).is_none() {
        run_until(
            load,
            rig,
            peer,
            samples,
            Instant::now() + Duration::from_millis(100),
        )
        .await;
    }
    let recovered = probe(load).map(|(t, u)| t.max(u));
    println!(
        "N4M3 hole={outage:?} tcp={:?} udp={:?}",
        load.first_completion_after(lift),
        load.first_udp_after(lift)
    );
    let tap = rig.relay.tap.lock().unwrap();
    let probe = loss_probe(&tap.trace, lift);
    println!(
        "N4M3 hole={outage:?} probe={probe:?} traced={}",
        tap.trace.len()
    );
    (recovered, tap.new_dcids(from, lift), probe)
}

/// M3: L through slow 20s (one-way +200ms, every 50th datagram dropped; the
/// first handshake happens inside it), black hole 8s twice, then 30s (Waiting).
#[tokio::test(flavor = "current_thread")]
async fn n4_m3_slow_then_black_holes_recover_within_bounds() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let t0 = Instant::now();
    rig.relay.tap.lock().unwrap().slow(
        t0,
        t0 + Duration::from_secs(20),
        Duration::from_millis(200),
        50,
    );
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let (stopped, mut samples, mut out) = (Cell::new(None), vec![], None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(180), async {
        let mut load = Load::new(&mut rig.apps, standard());
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            t0 + Duration::from_secs(20),
        )
        .await;
        let slow = load.report();
        // D155: count peer connections, not Initial DCIDs: at 400ms RTT one
        // connection's later Initial carries the server-chosen DCID (2 ids).
        let slow_dcids = peer.log.lock().unwrap().connections;
        println!("N4M3 slow {slow:?} dcids={slow_dcids}");
        let control = session_id(&rig);
        let mut holes = vec![];
        for _ in 0..2 {
            run_until(
                &mut load,
                &mut rig,
                &peer,
                &mut samples,
                Instant::now() + Duration::from_secs(5),
            )
            .await;
            let failures = load.report().round_failures;
            let (recovered, dcids, probe) = black_hole(
                &mut load,
                &mut rig,
                &peer,
                &mut samples,
                Duration::from_secs(8),
                Duration::from_secs(15),
            )
            .await;
            let survived = load.report().round_failures == failures;
            println!("N4M3 hole=8s recovered={recovered:?} dcids={dcids} long_survived={survived}");
            holes.push((recovered, dcids, survived, probe));
        }
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        let outage = Duration::from_secs(30);
        let (recovered, dcids, _) = black_hole(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            outage,
            Duration::from_secs(36),
        )
        .await;
        let waiting = native_waiting::node(&rig.saved).waiting();
        println!(
            "N4M3 hole=30s recovered={recovered:?} dcids={dcids} bound={} waiting={waiting:?}",
            a2_bound(outage)
        );
        let before = load.report();
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        let after = load.report();
        assert_eq!(session_id(&rig), control, "same SessionControl");
        out = Some((
            slow, slow_dcids, holes, recovered, dcids, waiting, before, after,
        ));
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let (slow, slow_dcids, holes, recovered, dcids, waiting, before, after) = out.unwrap();
    println!("N4M3 final {after:?} last={:?}", samples.last());
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
    assert!(no_q(&samples), "session Q");
    // Slow segment (MA3e target): the handshake and every long round complete.
    assert!(
        slow.rounds > 0 && slow.round_failures == 0 && slow_dcids == 1,
        "slow segment: {slow:?} dcids={slow_dcids}"
    );
    // D156: recovery after lift ≤ H + 1s (the first post-lift packet can
    // only be the next backed-off PTO probe), and D157 proves it is one.
    let hole = Duration::from_secs(8);
    for (recovered, dcids, survived, probe) in &holes {
        assert_eq!(*dcids, 0, "8s black hole minted a connection");
        assert!(*survived, "long flows failed across an 8s black hole");
        assert!(
            recovered.is_some_and(|r| r <= hole + Duration::from_secs(1)),
            "8s hole recovery {recovered:?}"
        );
        assert!(
            probe
                .as_ref()
                .is_some_and(|p| (1..=2).contains(&p.burst) && p.backoff >= 1.5),
            "8s hole first post-lift datagrams are not a loss probe: {probe:?}"
        );
    }
    let outage = Duration::from_secs(30);
    assert!(
        dcids <= a2_bound(outage),
        "30s hole dcids={dcids} bound={}",
        a2_bound(outage)
    );
    assert!(
        recovered.is_some_and(|r| r <= Duration::from_secs(36)),
        "30s hole recovery {recovered:?}"
    );
    let waiting = waiting.expect("Node alive");
    assert!(
        !waiting.first_failure && !waiting.quarantined,
        "{waiting:?}"
    );
    // Long flows crossing the 30s hole fail typed; their reopen then succeeds.
    assert!(
        after.rounds > before.rounds && after.round_failures == before.round_failures,
        "reopened flows"
    );
    assert!(
        after.short_ok > before.short_ok,
        "short flows after recovery"
    );
    assert_tasks_converge(rig, peer, tasks_before).await;
}

/// M4(a)(b): 30s black hole under L; after loss detection a 64-open storm and
/// an App RST of 8 long flows land in the recovery/Waiting phase. Every open
/// settles within 6s (F6 re-measured under L), attempts stay within the A2
/// bound, open/waiting inventory returns to 0 and L recovers.
#[tokio::test(flavor = "current_thread")]
async fn n4_m4_storm_and_app_rst_during_waiting_settle() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let (stopped, mut samples, mut out) = (Cell::new(None), vec![], None);
    let outage = Duration::from_secs(30);
    let report = native_waiting::drive(&mut service, Duration::from_secs(120), async {
        let mut load = Load::new(&mut rig.apps, standard());
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        let first = peer.latest().expect("live connection");
        let control = session_id(&rig);
        let from = Instant::now();
        let lift = from + outage;
        rig.relay.tap.lock().unwrap().outage(from, lift);
        let mut storm = Storm::new(Duration::from_millis(500), Duration::from_secs(29));
        let (mut detected, mut reset) = (None, None);
        let end = lift + Duration::from_secs(36);
        loop {
            let now = Instant::now();
            if detected.is_none() && first.close_reason().is_some() {
                detected = Some(now);
            }
            if let Some(d) =
                detected.filter(|d| reset.is_none() && now >= *d + Duration::from_secs(8))
            {
                load.reset_longs(&mut rig.apps, 8);
                reset = Some(now - d);
            }
            storm.step(&mut rig.apps, from, detected);
            load.turn(&mut rig.apps).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            let recovered =
                load.first_round_after(lift).is_some() && load.first_udp_after(lift).is_some();
            if now >= end || (now >= lift && recovered && storm.worst().is_some()) {
                break;
            }
            if samples.len() < 400 && now.duration_since(from).as_millis() % 1000 < 6 {
                samples.push(census(&rig.saved, &rig.budget, &peer));
            }
        }
        storm.report(&rig.apps);
        let recovered = load
            .first_round_after(lift)
            .zip(load.first_udp_after(lift))
            .map(|(t, u)| t.max(u));
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            Instant::now() + Duration::from_secs(3),
        )
        .await;
        let settled = census(&rig.saved, &rig.budget, &peer);
        let dcids = rig.relay.tap.lock().unwrap().new_dcids(from, lift);
        assert_eq!(session_id(&rig), control, "same SessionControl");
        out = Some((
            storm.worst(),
            recovered,
            dcids,
            detected.map(|d| d - from),
            reset,
            settled,
            load.report(),
        ));
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let (worst, recovered, dcids, detected, reset, settled, load) = out.unwrap();
    println!(
        "N4M4 worst_open={worst:?} margin={:?} recovered={recovered:?} dcids={dcids} bound={} detected={detected:?} rst_after_detect={reset:?} settled={settled:?} {load:?}",
        worst.map(|w| Duration::from_secs(6).saturating_sub(w)),
        a2_bound(outage)
    );
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
    assert!(no_q(&samples) && !settled.q, "session Q");
    assert!(reset.is_some(), "App RST did not land");
    assert!(
        worst.is_some_and(|w| w <= Duration::from_secs(6)),
        "slowest open {worst:?}"
    );
    assert!(
        dcids <= a2_bound(outage),
        "dcids={dcids} bound={}",
        a2_bound(outage)
    );
    assert!(
        recovered.is_some_and(|r| r <= Duration::from_secs(36)),
        "recovery {recovered:?}"
    );
    assert_eq!(
        (settled.opening, settled.waiting),
        (0, 0),
        "open inventory after recovery"
    );
    assert_tasks_converge(rig, peer, tasks_before).await;
}

/// M4(c): stop requested while the Node is parked in Waiting (30s black hole
/// under L): Phase Complete within 5s, budget 0, no Q, RunningResourcesSettled.
#[tokio::test(flavor = "current_thread")]
async fn n4_m4_stop_during_waiting_under_load_completes() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let tasks_before = alive_tasks();
    let (stopped, mut samples, mut parked) = (Cell::new(None), vec![], None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(90), async {
        let mut load = Load::new(&mut rig.apps, standard());
        run_until(
            &mut load,
            &mut rig,
            &peer,
            &mut samples,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        let from = Instant::now();
        rig.relay
            .tap
            .lock()
            .unwrap()
            .outage(from, from + Duration::from_secs(30));
        let observer = native_waiting::node(&rig.saved);
        while Instant::now() < from + Duration::from_secs(30) {
            load.run_for(&mut rig.apps, Duration::from_millis(50)).await;
            let sample = observer.waiting().expect("Node alive");
            if sample.parked == 1 {
                parked = Some((from.elapsed(), sample));
                break;
            }
        }
        samples.push(census(&rig.saved, &rig.budget, &peer));
        println!("N4M4c stop at {parked:?}");
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let elapsed = stopped.get().unwrap().elapsed();
    println!("N4M4c stop_elapsed={elapsed:?} report={report:?}");
    let (_, sample) = parked.expect("parked in Waiting before stop");
    assert!(!sample.quarantined && !sample.first_failure, "{sample:?}");
    assert!(no_q(&samples), "session Q");
    native_waiting::assert_clean_stop(report, elapsed, &rig.budget, &rig.handle, true);
    assert_tasks_converge(rig, peer, tasks_before).await;
}
