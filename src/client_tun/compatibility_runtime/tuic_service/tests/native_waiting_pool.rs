//! N2 F-N2b pool=2 acceptance (D131/D138): A2-60 and A6-60 at the default
//! `tcp_pool = 2`. A >15s outage closes BOTH slots; only one closed demand is
//! active at a time, so the queued one expires without its own handshake. D127
//! requires both to be Waiting (same Node, same slots, same session), every
//! demanded slot to recover after the lift (≤36s), and no Node/session failure.
//! Without traffic the closed aux slot is never demanded (recovered lazily on
//! next use), so A2-pool2 checks the post-lift round on either slot; the storm
//! (A6-pool2) demands both slots inside the outage. Owns no production
//! authority; reuses the `native_waiting` rig/storm and adds only an any-slot
//! round (`serve_any`) because TCP may land on either slot.
use super::*;
use fixture::{Apps, window};
use native_waiting::{
    Rig, Storm, a2_bound, accept, answer_storm, assert_clean_stop, drive, ledger, node, rig_pool,
    session, settle_flows, stop_polling,
};
use std::time::Instant;

/// One TCP + one UDP round where each may be carried by any pool slot,
/// including a slot the Node reconnects only on this round's demand (an idle
/// closed aux slot is recovered lazily). Newly accepted peers are appended.
async fn serve_any(
    server: &quinn::Endpoint,
    cfg: &TuicClientConfig,
    peers: &mut Vec<quinn::Connection>,
    apps: &mut Apps,
) {
    let target = |port| TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, port)));
    let (tcp_target, udp_target) = (target(10000), target(10001));
    let (bi_tx, mut bi_rx) = tokio::sync::mpsc::channel(4);
    let (dg_tx, mut dg_rx) = tokio::sync::mpsc::channel(4);
    // Dropping the set aborts the other readers before the next round.
    let mut readers = tokio::task::JoinSet::new();
    let watch = |readers: &mut tokio::task::JoinSet<()>, peer: quinn::Connection| {
        let (bi, dg, p) = (bi_tx.clone(), dg_tx.clone(), peer.clone());
        readers.spawn(async move {
            if let Ok(streams) = p.accept_bi().await {
                let _ = bi.send(streams).await;
            }
        });
        readers.spawn(async move {
            while let Ok(actual) = peer.read_datagram().await {
                if actual.len() >= 4 && actual[1] == 0x02 {
                    let _ = dg.send((peer.clone(), actual)).await; // TUIC Packet only.
                }
            }
        });
    };
    for peer in peers.iter() {
        watch(&mut readers, peer.clone());
    }
    let remote = async {
        let (mut tcp, mut udp, mut streams) = (false, false, None);
        while !(tcp && udp) {
            tokio::select! {
                Some((mut tx, mut rx)) = bi_rx.recv(), if !tcp => {
                    let mut want = encode_connect(&tcp_target);
                    want.extend_from_slice(b"service-tcp");
                    let mut actual = vec![0; want.len()];
                    rx.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, want);
                    tx.write_all(b"tcp-reply").await.unwrap();
                    tx.finish().unwrap();
                    (tcp, streams) = (true, Some((tx, rx)));
                }
                Some((peer, actual)) = dg_rx.recv(), if !udp => {
                    let assoc = u16::from_be_bytes([actual[2], actual[3]]);
                    assert_eq!(&actual[..], encode_packet(assoc, &udp_target, b"service-udp"));
                    peer.send_datagram(encode_packet(assoc, &udp_target, b"udp-reply").into())
                        .unwrap();
                    udp = true;
                }
                peer = accept(server, cfg) => {
                    watch(&mut readers, peer.clone());
                    peers.push(peer);
                }
            }
        }
        streams
    };
    let app = async {
        while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
            apps.turn().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let (streams, ()) = tokio::join!(remote, app);
    drop(streams);
}

struct Outcome {
    /// Peers stay open until after stop (closing one would mint a demand).
    _peers: Vec<quinn::Connection>,
    /// Lift → both slots recovered (no retry row left, two new connections).
    recovered: Duration,
    dcids: usize,
    worst_open: Option<Duration>,
    sample: crate::tuic::WaitingSample,
}

async fn pool_outage_round(rig: &mut Rig, outage: Duration, mut storm: Option<Storm>) -> Outcome {
    let boot = window(Duration::from_secs(5), async {
        let mut peers = vec![
            accept(&rig.server, &rig.cfg).await,
            accept(&rig.server, &rig.cfg).await,
        ];
        serve_any(&rig.server, &rig.cfg, &mut peers, &mut rig.apps).await;
        peers
    })
    .await
    .expect("round 0 DATA on two slots");
    settle_flows(&mut rig.apps, &rig.saved).await;
    let control = session(&rig.saved).map(|c| Arc::as_ptr(&c) as usize);
    let observer = node(&rig.saved);
    let from = Instant::now();
    let until = from + outage;
    rig.relay.tap.lock().unwrap().outage(from, until);
    let lift = until + Duration::from_millis(500);
    let fresh = std::cell::RefCell::new(Vec::new());
    let mut detected = None;
    let mut recovered = None;
    let peer_side = async {
        // Dropping the set when the round ends aborts every storm answerer
        // before the post-lift round accepts its own streams.
        let mut answers = tokio::task::JoinSet::new();
        loop {
            let peer = accept(&rig.server, &rig.cfg).await;
            fresh.borrow_mut().push(peer.clone());
            answers.spawn(async move { answer_storm(&peer).await });
        }
    };
    let app_side = async {
        loop {
            let now = Instant::now();
            if detected.is_none() && boot.iter().all(|p| p.close_reason().is_some()) {
                detected = Some(now);
            }
            if let Some(storm) = storm.as_mut() {
                storm.step(&mut rig.apps, from, detected);
            }
            let sample = observer.waiting().expect("Node alive");
            // No retry row left and a new connection: every demanded slot is
            // back (an idle aux slot without demand reconnects on next use).
            if recovered.is_none()
                && now >= lift
                && sample.retries_held == 0
                && !fresh.borrow().is_empty()
            {
                recovered = Some(until.elapsed());
            }
            let storm_done = storm.as_ref().is_none_or(|s| s.worst().is_some());
            if recovered.is_some() && storm_done {
                break;
            }
            assert!(
                now < lift + Duration::from_secs(40),
                "slots not recovered after lift: {sample:?} new={}",
                fresh.borrow().len()
            );
            rig.apps.turn().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::select! {
        () = app_side => {},
        () = peer_side => unreachable!(),
    }
    let mut peers = fresh.into_inner();
    rig.apps.restart();
    window(
        Duration::from_secs(5),
        serve_any(&rig.server, &rig.cfg, &mut peers, &mut rig.apps),
    )
    .await
    .expect("post-lift TCP+UDP on the recovered slots");
    settle_flows(&mut rig.apps, &rig.saved).await;
    assert_eq!(
        session(&rig.saved).map(|c| Arc::as_ptr(&c) as usize),
        control,
        "same SessionControl"
    );
    let (q, failed, _, opening, waiting) = ledger(&rig.saved);
    let first = session(&rig.saved)
        .and_then(|c| fixture::first_failure_identity(&c))
        .map(|f| (f.1, String::from_utf8_lossy(&f.2).into_owned()));
    if let Some(storm) = &storm {
        storm.report(&rig.apps);
    }
    let typed_open = first
        .as_ref()
        .is_some_and(|f| f.0.contains("phase: TcpOpen, resource: TcpOpen"));
    assert!(
        !q && (!failed || (storm.is_some() && typed_open)),
        "session Q={q} first_failure={first:?}"
    );
    let sample = observer.waiting().expect("Node alive");
    let dcids = rig.relay.tap.lock().unwrap().new_dcids(from, until);
    println!(
        "N2P outage={outage:?} recovered_after_outage_end={recovered:?} dcids={dcids} opening={opening} waiting={waiting} sample={sample:?}"
    );
    if storm.is_some() {
        assert_eq!((opening, waiting), (0, 0), "open inventory after recovery");
    }
    peers.extend(boot);
    Outcome {
        _peers: peers,
        recovered: recovered.unwrap(),
        dcids,
        worst_open: storm.as_ref().and_then(Storm::worst),
        sample,
    }
}

async fn run_pool_outage(outage: Duration, storm: Option<Storm>) -> Outcome {
    let (mut service, mut rig) = rig_pool(Duration::from_millis(5), 2).await;
    let (mut outcome, stopped) = (None, std::cell::Cell::new(None));
    let storm_run = storm.is_some();
    let report = drive(&mut service, outage + Duration::from_secs(70), async {
        outcome = Some(pool_outage_round(&mut rig, outage, storm).await);
        stop_polling(&mut rig, &stopped).await
    })
    .await;
    let outcome = outcome.expect("scenario finished before stop");
    assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        storm_run,
    );
    rig.server.close(0u32.into(), b"done");
    outcome
}

/// Shared pool=2 verdict: both slots were Waiting (not recorded), both
/// recovered ≤36s after the outage, attempts bounded per slot.
fn assert_both_slots_waited(outcome: &Outcome, outage: Duration) {
    let sample = outcome.sample;
    assert!(
        !sample.first_failure && !sample.quarantined,
        "a queued slot was recorded terminal: {sample:?}"
    );
    assert!(sample.waits >= 1 && sample.reprobes >= 1, "{sample:?}");
    assert_eq!((sample.retries_held, sample.parked), (0, 0), "{sample:?}");
    // D141: only one closed demand spends the original 2-handshake budget; the
    // other slot joins Waiting at 0 attempts, then each slot re-probes at most
    // once per backoff step ⇒ 2 + 2·steps = 2·a2_bound − 2 (D144 F5).
    assert!(
        outcome.dcids <= 2 * a2_bound(outage) - 2,
        "dcids={} pool2 bound={}",
        outcome.dcids,
        2 * a2_bound(outage) - 2
    );
    assert!(
        outcome.recovered <= Duration::from_secs(36),
        "{:?}",
        outcome.recovered
    );
}

#[tokio::test(flavor = "current_thread")]
async fn n2_a2_pool2_outage_60s_both_slots_wait_and_recover() {
    let outage = Duration::from_secs(60);
    let outcome = run_pool_outage(outage, None).await;
    assert_both_slots_waited(&outcome, outage);
}

#[tokio::test(flavor = "current_thread")]
async fn n2_a6_pool2_storm_during_waiting_60s_both_slots_recover() {
    let outage = Duration::from_secs(60);
    let outcome = run_pool_outage(
        outage,
        Some(Storm::new(Duration::from_secs(12), Duration::from_secs(58))),
    )
    .await;
    assert_both_slots_waited(&outcome, outage);
    let worst = outcome.worst_open.expect("every open settled");
    assert!(
        worst <= Duration::from_secs(6),
        "slowest open settled after {worst:?}"
    );
}
