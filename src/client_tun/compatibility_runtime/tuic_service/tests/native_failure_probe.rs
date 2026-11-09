//! N2 step 0 characterization probe (D126/O-N2e/O-N2f). Owns no production
//! authority. A real hard Native Service talks to a real quinn peer through the
//! C1/C3 `RttRelay`; after one healthy TCP+UDP round the relay drops every
//! datagram of every client socket for `outage`, then the path returns.
//! `service.run()` stays polled for the whole scenario including stop (no
//! cancellation), and a 1s sampler records phase/ledger/budget.
use super::*;
use fixture::{Apps, ServiceProfile, auth, capture, config, window};
use native_rebuild::{native_ordered_chunk, serve_generation};
use native_rebuild_high_rtt::{Dir, RttRelay, Tap};
use std::time::Instant;

fn snapshot(saved: &Arc<Control>, budget: &bytes::BackingBudget, handle: &Handle) -> String {
    let session = saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .and_then(std::sync::Weak::upgrade);
    let ledger = session.map(|c| {
        let flows = c
            .flow_observation()
            .map(|f| (f.live, f.running, f.opening_providers, f.waiting));
        let l = c.ledger();
        let (q, more) = (l.quarantined, l.additional_failures);
        drop(l);
        let first = fixture::first_failure_identity(&c).map(|f| f.1);
        format!("q={q} additional={more} flows={flows:?} first={first:?}")
    });
    format!(
        "phase={:?} used={} budget_q={} session={ledger:?}",
        handle.phase(),
        budget.used(),
        budget.quarantined()
    )
}

async fn outage_probe(
    outage: Duration,
    after_lift: Duration,
    app_close: Option<Duration>,
    drop_path: bool,
    settle_first: bool,
) {
    let profile = ServiceProfile::new(true);
    let server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let relay = RttRelay::start(
        server.local_addr().unwrap(),
        Duration::from_millis(5),
        Tap::default(),
    )
    .await;
    let policy = native_ordered_chunk();
    let cfg = config(relay.addr).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved
        .backing_budget
        .clone()
        .expect("hard Native shared budget");
    let mut peers: Vec<quinn::Connection> = Vec::new();
    let t0 = Instant::now();
    let stop_at = std::cell::Cell::new(None::<Instant>);
    let (from, until) = (std::cell::Cell::new(t0), std::cell::Cell::new(t0));
    let sampler = |tag: &'static str| {
        let (saved, budget, handle) = (&saved, &budget, &handle);
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                println!(
                    "N2S {tag} t={:?} {}",
                    t0.elapsed(),
                    snapshot(saved, budget, handle)
                );
            }
        }
    };
    let script = async {
        window(Duration::from_secs(5), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            serve_generation(&peer, &mut apps).await;
            peers.push(peer);
        })
        .await
        .expect("round 0 DATA");
        if settle_first {
            // Path up: the app closes round-0 TCP and the flow settles first.
            apps.sockets
                .get_mut::<smoltcp::socket::tcp::Socket>(apps.tcp)
                .close();
            let limit = Instant::now() + Duration::from_secs(5);
            while Instant::now() < limit {
                apps.turn().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                let live = saved
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .and_then(|c| c.flow_observation())
                    .map(|f| f.live);
                if live == Some(0) {
                    break;
                }
            }
            println!(
                "N2P settled-first t={:?} {}",
                t0.elapsed(),
                snapshot(&saved, &budget, &handle)
            );
        }
        from.set(Instant::now());
        until.set(from.get() + outage);
        println!("N2P outage-start t={:?}", t0.elapsed());
        if drop_path {
            relay.tap.lock().unwrap().outage(from.get(), until.get());
        }
        let old = peers[0].clone();
        let until_at = until.get();
        let work = async {
            let lift = until_at + Duration::from_millis(500);
            // A live app keeps its local stack polled (the outage only affects
            // the relay path) and closes its half-closed TCP at `from + close`.
            let mut closed = false;
            while let Some(close) = app_close.filter(|_| Instant::now() < lift) {
                if !closed && from.get().elapsed() >= close {
                    apps.sockets
                        .get_mut::<smoltcp::socket::tcp::Socket>(apps.tcp)
                        .close();
                    println!("N2P app-close t={:?}", t0.elapsed());
                    closed = true;
                }
                apps.turn().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep_until(lift.into()).await;
            apps.restart();
            if old.close_reason().is_none() {
                serve_generation(&old, &mut apps).await;
                return format!(
                    "old-connection t_tcp_udp_after_lift={:?}",
                    until_at.elapsed()
                );
            }
            let accepted = loop {
                let incoming = server.accept().await.unwrap();
                if let Ok(peer) = incoming.await {
                    break peer;
                }
            };
            let t_accept = until_at.elapsed();
            auth(&accepted, &cfg).await;
            serve_generation(&accepted, &mut apps).await;
            peers.push(accepted);
            format!(
                "new-connection t_accept={t_accept:?} t_tcp_udp_after_lift={:?}",
                until_at.elapsed()
            )
        };
        let outcome = tokio::select! {
            text = tokio::time::timeout(outage + after_lift, work) =>
                text.unwrap_or_else(|_| format!("no-recovery-within {:?}", outage + after_lift)),
            () = sampler("run") => unreachable!(),
        };
        println!(
            "N2P outcome={outcome} pre-stop {}",
            snapshot(&saved, &budget, &handle)
        );
        stop_at.set(Some(Instant::now()));
        handle.request_stop();
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(25)) => {},
            () = sampler("stop") => unreachable!(),
        }
    };
    let scenario = capture(async {
        tokio::select! {
            () = script => println!("N2P stop NOT complete 25s after request; {}", snapshot(&saved, &budget, &handle)),
            report = service.run() => println!(
                "N2P service-ended after_stop={:?} t={:?} report={report:?} {}",
                stop_at.get().map(|s| s.elapsed()), t0.elapsed(), snapshot(&saved, &budget, &handle)),
        }
    })
    .await;
    let old_reason = peers.first().map(|p| format!("{:?}", p.close_reason()));
    let (from, until) = (from.get(), until.get());
    let (attempts, out_initials, rebinds, log_overflow) = {
        let tap = relay.tap.lock().unwrap();
        let ids: std::collections::BTreeSet<u64> = tap
            .log
            .iter()
            .filter(|s| s.dir == Dir::Up && s.at >= from && s.at < until)
            .filter_map(|s| s.initial)
            .collect();
        let initials = tap
            .log
            .iter()
            .filter(|s| s.dir == Dir::Up && s.at >= from && s.at < until && s.initial.is_some())
            .count();
        (
            ids.len(),
            initials,
            relay.first_seen.lock().unwrap().len().saturating_sub(1),
            tap.log_overflow,
        )
    };
    let initial_times: Vec<_> = relay
        .tap
        .lock()
        .unwrap()
        .log
        .iter()
        .filter(|s| s.dir == Dir::Up && s.initial.is_some())
        .map(|s| s.at.saturating_duration_since(t0))
        .collect();
    let first_up_after = relay
        .tap
        .lock()
        .unwrap()
        .log
        .iter()
        .filter(|s| s.dir == Dir::Up && s.at >= until)
        .map(|s| s.at - until)
        .next();
    println!(
        "N2P outage={outage:?} old_close={old_reason:?} attempts_during_outage={attempts} \
        initials_during_outage={out_initials} rebinds={rebinds} first_up_after_lift={first_up_after:?} \
        log_overflow={log_overflow} peers={} initial_times={initial_times:?}",
        peers.len()
    );
    for peer in &peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    drop(relay);
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

#[tokio::test(flavor = "current_thread")]
async fn n2_probe_outage_8s() {
    outage_probe(
        Duration::from_secs(8),
        Duration::from_secs(20),
        None,
        true,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn n2_probe_outage_25s() {
    outage_probe(
        Duration::from_secs(25),
        Duration::from_secs(25),
        None,
        true,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn n2_probe_outage_60s() {
    outage_probe(
        Duration::from_secs(60),
        Duration::from_secs(25),
        None,
        true,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn n2_probe_outage_25s_live_app() {
    outage_probe(
        Duration::from_secs(25),
        Duration::from_secs(25),
        Some(Duration::ZERO),
        true,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn n2_probe_outage_25s_app_close_3s() {
    outage_probe(
        Duration::from_secs(25),
        Duration::from_secs(25),
        Some(Duration::from_secs(3)),
        true,
        false,
    )
    .await;
}

/// E1: no outage; the fixture app never closes its half-closed round-0 TCP.
#[tokio::test(flavor = "current_thread")]
async fn n2_probe_no_outage_idle_20s() {
    outage_probe(
        Duration::from_secs(20),
        Duration::from_secs(10),
        None,
        false,
        false,
    )
    .await;
}

/// E2: round-0 flow settles with the path up, then a 25s outage.
#[tokio::test(flavor = "current_thread")]
async fn n2_probe_outage_25s_settled() {
    outage_probe(
        Duration::from_secs(25),
        Duration::from_secs(25),
        Some(Duration::ZERO),
        true,
        true,
    )
    .await;
}
