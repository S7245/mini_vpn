//! N2 frozen acceptance A3/A4/A5 (D126): the false-positive guards of Waiting.
//! A slow-but-alive path (one-way 200ms + deterministic 2% loss), a 20s idle
//! connection, and one failing Target must never mint a new connection, rebuild
//! the session, or record a Node/session failure. Same real hard Native Service
//! ↔ real quinn peer rig as `native_waiting` (reused, `service.run()` never
//! cancelled before stop). Owns no production authority.
use super::*;
use fixture::{Apps, window};
use native_waiting::{
    accept, assert_clean_stop, drive, ledger, node, poll_until, rig, session, settle_flows,
    stop_polling,
};
use smoltcp::socket::tcp;
use std::cell::Cell;
use std::time::Instant;

/// One TCP+UDP round that tolerates datagram loss the way a live stream does:
/// the App resends its datagram every 300ms until a reply arrives, and the peer
/// answers every datagram. TCP rides a reliable QUIC stream, so it is unchanged.
async fn lossy_round(peer: &quinn::Connection, apps: &mut Apps) {
    let target = |port| TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, port)));
    let (tcp_target, udp_target) = (target(10000), target(10001));
    let tcp = async {
        let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
        let mut want = encode_connect(&tcp_target);
        want.extend_from_slice(b"service-tcp");
        let mut actual = vec![0; want.len()];
        rx.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, want);
        tx.write_all(b"tcp-reply").await.unwrap();
        tx.finish().unwrap();
        (tx, rx)
    };
    let udp = async {
        while let Ok(actual) = peer.read_datagram().await {
            if actual.len() < 4 || actual[1] != 0x02 {
                continue; // Not a TUIC Packet (e.g. a Heartbeat).
            }
            let assoc = u16::from_be_bytes([actual[2], actual[3]]);
            assert_eq!(
                &actual[..],
                encode_packet(assoc, &udp_target, b"service-udp")
            );
            let _ = peer.send_datagram(encode_packet(assoc, &udp_target, b"udp-reply").into());
        }
    };
    let app = async {
        let mut resend = Instant::now() + Duration::from_millis(300);
        while apps.tcp_reply != b"tcp-reply" || !apps.udp_reply.starts_with(b"udp-reply") {
            if apps.udp_reply.is_empty() && Instant::now() >= resend {
                apps.sent_udp = false;
                resend += Duration::from_millis(300);
            }
            apps.turn().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::select! {
        (streams, ()) = async { tokio::join!(tcp, app) } => drop(streams),
        () = udp => panic!("peer datagrams ended: {:?}", peer.close_reason()),
    }
}

fn control_id(saved: &Control) -> Option<usize> {
    session(saved).map(|c| Arc::as_ptr(&c) as usize)
}

/// Shared no-false-positive verdict: one connection, same session, no Node or
/// session failure/Q, Waiting never entered. `failed_open` admits only the
/// pre-existing typed per-open TcpOpen session record of one failed flow
/// (N1 per-open rule, D129(c)/D130); the Node itself must record nothing.
fn assert_no_reconnect(
    tag: &str,
    rig: &native_waiting::Rig,
    peer: &quinn::Connection,
    from: Instant,
    control: Option<usize>,
    failed_open: bool,
) {
    let dcids = rig
        .relay
        .tap
        .lock()
        .unwrap()
        .new_dcids(from, Instant::now());
    let (q, failed, live, opening, waiting) = ledger(&rig.saved);
    let sample = node(&rig.saved).waiting().expect("Node alive");
    let first = session(&rig.saved)
        .and_then(|c| fixture::first_failure_identity(&c))
        .map(|f| (f.1, String::from_utf8_lossy(&f.2).into_owned()));
    println!("N2{tag} session first_failure={first:?}");
    println!(
        "N2{tag} dcids={dcids} sockets={} close={:?} ledger=(q={q} first={failed} live={live} opening={opening} waiting={waiting}) sample={sample:?}",
        rig.relay.first_seen.lock().unwrap().len(),
        peer.close_reason()
    );
    let sockets = rig.relay.first_seen.lock().unwrap().len();
    assert_eq!(dcids, 0, "{tag}: a new connection was minted");
    assert_eq!(
        sockets, 1,
        "{tag}: client rebound/reconnected ({sockets} sockets)"
    );
    assert!(
        peer.close_reason().is_none(),
        "{tag}: original connection closed"
    );
    assert_eq!(
        control_id(&rig.saved),
        control,
        "{tag}: same SessionControl"
    );
    let typed_open = first
        .as_ref()
        .is_some_and(|f| f.0.contains("phase: TcpOpen, resource: TcpOpen"));
    assert!(
        !q && (!failed || (failed_open && typed_open)),
        "{tag}: session Q={q} first_failure={first:?}"
    );
    assert_eq!(
        (sample.waits, sample.reprobes),
        (0, 0),
        "{tag}: entered Waiting: {sample:?}"
    );
    assert!(
        !sample.first_failure && !sample.quarantined,
        "{tag}: {sample:?}"
    );
}

/// A3: one-way 200ms (RTT 400ms) + every 50th datagram dropped both ways
/// (2%), 30s of back-to-back TCP+UDP rounds on the first connection.
#[tokio::test(flavor = "current_thread")]
async fn n2_a3_slow_lossy_path_keeps_one_connection_for_30s() {
    let (mut service, mut rig) = rig(Duration::from_millis(200)).await;
    rig.relay.tap.lock().unwrap().loss_every(50);
    let (mut done, stopped) = (None, Cell::new(None));
    let report = drive(&mut service, Duration::from_secs(120), async {
        let peer = window(Duration::from_secs(15), accept(&rig.server, &rig.cfg))
            .await
            .expect("first connection");
        let (from, control) = (Instant::now(), control_id(&rig.saved));
        let mut rounds = 0u32;
        while from.elapsed() < Duration::from_secs(30) {
            if rounds > 0 {
                rig.apps.restart();
            }
            window(Duration::from_secs(15), lossy_round(&peer, &mut rig.apps))
                .await
                .expect("steady round");
            settle_flows(&mut rig.apps, &rig.saved).await;
            rounds += 1;
        }
        let injected = rig.relay.tap.lock().unwrap().injected;
        println!(
            "N2A3 rounds={rounds} injected={injected} elapsed={:?}",
            from.elapsed()
        );
        assert!(
            rounds >= 3 && injected > 0,
            "rounds={rounds} injected={injected}"
        );
        assert_no_reconnect("A3", &rig, &peer, from, control, false);
        done = Some(peer);
        stop_polling(&mut rig, &stopped).await
    })
    .await;
    let _peer = done.expect("scenario finished before stop");
    assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        false,
    );
    rig.server.close(0u32.into(), b"done");
}

/// A4: 20s with no App traffic (beyond QUIC idle 15s); keep-alive holds the
/// connection and the next TCP+UDP round runs on it.
#[tokio::test(flavor = "current_thread")]
async fn n2_a4_idle_20s_keeps_connection_for_next_round() {
    let (mut service, mut rig) = rig(Duration::from_millis(5)).await;
    let (mut done, stopped) = (None, Cell::new(None));
    let report = drive(&mut service, Duration::from_secs(60), async {
        let peer = window(Duration::from_secs(5), accept(&rig.server, &rig.cfg))
            .await
            .expect("first connection");
        window(Duration::from_secs(5), lossy_round(&peer, &mut rig.apps))
            .await
            .expect("round 0");
        settle_flows(&mut rig.apps, &rig.saved).await;
        let (from, control) = (Instant::now(), control_id(&rig.saved));
        poll_until(&mut rig.apps, from + Duration::from_secs(20)).await;
        rig.apps.restart();
        let next = Instant::now();
        window(Duration::from_secs(2), lossy_round(&peer, &mut rig.apps))
            .await
            .expect("round after idle");
        println!("N2A4 next_round={:?}", next.elapsed());
        settle_flows(&mut rig.apps, &rig.saved).await;
        assert_no_reconnect("A4", &rig, &peer, from, control, false);
        done = Some(peer);
        stop_polling(&mut rig, &stopped).await
    })
    .await;
    let _peer = done.expect("scenario finished before stop");
    assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        false,
    );
    rig.server.close(0u32.into(), b"done");
}

/// A5: two concurrent Connects; the peer resets one (its Target failed). The
/// other completes, the connection and the session continue, and a new round
/// runs on the same connection.
#[tokio::test(flavor = "current_thread")]
async fn n2_a5_one_failing_target_affects_only_its_flow() {
    let (mut service, mut rig) = rig(Duration::from_millis(5)).await;
    let (mut done, stopped) = (None, Cell::new(None));
    let report = drive(&mut service, Duration::from_secs(60), async {
        let peer = window(Duration::from_secs(5), accept(&rig.server, &rig.cfg))
            .await
            .expect("first connection");
        let (from, control) = (Instant::now(), control_id(&rig.saved));
        rig.apps.enable_second_tcp();
        let second = rig.apps.second_tcp.expect("second App socket");
        let target = TargetAddr::IpPort(std::net::SocketAddr::from((rig.apps.target, 10000)));
        let header = encode_connect(&target).len();
        let udp_target = TargetAddr::IpPort(std::net::SocketAddr::from((rig.apps.target, 10001)));
        let remote = async {
            let mut kept = Vec::new();
            for _ in 0..2 {
                let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
                let mut actual = vec![0; header + 11];
                rx.read_exact(&mut actual).await.unwrap();
                if actual.ends_with(b"service-two") {
                    tx.reset(7u32.into()).unwrap(); // This Target failed.
                    rx.stop(8u32.into()).unwrap();
                } else {
                    assert!(actual.ends_with(b"service-tcp"));
                    tx.write_all(b"tcp-reply").await.unwrap();
                    tx.finish().unwrap();
                    kept.push((tx, rx));
                }
            }
            let actual = peer.read_datagram().await.unwrap();
            let assoc = u16::from_be_bytes([actual[2], actual[3]]);
            peer.send_datagram(encode_packet(assoc, &udp_target, b"udp-reply").into())
                .unwrap();
            kept
        };
        let apps = &mut rig.apps;
        let app = async {
            loop {
                apps.turn().await;
                let state = apps.sockets.get::<tcp::Socket>(second).state();
                let failed = !matches!(
                    state,
                    tcp::State::SynSent | tcp::State::SynReceived | tcp::State::Established
                );
                if failed && apps.tcp_reply == b"tcp-reply" && apps.udp_reply == b"udp-reply" {
                    println!(
                        "N2A5 failed flow state={state:?} reply={:?}",
                        from.elapsed()
                    );
                    apps.sockets.get_mut::<tcp::Socket>(second).close(); // App hygiene.
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let (kept, ()) = window(Duration::from_secs(5), async { tokio::join!(remote, app) })
            .await
            .expect("A5 round");
        drop(kept);
        settle_flows(&mut rig.apps, &rig.saved).await;
        rig.apps.restart();
        window(Duration::from_secs(5), lossy_round(&peer, &mut rig.apps))
            .await
            .expect("round after Target failure");
        settle_flows(&mut rig.apps, &rig.saved).await;
        assert_no_reconnect("A5", &rig, &peer, from, control, true);
        done = Some(peer);
        stop_polling(&mut rig, &stopped).await
    })
    .await;
    let _peer = done.expect("scenario finished before stop");
    // The typed per-open record reports DeviceFailedSettled, as in A6.
    assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
    rig.server.close(0u32.into(), b"done");
}
