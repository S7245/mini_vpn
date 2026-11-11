//! N4 review F2 tracer: a NON-stop close tail under one-way (Node → peer)
//! loss on the real hard Native Service. 16 long echo flows + 2 UDP flows (no
//! DNS/short TCP, so the victim is the only flow that closes during the loss).
//! A victim flow to `DELAYED_RESET_PORT` opens 5 s before the loss; its peer
//! RESETs it ≈1 s after the loss starts (Down still delivers). The client's own
//! RESET/STOP cannot reach the peer, so the provider retirement is unfinished
//! when the victim's close tail begins (an App abort is a Rearming reset and is
//! not under the close-tail deadline, D178). The peer's retransmissions legally re-arm the client
//! idle timer, so retirement ends ≈ loss + 2×QUIC idle + the Node release,
//! later than the A1 provider deadline (tail + PROVIDER_LOSS_BOUND). Healthy
//! flows and the session must be unaffected: no session Q, and every long
//! flow completes rounds again after the loss lifts. RED under the old
//! idle + 5 s bound (D178), GREEN with the derived bound (D181).
//! Owns no production authority.
use super::*;
use native_n4_harness::{LoadSpec, Load, Peer};
use native_n4_peer::{DELAYED_RESET_AFTER, DELAYED_RESET_PORT};
use smoltcp::socket::tcp;
use native_n4_recovery::run_until;
use native_rebuild_high_rtt::Dir;
use std::cell::Cell;
use std::time::Instant;

/// One-way loss length: longer than both idle timers plus the Node release,
/// so the victim's retirement can only finish once the old connection is dead.
const HOLE: Duration = Duration::from_secs(45);
/// Victim opens this long before the loss; the peer reset lands at
/// loss + (DELAYED_RESET_AFTER − LEAD) = loss + 1 s (review: < loss + 5.5 s).
const LEAD: Duration = Duration::from_secs(5);
/// Recovery window after the loss lifts.
const AFTER: Duration = Duration::from_secs(15);

#[tokio::test(flavor = "current_thread")]
async fn n4_f2_one_way_loss_close_tail_keeps_session() {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let (stopped, mut samples, mut out) = (Cell::new(None), vec![], None);
    let first_q = Cell::new(None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(150), async {
        let spec = LoadSpec {
            long: 16,
            udp: 2,
            ..LoadSpec::default()
        };
        let mut load = Load::new(&mut rig.apps, spec);
        let start = Instant::now();
        run_until(&mut load, &mut rig, &peer, &mut samples, start + Duration::from_secs(5)).await;
        let target = smoltcp::wire::Ipv4Address::from(rig.apps.target);
        let victim = load.connect(&mut rig.apps, target, DELAYED_RESET_PORT, 1024);
        let mut sent = false;
        let loss = Instant::now() + LEAD;
        while Instant::now() < loss {
            let at = Instant::now() + Duration::from_millis(100);
            run_until(&mut load, &mut rig, &peer, &mut samples, at).await;
            let socket = rig.apps.sockets.get_mut::<tcp::Socket>(victim);
            if !sent && socket.can_send() {
                sent = socket.send_slice(b"n4f2").is_ok();
            }
        }
        assert!(sent, "victim never established");
        let lift = loss + HOLE;
        rig.relay.tap.lock().unwrap().one_way(Dir::Up, loss, lift);
        let mut closed = None;
        while closed.is_none() && Instant::now() < loss + DELAYED_RESET_AFTER {
            let at = Instant::now() + Duration::from_millis(50);
            run_until(&mut load, &mut rig, &peer, &mut samples, at).await;
            // The close tail starts at the peer RESET (the Service holds the
            // App socket during the tail).
            if peer.log.lock().unwrap().resets == 1 {
                closed = Some(Instant::now());
            }
        }
        let closed = closed.expect("victim not reset by its peer during the loss");
        while Instant::now() < lift + AFTER {
            let at = Instant::now() + Duration::from_secs(1);
            run_until(&mut load, &mut rig, &peer, &mut samples, at).await;
            if first_q.get().is_none() && samples.last().is_some_and(|c| c.q) {
                first_q.set(Some(Instant::now() - closed));
            }
            // Timeline: seconds since close, released flows, Node connections
            // (closed?), Tap drops so far, session (Q, live).
            let released = native_waiting::session(&rig.saved)
                .and_then(|c| c.flow_observation())
                .map(|f| (f.released, f.f, f.settled_providers));
            let conns: Vec<_> = native_waiting::node(&rig.saved)
                .sample()
                .map(|n| n.connections.iter().flatten().map(|c| c.closed).collect())
                .unwrap_or_default();
            let injected = rig.relay.tap.lock().unwrap().injected;
            let q = samples.last().map(|c| (c.q, c.live));
            // Printed live: a stuck stop ends in the drive window panic.
            eprintln!("N4F2 t={} (released,f,settled)={released:?} conns_closed={conns:?} injected={injected} (q,live)={q:?}", closed.elapsed().as_secs());
        }
        let recovered = load.first_round_after(lift);
        eprintln!("N4F2 stopping recovered={recovered:?}");
        out = Some((closed - loss, recovered, load.report()));
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    println!(
        "N4F2 out={:?} first_q_after_close={:?} last={:?} report={report:?}",
        out.as_ref().map(|o| (o.0, o.1)),
        first_q.get(),
        samples.last()
    );
    assert!(
        samples.iter().all(|c| !c.q),
        "session Q {:?} after the victim close (non-stop one-way loss)",
        first_q.get()
    );
    let (_, recovered, load) = out.expect("Service ended before the F2 script");
    assert!(
        recovered.is_some_and(|r| r <= AFTER),
        "long flows did not recover after the loss lifted: {recovered:?} {load:?}"
    );
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        true,
    );
}
