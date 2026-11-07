//! D62 no-lost-wakeup tracer for the D61 park repair (R-b). One real hard
//! Native generation (B7 rig, Chunk engine, App drains). The Service future is
//! polled ONLY when its own waker fires (`Gated`), so the test's 1ms App loop
//! cannot mask a lost wakeup. After an idle settle window, each external path
//! must make its flow progress observable within one 5ms tick + slack:
//! relay data arrival (peer write → App bytes), new flow publish (second App
//! SYN → peer stream), close/reset (peer reset → App socket leaves
//! Established), stop request (Handle stop → peer stream end). Pre-repair the
//! loop self-wakes, so this is GREEN by spinning (D63 records it); mutations
//! of one path's wake/registration must turn it RED. Owns no production
//! authority; the idle poll counts are printed for the D63 park evidence.
use super::*;
use fixture::{capture, Apps};
use native_rebuild_fatal::{close, rig, Rig};
use native_rebuild_loaded_cut::open_original;
use smoltcp::socket::tcp;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

/// Loop timer (driver `timer`) plus debug/parallel-gate slack.
const BOUND: Duration = Duration::from_millis(5 + 95);
/// A lost wakeup without any backstop never completes; fail well before.
const LOST: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_millis(100);

#[derive(Default)]
pub(super) struct GateWake {
    fired: AtomicBool,
    pub(super) polls: AtomicUsize,
    outer: std::sync::Mutex<Option<Waker>>,
}

impl Wake for GateWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.fired.store(true, Ordering::Release);
        if let Some(outer) = self.outer.lock().unwrap().as_ref() {
            outer.wake_by_ref();
        }
    }
}

/// Polls `inner` only after `inner`'s own waker fired (first poll included).
pub(super) async fn gated<F: std::future::Future>(inner: F, gate: &Arc<GateWake>) -> F::Output {
    let mut inner = std::pin::pin!(inner);
    gate.fired.store(true, Ordering::Release);
    let waker = Waker::from(gate.clone());
    std::future::poll_fn(|cx| {
        *gate.outer.lock().unwrap() = Some(cx.waker().clone());
        if !gate.fired.swap(false, Ordering::AcqRel) {
            return Poll::Pending;
        }
        gate.polls.fetch_add(1, Ordering::Relaxed);
        inner.as_mut().poll(&mut Context::from_waker(&waker))
    })
    .await
}

/// Drive the gated Service and the App until `peer` completes and `done(App)`
/// holds; returns the elapsed time (panics after `LOST`).
async fn observe<F, P>(rig: &mut Rig<F>, gate: &Arc<GateWake>, peer: P, mut done: impl FnMut(&Apps) -> bool) -> Duration
where
    F: std::future::Future<Output = Completion>,
    P: std::future::Future<Output = ()>,
{
    let started = tokio::time::Instant::now();
    let Rig { service, apps, .. } = rig;
    let mut peer = std::pin::pin!(peer);
    let mut peer_done = false;
    let drive = async {
        loop {
            tokio::select! {
                () = &mut peer, if !peer_done => peer_done = true,
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
            apps.turn().await;
            if peer_done && done(apps) {
                break started.elapsed();
            }
        }
    };
    tokio::time::timeout(LOST, async {
        tokio::select! {
            report = gated(service.run(), gate) => panic!("Service ended: {report:?}"),
            elapsed = drive => elapsed,
        }
    })
    .await
    .expect("lost wakeup: path never observed")
}

/// Idle settle; returns gated Service polls per 5ms tick (park evidence).
async fn settle<F: std::future::Future<Output = Completion>>(rig: &mut Rig<F>, gate: &Arc<GateWake>) -> f64 {
    let before = gate.polls.load(Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + SETTLE;
    let _ = observe(rig, gate, tokio::time::sleep_until(deadline), |_| true).await;
    (gate.polls.load(Ordering::Relaxed) - before) as f64 / (SETTLE.as_millis() as f64 / 5.0)
}

#[tokio::test(flavor = "current_thread")]
async fn parked_native_loop_observes_every_external_path_within_one_tick() {
    let mut rig = rig(|_| {});
    let gate = Arc::new(GateWake::default());
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (peer, (mut send, mut recv), _old) = open_original(&mut rig).await;
        let mut lat = Vec::new();
        let mut idle = vec![settle(&mut rig, &gate).await];
        // (1) relay data arrival.
        let before = rig.apps.tcp_reply.len();
        send.write_all(b"relay-wake").await.unwrap();
        lat.push(("relay_data", observe(&mut rig, &gate, async {}, |a| a.tcp_reply.len() == before + 10).await));
        idle.push(settle(&mut rig, &gate).await);
        // (2) new flow publish: a second App SYN reaches the peer as a stream.
        rig.apps.enable_second_tcp();
        let (mut second_send, mut second_recv) = (None, None);
        lat.push(("publish", observe(&mut rig, &gate, async {
            let (send, recv) = peer.accept_bi().await.unwrap();
            (second_send, second_recv) = (Some(send), Some(recv));
        }, |_| true).await));
        idle.push(settle(&mut rig, &gate).await);
        // (3) close/reset of the second flow by the peer.
        let second = rig.apps.second_tcp.expect("second App socket");
        assert_eq!(rig.apps.sockets.get::<tcp::Socket>(second).state(), tcp::State::Established);
        second_send.take().unwrap().reset(7u32.into()).unwrap();
        second_recv.take().unwrap().stop(8u32.into()).unwrap();
        lat.push(("reset", observe(&mut rig, &gate, async {}, |a| {
            a.sockets.get::<tcp::Socket>(second).state() != tcp::State::Established
        }).await));
        idle.push(settle(&mut rig, &gate).await);
        // (4) stop request closes the first flow toward the peer.
        rig.handle.request_stop();
        lat.push(("stop", observe(&mut rig, &gate, async {
            let mut byte = [0];
            while let Ok(Some(_)) = recv.read(&mut byte).await {}
        }, |_| true).await));
        eprintln!("D62 park-wakeup latency={lat:?} idle_polls_per_tick={idle:?}");
        drop(send);
        peers.push(peer);
        lat
    })
    .await;
    let Rig { server, service, .. } = rig;
    drop(service);
    let drained = capture(close(server, peers)).await;
    let lat = scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    for (path, elapsed) in lat {
        assert!(elapsed <= BOUND, "{path} observed after {elapsed:?} > {BOUND:?}");
    }
}
