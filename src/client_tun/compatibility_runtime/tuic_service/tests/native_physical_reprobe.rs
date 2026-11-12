//! C-M3 tracer (D66 mutation M3): a path that ONLY the periodic 5ms physical
//! re-probe of `OpenDispatch::poll_flows` settles. One real hard Native
//! generation (B7 rig, Chunk engine). The peer sends a reply and FIN; the flow
//! reaches Rearming, its first (stage-change) probe sends the local FIN, and
//! then waits on local socket activity. The App closes its side only after an
//! idle window; that App FIN changes smoltcp state but publishes no flow event,
//! so the physical receipt/release must come from a later same-stage re-probe.
//! M3 (`now < next || true`: never re-probe within a stage) never releases.
//! Owns no production authority.
use super::*;
use fixture::capture;
use native_rebuild_fatal::{close, rig, Rig};
use native_rebuild_loaded_cut::open_original;
use smoltcp::socket::tcp;

/// Idle window with no external event: several 5ms ticks and one 50ms census.
const IDLE: Duration = Duration::from_millis(60);
/// Re-probe tick (5ms) plus debug/parallel-gate slack; M3 never settles.
const BOUND: Duration = Duration::from_secs(2);

async fn drive<F, T>(rig: &mut Rig<F>, limit: Duration, mut done: impl FnMut(&mut Rig<F>) -> Option<T>) -> Option<T>
where
    F: std::future::Future<Output = Completion>,
{
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        rig.apps.turn().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), rig.service.run()).await.is_err(),
            "Service ended while the flow was live"
        );
        if let Some(value) = done(rig) {
            return Some(value);
        }
    }
    None
}

#[tokio::test(flavor = "current_thread")]
async fn rearming_flow_settles_on_periodic_reprobe_after_app_fin_only() {
    let mut rig = rig(|_| {});
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (peer, (mut send, recv), owner) = open_original(&mut rig).await;
        peers.push(peer);
        send.write_all(b"tcp-reply").await.unwrap();
        send.finish().unwrap();
        let app = rig.apps.tcp;
        // Precondition: reply delivered and the Service's local FIN reached the
        // App (CloseWait), i.e. the Rearming remote_eof probe ran once.
        let fin = drive(&mut rig, Duration::from_secs(5), |rig| {
            (rig.apps.tcp_reply == b"tcp-reply"
                && rig.apps.sockets.get::<tcp::Socket>(app).state() == tcp::State::CloseWait)
                .then_some(())
        })
        .await;
        if fin.is_none() {
            eprintln!("C-M3 precondition stalled: app={:?} state={} close={:?} flow={:?}",
                String::from_utf8_lossy(&rig.apps.tcp_reply), rig.apps.sockets.get::<tcp::Socket>(app).state(),
                owner.flow_close_for_test(), owner.flow_observation());
        }
        fin.expect("reply and local FIN reach the App within 5s");
        // No external event: the flow waits on its own local socket.
        assert!(drive(&mut rig, IDLE, |_| None::<()>).await.is_none());
        let flow = owner.flow_observation().expect("flow observation");
        assert_eq!(flow.released, 0, "precondition: release waits on the App FIN: {flow:?}");
        assert!(!owner.ledger().quarantined());
        // The App FIN is the only change; no peer/relay/stop event follows.
        rig.apps.sockets.get_mut::<tcp::Socket>(app).close();
        let started = tokio::time::Instant::now();
        let released = drive(&mut rig, BOUND, |_| {
            owner.flow_observation().filter(|flow| flow.released == 1).map(|_| started.elapsed())
        })
        .await;
        eprintln!("C-M3 reprobe release after App FIN: {released:?}");
        released.expect("periodic physical re-probe released the Rearming flow");
        let flow = owner.flow_observation().unwrap();
        assert!(flow.audit && flow.f == 0, "{flow:?}");
        assert_eq!(flow.last_data_disposition.map(|(m, b, t, d, _)| (m, b, t, d)), Some((1, 9, 9, 0)));
        assert!(!owner.ledger().quarantined() && owner.ledger().first_failure.is_none());
        drop((send, recv));
    })
    .await;
    let cleanup = capture(async {
        rig.handle.request_stop();
        let report = fixture::window(Duration::from_secs(15), rig.service.run()).await.unwrap();
        assert!(report.device_settled() && !report.device_failed(), "{report:?}");
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert!(!rig.service.completion.as_ref().unwrap().control.ledger().quarantined());
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0);
    })
    .await;
    close(rig.server, peers).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}
