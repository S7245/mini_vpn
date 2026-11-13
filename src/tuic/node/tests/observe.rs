//! N5-OBS (D241) tracers: recovery transition lines and open settlement classes.
use super::super::observe::{NodePhase, OpenClass, OpenClasses, Transitions};
use super::super::recovery::Census;
use crate::shared::ClientError;
use std::io;

fn census(pending: usize, waiting: usize, waits: u64, reprobes: u64) -> Census {
    Census {
        pending,
        waiting,
        active: (pending != 0).then_some((1, 4, 9)),
        waits,
        reprobes,
        waiting_failures: waits,
    }
}

#[test]
fn n5_obs_recovery_transitions_emit_one_line_each() {
    let mut t = Transitions::default();
    let start = t.observe(census(0, 0, 0, 0)).expect("first line");
    assert!(
        start.starts_with("🧭 Native Node: Start→Running"),
        "{start}"
    );
    assert_eq!(
        t.observe(census(0, 0, 0, 0)),
        None,
        "unchanged turn prints nothing"
    );
    let rec = t.observe(census(1, 0, 0, 0)).unwrap();
    assert!(
        rec.contains("Running→Recovering active=slot1/gen4/seq9"),
        "{rec}"
    );
    let wait = t.observe(census(0, 1, 1, 0)).unwrap();
    assert!(
        wait.contains("Recovering→Waiting") && wait.contains("events: waiting-enter"),
        "{wait}"
    );
    let probe = t.observe(census(1, 1, 1, 1)).unwrap();
    assert!(
        probe.contains("Waiting→Waiting") && probe.contains("reprobe"),
        "{probe}"
    );
    let back = t.observe(census(0, 0, 1, 1)).unwrap();
    assert!(
        back.contains("Waiting→Running") && back.contains("waiting-exit"),
        "{back}"
    );
    assert!(
        back.contains("waits=1 reprobes=1 waiting_failures=1"),
        "{back}"
    );
}

#[test]
fn n5_obs_open_settlement_classes_count_once_per_open() {
    let io = |kind| ClientError::Io(io::Error::new(kind, "x"));
    assert_eq!(
        OpenClass::of(true, Some(&io(io::ErrorKind::TimedOut))),
        OpenClass::Success
    );
    assert_eq!(
        OpenClass::of(false, Some(&io(io::ErrorKind::TimedOut))),
        OpenClass::Timeout
    );
    assert_eq!(
        OpenClass::of(false, Some(&io(io::ErrorKind::Interrupted))),
        OpenClass::Cancelled
    );
    assert_eq!(
        OpenClass::of(false, Some(&io(io::ErrorKind::ConnectionRefused))),
        OpenClass::Typed
    );
    assert_eq!(OpenClass::of(false, None), OpenClass::Cancelled);
    let classes = OpenClasses::default();
    for class in [
        OpenClass::Success,
        OpenClass::Success,
        OpenClass::Typed,
        OpenClass::Timeout,
    ] {
        classes.settle(class);
    }
    let counts = classes.counts();
    assert_eq!(
        (
            counts.success,
            counts.typed,
            counts.timeout,
            counts.cancelled
        ),
        (2, 1, 1, 0)
    );
}

/// Real Node: facts and the transition diff over the real recovery ledger as
/// one slot goes Running → Waiting (two refused handshakes) → Running.
#[tokio::test(flavor = "current_thread")]
async fn n5_obs_real_node_facts_follow_waiting_and_recovery() {
    use super::super::*;
    use super::recovery::wait_closed;
    use super::running::boot;
    use super::startup::{auth, config, finish, guarded, server};
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 3;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let observer = host.native_observer();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let mut lines = Transitions::default();
        let up = observer.facts().unwrap();
        assert_eq!(up.phase, NodePhase::Running, "{up:?}");
        assert_eq!(
            up.connections_started,
            host.resources.state().attempts_started
        );
        assert_eq!(
            (up.connections_live, up.waits, up.waiting_rows),
            (3, 0, 0),
            "{up:?}"
        );
        assert!(
            lines
                .observe(host.resources.state().recovery.census())
                .unwrap()
                .contains("Start→Running")
        );
        peers[1].close(17u32.into(), b"black-holed slot");
        wait_closed(&data, 1).await;
        let demand = data
            .request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), async {
            tokio::select! {
                _ = async {
                    for _ in 0..2 { server.accept().await.unwrap().refuse(); }
                    while observer.facts().unwrap().waiting_rows != 1 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert!(data.observe_aux_recovery(demand).is_err());
        let parked = observer.facts().unwrap();
        assert_eq!(parked.phase, NodePhase::Waiting, "{parked:?}");
        assert_eq!(
            (parked.waits, parked.waiting_rows, parked.connections_live),
            (1, 1, 2),
            "{parked:?}"
        );
        assert!(!parked.first_failure, "{parked:?}");
        let enter = lines
            .observe(host.resources.state().recovery.census())
            .unwrap();
        assert!(
            enter.contains("Running→Waiting") && enter.contains("waiting-enter"),
            "{enter}"
        );
        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::select! {
                _ = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                    while observer.facts().unwrap().phase != NodePhase::Running {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {},
                result = host.run() => panic!("Waiting ended the node: {result:?}"),
            }
        })
        .await
        .unwrap();
        let back = observer.facts().unwrap();
        assert_eq!(
            (back.waiting_rows, back.connections_live),
            (0, 3),
            "{back:?}"
        );
        assert!(
            back.connections_started > up.connections_started && back.reprobes >= 1,
            "{back:?}"
        );
        let exit = lines
            .observe(host.resources.state().recovery.census())
            .unwrap();
        assert!(
            exit.contains("Waiting→Running") && exit.contains("waiting-exit"),
            "{exit}"
        );
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
    })
    .await;
    finish(host, server, peers, result).await;
}
