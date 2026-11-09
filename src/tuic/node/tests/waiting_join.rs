//! N2 D141: once the Node holds a parked clean Waiting row (the path is known
//! black-holed), a NEW closed-slot demand on another slot joins Waiting at mint:
//! it is published Failed without a fresh 15s/2-handshake budget (opens fail
//! fast), shares the parked schedule, and later recovers on the same Node.
use super::super::*;
use super::recovery::wait_closed;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};

#[tokio::test(flavor = "current_thread")]
async fn new_closed_demand_joins_parked_waiting_without_fresh_budget() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 3;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let observer = host.native_weak_observer_for_test();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        // Slot 1: two refused handshakes ⇒ clean expiry ⇒ parked Waiting.
        peers[1].close(17u32.into(), b"first black-holed slot");
        wait_closed(&data, 1).await;
        let first = data
            .request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), async {
            tokio::select! {
                _ = async {
                    for _ in 0..2 { server.accept().await.unwrap().refuse(); }
                    while observer.waiting().unwrap().parked != 1 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert!(data.observe_aux_recovery(first).is_err());
        let attempts = host.resources.state().attempts_started;
        // Slot 2 closes while slot 1 is parked: its new demand joins Waiting.
        peers[2].close(17u32.into(), b"second slot on the dead path");
        wait_closed(&data, 2).await;
        let second = data
            .request_aux_recovery(2, data.tcp_pool_slots[2].state.lock().await)
            .unwrap();
        assert!(
            data.observe_aux_recovery(second).is_err(),
            "joined Waiting: observers fail fast"
        );
        let sample = observer.waiting().unwrap();
        assert_eq!(
            (sample.retries_held, sample.parked, sample.waits),
            (2, 2, 2),
            "{sample:?}"
        );
        assert!(!sample.first_failure && !sample.quarantined, "{sample:?}");
        assert!(
            tokio::time::timeout(Duration::from_millis(300), host.run())
                .await
                .is_err()
        );
        assert_eq!(
            host.resources.state().attempts_started,
            attempts,
            "no fresh handshake budget"
        );
        // Shared schedule: both slots re-probe (one handshake each) and recover.
        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::select! {
                _ = async {
                    for _ in 0..2 {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peers.push(peer);
                    }
                    while observer.waiting().unwrap().retries_held != 0 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {},
                result = host.run() => panic!("Waiting ended the node: {result:?}"),
            }
        })
        .await
        .unwrap();
        let state = host.resources.state();
        assert_eq!(
            state.attempts_started,
            attempts + 2,
            "one handshake per slot re-probe"
        );
        assert!(!state.quarantined && state.first.is_none());
        drop(state);
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
