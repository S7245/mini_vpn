//! N4 D152 A2 tracer (T1): once a closed slot's demand is parked in Waiting,
//! the Node no longer holds the dead connection's storage, so a stream witness
//! retiring on it (TUIC open `Retiring` custody) retires and its lease drains.
use super::super::*;
use super::running::boot;
use super::startup::{config, finish, guarded, server};
use super::waiting_terminal::park_slot_one;

#[tokio::test(flavor = "current_thread")]
async fn parked_waiting_slot_releases_dead_connection_storage() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let witness = {
            let conn = data.tcp_pool_slots[1]
                .current_transport()
                .await
                .connection()
                .unwrap();
            let (send, recv, witness) = conn.try_open_bi_1rtt_with_retirement().unwrap();
            drop((send, recv, conn)); // Only the slot keeps the storage now.
            witness
        };
        drop(data);
        assert!(!witness.is_retired(), "precondition: live stream maps");
        let (data, _) = park_slot_one(&mut host, &server, &peers).await;
        let parked = tokio::time::Instant::now();
        let retired = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                _ = async {
                    while !witness.is_retired() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await;
        assert!(
            retired.is_ok(),
            "witness on the parked dead connection not retired {:?} after park",
            parked.elapsed()
        );
        let sample = host.native_weak_observer_for_test().waiting().unwrap();
        assert_eq!(
            (sample.parked, sample.quarantined),
            (1, false),
            "{sample:?}"
        );
        drop(data);
    })
    .await;
    finish(host, server, peers, result).await;
}
