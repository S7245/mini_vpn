//! Real original Node gate, including retained Probe custody and actual task debt.
use super::super::*;
use super::running::boot;
use super::startup::{config, finish, guarded, server};
use crate::tuic::node::rebuild::Disposition;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl StartupResources {
    /// Called with the original Node guard held: never locks Node inventory or
    /// clones the connection. Quinn releases its own stats guard on return.
    pub(in crate::tuic::node) fn trace_first_close(connection: &Connection) {
        let stats = connection.stats();
        println!(
            "B31 firstPublishedClose id={} now={:?} alreadyClosed={} originalStats={stats:?}",
            connection.stable_id(),
            tokio::time::Instant::now(),
            connection.close_reason().is_some()
        );
    }

    /// Test-only cold cleanup stages. A taken slot is not a join certificate;
    /// the current future still owns it until the corresponding end marker.
    pub(in crate::tuic::node) fn trace_cleanup(&self, stage: &'static str, detail: Option<usize>) {
        let snapshot = {
            let state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut retirements = [0usize; 3];
            for attempt in state.connections.iter().flatten() {
                retirements[match attempt.retirement {
                    Retirement::Ready(_) => 0,
                    Retirement::Waiting(_) => 1,
                    Retirement::Failed => 2,
                }] += 1;
            }
            (
                (
                    state.monitor.is_some(),
                    state.monitor_started,
                    state.monitor_joined,
                ),
                (
                    state.udp.is_some(),
                    state.tasks.len(),
                    state.tasks_started,
                    state.tasks_joined,
                ),
                (state.attempts_started, state.attempts_drained, retirements),
                state
                    .terminal
                    .as_ref()
                    .map(|t| (t.invalid, t.generations.len(), t.admission.active_total())),
                (
                    state.endpoint.is_some(),
                    state.endpoint_drain.is_some(),
                    state.endpoint_created,
                    state.endpoint_drained,
                ),
                (
                    state.published,
                    state.stopping,
                    state.first.is_some(),
                    state.quarantined,
                ),
            )
        };
        println!(
            "B31 NodeCleanup resources={} now={:?} stage={stage} detail={detail:?} monitorSlotStartedJoined_udpSlotTasksLenStartedJoined_attemptsStartedDrainedReadyWaitingFailed_terminalInvalidGenerationsActive_endpointSlotDrainCreatedDrained_publishedStoppingFirstQ={snapshot:?}",
            self as *const _ as usize,
            tokio::time::Instant::now()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pool_two_completed_probe_custody_allows_only_actual_controlled_cleanup() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let source = data.start_udp_source().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary = data.open_tcp(&target).await.unwrap();
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(TUIC_TCP_POOL_STALE_RECONNECT_SECS)) => {},
            state = host.run() => panic!("original node ended {state:?}"),
        }
        let identity = data.tcp_pool_slots[1].state.lock().await.current.identity();
        let mut carrier = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                result = data.open_tcp(&target) => result.unwrap(),
                _ = async {
                    assert_eq!(peers[1].read_datagram().await.unwrap().as_ref(), encode_heartbeat());
                    peers[1].send_datagram(bytes::Bytes::from_static(b"alive")).unwrap();
                    std::future::pending::<()>().await
                } => unreachable!(),
                state = host.run() => panic!("original node ended {state:?}"),
            }
        }).await.unwrap();
        let (mut tx, mut rx) = peers[1].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        rx.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        carrier.write_all(b"probe-data").await.unwrap();
        let mut payload = [0; 10];
        rx.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"probe-data");
        tx.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        carrier.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        assert_eq!(data.tcp_pool_slots[1].state.lock().await.current.identity(), identity);
        assert_eq!(host.resources.state().recovery.completed_probe_custodies(), 1);
        assert!(!host.resources.state().recovery.has_rebuild_debt());
        assert!(host.begin_controlled_rebuild().is_err(), "actual leases forbid retirement");
        drop((carrier, tx, rx, primary, source, data));
        host.begin_controlled_rebuild().unwrap();
        assert_eq!(host.controlled_rebuild_disposition(), Disposition::Unsettled);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        assert_eq!(host.controlled_rebuild_disposition(), Disposition::Allowed);
        let root = host.resources.state();
        assert_eq!(root.attempts_started, 2);
        assert_eq!(root.attempts_started, root.attempts_drained);
        assert_eq!(root.tasks_started, root.tasks_joined);
        assert!(root.endpoint_drained);
        assert_eq!(root.recovery.completed_probe_custodies(), 1);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actually_settled_controlled_node_expires_at_unchanged_first_deadline() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        assert!(
            host.begin_controlled_rebuild().is_err(),
            "startup Ready is not DATA publication"
        );
        assert!(host.deadline.is_none());
        let data = host.data().unwrap();
        let source = data.start_udp_source().unwrap();
        drop((source, data));
        host.begin_controlled_rebuild().unwrap();
        let first = host.controlled_rebuild_deadline().unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert_eq!(host.controlled_rebuild_disposition(), Disposition::Allowed);
        tokio::time::pause();
        tokio::time::advance(first.saturating_duration_since(tokio::time::Instant::now())).await;
        assert_eq!(host.controlled_rebuild_disposition(), Disposition::Expired);
        assert_eq!(host.controlled_rebuild_deadline(), Some(first));
        assert!(host.resources.state().endpoint_drained);
        tokio::time::resume();
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cached_settlement_with_actual_unjoined_root_actor_is_sticky_quarantine() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        assert!(
            host.begin_controlled_rebuild().is_err(),
            "startup Ready is not DATA publication"
        );
        assert!(host.deadline.is_none());
        let data = host.data().unwrap();
        let source = data.start_udp_source().unwrap();
        drop((source, data));
        host.begin_controlled_rebuild().unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        // Inject a physical late publication through the original owner API.
        // The prior actual cleanup result is retained, not manufactured.
        host.resources
            .spawn_stats(|| {
                tokio::spawn(async {
                    let _ = wait.await;
                })
            })
            .unwrap();
        assert_eq!(
            host.controlled_rebuild_disposition(),
            Disposition::Quarantined
        );
        assert_eq!(host.result, Some(NodeShutdown::Quarantined));
        let original = std::ptr::from_ref(host.resources.state().first.as_ref().unwrap()) as usize;
        assert_eq!(host.drive_shutdown().await, NodeShutdown::Quarantined);
        let actor = host.resources.state().tasks.pop().unwrap();
        assert!(!actor.handle.is_finished());
        release.send(()).unwrap();
        actor.handle.await.unwrap();
        host.resources.state().tasks_joined += 1;
        assert_eq!(
            host.controlled_rebuild_disposition(),
            Disposition::Quarantined
        );
        assert_eq!(
            std::ptr::from_ref(host.resources.state().first.as_ref().unwrap()) as usize,
            original
        );
        assert!(host.resources.state().quarantined);
    })
    .await;
    finish(host, server, peers, result).await;
}
