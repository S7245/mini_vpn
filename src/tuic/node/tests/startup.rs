use super::super::*;
use crate::tuic::tcp_fixture::UdpPair;

pub(super) fn config(address: SocketAddr) -> TuicClientConfig {
    let mut cfg = TuicClientConfig::from_sources(
        Some(&address.to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    cfg.tcp_pool = 1;
    cfg.quic_stats_secs = Some(1);
    cfg
}

#[tokio::test]
async fn cold_prepare_and_stop_construct_no_endpoint() {
    let cfg = config("127.0.0.1:9".parse().unwrap());
    let mut host = NodeHost::prepare(cfg, Arc::new(Metrics::default()));
    assert!(!host.resources.state().endpoint_created);
    assert_eq!(
        host.observe_shutdown().await,
        NodeShutdown::StartupResourcesSettled
    );
    assert!(!host.resources.state().endpoint_created);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_connecting_observation_retains_original_attempt_until_real_drain() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut host = NodeHost::prepare(
        config(socket.local_addr().unwrap()),
        Arc::new(Metrics::default()),
    );
    let result = guarded(async {
        let observed = tokio::time::timeout(Duration::from_millis(30), host.start()).await;
        assert!(observed.is_err());
        assert_eq!(host.resources.state().attempts_started, 1);
        let observed = tokio::time::timeout(Duration::from_millis(30), host.start()).await;
        assert!(observed.is_err());
        assert_eq!(host.resources.state().attempts_started, 1);
        assert_eq!(
            host.observe_shutdown().await,
            NodeShutdown::StartupResourcesSettled
        );
        let state = host.resources.state();
        assert_eq!(state.attempts_drained, 1);
        assert!(state.endpoint_drained);
        assert!(state.connections.iter().all(Option::is_none));
    })
    .await;
    let cleanup = guarded(async {
        tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
            .await
            .unwrap();
    })
    .await;
    drop(socket);
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
}

// Control-only fault injection. This does not stand in for physical Quinn drain.
#[tokio::test]
async fn failed_witness_stays_charged_without_repolling_completed_future() {
    let owner = StartupResources::new(1);
    owner.state().connections[0] = Some(Attempt {
        stable_id: None,
        published: false,
        connection: None,
        retirement: Retirement::Waiting(Box::pin(async { false })),
    });
    assert!(owner.retire(0).await.is_err());
    assert!(owner.retire(0).await.is_err());
    let state = owner.state();
    assert!(state.quarantined);
    assert!(state.connections[0].is_some());
    assert_eq!(state.attempts_drained, 0);
}

pub(super) fn server(uni: u32) -> Endpoint {
    Endpoint::server(
        UdpPair::server_config(uni, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap()
}
pub(super) async fn auth(peer: &Connection, cfg: &TuicClientConfig) {
    let mut recv = peer.accept_uni().await.unwrap();
    let actual = recv.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
        .unwrap();
    assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
}
async fn finish_server(mut server: Endpoint, peers: Vec<Connection>) {
    let drain = server.take_driver_drain_handle().unwrap();
    for peer in &peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    drop(peers);
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap()
    );
}
pub(super) async fn guarded(
    work: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::any::Any + Send>> {
    let mut work = Box::pin(work);
    let result = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await;
    let dropped = catch_unwind(AssertUnwindSafe(|| drop(work)));
    result.and(dropped)
}
pub(super) async fn finish(
    mut host: NodeHost,
    server: Endpoint,
    peers: Vec<Connection>,
    scenario: Result<(), Box<dyn std::any::Any + Send>>,
) {
    let cleanup = guarded(async {
        let state = tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
            .await
            .unwrap();
        assert_ne!(state, NodeShutdown::Incomplete);
    })
    .await;
    let server_cleanup = guarded(finish_server(server, peers)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = server_cleanup {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn all_slots_authenticate_and_stats_join_before_exact_endpoint_drain() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.start(), async {
                for _ in 0..cfg.tcp_pool {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Ready);
        assert_eq!(host.resources.state().attempts_started, 2);
        assert_eq!(host.ready.as_ref().unwrap().tcp_pool_slots.len(), 2);
        assert_eq!(
            host.observe_shutdown().await,
            NodeShutdown::StartupResourcesSettled
        );
        let state = host.resources.state();
        assert_eq!(state.attempts_drained, 2);
        assert_eq!(state.tasks_started, 2);
        assert_eq!(state.tasks_joined, 2);
        assert!(state.endpoint_drained);
        assert_eq!(state.connections.iter().flatten().count(), 0);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn zero_rtt_without_ticket_awaits_original_connecting_once() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.zero_rtt = true;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.start(), async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peers.push(peer);
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Ready);
        assert_eq!(host.resources.state().attempts_started, 1);
        assert_eq!(
            host.observe_shutdown().await,
            NodeShutdown::StartupResourcesSettled
        );
        assert!(host.resources.state().endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn auth_observer_cancel_resumes_same_connection_and_stop_cleans_blocked_auth() {
    for resume in [false, true] {
        let server = server(0);
        let cfg = config(server.local_addr().unwrap());
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            let (observation, peer) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    tokio::time::timeout(Duration::from_millis(50), host.start()),
                    async { server.accept().await.unwrap().await.unwrap() }
                )
            })
            .await
            .unwrap();
            peers.push(peer);
            assert!(observation.is_err());
            assert!(host.ready.is_none());
            assert_eq!(host.resources.state().attempts_started, 1);
            let id = host.resources.state().connections[0]
                .as_ref()
                .unwrap()
                .connection
                .as_ref()
                .unwrap()
                .stable_id();
            if resume {
                peers[0].set_max_concurrent_uni_streams(8u32.into());
                let (ready, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(host.start(), auth(&peers[0], &cfg))
                })
                .await
                .unwrap();
                assert_eq!(ready, NodeStartup::Ready);
                assert_eq!(
                    host.resources.state().connections[0]
                        .as_ref()
                        .unwrap()
                        .connection
                        .as_ref()
                        .unwrap()
                        .stable_id(),
                    id
                );
            }
            assert_eq!(
                host.observe_shutdown().await,
                NodeShutdown::StartupResourcesSettled
            );
            assert_eq!(host.resources.state().attempts_started, 1);
            assert_eq!(host.resources.state().attempts_drained, 1);
            assert!(host.resources.state().endpoint_drained);
            assert_eq!(host.start().await, NodeStartup::Stopping);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn auxiliary_retries_follow_actual_drain_and_never_publish_partial_pool() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let resources = Arc::clone(&host.resources);
    let mut peers = Vec::new();
    let result = guarded(async {
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(4), async {
            tokio::join!(host.start(), async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peers.push(peer);
                for prior_failures in 0..3 {
                    let incoming = server.accept().await.unwrap();
                    assert_eq!(resources.state().attempts_drained, prior_failures);
                    assert_eq!(resources.state().connections.iter().flatten().count(), 2);
                    incoming.refuse();
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Failed);
        assert!(host.ready.is_none());
        assert_eq!(host.resources.state().attempts_started, 4);
        assert_eq!(host.resources.state().attempts_drained, 3);
        assert_eq!(
            host.resources.state().additional,
            2,
            "only three actual attempt failures"
        );
        let first = format!(
            "{:?}",
            host.resources.state().first.as_ref().unwrap().facts()
        );
        assert_eq!(host.observe_shutdown().await, NodeShutdown::FailedSettled);
        assert_eq!(
            format!(
                "{:?}",
                host.resources.state().first.as_ref().unwrap().facts()
            ),
            first
        );
        assert_eq!(host.resources.state().attempts_drained, 4);
        assert!(host.resources.state().endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test]
async fn invalid_configuration_keeps_first_failure_without_constructing_endpoint() {
    let mut cfg = config("127.0.0.1:9".parse().unwrap());
    cfg.quic_stats_secs = Some(0);
    let mut host = NodeHost::prepare(cfg, Arc::new(Metrics::default()));
    assert_eq!(host.start().await, NodeStartup::Failed);
    assert_eq!(host.observe_shutdown().await, NodeShutdown::FailedSettled);
    assert!(!host.resources.state().endpoint_created);
    assert!(host.resources.state().first.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_observer_cancellation_retains_real_endpoint_driver_wait() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.start(), async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peers.push(peer);
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Ready);
        let held_endpoint = host.resources.state().endpoint.as_ref().unwrap().clone();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), host.observe_shutdown())
                .await
                .is_err()
        );
        let original = host.deadline;
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert!(!host.resources.state().endpoint_drained);
        assert!(host.cleanup.is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), host.observe_shutdown())
                .await
                .is_err()
        );
        assert_eq!(host.deadline, original);
        drop(held_endpoint);
        assert_eq!(
            host.drive_shutdown().await,
            NodeShutdown::StartupResourcesSettled
        );
        assert!(host.resources.state().endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

// Isolated task/control clock only; no live QUIC or socket time is advanced.
#[tokio::test(start_paused = true)]
async fn original_shutdown_deadline_keeps_saved_join_after_incomplete() {
    let cfg = config("127.0.0.1:9".parse().unwrap());
    let mut host = NodeHost::prepare(cfg, Arc::new(Metrics::default()));
    let release = Arc::new(Notify::new());
    let child = Arc::clone(&release);
    host.resources
        .spawn_stats(|| tokio::spawn(async move { child.notified().await }))
        .unwrap();
    let result = guarded(async {
        let start = tokio::time::Instant::now();
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Incomplete);
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(15));
        let deadline = host.deadline;
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Incomplete);
        assert_eq!(host.deadline, deadline);
        assert_eq!(host.resources.state().tasks_joined, 0);
    })
    .await;
    release.notify_one();
    let cleanup = guarded(async {
        assert_eq!(
            host.drive_shutdown().await,
            NodeShutdown::StartupResourcesSettled
        );
        assert_eq!(host.resources.state().tasks_joined, 1);
    })
    .await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
}

#[derive(Debug)]
struct PanickingSource;
impl std::fmt::Display for PanickingSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("injected error")
    }
}
impl std::error::Error for PanickingSource {}
impl Drop for PanickingSource {
    fn drop(&mut self) {
        panic!("injected source drop");
    }
}
struct FailingStartup;
impl Future for FailingStartup {
    type Output = Result<TuicUpstream, ClientError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(ClientError::Io(io::Error::new(
            io::ErrorKind::BrokenPipe,
            PanickingSource,
        ))))
    }
}
impl Drop for FailingStartup {
    fn drop(&mut self) {
        panic!("injected startup drop");
    }
}

#[tokio::test]
async fn original_error_precedes_source_and_startup_destructor_panics() {
    let mut host = NodeHost::prepare(
        config("127.0.0.1:9".parse().unwrap()),
        Arc::new(Metrics::default()),
    );
    host.startup = Some(Box::pin(FailingStartup));
    assert_eq!(host.start().await, NodeStartup::Failed);
    assert_eq!(host.observe_shutdown().await, NodeShutdown::Quarantined);
    let state = host.resources.state();
    assert_eq!(
        state.first.as_ref().unwrap().facts().io_kind,
        Some(io::ErrorKind::BrokenPipe)
    );
    assert_eq!(state.additional, 2);
    assert!(state.quarantined);
}

#[tokio::test]
async fn task_failure_and_poison_keep_first_error_and_exact_counts() {
    let mut host = NodeHost::prepare(
        config("127.0.0.1:9".parse().unwrap()),
        Arc::new(Metrics::default()),
    );
    host.resources
        .record(ClientError::InvalidTarget("original".into()));
    host.resources
        .spawn_stats(|| tokio::spawn(async { panic!("injected stats panic") }))
        .unwrap();
    assert_eq!(host.observe_shutdown().await, NodeShutdown::Quarantined);
    assert_eq!(
        host.resources.state().first.as_ref().unwrap().text_bytes(),
        b"original"
    );
    assert_eq!(host.resources.state().additional, 1);
    assert_eq!(host.resources.state().tasks_joined, 1);
    let owner = StartupResources::new(1);
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let _guard = owner.state();
        panic!("injected poison");
    }));
    assert!(owner.state().quarantined);
    assert!(owner.state().first.is_some());
    assert_eq!(owner.state().additional, 0);
    assert_eq!(owner.state().additional, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn drain_poll_panic_keeps_debt_but_cleans_other_real_connection() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.start(), async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peers.push(peer);
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Ready);
        {
            let mut state = host.resources.state();
            state.connections.swap(0, 1);
            // Fault-only unknown slot; the real candidate/witness at slot1 is unchanged.
            state.connections[0] = Some(Attempt {
                stable_id: None,
                published: false,
                connection: None,
                retirement: Retirement::Waiting(Box::pin(async { panic!("injected drain poll") })),
            });
        }
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Quarantined);
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert!(host.resources.state().endpoint_drained);
        assert!(matches!(
            host.resources.state().connections[0]
                .as_ref()
                .unwrap()
                .retirement,
            Retirement::Failed
        ));
        let errors = host.resources.state().additional;
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Quarantined);
        assert_eq!(host.resources.state().additional, errors);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn recovered_auxiliary_is_ready_with_original_failure_still_retained() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.start(), async {
                let primary = server.accept().await.unwrap().await.unwrap();
                auth(&primary, &cfg).await;
                peers.push(primary);
                server.accept().await.unwrap().refuse();
                let auxiliary = server.accept().await.unwrap().await.unwrap();
                auth(&auxiliary, &cfg).await;
                peers.push(auxiliary);
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Ready);
        assert_eq!(host.ready.as_ref().unwrap().tcp_pool_slots.len(), 2);
        assert_eq!(host.resources.state().attempts_started, 3);
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert_eq!(host.resources.state().additional, 0);
        assert_eq!(host.observe_shutdown().await, NodeShutdown::FailedSettled);
        assert_eq!(host.resources.state().attempts_drained, 3);
        assert!(host.resources.state().endpoint_drained);
        assert_eq!(host.resources.state().additional, 0);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test]
async fn stats_capacity_rejects_before_spawning_and_joins_all_reserved_roles() {
    let mut host = NodeHost::prepare(
        config("127.0.0.1:9".parse().unwrap()),
        Arc::new(Metrics::default()),
    );
    assert_eq!(host.resources.state().task_limit, 6);
    for _ in 0..6 {
        host.resources
            .spawn_stats(|| tokio::spawn(async {}))
            .unwrap();
    }
    let spawned = AtomicBool::new(false);
    assert!(
        host.resources
            .spawn_stats(|| {
                spawned.store(true, Ordering::Relaxed);
                tokio::spawn(async {})
            })
            .is_err()
    );
    assert!(!spawned.load(Ordering::Relaxed));
    assert_eq!(
        host.observe_shutdown().await,
        NodeShutdown::StartupResourcesSettled
    );
    assert_eq!(host.resources.state().tasks_started, 6);
    assert_eq!(host.resources.state().tasks_joined, 6);
}
