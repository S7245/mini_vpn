use super::super::*;
use super::failover_staged::{phase, reality, request};
use super::running::boot;
use super::startup::{config, finish, guarded, server};
use crate::failover::{FailoverUpstream, FallbackAdmission, HealthProbe, TcpLeg};
use crate::upstream::{TcpOpenPhase, TcpOpenUnavailable};

#[tokio::test(flavor = "current_thread")]
async fn actual_tls_rejection_blocks_backup_until_original_node_physically_settles() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let owner = host.resources.clone();
        let backup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality(backup.local_addr().unwrap().to_string(), [0; 32]),
        ));
        peers[0].close(17u32.into(), b"primary before actual TLS rejection");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !data.is_dead().await {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        host.config.sni = "recovery.invalid".into();
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            tokio::select! {
                result = host.run() => panic!("node ended {result:?}"),
                _ = async {
                    tokio::join!(phase(open.as_mut(), TcpOpenPhase::Settled), async {
                        assert!(server.accept().await.unwrap().await.is_err());
                    });
                    // Config refusal can be published before original retirement.
                    // Keep driving the same node until the real demand receipt.
                    loop {
                        let done = {
                            let state = owner.state();
                            state.attempts_drained == 1 && !state.recovery.fallback_pending()
                        };
                        if done { break; }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {}
            }
        })
        .await
        .unwrap();
        assert_eq!(
            data.snapshot().unwrap().fallback,
            FallbackAdmission::ConfigurationRejected
        );
        assert_eq!(factory.state().active_leg(), TcpLeg::Tuic);
        assert!(open.failure().is_some());
        assert_eq!(owner.state().attempts_started, 2);
        assert_eq!(owner.state().attempts_drained, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), backup.accept())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err(),
            "typed rejection has no same-config retry"
        );
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
        assert!(owner.state().endpoint_drained);
        assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_fallback_reports_final_reality_error_and_retained_failed_aggregate_drains() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_, pk) = crate::reality::auth::generate_ephemeral_keypair();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality(listener.local_addr().unwrap().to_string(), pk),
        ));
        peers[0].close(17u32.into(), b"primary before exhausted recovery");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !data.is_dead().await {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            tokio::select! {
                result = host.run() => panic!("node ended {result:?}"),
                _ = async {
                    tokio::join!(phase(open.as_mut(), TcpOpenPhase::Settled), async {
                        for _ in 0..2 { server.accept().await.unwrap().refuse(); }
                        let (mut io, _) = listener.accept().await.unwrap();
                        use tokio::io::AsyncReadExt;
                        let mut first = [0; 1];
                        io.read_exact(&mut first).await.unwrap();
                        drop(io); // Actual peer rejects the REALITY handshake.
                    });
                } => {}
            }
        })
        .await
        .unwrap();
        assert_eq!(factory.state().active_leg(), TcpLeg::Reality);
        assert!(
            matches!(open.failure(), Some(ClientError::Reality(_))),
            "final REALITY source, not prior Node marker: {:?}",
            open.failure()
        );
        let first = open.failure().unwrap().to_string();
        open.step(
            &mut Context::from_waker(std::task::Waker::noop()),
            Instant::now(),
        );
        assert_eq!(open.failure().unwrap().to_string(), first);
        assert!(!open.view().lease_held && !open.view().retirement_pending);
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
        assert!(host.resources.state().endpoint_drained);
        assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn both_real_cold_factories_reject_invalid_targets_without_io() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let backup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality(backup.local_addr().unwrap().to_string(), [0; 32]),
        ));
        for reality_leg in [false, true] {
            if reality_leg {
                assert!(factory.state().record_blackhole(0));
            }
            let mut req = request();
            req.target = TargetAddr::DomainPort {
                host: String::new(),
                port: 443,
            };
            assert!(matches!(
                factory.clone().staged_tcp_open(req),
                Err(TcpOpenUnavailable::InvalidTarget)
            ));
            assert_eq!(host.resources.state().attempts_started, 1);
            assert_eq!(data.tcp_pool_admission.active_total(), 0);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), backup.accept())
                    .await
                    .is_err()
            );
        }
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
    })
    .await;
    finish(host, server, peers, result).await;
}

// Cold unsupported-contract fixture only; the first leg and cleanup are real.
struct NoStaged(std::sync::atomic::AtomicU64);
#[async_trait::async_trait]
impl ProxyUpstream for NoStaged {
    async fn open_tcp(&self, _: &TargetAddr) -> Result<RelayStream, ClientError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(io_err("test fixture", "legacy open must not be called"))
    }
}
#[tokio::test(flavor = "current_thread")]
async fn cold_fallback_factory_rejection_releases_factory_with_prior_real_failure_retained() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let backup = Arc::new(NoStaged(std::sync::atomic::AtomicU64::new(0)));
        let factory = Arc::new(FailoverUpstream::new(data.clone(), backup.clone()));
        peers[0].close(17u32.into(), b"primary before cold factory rejection");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !data.is_dead().await { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            tokio::select! {
                result = host.run() => panic!("node ended {result:?}"),
                _ = async {
                    tokio::join!(phase(open.as_mut(), TcpOpenPhase::Settled), async {
                        for _ in 0..2 { server.accept().await.unwrap().refuse(); }
                    });
                } => {}
            }
        }).await.unwrap();
        assert_eq!(factory.state().active_leg(), TcpLeg::Reality);
        assert!(matches!(open.failure(), Some(ClientError::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported));
        assert_eq!(backup.0.load(Ordering::Relaxed), 0);
        assert!(!open.view().lease_held && !open.view().retirement_pending);
        drop(factory); drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown()).await.unwrap(), NodeShutdown::FailedSettled);
        assert!(host.resources.state().endpoint_drained);
        assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    }).await;
    finish(host, server, peers, result).await;
}
