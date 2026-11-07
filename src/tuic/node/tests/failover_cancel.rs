use super::super::*;
use super::failover_staged::{phase, reality, reality_peer, request, settled};
use super::running::boot;
use super::startup::{config, finish, guarded, server};
use crate::failover::{FailoverUpstream, HealthProbe, TcpLeg};
use crate::upstream::{OpenedTcpRelay, TcpOpenCancel, TcpOpenPhase};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "current_thread")]
async fn cold_cancel_busy_original_deadline_and_node_stop_never_classify_failure() {
    for mode in 0..5 {
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
            let req = request();
            let original_deadline = req.deadline;
            let mut open = factory.clone().staged_tcp_open(req).unwrap();
            let first = Instant::now() + Duration::from_secs(5);
            if mode == 3 {
                let busy = data.tcp_pool_slots[0].state.lock().await;
                for _ in 0..4 {
                    open.step(
                        &mut Context::from_waker(std::task::Waker::noop()),
                        Instant::now(),
                    );
                }
                assert_eq!(open.view().phase, TcpOpenPhase::Acquiring);
                assert!(!open.view().lease_held && !open.view().retirement_pending);
                assert_eq!(data.tcp_pool_admission.active_total(), 0);
                assert!(!data.tcp_pool_admission.preparing_slots[0].load(Ordering::Acquire));
                assert!(open.failure().is_none());
                assert_eq!(data.snapshot(), None);
                // Value-clock deadline test; no wall-clock/SLO claim.
                open.step(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    original_deadline,
                );
                assert_eq!(open.view().phase, TcpOpenPhase::Settled);
                assert!(!open.view().lease_held && !open.view().retirement_pending);
                assert_eq!(data.tcp_pool_admission.active_total(), 0);
                assert!(!data.tcp_pool_admission.preparing_slots[0].load(Ordering::Acquire));
                assert!(matches!(open.failure(), Some(ClientError::Io(error))
                    if error.kind() == io::ErrorKind::TimedOut));
                drop(busy);
            } else if mode == 4 {
                host.request_stop();
            } else {
                let reason = [
                    TcpOpenCancel::Cancelled,
                    TcpOpenCancel::Deadline,
                    TcpOpenCancel::Shutdown,
                ][mode];
                open.cancel(reason, first);
                open.cancel(reason, first + Duration::from_secs(50));
            }
            phase(open.as_mut(), TcpOpenPhase::Settled).await;
            assert!(open.take_carrier(Instant::now()).is_none());
            assert!(!open.view().lease_held && !open.view().retirement_pending);
            assert_eq!(factory.state().active_leg(), TcpLeg::Tuic);
            assert!(
                !factory.state().record_tuic_failure(false, 0),
                "cancel/pressure do not increment policy failures"
            );
            assert!(!factory.state().record_tuic_failure(false, 0));
            assert_eq!(host.resources.state().attempts_started, 1);
            assert_eq!(data.tcp_pool_admission.active_total(), 0);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), backup.accept())
                    .await
                    .is_err()
            );
            drop(factory);
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::RunningResourcesSettled
            );
            assert!(host.resources.state().endpoint_drained);
            assert_eq!(open.view().phase, TcpOpenPhase::Settled);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_tuic_ready_cancel_keeps_failure_counter_and_settles_real_stream() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality("127.0.0.1:9".into(), [0; 32]),
        ));
        assert!(!factory.state().record_tuic_failure(false, 0));
        assert!(!factory.state().record_tuic_failure(false, 0));
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        tokio::select! {
            result = host.run() => panic!("node ended {result:?}"),
            _ = phase(open.as_mut(), TcpOpenPhase::Ready) => {}
        }
        let (mut tx, mut rx) = tokio::time::timeout(Duration::from_secs(2), peers[0].accept_bi())
            .await
            .unwrap()
            .unwrap();
        let first = Instant::now() + Duration::from_secs(5);
        open.cancel(TcpOpenCancel::Cancelled, first);
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(settled(open.as_mut()), async {
                assert!(rx.read_to_end(128).await.is_err());
                tx.reset(0u32.into()).unwrap_or(());
                drop(tx);
                drop(rx);
            });
        })
        .await
        .unwrap();
        assert!(open.take_carrier(Instant::now()).is_none());
        assert!(
            factory.state().record_tuic_failure(false, 0),
            "Ready/cancel never resets the two existing failures"
        );
        assert_eq!(data.tcp_pool_admission.active_total(), 0);
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_reality_transferred_cancel_first_five_seconds_never_extends_and_q_retains() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let (backup, peer) = reality_peer().await;
        let factory = Arc::new(FailoverUpstream::new(data.clone(), backup));
        assert!(factory.state().record_blackhole(0));
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        phase(open.as_mut(), TcpOpenPhase::Ready).await;
        let OpenedTcpRelay::Generic(mut io) = open.take_carrier(Instant::now()).unwrap() else {
            panic!("Generic");
        };
        io.write_all(b"ping").await.unwrap();
        io.shutdown().await.unwrap();
        let mut response = Vec::new();
        io.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        let now = Instant::now();
        let first = now + Duration::from_secs(5);
        open.cancel(TcpOpenCancel::Shutdown, first);
        open.cancel(TcpOpenCancel::Shutdown, first + Duration::from_secs(50));
        open.step(&mut Context::from_waker(std::task::Waker::noop()), now);
        assert_eq!(open.view().phase, TcpOpenPhase::Retiring);
        assert!(open.view().lease_held && open.view().retirement_pending);
        // Actual carrier remains live; synthetic control observation at the first
        // deadline proves no extension. This is not a five-second timing claim.
        open.step(&mut Context::from_waker(std::task::Waker::noop()), first);
        assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
        let original = open.failure().unwrap().to_string();
        drop(io);
        open.settle(
            &mut Context::from_waker(std::task::Waker::noop()),
            first,
            first + Duration::from_secs(50),
        );
        assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
        assert!(open.view().lease_held && open.view().retirement_pending);
        assert_eq!(open.failure().unwrap().to_string(), original);
        // Forced teardown of Q's actual TCP witness is NOT async-clean acceptance.
        drop(open);
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
