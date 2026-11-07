use super::super::*;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};
use crate::owned_upstream::{LegacyOwnedUpstream, OpenedOwnedTcp, OwnedUpstream};
use crate::upstream::{TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Open = Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>;

fn step(open: &mut Open) {
    open.step(
        &mut Context::from_waker(std::task::Waker::noop()),
        Instant::now(),
    );
}
fn start(data: &Arc<TuicUpstream>, duration: Duration) -> Open {
    Arc::new(LegacyOwnedUpstream::new(data.clone()))
        .staged_tcp_open(TcpOpenRequest {
            target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
            deadline: Instant::now() + duration,
        })
        .unwrap()
}
async fn until(open: &mut Open, phase: TcpOpenPhase) {
    while open.view().phase != phase {
        step(open);
        assert!(open.failure().is_none(), "{:?}", open.failure());
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
async fn settle(open: &mut Open) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while open.view().phase != TcpOpenPhase::Settled {
        assert!(Instant::now() < deadline, "{:?}", open.view());
        open.settle(
            &mut Context::from_waker(std::task::Waker::noop()),
            Instant::now(),
            deadline,
        );
        assert_ne!(open.view().phase, TcpOpenPhase::Quarantined);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
async fn stale(data: &TuicUpstream) {
    let mut slot = data.tcp_pool_slots[1].state.lock().await;
    slot.current.successor_service_certificate = Some(TcpPoolSuccessorServiceCertificate {
        identity: slot.current.identity(),
        path_generation: slot.current.transport.current_path_generation() + 1,
        proved_cwnd_floor: 1,
    });
}

#[tokio::test(flavor = "current_thread")]
async fn staged_healthy_replacement_transfers_real_data_and_keeps_old_carrier() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let mut primary = data.open_tcp(&target).await.unwrap();
        let mut old = data.open_tcp(&target).await.unwrap();
        let (mut old_send, mut old_recv) = peers[1].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        old_recv.read_exact(&mut header).await.unwrap();
        let (mut primary_send, mut primary_recv) = peers[0].accept_bi().await.unwrap();
        primary_recv.read_exact(&mut header).await.unwrap();
        // Use the real clock: without the receipt this idle successor needs a
        // probe. Holding old carriers does not turn the new generation active.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(TUIC_TCP_POOL_STALE_RECONNECT_SECS)) => {},
            result = host.run() => panic!("node ended: {result:?}"),
        }
        stale(&data).await;
        let mut downlink = data.start_udp();
        let mut open = start(&data, Duration::from_secs(5));
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    tokio::join!(until(&mut open, TcpOpenPhase::Reserved), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peers.push(peer);
                    });
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        let slot = data.tcp_pool_slots[1].state.lock().await;
        assert!(slot.current.open_state.last_success_secs().is_none());
        assert!(should_probe_tcp_pool_generation(1, data.clock.elapsed().as_secs(), 0, true, false));
        for _ in 0..8 {
            step(&mut open);
            assert_eq!(open.view().phase, TcpOpenPhase::Reserved);
            assert!(open.failure().is_none());
        }
        drop(slot);
        tokio::time::timeout(Duration::from_secs(2), until(&mut open, TcpOpenPhase::Ready)).await.unwrap();
        let OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(mut carrier)) = open.take_carrier(Instant::now()).unwrap() else { panic!("Generic family changed"); };
        let (mut send, mut recv) = peers[2].accept_bi().await.unwrap();
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = async {
                    let mut bytes = [0; 3];
                    for (stream, tx, rx) in [(&mut carrier, &mut send, &mut recv), (&mut old, &mut old_send, &mut old_recv), (&mut primary, &mut primary_send, &mut primary_recv)] {
                        stream.write_all(b"tcp").await.unwrap();
                        rx.read_exact(&mut bytes).await.unwrap();
                        assert_eq!(&bytes, b"tcp");
                        tx.write_all(b"ack").await.unwrap();
                        stream.read_exact(&mut bytes).await.unwrap();
                        assert_eq!(&bytes, b"ack");
                    }
                    data.send_udp(b"udp".to_vec()).await;
                    assert_eq!(peers[0].read_datagram().await.unwrap().as_ref(), b"udp");
                    peers[0].send_datagram(bytes::Bytes::from_static(b"back")).unwrap();
                    assert_eq!(downlink.recv().await.unwrap(), b"back");
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        assert!(peers[1].close_reason().is_none());
        assert_eq!(host.resources.state().attempts_started, 3);
        assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        drop(carrier); drop(send); drop(recv);
        settle(&mut open).await;
        assert!(open.failure().is_none());
        drop(open); drop(old); drop(old_send); drop(old_recv);
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = async {
                    while data.tcp_pool_draining_predecessor.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert!(peers[1].close_reason().is_some());
        drop(primary); drop(primary_send); drop(primary_recv); drop(downlink); drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.endpoint_drained);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn staged_unsubmitted_cancel_and_original_deadline_return_original_preparation() {
    for deadline in [false, true] {
        let server = server(8);
        let mut cfg = config(server.local_addr().unwrap());
        cfg.tcp_pool = 2;
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
            let primary = data.open_tcp(&target).await.unwrap();
            let old = data.open_tcp(&target).await.unwrap();
            stale(&data).await;
            let mut open = start(&data, Duration::from_millis(60));
            step(&mut open);
            step(&mut open);
            let preparing = data.tcp_pool_admission.preparing_slots[1].clone();
            assert!(preparing.load(Ordering::Acquire));
            let slot = data.tcp_pool_slots[1].state.lock().await;
            for _ in 0..5 {
                step(&mut open);
            }
            assert_eq!(open.view().phase, TcpOpenPhase::Acquiring);
            assert!(preparing.load(Ordering::Acquire));
            if deadline {
                tokio::time::sleep(Duration::from_millis(65)).await;
                step(&mut open);
                assert!(format!("{:?}", open.failure()).contains("TimedOut"));
            } else {
                open.cancel(
                    TcpOpenCancel::Cancelled,
                    Instant::now() + Duration::from_secs(2),
                );
            }
            settle(&mut open).await;
            assert!(!preparing.load(Ordering::Acquire));
            assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
            assert_eq!(host.resources.state().attempts_started, 2);
            drop(slot);
            drop(open);
            drop(primary);
            drop(old);
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::RunningResourcesSettled
            );
            let root = host.resources.state();
            assert_eq!(root.attempts_started, root.attempts_drained);
            assert_eq!(root.tasks_started, root.tasks_joined);
            assert!(root.endpoint_drained);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn staged_submitted_cancel_and_deadline_leave_real_candidate_with_root() {
    for deadline in [false, true] {
        let server = server(8);
        let mut cfg = config(server.local_addr().unwrap());
        cfg.tcp_pool = 2;
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
            let primary = data.open_tcp(&target).await.unwrap();
            let old = data.open_tcp(&target).await.unwrap();
            stale(&data).await;
            let mut open = start(&data, Duration::from_millis(60));
            step(&mut open);
            step(&mut open);
            step(&mut open);
            assert_eq!(open.view().phase, TcpOpenPhase::Acquiring);
            assert!(
                tokio::time::timeout(Duration::from_millis(80), host.run())
                    .await
                    .is_err()
            );
            let preparing = data.tcp_pool_admission.preparing_slots[1].clone();
            let permit = data.tcp_pool_draining_predecessor.clone();
            assert_eq!(host.resources.state().attempts_started, 3);
            assert_eq!(host.resources.state().attempts_drained, 0);
            if deadline {
                step(&mut open);
                assert!(format!("{:?}", open.failure()).contains("TimedOut"));
            } else {
                open.cancel(
                    TcpOpenCancel::Cancelled,
                    Instant::now() + Duration::from_secs(2),
                );
            }
            settle(&mut open).await;
            assert!(
                preparing.load(Ordering::Acquire),
                "root still owns original preparation"
            );
            assert!(
                permit.load(Ordering::Acquire),
                "caller cannot clear original permit"
            );
            assert_eq!(host.resources.state().attempts_drained, 0);
            drop(open);
            drop(primary);
            drop(old);
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::RunningResourcesSettled
            );
            assert!(!preparing.load(Ordering::Acquire));
            assert!(!permit.load(Ordering::Acquire));
            let root = host.resources.state();
            assert_eq!(root.attempts_started, root.attempts_drained);
            assert_eq!(root.tasks_started, root.tasks_joined);
            assert!(root.endpoint_drained);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn staged_settled_service_failure_falls_back_once_and_new_open_can_replace() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary = data.open_tcp(&target).await.unwrap();
        let old = data.open_tcp(&target).await.unwrap();
        let (primary_send, mut primary_recv) = peers[0].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        primary_recv.read_exact(&mut header).await.unwrap();
        stale(&data).await;
        let mut fallback = start(&data, Duration::from_secs(5));
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    tokio::join!(until(&mut fallback, TcpOpenPhase::Ready), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer.close(17u32.into(), b"reject actual forward service");
                        peers.push(peer);
                    });
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(host.resources.state().attempts_started, 3);
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert!(host.resources.state().first.is_some());
        assert!(!data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
        assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        let fallback_carrier = fallback.take_carrier(Instant::now()).unwrap();
        let (fallback_send, mut fallback_recv) = peers[0].accept_bi().await.unwrap();
        fallback_recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        drop(fallback_carrier);
        drop(fallback_send);
        drop(fallback_recv);
        settle(&mut fallback).await;
        let mut fresh = start(&data, Duration::from_secs(5));
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    tokio::join!(until(&mut fresh, TcpOpenPhase::Ready), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peers.push(peer);
                    });
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(host.resources.state().attempts_started, 4);
        let fresh_carrier = fresh.take_carrier(Instant::now()).unwrap();
        let (fresh_send, mut fresh_recv) = peers[3].accept_bi().await.unwrap();
        fresh_recv.read_exact(&mut header).await.unwrap();
        drop(fresh_carrier);
        drop(fresh_send);
        drop(fresh_recv);
        settle(&mut fresh).await;
        drop(fresh);
        drop(fallback);
        drop(primary);
        drop(primary_send);
        drop(primary_recv);
        drop(old);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
        let root = host.resources.state();
        assert_eq!(root.attempts_started, root.attempts_drained);
        assert_eq!(root.tasks_started, root.tasks_joined);
        assert!(root.endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}
