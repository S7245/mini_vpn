use super::super::*;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};
use crate::owned_upstream::{LegacyOwnedUpstream, OpenedOwnedTcp, OwnedUpstream};
use crate::upstream::{TcpOpenAttempt, TcpOpenPhase, TcpOpenRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Open = Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>;
fn start(data: &Arc<TuicUpstream>) -> Open {
    Arc::new(LegacyOwnedUpstream::new(data.clone()))
        .staged_tcp_open(TcpOpenRequest {
            target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
            deadline: Instant::now() + Duration::from_secs(5),
        })
        .unwrap()
}
async fn ready(open: &mut Open) {
    while open.view().phase != TcpOpenPhase::Ready {
        open.step(
            &mut Context::from_waker(std::task::Waker::noop()),
            Instant::now(),
        );
        if let Some(error) = open.failure() {
            let error = format!("{error:?}");
            settle(open).await;
            panic!("{error}");
        }
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

#[tokio::test(flavor = "current_thread")]
async fn staged_idle_failure_and_previous_failure_use_root_auth_and_ready_probe() {
    for idle_failure in [false, true] {
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
        let old_id = {
            let slot = data.tcp_pool_slots[1].state.lock().await;
            if !idle_failure { slot.current.open_state.note_open_failure(1); }
            slot.current.identity()
        };
        if idle_failure {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(TUIC_TCP_POOL_STALE_RECONNECT_SECS)) => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }
        let mut open = start(&data);
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = ready(&mut open) => {},
                _ = async {
                    if idle_failure {
                        assert_eq!(peers[1].read_datagram().await.unwrap().as_ref(), encode_heartbeat());
                        peers[1].close(19u32.into(), b"actual idle probe failure");
                    }
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peer.read_datagram().await.unwrap();
                    peer.send_datagram(bytes::Bytes::from_static(b"probe-reply")).unwrap();
                    peers.push(peer);
                    std::future::pending::<()>().await;
                } => unreachable!(),
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        let OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(mut carrier)) =
            open.take_carrier(Instant::now()).unwrap()
        else {
            panic!("family");
        };
        let (mut send, mut recv) = peers[2].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        carrier.write_all(b"tcp").await.unwrap();
        let mut bytes = [0; 3];
        recv.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"tcp");
        send.write_all(b"ack").await.unwrap();
        carrier.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"ack");
        let new_id = data.tcp_pool_slots[1].state.lock().await.current.identity();
        assert_ne!(new_id.stable_id, old_id.stable_id);
        assert_eq!(new_id.generation, old_id.generation + 1);
        assert_eq!(host.resources.state().attempts_started, 3);
        drop(carrier);
        drop(send);
        drop(recv);
        settle(&mut open).await;
        drop(open);
        drop(primary);
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
async fn legacy_and_staged_idle_probe_keep_original_transport_and_primary_udp() {
    for staged in [false, true] {
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
        let identity = data.tcp_pool_slots[1].state.lock().await.current.identity();
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(TUIC_TCP_POOL_STALE_RECONNECT_SECS)) => {},
            result = host.run() => panic!("node ended: {result:?}"),
        }
        let mut open = staged.then(|| start(&data));
        let mut carrier = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                carrier = async {
                    if let Some(open) = open.as_mut() {
                        ready(open).await;
                        let OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(carrier)) =
                            open.take_carrier(Instant::now()).unwrap() else { panic!("family"); };
                        carrier
                    } else { data.open_tcp(&target).await.unwrap() }
                } => carrier,
                _ = async {
                    let heartbeat = peers[1].read_datagram().await.unwrap();
                    assert_eq!(heartbeat.as_ref(), encode_heartbeat());
                    peers[1].send_datagram(bytes::Bytes::from_static(b"alive")).unwrap();
                    std::future::pending::<()>().await;
                } => unreachable!(),
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(
            data.tcp_pool_slots[1].state.lock().await.current.identity(),
            identity
        );
        assert_eq!(host.resources.state().attempts_started, 2);
        assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        let (mut send, mut recv) = peers[1].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        carrier.write_all(b"tcp").await.unwrap();
        let mut bytes = [0; 3];
        recv.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"tcp");
        send.write_all(b"ack").await.unwrap();
        carrier.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"ack");
        let mut downlink = data.start_udp();
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = async {
                    data.send_udp(b"udp".to_vec()).await;
                    assert_eq!(peers[0].read_datagram().await.unwrap().as_ref(), b"udp");
                    peers[0].send_datagram(bytes::Bytes::from_static(b"back")).unwrap();
                    assert_eq!(downlink.recv().await.unwrap(), b"back");
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        drop(carrier);
        drop(send);
        drop(recv);
        if let Some(open) = open.as_mut() { settle(open).await; }
        drop(open);
        drop(primary);
        drop(downlink);
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

fn idle_reservation(data: &TuicUpstream) -> TcpPoolSlotReservation {
    match data
        .tcp_pool_admission
        .try_decide_once(
            &data.sample_tcp_pool_observations(),
            &data.tcp_pool_replacement_allowed(),
            &[],
            false,
        )
        .unwrap()
    {
        TcpPoolAdmissionDecision::ReserveCurrent(r) => r,
        _ => panic!("expected original idle reservation"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn distinct_real_slots_submit_concurrently_and_root_serializes_candidates() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 3;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary = data.open_tcp(&target).await.unwrap();
        let a = idle_reservation(&data);
        let b = idle_reservation(&data);
        assert_ne!(a.index, b.index);
        let slot_a = data.tcp_pool_slots[a.index].state.lock().await;
        let slot_b = data.tcp_pool_slots[b.index].state.lock().await;
        let kind = recovery::ProbeKind::PreviousFailure;
        slot_a.current.open_state.note_open_failure(a.index);
        slot_b.current.open_state.note_open_failure(b.index);
        let barrier = std::sync::Barrier::new(2);
        let deadline = kind.deadline();
        let (a, b) = std::thread::scope(|scope| {
            let one = scope.spawn(|| {
                barrier.wait();
                data.submit_probe(a, slot_a, kind, deadline)
                    .map_err(|e| e.error)
            });
            let two = scope.spawn(|| {
                barrier.wait();
                data.submit_probe(b, slot_b, kind, deadline)
                    .map_err(|e| e.error)
            });
            (one.join().unwrap().unwrap(), two.join().unwrap().unwrap())
        });
        assert_ne!(a, b);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    data.await_aux_recovery(a).await.unwrap();
                    data.await_aux_recovery(b).await.unwrap();
                } => {},
                _ = async {
                    for _ in 0..2 {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer.read_datagram().await.unwrap();
                        peer.send_datagram(bytes::Bytes::from_static(b"alive")).unwrap();
                        peers.push(peer);
                    }
                    std::future::pending::<()>().await;
                } => unreachable!(),
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(host.resources.state().attempts_started, 5);
        assert!(data.poll_maintenance_receipt(a).unwrap().is_some());
        assert!(data.poll_maintenance_receipt(a).is_err());
        assert!(data.poll_maintenance_receipt(b).unwrap().is_some());
        assert!(peers[0].close_reason().is_none());
        drop(primary);
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

#[tokio::test(flavor = "current_thread")]
async fn expired_queued_probe_returns_original_lease_without_closing_or_connecting() {
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
        let reservation = idle_reservation(&data);
        assert_eq!(reservation.index, 1);
        let activity = reservation
            .lease
            .generation_active
            .as_ref()
            .unwrap()
            .clone();
        let preparing = reservation.preparation.preparing.clone();
        let slot = data.tcp_pool_slots[1].state.lock().await;
        let ticket = data
            .submit_probe(
                reservation,
                slot,
                recovery::ProbeKind::PreviousFailure,
                tokio::time::Instant::now() + Duration::from_millis(20),
            )
            .map_err(|e| e.error)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(data.observe_aux_recovery(ticket).is_err());
        assert_eq!(activity.active(), 0);
        assert!(!preparing.load(Ordering::Acquire));
        assert!(
            data.tcp_pool_slots[1]
                .state
                .lock()
                .await
                .current
                .transport
                .close_reason()
                .is_none()
        );
        assert_eq!(host.resources.state().attempts_started, 2);
        assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert!(data.poll_maintenance_receipt(ticket).is_err());
        drop(primary);
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

#[tokio::test(flavor = "current_thread")]
async fn probe_handshake_and_ready_failure_never_retry_or_fallback() {
    for ready_failure in [false, true] {
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
            data.tcp_pool_slots[1]
                .state
                .lock()
                .await
                .current
                .open_state
                .note_open_failure(1);
            let failed = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    result = data.open_tcp(&target) => result,
                    _ = async {
                        let incoming = server.accept().await.unwrap();
                        if ready_failure {
                            let peer = incoming.await.unwrap();
                            auth(&peer, &cfg).await;
                            peer.close(17u32.into(), b"actual ready probe failure");
                            peers.push(peer);
                        } else { incoming.refuse(); }
                        std::future::pending::<()>().await;
                    } => unreachable!(),
                    result = host.run() => panic!("node ended: {result:?}"),
                }
            })
            .await
            .unwrap();
            assert!(
                failed.is_err(),
                "Probe never uses Healthy same-open fallback"
            );
            assert_eq!(host.resources.state().attempts_started, 3);
            assert_eq!(host.resources.state().attempts_drained, 1);
            assert!(!data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
            assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
            assert!(peers[0].close_reason().is_none());
            let mut fresh = start(&data);
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    (_, peer) = async {
                        tokio::join!(ready(&mut fresh), async {
                            let peer = server.accept().await.unwrap().await.unwrap();
                            auth(&peer, &cfg).await;
                            peer
                        })
                    } => { peers.push(peer); },
                    result = host.run() => panic!("node ended: {result:?}"),
                }
            })
            .await
            .unwrap();
            assert_eq!(host.resources.state().attempts_started, 4);
            let OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(mut carrier)) =
                fresh.take_carrier(Instant::now()).unwrap()
            else {
                panic!("family");
            };
            let (mut send, mut recv) = peers.last().unwrap().accept_bi().await.unwrap();
            let mut header = vec![0; encode_connect(&target).len()];
            recv.read_exact(&mut header).await.unwrap();
            assert_eq!(header, encode_connect(&target));
            carrier.write_all(b"x").await.unwrap();
            let mut byte = [0; 1];
            recv.read_exact(&mut byte).await.unwrap();
            assert_eq!(&byte, b"x");
            send.write_all(b"y").await.unwrap();
            carrier.read_exact(&mut byte).await.unwrap();
            assert_eq!(&byte, b"y");
            drop((carrier, send, recv));
            settle(&mut fresh).await;
            drop(fresh);
            drop(primary);
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
}

#[tokio::test(flavor = "current_thread")]
async fn staged_probe_caller_cancel_or_deadline_keeps_saved_root_candidate_until_shutdown() {
    for expire in [false, true] {
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
            data.tcp_pool_slots[1]
                .state
                .lock()
                .await
                .current
                .open_state
                .note_open_failure(1);
            let mut open = start(&data);
            for _ in 0..4 {
                open.step(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    Instant::now(),
                );
            }
            assert_eq!(open.view().phase, TcpOpenPhase::Acquiring);
            assert!(
                tokio::time::timeout(Duration::from_millis(40), host.run())
                    .await
                    .is_err()
            );
            assert_eq!(host.resources.state().attempts_started, 3);
            if expire {
                // Observer cancellation suspended run; its saved physical candidate
                // remains owned while the independent original caller clock expires.
                tokio::time::sleep(Duration::from_secs(5)).await;
                open.step(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    Instant::now(),
                );
                assert!(open.failure().is_some());
            }
            settle(&mut open).await;
            let preparing = data.tcp_pool_admission.preparing_slots[1].clone();
            let permit = data.tcp_pool_draining_predecessor.clone();
            assert!(preparing.load(Ordering::Acquire));
            assert!(permit.load(Ordering::Acquire));
            assert_eq!(
                data.tcp_pool_admission.active_slots[1].load(Ordering::Acquire),
                0
            );
            drop(open);
            drop(primary);
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
