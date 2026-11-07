use super::super::*;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn stale_preparation(
    data: &TuicUpstream,
    index: usize,
) -> TcpPoolAuxiliaryReplacementPreparation {
    {
        let mut slot = data.tcp_pool_slots[index].state.lock().await;
        slot.current.successor_service_certificate = Some(TcpPoolSuccessorServiceCertificate {
            identity: slot.current.identity(),
            path_generation: slot.current.transport.current_path_generation() + 1,
            proved_cwnd_floor: 1,
        });
    }
    match data
        .tcp_pool_admission
        .try_decide_excluding(
            &data.sample_tcp_pool_observations(),
            &data.tcp_pool_replacement_allowed(),
            &[],
            false,
        )
        .unwrap()
    {
        TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) => {
            assert_eq!(replacement.index, index);
            replacement
        }
        _ => panic!("real admission must request stale auxiliary replacement"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_handshake_refusal_retries_and_receipt_is_unique_and_generation_bound() {
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
        let old_tcp = data.open_tcp(&target).await.unwrap();
        let replacement = stale_preparation(&data, 1).await;
        let ticket = data.request_healthy_replacement(replacement).await.unwrap();
        let started = Instant::now();
        let receipt = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = async {
                    let (receipt, peer) = tokio::join!(data.await_healthy_replacement(ticket), async {
                        server.accept().await.unwrap().refuse();
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer
                    });
                    peers.push(peer);
                    receipt.unwrap()
                } => result,
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(host.resources.state().attempts_started, 4);
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert_eq!(data.tcp_pool_slots[1].state.lock().await.current.auth_attempts, 2);
        assert!(data.await_healthy_replacement(ticket).await.is_err(), "root cannot issue the receipt twice");
        drop(primary);
        let reservation = match data.tcp_pool_admission.try_decide_excluding(
            &data.sample_tcp_pool_observations(), &data.tcp_pool_replacement_allowed(), &[], false,
        ).unwrap() {
            TcpPoolAdmissionDecision::ReserveCurrent(reservation) => reservation,
            _ => panic!("must re-admit to current generation"),
        };
        assert_eq!(reservation.index, 0);
        assert!(!receipt.consume(&reservation), "another actual slot cannot use the successor proof");
        assert!(data.await_healthy_replacement(ticket).await.is_err());
        drop(reservation); drop(old_tcp); drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::FailedSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.endpoint_drained);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn submission_close_race_returns_preparation_but_stop_is_not_fallback() {
    for stop in [false, true] {
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
            let old_tcp = data.open_tcp(&target).await.unwrap();
            let replacement = stale_preparation(&data, 1).await;
            let preparing = data.tcp_pool_admission.preparing_slots[1].clone();
            assert!(preparing.load(Ordering::Acquire));
            if stop {
                host.request_stop();
            } else {
                peers[1].close(17u32.into(), b"close between admission and root transfer");
                tokio::time::timeout(Duration::from_secs(2), async {
                    while data.tcp_pool_slots[1]
                        .state
                        .lock()
                        .await
                        .current
                        .transport
                        .close_reason()
                        .is_none()
                    {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
            }
            let failure = match data.request_healthy_replacement(replacement).await {
                Err(failure) => failure,
                Ok(_) => panic!("closed or stopping submission must reject"),
            };
            assert_eq!(failure.settled, !stop);
            assert!(!preparing.load(Ordering::Acquire));
            assert_eq!(host.resources.state().attempts_started, 2);
            drop(primary);
            drop(old_tcp);
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::RunningResourcesSettled
            );
            let state = host.resources.state();
            assert_eq!(state.attempts_started, state.attempts_drained);
            assert_eq!(state.tasks_started, state.tasks_joined);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_observers_leave_candidate_and_preparation_for_node_shutdown() {
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
        let old_tcp = data.open_tcp(&target).await.unwrap();
        let replacement = stale_preparation(&data, 1).await;
        let ticket = data.request_healthy_replacement(replacement).await.unwrap();
        let preparing = data.tcp_pool_admission.preparing_slots[1].clone();
        assert!(tokio::time::timeout(Duration::from_millis(40), async {
            tokio::select! {
                result = data.await_healthy_replacement(ticket) => panic!("receipt before peer AUTH: {}", result.is_ok()),
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.is_err());
        assert_eq!(host.resources.state().attempts_started, 3);
        assert!(preparing.load(Ordering::Acquire));
        assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        let permit = data.tcp_pool_draining_predecessor.clone();
        drop(primary); drop(old_tcp); drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        assert!(!preparing.load(Ordering::Acquire));
        assert!(!permit.load(Ordering::Acquire));
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.endpoint_drained);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn queued_preparation_expires_behind_live_predecessor_without_starting_candidate() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 3;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let mut carriers = Vec::new();
        for _ in 0..3 { carriers.push(data.open_tcp(&target).await.unwrap()); }
        let first = data.request_healthy_replacement(stale_preparation(&data, 1).await).await.unwrap();
        let queued = data.request_healthy_replacement(stale_preparation(&data, 2).await).await.unwrap();
        let preparing = data.tcp_pool_admission.preparing_slots[2].clone();
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    let (receipt, peer) = tokio::join!(data.await_healthy_replacement(first), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer
                    });
                    assert!(receipt.is_ok());
                    peers.push(peer);
                } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        assert!(preparing.load(Ordering::Acquire));
        assert!(host.resources.state().recovery.predecessor.is_some());
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = data.await_healthy_replacement(queued) => assert!(result.is_err()),
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        assert!(data.healthy_fallback_allowed(queued));
        assert!(!preparing.load(Ordering::Acquire));
        assert_eq!(host.resources.state().attempts_started, 4);
        assert!(host.resources.state().recovery.predecessor.is_some());
        drop(carriers); drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::FailedSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_healthy_attempt_does_not_poison_later_closed_recovery() {
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
        let (primary_send, mut primary_recv) = peers[0].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        primary_recv.read_exact(&mut header).await.unwrap();
        let old_tcp = data.open_tcp(&target).await.unwrap();
        let (old_send, mut old_recv) = peers[1].accept_bi().await.unwrap();
        old_recv.read_exact(&mut header).await.unwrap();
        {
            let mut slot = data.tcp_pool_slots[1].state.lock().await;
            slot.current.successor_service_certificate = Some(TcpPoolSuccessorServiceCertificate {
                identity: slot.current.identity(),
                path_generation: slot.current.transport.current_path_generation() + 1,
                proved_cwnd_floor: 1,
            });
        }
        let fallback = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = async {
                    let (opened, ()) = tokio::join!(data.open_tcp(&target), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer.close(17u32.into(), b"reject actual forward service");
                        peers.push(peer);
                    });
                    opened.expect("settled healthy failure permits current-generation fallback")
                } => result,
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(host.resources.state().attempts_started, 3);
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert!(!data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
        assert!(
            host.resources.state().first.is_some(),
            "retain original failure"
        );
        let (fallback_send, mut fallback_recv) = peers[0].accept_bi().await.unwrap();
        fallback_recv.read_exact(&mut header).await.unwrap();
        drop(fallback);
        drop(fallback_send);
        drop(fallback_recv);
        drop(old_tcp);
        drop(old_send);
        drop(old_recv);
        peers[1].close(18u32.into(), b"old generation later closes");
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.tcp_pool_slots[1]
                .state
                .lock()
                .await
                .current
                .transport
                .close_reason()
                .is_none()
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let reopened = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                result = async {
                    let (stream, peer) = tokio::join!(data.open_tcp(&target), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer
                    });
                    peers.push(peer);
                    stream.expect("fresh Closed ticket must recover after settled Healthy failure")
                } => result,
                result = host.run() => panic!("node ended: {result:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(host.resources.state().attempts_started, 4);
        assert!(!host.resources.state().quarantined);
        drop(reopened);
        drop(primary);
        drop(primary_send);
        drop(primary_recv);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn healthy_replacement_keeps_old_tcp_until_actual_return() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    cfg.quic_stats_secs = Some(60);
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let mut primary = data.open_tcp(&target).await.unwrap();
        let (mut primary_send, mut primary_recv) = peers[0].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        primary_recv.read_exact(&mut header).await.unwrap();
        let mut old_tcp = data.open_tcp(&target).await.unwrap();
        let (mut old_send, mut old_recv) = peers[1].accept_bi().await.unwrap();
        old_recv.read_exact(&mut header).await.unwrap();
        let old = {
            let mut slot = data.tcp_pool_slots[1].state.lock().await;
            let old = slot.current.identity();
            // Fault injection changes only admission's stale certificate trigger.
            // AUTH, service ACKs, carrier traffic and retirement below are real.
            slot.current.successor_service_certificate = Some(TcpPoolSuccessorServiceCertificate {
                identity: old,
                path_generation: slot.current.transport.current_path_generation() + 1,
                proved_cwnd_floor: 1,
            });
            old
        };
        let mut downlink = data.start_udp();
        let mut successor = {
            let business = data.open_tcp(&target);
            tokio::pin!(business);
            assert!(tokio::time::timeout(Duration::from_millis(40), async {
                tokio::select! {
                    result = &mut business => panic!("open before real successor AUTH: {}", result.is_ok()),
                    result = host.run() => panic!("node ended: {result:?}"),
                }
            }).await.is_err());
            assert_eq!(host.resources.state().attempts_started, 3);
            // Cancelling only run observation must retain the same candidate.
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    result = async {
                        let (stream, peer) = tokio::join!(&mut business, async {
                            let peer = server.accept().await.unwrap().await.unwrap();
                            auth(&peer, &cfg).await;
                            peer
                        });
                        peers.push(peer);
                        stream.unwrap()
                    } => result,
                    result = host.run() => panic!("node ended: {result:?}"),
                }
            }).await.unwrap()
        };
        assert_eq!(host.resources.state().attempts_started, 3);
        let current = data.tcp_pool_slots[1].state.lock().await.current.identity();
        assert_eq!(current.generation, old.generation + 1);
        assert_ne!(current.stable_id, old.stable_id);
        assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert!(data.tcp_pool_slots[1].state.lock().await.draining.is_none());
        let traffic = async {
            let (mut send, mut recv) = peers[2].accept_bi().await.unwrap();
            recv.read_exact(&mut header).await.unwrap();
            assert_eq!(header, encode_connect(&target));
            successor.write_all(b"new").await.unwrap();
            let mut bytes = [0; 3];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"new");
            send.write_all(b"ack").await.unwrap();
            successor.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"ack");
            for (stream, tx, rx) in [(&mut primary, &mut primary_send, &mut primary_recv), (&mut old_tcp, &mut old_send, &mut old_recv)] {
                stream.write_all(b"old").await.unwrap();
                rx.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"old");
                tx.write_all(b"yes").await.unwrap();
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"yes");
            }
            data.send_udp(b"udp".to_vec()).await;
            assert_eq!(peers[0].read_datagram().await.unwrap().as_ref(), b"udp");
            peers[0].send_datagram(bytes::Bytes::from_static(b"back")).unwrap();
            assert_eq!(downlink.recv().await.unwrap(), b"back");
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! { _ = traffic => {}, result = host.run() => panic!("node ended: {result:?}") }
        }).await.unwrap();
        assert!(peers[1].close_reason().is_none());
        drop(old_tcp);
        drop(old_send);
        drop(old_recv);
        let resources = host.resources.clone();
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async { while resources.state().recovery.predecessor.is_some() { tokio::time::sleep(Duration::from_millis(1)).await; } } => {},
                result = host.run() => panic!("node ended: {result:?}"),
            }
        }).await.unwrap();
        assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert_eq!(host.resources.state().attempts_drained, 1);
        assert!(host.resources.state().tasks.iter().all(|t| t.stable_id != Some(old.stable_id)));
        drop(successor); drop(primary); drop(primary_send); drop(primary_recv); drop(downlink); drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.endpoint_drained);
    }).await;
    finish(host, server, peers, result).await;
}
