use super::super::*;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "current_thread")]
async fn actual_auxiliary_open_recovers_once_across_observer_cancel_with_primary_alive() {
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
        let (mut primary, mut primary_send, mut primary_recv) = tokio::time::timeout(Duration::from_secs(2), async {
            let primary = data.open_tcp(&target).await.unwrap();
            let (send, mut recv) = peers[0].accept_bi().await.unwrap();
            let mut header = vec![0; encode_connect(&target).len()];
            recv.read_exact(&mut header).await.unwrap();
            assert_eq!(header, encode_connect(&target));
            (primary, send, recv)
        }).await.unwrap();
        let primary_id = data.tcp_pool_slots[0].state.lock().await.current.identity();
        let old_aux = data.tcp_pool_slots[1].state.lock().await.current.identity();
        let mut downlink = data.start_udp();
        peers[1].close(17u32.into(), b"actual auxiliary loss");
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.tcp_pool_slots[1].state.lock().await.current.transport.close_reason().is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        {
            let business = async {
                let mut stream = data.open_tcp(&target).await.unwrap();
                stream.write_all(b"new-request").await.unwrap();
                stream.shutdown().await.unwrap();
                let mut reply = Vec::new();
                stream.read_to_end(&mut reply).await.unwrap();
                assert_eq!(reply, b"new-response");
            };
            tokio::pin!(business);
            assert!(tokio::time::timeout(Duration::from_millis(40), async {
                tokio::select! {
                    _ = &mut business => panic!("success before successor AUTH"),
                    result = host.run() => panic!("owner ended: {result:?}"),
                }
            }).await.is_err());
            assert_eq!(host.resources.state().attempts_started, 3);
            tokio::time::timeout(Duration::from_secs(2), async {
                primary.write_all(b"healthy").await.unwrap();
                let mut bytes = [0; 7];
                primary_recv.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"healthy");
                primary_send.write_all(b"alive").await.unwrap();
                let mut reply = [0; 5];
                primary.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"alive");
                data.send_udp(b"original-uplink".to_vec()).await;
                assert_eq!(peers[0].read_datagram().await.unwrap().as_ref(), b"original-uplink");
                peers[0].send_datagram(bytes::Bytes::from_static(b"original-downlink")).unwrap();
                assert_eq!(downlink.recv().await.unwrap(), b"original-downlink");
            }).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = async {
                        tokio::join!(&mut business, async {
                            let peer = server.accept().await.unwrap().await.unwrap();
                            auth(&peer, &cfg).await;
                            let (mut send, mut recv) = peer.accept_bi().await.unwrap();
                            assert_eq!(recv.read_to_end(1024).await.unwrap(), [encode_connect(&target), b"new-request".to_vec()].concat());
                            send.write_all(b"new-response").await.unwrap();
                            send.finish().unwrap();
                            peers.push(peer);
                        });
                    } => {},
                    result = host.run() => panic!("owner ended: {result:?}"),
                }
            }).await.unwrap();
        }
        assert_eq!(host.resources.state().attempts_started, 3);
        assert_eq!(data.tcp_pool_slots[0].state.lock().await.current.identity(), primary_id);
        let new_aux = data.tcp_pool_slots[1].state.lock().await.current.identity();
        assert_ne!(new_aux.stable_id, old_aux.stable_id);
        assert_eq!(new_aux.generation, old_aux.generation + 1);
        drop(primary);
        drop(primary_send);
        drop(primary_recv);
        drop(downlink);
        drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.endpoint_drained);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn stop_wakes_actual_open_waiter_before_terminal_arc_return() {
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
        peers[1].close(17u32.into(), b"aux failure before user stop");
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.tcp_pool_slots[1].state.lock().await.current.transport.close_reason().is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        {
            let business = data.open_tcp(&target);
            tokio::pin!(business);
            assert!(tokio::time::timeout(Duration::from_millis(40), async {
                tokio::select! {
                    result = &mut business => panic!("unexpected business result: {}", result.is_ok()),
                    result = host.run() => panic!("owner ended: {result:?}"),
                }
            }).await.is_err());
            assert_eq!(host.resources.state().attempts_started, 3);
            host.request_stop();
            // The waiting business must receive stop itself; dropping it first
            // would hide a saved node -> waiter -> DATA Arc shutdown cycle.
            assert!(tokio::time::timeout(Duration::from_secs(2), &mut business).await.unwrap().is_err());
        }
        drop(primary);
        drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn staged_factory_rebuilds_three_aux_generations_and_joins_each_stats_role() {
    use crate::owned_upstream::{LegacyOwnedUpstream, OpenedOwnedTcp, OwnedUpstream};
    use crate::upstream::{TcpOpenPhase, TcpOpenRequest};
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
        let primary = tokio::time::timeout(Duration::from_secs(2), data.open_tcp(&target)).await.unwrap().unwrap();
        for cycle in 0..3 {
            let old = data.tcp_pool_slots[1].state.lock().await.current.identity();
            peers.last().unwrap().close(17u32.into(), b"repeat auxiliary failure");
            wait_closed(&data, 1).await;
            let mut attempt = Arc::new(LegacyOwnedUpstream::new(data.clone()))
                .staged_tcp_open(TcpOpenRequest {
                    target: target.clone(), deadline: Instant::now() + Duration::from_secs(5),
                }).unwrap();
            let business = async {
                while attempt.view().phase != TcpOpenPhase::Ready {
                    attempt.step(&mut Context::from_waker(std::task::Waker::noop()), Instant::now());
                    assert!(attempt.failure().is_none(), "{:?}", attempt.failure());
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let mut stream = match attempt.take_carrier(Instant::now()).unwrap() {
                    OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(stream)) => stream,
                    _ => panic!("Generic family changed"),
                };
                stream.write_all(b"staged-request").await.unwrap();
                stream.shutdown().await.unwrap();
                let mut reply = Vec::new();
                stream.read_to_end(&mut reply).await.unwrap();
                assert_eq!(reply, b"staged-response");
                drop(stream);
                let deadline = Instant::now() + Duration::from_secs(2);
                while attempt.view().phase != TcpOpenPhase::Settled {
                    attempt.settle(&mut Context::from_waker(std::task::Waker::noop()), Instant::now(), deadline);
                    assert!(attempt.failure().is_none(), "{:?}", attempt.failure());
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = async {
                        tokio::join!(business, async {
                            let peer = server.accept().await.unwrap().await.unwrap();
                            auth(&peer, &cfg).await;
                            let (mut send, mut recv) = peer.accept_bi().await.unwrap();
                            assert_eq!(recv.read_to_end(1024).await.unwrap(), [encode_connect(&target), b"staged-request".to_vec()].concat());
                            send.write_all(b"staged-response").await.unwrap();
                            send.finish().unwrap();
                            peers.push(peer);
                        });
                    } => {},
                    result = host.run() => panic!("owner ended: {result:?}"),
                }
            }).await.unwrap();
            drop(attempt);
            let current = data.tcp_pool_slots[1].state.lock().await.current.identity();
            assert_eq!(current.generation, old.generation + 1);
            assert_ne!(current.stable_id, old.stable_id);
            let resources = host.resources.clone();
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = async {
                        while resources.state().recovery.predecessor.is_some() {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    } => {},
                    result = host.run() => panic!("owner ended: {result:?}"),
                }
            }).await.unwrap();
            let state = host.resources.state();
            assert_eq!(state.attempts_started, 3 + cycle);
            assert_eq!(state.attempts_drained, 1 + cycle);
            assert!(state.tasks.iter().all(|task| task.stable_id != Some(old.stable_id)));
            assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        }
        drop(primary);
        drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
    }).await;
    finish(host, server, peers, result).await;
}

async fn wait_closed(data: &TuicUpstream, index: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while data.tcp_pool_slots[index]
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

// Control-only false witness; never used as an actual Quinn settlement proof.
#[tokio::test]
async fn false_candidate_witness_preserves_original_permits_through_shutdown() {
    let owner = Arc::new(StartupResources::new(2));
    let preparing = Arc::new(AtomicBool::new(true));
    let draining = Arc::new(AtomicBool::new(false));
    {
        let mut state = owner.state();
        state.recovery.preparation = Some(TcpPoolSlotPreparation {
            preparing: preparing.clone(),
            available: Arc::new(Notify::new()),
        });
        state.recovery.permit = TcpPoolDrainingPermit::try_acquire(draining.clone());
        state.connections[0] = Some(Attempt {
            stable_id: None,
            published: false,
            connection: None,
            retirement: Retirement::Waiting(Box::pin(async { false })),
        });
        state.attempts_started = 1;
    }
    assert!(!owner.retire_unpublished().await);
    assert_eq!(owner.clone().cleanup(None).await, NodeShutdown::Quarantined);
    let state = owner.state();
    assert!(state.first.is_some());
    assert!(state.connections[0].is_some());
    assert!(state.recovery.preparation.is_some() && state.recovery.permit.is_some());
    assert!(preparing.load(Ordering::Acquire) && draining.load(Ordering::Acquire));
    assert_eq!(state.attempts_drained, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn original_deadlines_expire_while_candidate_drain_waits_without_ticket_reset() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 3;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        peers[1].close(17u32.into(), b"queued closed slot");
        peers[2].close(17u32.into(), b"queued closed slot");
        wait_closed(&data, 1).await;
        wait_closed(&data, 2).await;
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary = tokio::time::timeout(Duration::from_secs(2), data.open_tcp(&target)).await.unwrap().unwrap();
        let held = reserve(&data);
        assert_eq!(held.index, 1);
        let first = data.request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await).unwrap();
        let second = data.request_aux_recovery(2, data.tcp_pool_slots[2].state.lock().await).unwrap();
        assert_eq!(first, data.request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await).unwrap());
        let (finish_drain, receive) = tokio::sync::oneshot::channel();
        // Control-only delayed witness, separate from all three real published
        // Quinn witnesses. This tests waiting discipline, not physical acceptance.
        {
            let mut state = host.resources.state();
            state.connections[3] = Some(Attempt { stable_id: None, published: false, connection: None,
                retirement: Retirement::Waiting(Box::pin(async move { receive.await.unwrap_or(false) })) });
            state.attempts_started += 1;
        }
        assert!(data.request_aux_recovery(2, data.tcp_pool_slots[1].state.lock().await).is_err());
        assert!(tokio::time::timeout(Duration::from_millis(30), host.run()).await.is_err());
        assert_eq!(host.resources.state().attempts_started, 4, "held preparation prohibits any handshake");
        assert_eq!(first, data.request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await).unwrap());
        tokio::time::timeout(Duration::from_secs(17), async {
            tokio::select! {
                result = host.run() => panic!("owner completed before original drain: {result:?}"),
                _ = async {
                    let (a, b) = tokio::join!(data.await_aux_recovery(first), data.await_aux_recovery(second));
                    assert!(a.is_err() && b.is_err());
                } => {},
            }
        }).await.unwrap();
        assert!(host.resources.state().connections[3].is_some(), "failed notification must precede physical retirement");
        assert_eq!(first, data.request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await).unwrap());
        drop(held);
        finish_drain.send(true).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(40), host.run()).await.is_err());
        assert!(host.resources.state().connections[3].is_none());
        assert!(data.observe_aux_recovery(first).is_err());
        assert_eq!(host.resources.state().attempts_started, 4, "returning preparation after deadline must not retry");
        drop(primary);
        drop(data);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::FailedSettled);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn incomplete_release_retains_first_report_and_blocks_existing_data_admission() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let mut a = reserve(&data);
        let mut b = reserve(&data);
        data.tcp_pool_admission
            .pool_active_total
            .store(0, Ordering::Release);
        let slot = data.tcp_pool_slots[a.index].state.lock().await;
        let identity = slot.current.identity();
        let activity = slot.current.activity.clone();
        drop(slot);
        assert!(
            data.release_recovery_lease(a.index, identity, &activity, &mut a.lease)
                .is_err()
        );
        let first = host.resources.state().recovery.release_failure;
        let slot = data.tcp_pool_slots[b.index].state.lock().await;
        let identity = slot.current.identity();
        let activity = slot.current.activity.clone();
        drop(slot);
        assert!(
            data.release_recovery_lease(b.index, identity, &activity, &mut b.lease)
                .is_err()
        );
        {
            let state = host.resources.state();
            assert!(state.quarantined && state.first.is_some());
            assert_eq!(state.recovery.release_failure, first);
            assert_eq!(state.additional, 1);
            assert_eq!(state.attempts_started, 2);
        }
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), data.open_tcp(&target))
                .await
                .unwrap()
                .is_err()
        );
        drop(a);
        drop(b);
        assert_eq!(data.tcp_pool_admission.active_total(), 0);
        assert!(host.run().await.is_err());
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
    })
    .await;
    finish(host, server, peers, result).await;
}

fn reserve(data: &TuicUpstream) -> TcpPoolSlotReservation {
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
        TcpPoolAdmissionDecision::ReserveCurrent(reservation) => reservation,
        _ => panic!("expected original physical reservation"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_lease_identity_never_debits_or_erases_first_failure() {
    for incomplete_first in [false, true] {
        let server = server(8);
        let mut cfg = config(server.local_addr().unwrap());
        cfg.tcp_pool = 2;
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            let mut a = reserve(&data);
            let mut b = reserve(&data);
            let slot = data.tcp_pool_slots[a.index].state.lock().await;
            let a_identity = slot.current.identity();
            let a_activity = slot.current.activity.clone();
            drop(slot);
            let slot = data.tcp_pool_slots[b.index].state.lock().await;
            let b_identity = slot.current.identity();
            let b_activity = slot.current.activity.clone();
            drop(slot);
            if incomplete_first {
                data.tcp_pool_admission
                    .pool_active_total
                    .store(0, Ordering::Release);
                assert!(
                    data.release_recovery_lease(a.index, a_identity, &a_activity, &mut a.lease)
                        .is_err()
                );
            } else {
                assert!(
                    data.release_recovery_lease(a.index, a_identity, &b_activity, &mut a.lease)
                        .is_err()
                );
                assert_eq!(a_activity.active(), 1);
                assert_eq!(
                    data.tcp_pool_admission.active_slots[a.index].load(Ordering::Acquire),
                    1
                );
                assert_eq!(data.tcp_pool_admission.active_total(), 2);
            }
            let first = host.resources.state().recovery.release_failure;
            assert_eq!(first.unwrap().2.is_some(), incomplete_first);
            let before_total = data.tcp_pool_admission.active_total();
            assert!(
                data.release_recovery_lease(b.index, b_identity, &a_activity, &mut b.lease)
                    .is_err()
            );
            assert_eq!(b_activity.active(), 1);
            assert_eq!(
                data.tcp_pool_admission.active_slots[b.index].load(Ordering::Acquire),
                1
            );
            assert_eq!(data.tcp_pool_admission.active_total(), before_total);
            assert_eq!(host.resources.state().recovery.release_failure, first);
            drop(a);
            drop(b);
            assert_eq!(
                b_activity.active(),
                1,
                "Drop cannot debit rejected inventory"
            );
            if incomplete_first {
                // Later apparent numeric repair must not erase contradictory history.
                b_activity.active.store(0, Ordering::Release);
                data.tcp_pool_admission.active_slots[1].store(0, Ordering::Release);
            }
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::Quarantined
            );
            let state = host.resources.state();
            assert!(state.terminal.as_ref().unwrap().invalid);
            assert!(state.endpoint_drained && state.first.is_some());
            assert_eq!(state.recovery.release_failure, first);
            assert_eq!(state.attempts_started, state.attempts_drained);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_tls_configuration_rejection_stops_same_config_without_retry() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result =
        guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
            let primary = tokio::time::timeout(Duration::from_secs(2), data.open_tcp(&target))
                .await
                .unwrap()
                .unwrap();
            peers[1].close(
                17u32.into(),
                b"auxiliary failure before rejected successors",
            );
            wait_closed(&data, 1).await;
            // Local TLS peer rejects the actual recovery config; no fake success or
            // mocked Connecting/drain replaces either real failed attempt.
            host.config.sni = "recovery.invalid".into();
            let mut starts = Vec::new();
            tokio::time::timeout(Duration::from_secs(4), async {
            tokio::select! {
                _ = async {
                    tokio::join!(async { assert!(data.open_tcp(&target).await.is_err()); }, async {
                        for _ in 0..1 {
                            let incoming = server.accept().await.unwrap();
                            starts.push(tokio::time::Instant::now());
                            assert!(incoming.await.is_err());
                        }
                    });
                } => {},
                result = host.run() => panic!("local exhaustion ended unrelated node: {result:?}"),
            }
        }).await.unwrap();
            assert_eq!(starts.len(), 1);
            {
                let state = host.resources.state();
                assert_eq!(state.attempts_started, 3);
                assert_eq!(state.attempts_drained, 1);
                assert!(!state.quarantined && state.first.is_some());
                assert!(
                    state
                        .connections
                        .iter()
                        .flatten()
                        .all(|entry| entry.published)
                );
            }
            assert!(
                tokio::time::timeout(Duration::from_secs(1), data.open_tcp(&target))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(host.resources.state().attempts_started, 3);
            assert!(!data.udp_runtime.node_stopping());
            drop(primary);
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::FailedSettled
            );
        })
        .await;
    finish(host, server, peers, result).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_transient_refusal_retries_twice_with_original_spacing() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result =
        guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
            let primary = tokio::time::timeout(Duration::from_secs(2), data.open_tcp(&target))
                .await
                .unwrap()
                .unwrap();
            peers[1].close(
                17u32.into(),
                b"auxiliary failure before rejected successors",
            );
            wait_closed(&data, 1).await;
            // Valid original configuration; actual QUIC refusal is transient.
            let mut starts = Vec::new();
            tokio::time::timeout(Duration::from_secs(4), async {
            tokio::select! {
                _ = async {
                    tokio::join!(async { assert!(data.open_tcp(&target).await.is_err()); }, async {
                        for _ in 0..2 {
                            let incoming = server.accept().await.unwrap();
                            starts.push(tokio::time::Instant::now());
                            incoming.refuse();
                        }
                    });
                } => {},
                result = host.run() => panic!("local exhaustion ended unrelated node: {result:?}"),
            }
        }).await.unwrap();
            assert!(starts[1].duration_since(starts[0]) >= Duration::from_millis(500));
            {
                let state = host.resources.state();
                assert_eq!(state.attempts_started, 4);
                assert_eq!(state.attempts_drained, 2);
                assert!(!state.quarantined && state.first.is_some());
                assert!(
                    state
                        .connections
                        .iter()
                        .flatten()
                        .all(|entry| entry.published)
                );
            }
            assert!(
                tokio::time::timeout(Duration::from_secs(1), data.open_tcp(&target))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(host.resources.state().attempts_started, 4);
            assert!(!data.udp_runtime.node_stopping());
            drop(primary);
            drop(data);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::FailedSettled
            );
        })
        .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn old_actual_aux_carrier_keeps_predecessor_permit_through_stop_observation_cancel() {
    old_carrier_case(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_published_witness_keeps_terminal_predecessor_permit_after_logical_zero() {
    old_carrier_case(true).await;
}

async fn old_carrier_case(failed_witness: bool) {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary = tokio::time::timeout(Duration::from_secs(2), data.open_tcp(&target)).await.unwrap().unwrap();
        let old_aux = tokio::time::timeout(Duration::from_secs(2), data.open_tcp(&target)).await.unwrap().unwrap();
        peers[1].close(17u32.into(), b"held old auxiliary carrier");
        wait_closed(&data, 1).await;
        let ticket = data.request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    tokio::join!(async { data.await_aux_recovery(ticket).await.unwrap(); }, async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peers.push(peer);
                    });
                } => {},
                result = host.run() => panic!("owner ended: {result:?}"),
            }
        }).await.unwrap();
        let resources = host.resources.clone();
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = async {
                    loop {
                        if resources.state().recovery.predecessor.as_ref().is_some_and(|old| old.generation.physical_drained) { break; }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                result = host.run() => panic!("owner ended: {result:?}"),
            }
        }).await.unwrap();
        {
            let state = resources.state();
            let old = state.recovery.predecessor.as_ref().unwrap();
            assert_eq!(old.generation.activity.active(), 1);
            assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        }
        let actual_drained = Arc::new(AtomicBool::new(false));
        if failed_witness {
            let primary_id = data.tcp_pool_slots[0].state.lock().await.current.identity().stable_id;
            let mut state = resources.state();
            let entry = state.connections.iter_mut().flatten().find(|entry| entry.stable_id == Some(primary_id)).unwrap();
            let Retirement::Ready(drain) = std::mem::replace(&mut entry.retirement, Retirement::Failed) else { panic!("original physical witness missing"); };
            if let Some(conn) = entry.connection.take() { conn.close(0u32.into(), b"controlled retirement fault"); }
            let observed = actual_drained.clone();
            // Preserve and poll the real witness; only its returned control verdict
            // is fault-injected. This is not a naturally occurring Quinn failure.
            entry.retirement = Retirement::Waiting(Box::pin(async move {
                observed.store(drain.drained().await, Ordering::Release);
                false
            }));
        }
        drop(primary);
        drop(data);
        if failed_witness {
            drop(old_aux);
            assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::Quarantined);
            let state = resources.state();
            assert!(actual_drained.load(Ordering::Acquire));
            let terminal = state.terminal.as_ref().expect("failed witness retains original terminal");
            assert!(terminal.invalid && terminal.predecessor_permit.is_some());
            assert!(terminal.draining.load(Ordering::Acquire));
            assert!(state.endpoint_drained && state.first.is_some());
        } else {
            assert!(tokio::time::timeout(Duration::from_millis(40), host.observe_shutdown()).await.is_err());
            assert!(resources.state().terminal.as_ref().unwrap().predecessor_permit.is_some());
            drop(old_aux);
            assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
            let state = resources.state();
            assert!(state.terminal.is_none() && state.endpoint_drained);
            assert_eq!(state.attempts_started, state.attempts_drained);
        }
    }).await;
    finish(host, server, peers, result).await;
}
