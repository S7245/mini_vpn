//! Real Service ByteOwned pending/payload stop and original three-role join.
//! Owns only the peer/App scenario and passive terminal checks. The saved Root
//! owns F, D16 physical backing, role handles and Node settlement.

use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_pending_quinn_reader_stops_joins_and_settles_original_service() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        d16_byte_owned: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::D16DirectOrdered
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let mut held_send = None;
    let mut backing = None;

    let setup = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        held_send = Some(send);
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 3 {
                                backing = owner.flow_d16_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("ByteOwned service ended before pending read: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());
        assert!(backing.is_some());
    }).await;

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let joined_while_peer_open = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), service.run())
                        .await
                        .is_err()
                );
                let owner = saved
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 3 {
                    assert_eq!((flow.published, flow.released, flow.f), (1, 0, 1));
                    assert_eq!((flow.w, flow.reserved, flow.escrow_bytes), (0, 0, 0));
                    assert!(flow.audit);
                    assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
                    assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                    break;
                }
            }
        })
        .await
        .unwrap();
    })
    .await;
    let peer_finish = capture(async {
        if let Some(mut send) = held_send.take() {
            let stopped = tokio::time::timeout(Duration::from_secs(1), send.stopped())
                .await
                .unwrap()
                .unwrap();
            assert!(stopped.is_some(), "original reader signals STOP_SENDING");
            send.finish().unwrap();
        }
    })
    .await;
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined), (1, 1, 3));
        assert_eq!((flow.live, flow.w, flow.f, flow.running), (0, 0, 0, 0));
        assert_eq!((flow.reserved, flow.escrow_bytes), (0, 0));
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (
                physical.allocations,
                physical.payload_bytes,
                physical.physical_bytes,
                physical.quarantined
            ),
            (0, 0, 0, false)
        );
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = setup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = joined_while_peer_open {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = peer_finish {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_charged_payload_stops_joins_without_false_budget_release() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        d16_byte_owned: Some("1"),
        ..Default::default()
    }));
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let mut held_send = None;
    let mut backing = None;
    let mut charged = None;

    let setup = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        held_send = Some(send);
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 3 {
                                backing = owner.flow_d16_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("ByteOwned service ended before DATA: {report:?}"),
            }
        }).await.unwrap();
        assert!(backing.is_some());
        assert!(app.tcp_reply.is_empty());

        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let send = held_send.as_mut().unwrap();
                    send.write_all(&vec![0x5a; 256 * 1024]).await.unwrap();
                    loop {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        if let Some((queue, budget, same_budget)) = owner.flow_d16_physical_snapshots() {
                            if queue.queued_bytes + queue.leased_bytes >= 65536 {
                                assert!(same_budget);
                                assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
                                assert_eq!(queue.capacity_bytes, crate::client_tun::D16_PER_FLOW_BYTE_CAPACITY);
                                assert!(budget.total_bytes() <= budget.capacity_bytes);
                                charged = Some((queue, budget));
                                break;
                            }
                        }
                    }
                    assert!(charged.is_some(), "actual D16 DATA must remain charged under App backpressure");
                } => {},
                report = service.run() => panic!("ByteOwned service ended before charged payload: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());
    }).await;

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let joined = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), service.run())
                        .await
                        .is_err()
                );
                let owner = saved
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 3 {
                    assert_eq!((flow.published, flow.released, flow.f), (1, 0, 1));
                    assert_eq!((flow.w, flow.reserved, flow.escrow_bytes), (0, 0, 0));
                    assert!(flow.audit);
                    assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
                    assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                    let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
                    assert!(same_budget);
                    assert_eq!(queue.reserved_bytes, 0);
                    assert!(queue.queued_bytes + queue.leased_bytes > 0);
                    assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
                    assert!(budget.total_bytes() <= budget.capacity_bytes);
                    assert!(queue.high_water_bytes > 0 && budget.high_water_bytes > 0);
                    break;
                }
            }
        })
        .await
        .unwrap();
    })
    .await;
    let cleanup = capture(async {
        let mut observer = Box::pin(handle.observe_shutdown());
        let phase =
            tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
                tokio::select! {
                    phase = observer.as_mut() => phase,
                    report = service.run() => panic!("charged D16 falsely settled: {report:?}"),
                }
            })
            .await
            .unwrap();
        assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
        if phase == Phase::Incomplete {
            assert!(tokio::time::Instant::now() >= original_deadline);
            tokio::time::timeout(Duration::from_secs(1), async {
                while handle.phase() != Phase::Quarantined {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(10), service.run())
                            .await
                            .is_err()
                    );
                }
            })
            .await
            .unwrap();
        }
        if tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("charged D16 falsely settled before deadline: {report:?}"),
            }
        }
        assert!(tokio::time::Instant::now() >= original_deadline);
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert!(service.completion.is_none());
        let owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined), (1, 0, 3));
        assert_eq!((flow.live, flow.w, flow.f, flow.running), (1, 0, 1, 0));
        assert_eq!((flow.reserved, flow.escrow_bytes), (0, 0));
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && owner.ledger().quarantined);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
        assert!(same_budget);
        assert!(queue.queued_bytes + queue.leased_bytes > 0);
        assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
        assert_eq!(queue.reserved_bytes, 0);
        assert!(budget.total_bytes() <= budget.capacity_bytes);
        let physical = backing.as_ref().unwrap().snapshot();
        assert!(!physical.quarantined);
    })
    .await;
    drop(held_send.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    for outcome in [setup, joined, cleanup] {
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_full_delivery_releases_original_flow_permit() {
    run_full_delivery(8192).await;
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_backpressured_quarter_mib_delivery_releases_original_flow() {
    run_full_delivery(256 * 1024).await;
}

async fn run_full_delivery(reply_bytes: usize) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        d16_byte_owned: Some("1"),
        ..Default::default()
    }));
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let mut held_send = None;
    let mut backing = None;

    let setup = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        held_send = Some(send);
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 3 {
                                backing = owner.flow_d16_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("ByteOwned service ended before DATA: {report:?}"),
            }
        }).await.unwrap();
        assert!(backing.is_some());
        assert!(app.tcp_reply.is_empty());

        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    held_send.as_mut().unwrap().write_all(&vec![0x5a; reply_bytes]).await.unwrap();
                    loop {
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        if let Some((queue, budget, same_budget)) = owner.flow_d16_physical_snapshots() {
                            let min_backlog = if reply_bytes > 8192 { 65536 } else { 1 };
                            if queue.queued_bytes + queue.leased_bytes >= min_backlog && budget.owned_bytes > 0 {
                                assert!(same_budget);
                                assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
                                assert!(queue.queued_bytes + queue.leased_bytes <= queue.capacity_bytes);
                                assert!(budget.total_bytes() <= budget.capacity_bytes);
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("ByteOwned service ended before charge: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());

        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    loop {
                        app.turn().await;
                        assert!(app.tcp_reply.len() <= reply_bytes);
                        if app.tcp_reply.len() == reply_bytes {
                            assert!(app.tcp_reply.iter().all(|byte| *byte == 0x5a));
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
                            if same_budget && queue.queued_bytes == 0 && queue.leased_bytes == 0
                                && queue.reserved_bytes <= budget.reserved_bytes {
                                assert!(budget.total_bytes() <= budget.capacity_bytes);
                                break;
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("ByteOwned service ended before ACK: {report:?}"),
            }
        }).await.unwrap_or_else(|elapsed| {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("D16 full-delivery stop: app={} physical={:?} flow={:?}",
                app.tcp_reply.len(), owner.flow_d16_physical_snapshots(), owner.flow_observation());
            panic!("D16 full-delivery gate: {elapsed}");
        });
    }).await;

    if let Err(error) = setup {
        drop(held_send.take());
        if let Some(peer) = peer.take() {
            peer.close(0u32.into(), b"done");
        }
        server.close(0u32.into(), b"done");
        std::panic::resume_unwind(error);
    }

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let joined = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), service.run())
                        .await
                        .is_err()
                );
                let owner = saved
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 3 {
                    assert_eq!((flow.published, flow.released, flow.f), (1, 0, 1));
                    let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
                    assert!(same_budget);
                    assert_eq!(
                        (queue.queued_bytes, queue.leased_bytes, queue.reserved_bytes),
                        (0, 0, 0)
                    );
                    assert!(budget.total_bytes() <= budget.capacity_bytes);
                    assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                    break;
                }
            }
        })
        .await
        .unwrap();
    })
    .await;
    let peer_finish = capture(async {
        if let Some(mut send) = held_send.take() {
            let stopped = tokio::time::timeout(Duration::from_secs(1), send.stopped())
                .await
                .unwrap()
                .unwrap();
            assert!(stopped.is_some());
            send.finish().unwrap();
        }
    })
    .await;
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined), (1, 1, 3));
        assert_eq!((flow.live, flow.w, flow.f, flow.running), (0, 0, 0, 0));
        assert_eq!((flow.reserved, flow.escrow_bytes), (0, 0));
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (
                physical.allocations,
                physical.payload_bytes,
                physical.physical_bytes,
                physical.quarantined
            ),
            (0, 0, 0, false)
        );
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    for outcome in [setup, joined, peer_finish, cleanup] {
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
    }
    assert!(drained.unwrap());
}
