//! Real App TCP abort of a published, charged D16 Service Flow.
//! The test owns only App/peer stimulus and passive observations; original Root
//! owns queued backing, all three joins, F, physical disposition and Node.

use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_charged_app_abort_joins_and_disposes_original_queue() {
    local_abort_case(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_charged_app_abort_keeps_f_when_peer_stream_never_retires() {
    local_abort_case(false).await;
}

async fn local_abort_case(peer_finishes: bool) {
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
    let mut stream = None;
    let mut backing = None;
    let mut flow_queue = None;

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
                        stream = Some((send, recv));
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            if owner.flow_observation().unwrap().published == 1 {
                                backing = owner.flow_d16_backing_meter();
                                flow_queue = owner.flow_d16_queue_for_test();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("D16 Service ended before local abort: {report:?}"),
            }
        }).await.unwrap();
        assert!(backing.is_some());
        let queue = flow_queue.as_ref().unwrap();
        assert!(app.tcp_reply.is_empty());
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    stream.as_mut().unwrap().0.write_all(&[0x5a; 8192]).await.unwrap();
                    loop {
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        if let Some((queue, budget, same_budget)) = owner.flow_d16_physical_snapshots() {
                            if queue.queued_bytes + queue.leased_bytes > 0
                                && queue.reserved_bytes > 0 {
                                assert_eq!(queue.closure, crate::tcp_downlink_pump::LeasedByteQueueClosure::Open);
                                assert!(same_budget);
                                assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
                                assert!(budget.total_bytes() <= budget.capacity_bytes);
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("D16 Service ended before charged DATA: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());
    }).await;
    if let Err(error) = setup {
        if let Some(peer) = peer.take() {
            peer.close(0u32.into(), b"done");
        }
        server.close(0u32.into(), b"done");
        std::panic::resume_unwind(error);
    }

    app.sockets
        .get_mut::<smoltcp::socket::tcp::Socket>(app.tcp)
        .abort();
    let flow_queue = flow_queue.unwrap();
    let settled = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
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
                if flow.joined == 3
                    && owner.flow_d16_physical_snapshots().is_some_and(|(queue, budget, same)| {
                        same && queue.queued_bytes == 0 && queue.leased_bytes == 0
                            && queue.reserved_bytes == 0
                            && budget.total_bytes() <= budget.capacity_bytes
                    })
                {
                    assert_eq!((flow.live, flow.f, flow.released), (1, 1, 0));
                    assert!(!owner.ledger().quarantined);
                    assert!(matches!(
                        flow_queue.snapshot_now().closure.terminal_cause(),
                        Some(cause) if cause.direction == "local_to_remote"
                            && matches!(cause.reason, "reader_stopped" | "local_socket_terminal")
                    ));
                    break;
                }
            }
        })
        .await
        .unwrap_or_else(|elapsed| {
            let owner = saved
                .session
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            let ledger = owner.ledger();
            let first = ledger
                .first_failure
                .as_ref()
                .map(|first| format!("{:?}", first.facts()));
            let quarantined = ledger.quarantined;
            drop(ledger);
            eprintln!(
                "D16 App abort settle: flow={:?} debug={:?} physical={:?} first={:?} q={} peer={:?}",
                owner.flow_observation(),
                owner.flow_debug(),
                owner.flow_d16_physical_snapshots(),
                first,
                quarantined,
                peer.as_ref().and_then(quinn::Connection::close_reason)
            );
            panic!("D16 App abort physical JOIN remained: {elapsed}");
        });
        if !peer_finishes {
            handle.request_stop();
            let original_deadline = saved.deadline.lock().unwrap().unwrap();
            let mut observer = Box::pin(handle.observe_shutdown());
            let phase = tokio::time::timeout_at(
                original_deadline + Duration::from_millis(100),
                async {
                    tokio::select! {
                        phase = observer.as_mut() => phase,
                        report = service.run() => panic!("unretired D16 provider falsely settled: {report:?}"),
                    }
                },
            )
            .await
            .unwrap();
            assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
            if tokio::time::Instant::now() < original_deadline {
                tokio::select! {
                    _ = tokio::time::sleep_until(original_deadline) => {},
                    report = service.run() => panic!("unretired D16 provider falsely completed before parent deadline: {report:?}"),
                }
            }
            assert!(tokio::time::Instant::now() >= original_deadline);
            if phase == Phase::Incomplete {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while handle.phase() != Phase::Quarantined {
                        assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                    }
                }).await.unwrap();
            }
            assert_eq!(handle.phase(), Phase::Quarantined);
            assert!(service.completion.is_none());
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.published, flow.released, flow.joined), (1, 0, 3));
            assert_eq!((flow.live, flow.w, flow.f, flow.running), (1, 0, 1, 0));
            assert!(flow.audit);
            assert!(owner.ledger().first_failure.is_none() && owner.ledger().quarantined);
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            let (queue, budget, same) = owner.flow_d16_physical_snapshots().unwrap();
            assert!(same);
            assert_eq!((queue.queued_bytes, queue.leased_bytes, queue.reserved_bytes), (0, 0, 0));
            assert!(budget.total_bytes() <= budget.capacity_bytes);
            return;
        }
        stream.as_mut().unwrap().0.finish().unwrap();
        drop(stream.take());
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                if owner.flow_observation().unwrap().released == 1 || owner.ledger().quarantined {
                    break;
                }
            }
        }).await.expect("D16 App abort peer-completed Flow retirement");
        let owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined), (1, 1, 3));
        assert_eq!((flow.live, flow.w, flow.f, flow.running), (0, 0, 0, 0));
        assert!(flow.audit);
        assert!(!owner.ledger().quarantined);
        let (queued, transferred, discarded) = owner.flow_d16_disposition().unwrap();
        assert!(queued > 0);
        assert_eq!(transferred + discarded, queued);
        assert!(owner.flow_d16_physical_snapshots().is_none());
    })
    .await;

    if !peer_finishes {
        drop(stream.take());
        if let Some(peer) = peer.take() {
            peer.close(0u32.into(), b"done");
        }
        let drain = server.take_driver_drain_handle().unwrap();
        server.close(0u32.into(), b"done");
        drop(server);
        let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
        if let Err(error) = settled {
            std::panic::resume_unwind(error);
        }
        assert!(drained.unwrap());
        return;
    }

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap_or_else(|elapsed| {
                let owner = saved
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let ledger = owner.ledger();
                let first = ledger
                    .first_failure
                    .as_ref()
                    .map(|first| format!("{:?}", first.facts()));
                let quarantined = ledger.quarantined;
                drop(ledger);
                eprintln!(
                    "D16 App abort parent: flow={:?} physical={:?} first={:?} q={} phase={:?}",
                    owner.flow_observation(),
                    owner.flow_d16_physical_snapshots(),
                    first,
                    quarantined,
                    handle.phase()
                );
                panic!("D16 App abort parent deadline: {elapsed}");
            });
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        let owner = &service.completion.as_ref().unwrap().control;
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined), (1, 1, 3));
        assert_eq!((flow.live, flow.w, flow.f), (0, 0, 0));
        assert!(flow.audit);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        service
            .completion
            .as_ref()
            .unwrap()
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
    })
    .await;
    drop(stream.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    for outcome in [setup, settled, cleanup] {
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
    }
    assert!(!backing.unwrap().snapshot().quarantined);
    assert!(drained.unwrap());
}
