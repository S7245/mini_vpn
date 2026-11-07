//! Real Quinn RESET through the saved D16 Service and original Root first error.
//! Owns only peer/App stimulus and passive observations; Root owns reader,
//! queue, global budget, F, roles and Node settlement. Tests are below.

use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_remote_reset_retains_first_error_and_joins_original_roles() {
    run_remote_reset(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_loaded_remote_reset_retains_charged_first_error() {
    run_remote_reset(true).await;
}

async fn run_remote_reset(loaded: bool) {
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

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_traffic = async {
                        let (mut send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        if loaded {
                            send.write_all(&vec![0x5a; 256 * 1024]).await.unwrap();
                        } else {
                            send.write_all(b"tcp-reply").await.unwrap();
                        }
                        stream = Some((send, recv));
                    };
                    let app_traffic = async {
                        loop {
                            if loaded {
                                let published = saved.session.lock().unwrap().as_ref()
                                    .and_then(std::sync::Weak::upgrade)
                                    .and_then(|owner| owner.flow_observation())
                                    .is_some_and(|flow| flow.published == 1);
                                if published { break; }
                            } else if app.tcp_reply == b"tcp-reply" {
                                break;
                            }
                            app.turn().await;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        if loaded {
                            loop {
                                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                                if owner.flow_d16_physical_snapshots().is_some_and(|(queue, budget, same)| {
                                    same && queue.queued_bytes + queue.leased_bytes >= 65536
                                        && budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes
                                }) { break; }
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                        }
                    };
                    tokio::join!(remote_traffic, app_traffic);
                } => {},
                report = service.run() => panic!("D16 service ended before remote reset: {report:?}"),
            }
        }).await.unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.running), (1, 3));
        backing = owner.flow_d16_backing_meter();
        assert!(backing.is_some());
        let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
        assert!(same_budget);
        if loaded {
            assert!(app.tcp_reply.is_empty());
            assert!(queue.queued_bytes + queue.leased_bytes >= 65536);
            assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
        } else {
            assert!(queue.local_progress_bytes >= 9);
        }
        assert!(budget.total_bytes() <= budget.capacity_bytes);
        let (mut send, mut recv) = stream.take().unwrap();
        send.reset(77u32.into()).unwrap();
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                if loaded {
                    app.turn().await;
                }
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 3 && owner.ledger().first_failure.is_some() {
                    break;
                }
            }
        }).await.unwrap_or_else(|elapsed| {
            let ledger = owner.ledger();
            eprintln!("D16 RESET stop: flow={:?} physical={:?} first={:?} q={}",
                owner.flow_observation(), owner.flow_d16_physical_snapshots(),
                ledger.first_failure.as_ref().map(|first| first.facts()), ledger.quarantined);
            panic!("D16 RESET disposition: {elapsed}");
        });
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.released, flow.joined, flow.f), (1, 0, 3, 1));
        assert!(flow.audit);
        let ledger = owner.ledger();
        assert!(ledger.quarantined);
        let first = ledger.first_failure.as_ref().expect("D16 reader original IO failure");
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        assert_eq!(first.facts().origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
        assert!(first.facts().opaque && first.facts().source_content_omitted);
        assert!(first.text_bytes().is_empty());
        let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
        assert!(same_budget);
        let charged = (queue.queued_bytes, queue.leased_bytes, queue.reserved_bytes);
        if loaded {
            assert!(queue.queued_bytes + queue.leased_bytes > 0);
            assert_eq!(queue.reserved_bytes, 0);
            assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
        } else {
            assert_eq!(charged, (0, 0, 0));
        }
        assert!(budget.total_bytes() <= budget.capacity_bytes);
        (
            first as *const _ as usize,
            format!("{:?}", first.facts()),
            first.text_bytes().to_vec(),
            charged,
        )
    }).await;

    if let Err(error) = scenario {
        if let Some(peer) = peer.take() {
            peer.close(0u32.into(), b"done");
        }
        server.close(0u32.into(), b"done");
        std::panic::resume_unwind(error);
    }

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let mut observer = Box::pin(handle.observe_shutdown());
        let phase =
            tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
                tokio::select! {
                    phase = observer.as_mut() => phase,
                    report = service.run() => panic!("fatal D16 falsely settled: {report:?}"),
                }
            })
            .await
            .unwrap();
        assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
        if phase == Phase::Incomplete {
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
        if loaded && tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("fatal D16 falsely settled before deadline: {report:?}"),
            }
        }
        if loaded {
            assert!(tokio::time::Instant::now() >= original_deadline);
        }
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
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.w, flow.f, flow.running), (1, 0, 1, 0));
        assert_eq!((flow.published, flow.released, flow.joined), (1, 0, 3));
        assert!(flow.audit);
        let ledger = owner.ledger();
        assert!(ledger.quarantined);
        let first = ledger.first_failure.as_ref().unwrap();
        let (queue, budget, same_budget) = owner.flow_d16_physical_snapshots().unwrap();
        assert!(same_budget);
        let charged = (queue.queued_bytes, queue.leased_bytes, queue.reserved_bytes);
        if loaded {
            assert!(queue.queued_bytes + queue.leased_bytes > 0);
            assert_eq!(queue.reserved_bytes, 0);
            assert!(budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes);
        }
        assert!(budget.total_bytes() <= budget.capacity_bytes);
        (
            first as *const _ as usize,
            format!("{:?}", first.facts()),
            first.text_bytes().to_vec(),
            charged,
        )
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let first = scenario.unwrap_or_else(|error| std::panic::resume_unwind(error));
    let after = cleanup.unwrap_or_else(|error| std::panic::resume_unwind(error));
    assert_eq!(first, after);
    let physical = backing.as_ref().unwrap().snapshot();
    assert!(!physical.quarantined);
    assert!(drained.unwrap());
}
