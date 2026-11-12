//! Real Quinn DATA rejected by the original published D16 queue.
//! Owns only a test-only close at the queue seam after the original reader has
//! reserved capacity. Root owns the reader, backing, roles, F and deadline.
//! The test below checks this single closed-commit disposition.

use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_published_closed_commit_retains_real_quinn_backing() {
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

    let scenario = capture(async {
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
                            let owner = saved.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                            if owner.and_then(|owner| owner.flow_observation())
                                .is_some_and(|flow| flow.published == 1 && flow.running == 3) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("D16 service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let queue = owner.flow_d16_queue_for_test().unwrap();
        let backing = owner.flow_d16_backing_meter().unwrap();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                let (physical, budget, same) = owner.flow_d16_physical_snapshots().unwrap();
                if physical.reserved_bytes > 0 {
                    assert!(same);
                    assert_eq!(physical.queued_bytes + physical.leased_bytes, 0);
                    assert!(budget.reserved_bytes >= physical.reserved_bytes);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        assert!(queue.close_for_terminal_and_mark_ready("root", "test_closed_after_reservation").await);
        held_send.as_mut().unwrap().write_all(&[0x5a; 8192]).await.unwrap();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 3 && flow.running == 0 {
                    break;
                }
            }
        }).await.unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.published, flow.released, flow.joined, flow.f), (1, 1, 0, 3, 1));
        assert_eq!(owner.flow_d16_close_reason_for_test(), Some(("remote_to_local", "d16_queue_commit_failed")));
        assert!(flow.audit && owner.ledger().quarantined());
        assert!(owner.ledger().first_failure.is_none());
        let (physical, budget, same) = owner.flow_d16_physical_snapshots().unwrap();
        assert!(same);
        assert_eq!((physical.queued_bytes, physical.leased_bytes, physical.reserved_bytes), (0, 0, 0));
        assert!(budget.total_bytes() <= budget.capacity_bytes);
        assert!(backing.snapshot().payload_bytes > 0);
        (queue, backing)
    }).await;

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let mut observer = Box::pin(handle.observe_shutdown());
        let phase = tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
            tokio::select! {
                phase = observer.as_mut() => phase,
                report = service.run() => panic!("rejected D16 falsely settled: {report:?}"),
            }
        }).await.unwrap();
        assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
        if phase == Phase::Incomplete {
            tokio::time::timeout(Duration::from_secs(1), async {
                while handle.phase() != Phase::Quarantined {
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.unwrap();
        }
        if tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("rejected D16 falsely settled before deadline: {report:?}"),
            }
        }
        assert!(tokio::time::Instant::now() >= original_deadline);
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert!(service.completion.is_none());
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.w, flow.f, flow.running, flow.joined), (1, 0, 1, 0, 3));
        assert_eq!(owner.flow_d16_close_reason_for_test(), Some(("remote_to_local", "d16_queue_commit_failed")));
        assert!(flow.audit && owner.ledger().quarantined());
        assert!(owner.ledger().first_failure.is_none());
        let (physical, budget, same) = owner.flow_d16_physical_snapshots().unwrap();
        assert!(same);
        assert_eq!((physical.queued_bytes, physical.leased_bytes, physical.reserved_bytes), (0, 0, 0));
        assert!(budget.total_bytes() <= budget.capacity_bytes);
    }).await;
    drop(held_send.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let (queue, backing) = scenario.unwrap_or_else(|error| std::panic::resume_unwind(error));
    cleanup.unwrap_or_else(|error| std::panic::resume_unwind(error));
    let physical = queue.snapshot_now();
    assert_eq!(
        (
            physical.queued_bytes,
            physical.leased_bytes,
            physical.reserved_bytes
        ),
        (0, 0, 0)
    );
    assert!(backing.snapshot().payload_bytes > 0);
    assert!(drained.unwrap());
}
