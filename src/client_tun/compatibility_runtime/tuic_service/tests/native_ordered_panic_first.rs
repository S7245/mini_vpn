//! Real Service/Quinn ordered pump: invoke original Control unwind before RESET.
//! The trigger is test-only; Root, roles, provider, F and parent clock are real.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, config, Apps};
use std::sync::atomic::{AtomicUsize, Ordering};

struct OriginalPanic(Arc<AtomicUsize>);
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_original_pump_unwind_is_root_first_and_keeps_charged_f() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::ChunkPump);
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
    let drops = Arc::new(AtomicUsize::new(0));
    let mut peer = None;
    let mut send = None;
    let mut held_recv = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (mut stream, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream.write_all(b"panic-first").await.unwrap();
                        send = Some(stream);
                        held_recv = Some(recv);
                    };
                    let app_open = async {
                        while app.tcp_reply != b"panic-first" {
                            app.turn().await;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("ordered Service ended before original fault: {report:?}"),
            }
        }).await.unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert_eq!(owner.flow_observation().unwrap().running, 5);
        owner
            .unwind_original_native_pump_for_test(Box::new(OriginalPanic(drops.clone())))
            .unwrap_or_else(|_| panic!("original ordered pump Control required"));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let mut stream = send.take().unwrap();
        stream.reset(77u32.into()).unwrap();
        drop(stream);
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                app.turn().await;
                let flow = owner.flow_observation().unwrap();
                let ledger = owner.ledger();
                if flow.joined == 5 && ledger.first_failure.is_some() {
                    assert_eq!((flow.f, flow.released), (1, 0));
                    assert!(flow.audit);
                    let first = ledger.first_failure.as_ref().unwrap();
                    assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Panic);
                    assert_eq!(first.facts().origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap();
        (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec())
    }).await;

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let observed = owner.flow_observation().unwrap();
        assert_eq!((observed.f, observed.released, observed.joined), (1, 0, 5));
        if tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("held original provider falsely settled: {report:?}"),
            }
        }
        assert!(tokio::time::timeout(Duration::from_millis(100), service.run()).await.is_err());
        assert!(tokio::time::Instant::now() >= original_deadline);
        assert!(service.completion.is_none());
        assert_eq!(handle.phase(), Phase::Quarantined);
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.f, flow.released, flow.joined), (1, 0, 5));
        assert!(owner.ledger().quarantined);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap();
        let identity = (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec());
        identity
    }).await;
    drop(held_recv.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .unwrap()
    );
    let original = scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    let final_first = cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(original, final_first);
}
