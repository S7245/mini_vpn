//! Real Service shutdown-census timing fault injection.
//! Owns only the test's stored-count corruption and observations; production
//! ownership and settlement remain with the original SessionControl/Root.
//! The entry test keeps a real Native flow charged across two driver censuses.

use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride};
use fixture::{auth, config, Apps};
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "current_thread")]
async fn real_driver_rechecks_root_mismatch_between_shutdown_censuses() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeChunkPump
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let service_control = service.control.clone();
    let mut peer = None;
    let mut held_send = None;

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
                        apps.turn().await;
                        let owner = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        let flow = owner.flow_observation().unwrap();
                        if flow.published == 1 && flow.running == 4 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                tokio::join!(remote_open, app_open);
            } => {},
            report = service.run() => panic!("service ended before pending read: {report:?}"),
        }
    }).await.unwrap();

    let owner = service_control
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    assert!(owner.ledger().first_failure.is_none());
    handle.request_stop();
    crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        loop {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
            let flow = owner.flow_observation().unwrap();
            if flow.joined == 4 && owner.shutdown_census_runs.load(Ordering::Relaxed) > 0 {
                assert_eq!((flow.f, flow.released), (1, 0));
                assert!(flow.audit);
                break;
            }
        }
    })
    .await
    .unwrap();
    let before = owner.shutdown_census_runs.load(Ordering::Relaxed);
    assert!(!owner.ledger().quarantined());
    assert_eq!(owner.opens().live, 1);

    owner.opens().live += 1;
    assert_eq!(owner.shutdown_census_runs.load(Ordering::Relaxed), before);
    assert!(!owner.ledger().quarantined());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            assert!(
                tokio::time::timeout(Duration::from_millis(1), service.run())
                    .await
                    .is_err()
            );
            if owner.shutdown_census_runs.load(Ordering::Relaxed) > before {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(owner.ledger().quarantined());
    assert!(owner.ledger().first_failure.is_none());
    assert_eq!(owner.opens().live, 2, "uncertain total stays charged");
    assert_eq!(
        (
            owner.flow_observation().unwrap().f,
            owner.flow_observation().unwrap().released
        ),
        (1, 0)
    );
    assert!(service.completion.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), service.run())
            .await
            .is_err()
    );
    assert!(owner.ledger().quarantined());

    drop(held_send);
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
}
