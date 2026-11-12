//! Original production service, real QUIC and actually pumped device fault gates.
use super::*;
use device::{device, Gate};
use fixture::{auth, capture, config};
use std::future::Future;
use std::sync::atomic::Ordering;
use std::task::Poll;

fn server() -> quinn::Endpoint {
    quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap()
}
fn gate() -> Arc<Gate> {
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    gate
}
async fn peer<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    server: &quinn::Endpoint,
    cfg: &TuicClientConfig,
) -> quinn::Connection {
    tokio::time::timeout(Duration::from_secs(3),async {
        tokio::select! {
            p = async { let p=server.accept().await.unwrap().await.unwrap();auth(&p,cfg).await;p } => p,
            r = service.run() => panic!("premature completion {r:?}"),
        }
    }).await.unwrap()
}
async fn until<F: Future<Output = Completion>>(service: &mut Service<F>, ready: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            _ = async { while !ready() { tokio::time::sleep(Duration::from_millis(1)).await; } } => {},
            r = service.run() => panic!("resource gate bypassed {r:?}"),
        }
    }).await.unwrap();
}
async fn close(mut server: quinn::Endpoint, peers: Vec<quinn::Connection>) {
    for p in peers {
        p.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn resume_error_retains_original_error_and_waits_real_device_before_node() {
    let server = server();
    let cfg = config(server.local_addr().unwrap());
    let gate = gate();
    gate.resume_error.store(true, Ordering::Release);
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peers = Vec::new();
    let scenario = capture(async {
        peers.push(peer(&mut service, &server, &cfg).await);
        until(&mut service, || handle.phase() == Phase::Running).await;
        assert!(handle.request_rebuild());
        peers.push(peer(&mut service, &server, &cfg).await);
        until(&mut service, || gate.polls.load(Ordering::Acquire) > 0).await;
        assert_eq!(gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(gate.resumes.load(Ordering::Acquire), 1);
        assert_eq!(gate.resumed.load(Ordering::Acquire), 0);
        assert_eq!(gate.joins.load(Ordering::Acquire), 0);
        assert_eq!(gate.drops.load(Ordering::Acquire), 0);
        assert!(
            peers[1].close_reason().is_none(),
            "new Node must stay while original device is unjoined"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), peers[1].accept_bi())
                .await
                .is_err()
        );
        assert!(!handle.request_rebuild());
    })
    .await;
    handle.request_stop();
    gate.open();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert!(report.device_settled());
        assert!(report.device_failed());
        assert_eq!(gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(gate.drops.load(Ordering::Acquire), 1);
        let completion = service.completion.as_ref().unwrap();
        assert_eq!(
            completion
                .control
                .ledger()
                .first_failure
                .as_ref()
                .unwrap()
                .facts()
                .raw_os_code,
            Some(13)
        );
        completion
            ._retired
            .as_ref()
            .unwrap()
            .node
            .assert_service_clean_for_test();
        completion
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
    })
    .await;
    let drained = capture(close(server, peers)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = drained {
        std::panic::resume_unwind(e)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resume_secondary_panic_retains_original_paused_device_and_nodes_in_q() {
    let server = server();
    let cfg = config(server.local_addr().unwrap());
    let gate = gate();
    gate.resume_panic.store(true, Ordering::Release);
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peers = Vec::new();
    let scenario = capture(async {
        peers.push(peer(&mut service, &server, &cfg).await);
        until(&mut service, || handle.phase() == Phase::Running).await;
        let original = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(handle.request_rebuild());
        peers.push(peer(&mut service, &server, &cfg).await);
        until(&mut service, || handle.phase() == Phase::Quarantined).await;
        assert!(original.ledger().quarantined());
        assert_eq!(
            original
                .ledger()
                .first_failure
                .as_ref()
                .unwrap()
                .facts()
                .origin
                .phase,
            FailurePhase::DeviceShutdown
        );
        assert!(original.business_complete.load(Ordering::Acquire));
        assert!(!original.driver_complete.load(Ordering::Acquire));
        assert_eq!(gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(gate.resumes.load(Ordering::Acquire), 1);
        assert_eq!(gate.resumed.load(Ordering::Acquire), 0);
        for _ in 0..3 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
            assert_eq!(gate.joins.load(Ordering::Acquire), 0);
            assert_eq!(gate.drops.load(Ordering::Acquire), 0);
            assert!(peers[1].close_reason().is_none());
            assert!(!handle.request_rebuild());
        }
    })
    .await;
    // Forced shell destruction is explicitly not asynchronous Node/device cleanup.
    drop(service);
    let drained = capture(close(server, peers)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = drained {
        std::panic::resume_unwind(e)
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn cut_cancel_keeps_original_first_deadline_for_already_waiting_observer() {
    let server = server();
    let cfg = config(server.local_addr().unwrap());
    let gate = gate();
    gate.hold_cut.store(true, Ordering::Release);
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peers = Vec::new();
    let mut paused = false;
    let scenario = capture(async {
        peers.push(peer(&mut service, &server, &cfg).await);
        until(&mut service, || handle.phase() == Phase::Running).await;
        let original = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(handle.request_rebuild());
        until(&mut service, || gate.cuts.load(Ordering::Acquire) == 1).await;
        let first = original.ledger().shutdown_deadline.unwrap();
        for _ in 0..3 {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), service.run())
                    .await
                    .is_err()
            );
        }
        assert_eq!(original.ledger().shutdown_deadline, Some(first));
        tokio::time::pause();
        paused = true;
        tokio::time::advance(
            first.saturating_duration_since(tokio::time::Instant::now()) - Duration::from_secs(1),
        )
        .await;
        let observer = handle.observe_shutdown();
        tokio::pin!(observer);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        until(&mut service, || gate.polls.load(Ordering::Acquire) > 0).await;
        let adopted = *saved.deadline.lock().unwrap();
        assert_eq!(gate.resumes.load(Ordering::Acquire), 0);
        assert_eq!(gate.joins.load(Ordering::Acquire), 0);
        assert_eq!(gate.drops.load(Ordering::Acquire), 0);
        assert!(peers[0].close_reason().is_none());
        assert!(!handle.request_rebuild());
        tokio::time::advance(first.saturating_duration_since(tokio::time::Instant::now())).await;
        let observed = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx))).await;
        assert_eq!(
            observed,
            Poll::Ready(Phase::Incomplete),
            "original={first:?}, stored={adopted:?}"
        );
        assert_eq!(adopted, Some(first));
        assert_eq!(original.ledger().shutdown_deadline, Some(first));
    })
    .await;
    if paused {
        tokio::time::resume();
    }
    handle.request_stop();
    gate.open();
    gate.release_cut.store(true, Ordering::Release);
    gate.cut_wake.notify_waiters();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(report.rebuilds, 0);
        assert!(report.device_settled());
        assert_eq!(gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(gate.drops.load(Ordering::Acquire), 1);
        assert!(service.completion.as_ref().unwrap()._retired.is_none());
    })
    .await;
    let drained = capture(close(server, peers)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = drained {
        std::panic::resume_unwind(e)
    }
}

pub(super) fn rejected_alpn() -> quinn::ServerConfig {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(dir.join("server-cert.pem")).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(dir.join("server-key.pem")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    tls.alpn_protocols = vec![b"n1-controlled-refusal".to_vec()];
    quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn successor_real_alpn_rejection_never_resumes_or_builds_third_node() {
    let server = server();
    let cfg = config(server.local_addr().unwrap());
    let gate = gate();
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peers = Vec::new();
    let scenario = capture(async {
        peers.push(peer(&mut service, &server, &cfg).await);
        until(&mut service, || handle.phase() == Phase::Running).await;
        server.set_server_config(Some(rejected_alpn()));
        assert!(handle.request_rebuild());
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                denied=async {server.accept().await.unwrap().await}=>assert!(denied.is_err()),
                r=service.run()=>panic!("device must retain original gate {r:?}"),
            }
        })
        .await
        .unwrap();
        until(&mut service, || gate.polls.load(Ordering::Acquire) > 0).await;
        assert_eq!(gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(gate.resumes.load(Ordering::Acquire), 0);
        assert_eq!(gate.joins.load(Ordering::Acquire), 0);
        assert_eq!(gate.drops.load(Ordering::Acquire), 0);
        assert!(!handle.request_rebuild());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    })
    .await;
    handle.request_stop();
    gate.open();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert!(report.device_settled());
        assert_eq!(report.node, Some(NodeShutdown::FailedSettled));
        assert_eq!(gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(gate.drops.load(Ordering::Acquire), 1);
        let completion = service.completion.as_mut().unwrap();
        completion
            ._retired
            .as_ref()
            .unwrap()
            .node
            .assert_service_clean_for_test();
        assert!(completion
            ._node
            .as_mut()
            .unwrap()
            .controlled_rebuild_disposition()
            .configuration_rejected_for_test());
    })
    .await;
    let drained = capture(close(server, peers)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e)
    }
    if let Err(e) = drained {
        std::panic::resume_unwind(e)
    }
}
