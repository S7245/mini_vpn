use super::*;
use crate::tuic::node::{NodeHost, NodeShutdown, NodeStartup};

#[tokio::test(flavor = "current_thread")]
async fn actual_ordered_cold_full_keeps_original_fail_deadline_and_retirement_while_d16_needs_no_slots()
 {
    for (direct, pressure) in [(false, true), (true, true), (false, false)] {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec());
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()),
        );
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        // Public trust certificate only; ephemeral private key never leaves memory.
        let ca = std::env::temp_dir().join(format!(
            "n1-ordered-ca-{}-{}.pem",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&ca, identity.cert.pem()).unwrap();
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            tuic_native_ordered: (!direct).then_some("1"),
            d16_byte_owned: direct.then_some("1"),
            ..Default::default()
        }));
        let mut cfg = TuicClientConfig::from_sources(
            Some(&server.local_addr().unwrap().to_string()),
            Some("00000000-0000-0000-0000-000000000000"),
            Some("local-test-only"),
            Some("localhost"),
            Some(ca.to_str().unwrap()),
            Some("h3"),
        )
        .unwrap()
        .with_relay_policy(policy);
        cfg.tcp_pool = 1;
        let budget = bytes::BackingBudget::new(32 * 1024 * 1024).unwrap();
        let app_meter = bytes::BackingMeter::with_budget(&budget);
        let app_storage =
            bytes::Bytes::copy_from_slice_metered(b"live App owner", &app_meter).unwrap();
        let mut host =
            NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
        let mut upstream = None;
        let mut peer = None;
        let mut attempt = None;
        let mut held = None;
        let (source_audit, _registration) =
            crate::tuic::tcp_readers::tests::native_prepared::register();
        let scenario = captured(async {
            let (started, remote) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(host.start(), async {
                    server.accept().await.unwrap().await.unwrap()
                })
            })
            .await
            .unwrap();
            assert_eq!(started, NodeStartup::Ready);
            peer = Some(remote);
            let p = peer.as_ref().unwrap();
            let mut auth = p.accept_uni().await.unwrap();
            let actual = auth.read_to_end(64).await.unwrap();
            let mut token = [0; 32];
            p.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
                .unwrap();
            assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
            drop(auth);
            upstream = Some(host.data().unwrap());
            let u = upstream.as_ref().unwrap();
            let meter = u.endpoint.receive_backing_meter().unwrap();
            assert!(!meter.same_authority(&app_meter));
            assert_eq!(meter.budget_limit(), Some(budget.limit()));
            let limit = Instant::now() + TUIC_OPEN_TIMEOUT;
            attempt = Some(
                create(
                    u.clone(),
                    TcpOpenRequest {
                        target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
                        deadline: limit,
                    },
                )
                .unwrap(),
            );
            let a = attempt.as_mut().unwrap();
            tokio::time::timeout_at(limit.into(), async {
                while a.view().phase != TcpOpenPhase::Ready {
                    a.step(
                        &mut Context::from_waker(std::task::Waker::noop()),
                        Instant::now(),
                    );
                    assert!(a.failure().is_none());
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            let (mut tx, mut rx) = p.accept_bi().await.unwrap();
            let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
            let expected = encode_connect(&target);
            let mut actual = vec![0; expected.len()];
            rx.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, expected);
            if pressure {
                held = Some(budget.try_reserve(budget.limit() - budget.used()).unwrap());
            }
            let now = Instant::now();
            let carrier = a.take_carrier(now);
            if direct || !pressure {
                if direct {
                    assert!(matches!(carrier, Some(OpenedTcpRelay::NativeByteOwned(_))));
                    drop(carrier);
                } else {
                    let Some(OpenedTcpRelay::Native(native)) = carrier else {
                        panic!("actual prepared carrier")
                    };
                    let original = native
                        .try_start_legacy()
                        .err()
                        .expect("cold hard rejection");
                    assert!(original.has_prepared_pump());
                    for _ in 0..16 {
                        tokio::task::yield_now().await;
                    }
                    assert_eq!(
                        crate::tuic::tcp_readers::tests::native_prepared::cold_counts(
                            &source_audit
                        ),
                        (1, 0, 0)
                    );
                    assert!(
                        original
                            .reader
                            .receive_backing_meter()
                            .unwrap()
                            .same_authority(&meter)
                    );
                    drop(original);
                }
                a.cancel(TcpOpenCancel::Shutdown, now + TUIC_OPEN_TIMEOUT);
            } else {
                assert!(carrier.is_none());
                assert!(matches!(a.state, State::Stream(_)));
                assert_eq!(first(a).1, io::ErrorKind::WouldBlock);
                let Some(ClientError::Io(error)) = a.failure() else {
                    panic!("original typed cold error")
                };
                assert_eq!(
                    error
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<quinn::owned_events::AdmissionError>()
                        .unwrap()
                        .0,
                    bytes::MeteredCopyError::BudgetFull
                );
            }
            assert_eq!(a.request.deadline, limit);
            let original = first(a);
            let close_limit = a.settle_deadline.unwrap();
            assert_eq!(close_limit, now + TUIC_OPEN_TIMEOUT);
            a.cancel(
                TcpOpenCancel::Shutdown,
                close_limit + Duration::from_secs(20),
            );
            assert_eq!(a.settle_deadline, Some(close_limit));
            assert_eq!(first(a), original);
            assert_eq!(u.tcp_pool_admission.active_total(), 1);
            assert!(!budget.quarantined());
            drop(held.take());
            a.settle(
                &mut Context::from_waker(std::task::Waker::noop()),
                Instant::now(),
                close_limit,
            );
            assert_eq!(a.view().phase, TcpOpenPhase::Retiring);
            assert_eq!(u.tcp_pool_admission.active_total(), 1);
            tokio::time::timeout_at(close_limit.into(), async {
                tokio::join!(
                    async {
                        let code = tx.stopped().await.unwrap().unwrap();
                        let _ = rx.read_to_end(0).await;
                        tx.reset(code).unwrap();
                    },
                    async {
                        while a.view().phase != TcpOpenPhase::Settled {
                            a.settle(
                                &mut Context::from_waker(std::task::Waker::noop()),
                                Instant::now(),
                                close_limit,
                            );
                            assert_ne!(a.view().phase, TcpOpenPhase::Quarantined);
                            assert_eq!(a.settle_deadline, Some(close_limit));
                            assert_eq!(first(a), original);
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                );
            })
            .await
            .unwrap();
            assert_eq!(u.tcp_pool_admission.active_total(), 0);
            assert!(!budget.quarantined());
        })
        .await;
        let cleanup = captured(async {
            drop(held.take());
            drop(attempt.take());
            drop(upstream.take());
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::RunningResourcesSettled | NodeShutdown::FailedSettled
            ));
            drop(host);
            assert!(
                budget.used() > 0,
                "actual Node settled while original App backing remains"
            );
            assert!(app_meter.snapshot().physical_bytes > 0);
            if let Some(peer) = peer.take() {
                peer.close(0u32.into(), b"done");
            }
            server.close(0u32.into(), b"done");
            tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
                .await
                .unwrap();
            drop(server);
            std::fs::remove_file(&ca).unwrap();
        })
        .await;
        if let Err(error) = scenario {
            std::panic::resume_unwind(error);
        }
        if let Err(error) = cleanup {
            std::panic::resume_unwind(error);
        }
        drop(app_storage);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
