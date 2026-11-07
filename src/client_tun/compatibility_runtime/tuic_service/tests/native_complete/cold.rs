//! Stop from an actual Native carrier probe before the original publication.
//! Real Service, AUTH/CONNECT, App smoltcp and Device; the reader decorator only
//! adds a stop/lock observation and delegates to the actual transport reader.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_probe_stop_disposes_carrier_without_spawning_roles() {
    cold_case(0).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_wrong_epoch_with_stop_retains_original_carrier() {
    cold_case(1).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_known_full_preserves_typed_refusal_and_disposes_originals() {
    cold_case(2).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_stop_after_roles_reclaims_unstarted_package_without_join() {
    cold_case(3).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_probe_panic_retains_original_and_two_opaque_fields() {
    cold_case(4).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_reader_drop_panic_retains_original_writer() {
    cold_case(5).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_full_then_drop_panic_keeps_capacity_first() {
    cold_case(6).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_lost_turn_retains_originals_without_join() {
    cold_case(7).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_stop_with_existing_q_retains_originals() {
    cold_case(8).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_ordered_stop_disposes_unstarted_pump() {
    cold_case(9).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_cold_ordered_after_roles_disposes_unstarted_pump() {
    cold_case(10).await;
}
async fn cold_case(mode: u8) {
    let corrupt_slot = mode == 1;
    let identity = PublicCa::new();
    let server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: (mode < 9).then_some("1"),
        tuic_native_ordered: (mode >= 9).then_some("1"),
        ..Default::default()
    }));
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy)
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(32 << 20), 64)
        .unwrap();
    runtime.tun_mtu = 1500;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    app.sent_udp = true;
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            Ok::<_, ClientError>(LoopbackTunDevice::native_with_factory(
                64,
                1500,
                &meter.unwrap(),
                || Ok((inbound, outbound)),
            )?)
        },
        Configuration {
            tuic: cfg.clone(),
            composition: (),
        },
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let mut peer = None;
    let mut observed = None;
    let mut owner = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    // No App packet has yet entered the original Service.
                    let control = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                    let probe = control.cold_stop_probe(mode, budget.clone());
                    owner = Some(control.clone());
                    observed = Some(probe.clone());
                    let remote_close = async {
                        let (mut send, mut recv) = remote.accept_bi().await.unwrap();
                        let header = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        let actual = recv.read_to_end(1024).await.unwrap();
                        assert_eq!(actual, header, "no escrow DATA may publish after cold stop");
                        let _ = send.finish();
                        drop((send, recv));
                    };
                    let app_open = async {
                        while probe.fired() == 0 {
                            app.turn().await;
                            tokio::task::yield_now().await;
                        }
                        if mode == 2 {
                            assert!(!budget.quarantined(), "known Full must not turn into Q");
                            assert_eq!(control.ledger().first_failure.as_ref().unwrap().facts().io_kind, Some(std::io::ErrorKind::WouldBlock));
                            assert_eq!(probe.dropped(), 1, "cold rejected original reader was actually disposed");
                            probe.release_pressure();
                        }
                    };
                    if corrupt_slot || (4..=8).contains(&mode) { app_open.await; }
                    else { tokio::join!(remote_close, app_open); }
                } => {},
                report = service.run() => {
                    assert!(observed.as_ref().is_some_and(|p| p.fired() == 1), "Service ended before actual probe: {report:?}");
                }
            }
        }).await.unwrap();
    }).await;
    handle.request_stop();
    let deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, deadline, None).await;
            return;
        }
        if corrupt_slot || (4..=8).contains(&mode) {
            fixture::observe_failed_native_stop(service.run(), &handle, deadline, None).await;
            let control = owner.as_ref().unwrap();
            eprintln!("[N1-cold-wrong-epoch] budget={} Q={} reader_drops={} flow={:?}",
                budget.used(), budget.quarantined(), observed.as_ref().unwrap().dropped(), control.flow_observation());
            assert!(budget.quarantined() && budget.used() > 0);
            let probe = observed.as_ref().unwrap();
            assert_eq!(probe.dropped(), usize::from(matches!(mode, 5 | 6)), "unknown return preserves every not-yet-disposed original");
            assert_eq!(probe.writer_drops(), 0);
            assert_eq!(probe.payload_drops(), 0);
            if matches!(mode, 7 | 8) { probe.assert_retained_originals(control); }
            if (4..=6).contains(&mode) {
                let site = usize::from(matches!(mode, 5 | 6));
                assert_eq!(control.cold_original_panic_site(), Some((site, probe.payload_ptr(site))));
                let ledger = control.ledger();
                let facts = ledger.first_failure.as_ref().unwrap().facts();
                if mode == 6 {
                    eprintln!("[N1-cold-failure-census] additional={} egress={} first_egress={:?}", ledger.additional_failures, ledger.egress_failures, ledger.first_egress_io.as_ref().map(|e| (e.kind(), e.raw_os_error())));
                    assert_eq!(facts.io_kind, Some(std::io::ErrorKind::WouldBlock), "cold Capacity precedes opaque disposal");
                    assert_eq!(ledger.additional_failures, 1, "one later actual Drop panic, no duplicate JOIN report");
                } else {
                    assert_eq!(facts.class, crate::shared::errors::FailureClass::Panic);
                }
            }
            assert_eq!(control.flow_observation().unwrap().joined, 0);
            return;
        }
        let report = tokio::time::timeout_at(deadline, service.run()).await;
        if report.is_err() {
            let control = owner.as_ref().unwrap();
            eprintln!("[N1-cold-stop] phase={:?} budget={} Q={} flow={:?} contracts={:?} first={:?} original={:?} session={:?} probe_dropped={}",
                handle.phase(), budget.used(), budget.quarantined(), control.flow_observation(),
                (control.flow_contracts(), control.flow_debug()), fixture::first_failure_identity(control), deadline,
                control.ledger().shutdown_deadline, observed.as_ref().unwrap().dropped());
        }
        let report = report.unwrap();
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.session, if mode == 2 { SessionDeviceState::DeviceFailedSettled } else { SessionDeviceState::DeviceSettled });
        let control = owner.as_ref().unwrap();
        let probe = observed.as_ref().unwrap();
        assert_eq!((probe.fired(), probe.dropped(), probe.writer_drops()), (1, 1, 1));
        if mode == 2 {
            assert_eq!(control.ledger().first_failure.as_ref().unwrap().facts().io_kind, Some(std::io::ErrorKind::WouldBlock));
        } else {
            assert_eq!(control.ledger().shutdown_deadline, probe.deadline());
            assert!(control.ledger().first_failure.is_none());
        }
        let flow = control.flow_observation().unwrap();
        assert_eq!((flow.factory_started, flow.published, flow.joined, flow.released, flow.live), (1, 0, 0, 1, 0));
        assert_eq!((flow.w, flow.f, flow.escrow_bytes, flow.running), (0, 0, 0, 0));
        assert!(flow.audit);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }).await;
    drop((service, handle, owner, observed, peer));
    server.close(0u32.into(), b"cold probe complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    scenario.unwrap_or_else(|p| std::panic::resume_unwind(p));
    cleanup.unwrap_or_else(|p| std::panic::resume_unwind(p));
}
