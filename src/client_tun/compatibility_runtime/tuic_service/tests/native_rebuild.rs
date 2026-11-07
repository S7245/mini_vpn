//! N1 stage B first slice: one explicit controlled rebuild of a hard Native
//! Service through the original saved driver (`rebuild::drive`). Owns no
//! production authority; asserts the success disposition only: old generation
//! settles business-first, one successor Node on the same shared budget carries
//! new TCP + UDP, no third generation, and final stop returns the budget to zero.
use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride};
use fixture::{auth, capture, config, window, Apps, ServiceProfile};

pub(super) fn native_ordered_chunk() -> Arc<RelayFamilyPolicy> {
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        ..Default::default()
    }));
    assert_eq!(policy.tuic_native_override(), TuicNativeOverride::NativeOrderedPump);
    policy
}

pub(super) async fn serve_generation(peer: &quinn::Connection, apps: &mut Apps) {
    let target = |port| TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, port)));
    let (tcp_target, udp_target) = (target(10000), target(10001));
    let remote = async {
        let tcp = async {
            let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
            let mut want = encode_connect(&tcp_target);
            want.extend_from_slice(b"service-tcp");
            let mut actual = vec![0; want.len()];
            rx.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, want);
            tx.write_all(b"tcp-reply").await.unwrap();
            tx.finish().unwrap();
            (tx, rx)
        };
        let udp = async {
            let actual = peer.read_datagram().await.unwrap();
            let assoc = u16::from_be_bytes([actual[2], actual[3]]);
            assert_eq!(&actual[..], encode_packet(assoc, &udp_target, b"service-udp"));
            peer.send_datagram(encode_packet(assoc, &udp_target, b"udp-reply").into())
                .unwrap();
        };
        tokio::join!(tcp, udp)
    };
    let app = async {
        while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
            apps.turn().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let (streams, ()) = tokio::join!(remote, app);
    drop(streams);
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_controlled_rebuild_restores_new_tcp_and_udp_on_shared_budget() {
    let profile = ServiceProfile::new(true);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = native_ordered_chunk();
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut old_control: Option<Arc<SessionControl>> = None;
    let mut accepted_used = Vec::new();
    let scenario = capture(async {
        for round in 0..2 {
            window(Duration::from_secs(5), async {
                tokio::select! {
                    _ = async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        accepted_used.push(budget.used());
                        auth(&peer, &cfg).await;
                        if round == 1 {
                            // Native rebuild is serial (C+D+H): the original
                            // business settled before the successor Node existed.
                            let old = old_control.as_ref().unwrap();
                            assert!(old.business_complete.load(Ordering::Acquire),
                                "successor AUTH only after original business settlement");
                            let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            assert!(!Arc::ptr_eq(old, &current), "successor owns a new session");
                            apps.restart();
                        }
                        serve_generation(&peer, &mut apps).await;
                        peers.push(peer);
                    } => {},
                    report = service.run() => panic!("Native Service ended in round {round}: {report:?}"),
                }
            })
            .await
            .unwrap_or_else(|_| panic!("round {round} DATA within the original 5s window"));
            let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert!(current.ledger().first_failure.is_none());
            assert!(!budget.quarantined());
            if round == 0 {
                old_control = Some(current);
                assert!(handle.request_rebuild(), "first explicit rebuild accepted");
            } else {
                let old = old_control.as_ref().unwrap();
                assert!(!Arc::ptr_eq(old, &current), "one distinct successor generation");
                assert!(old.business_complete.load(Ordering::Acquire));
                assert_eq!(old.flow_observation().unwrap().live, 0);
                assert!(!handle.request_rebuild(), "no third generation");
                // I1/B3: the old protocol tier fully returned before the
                // successor charged anything; no cross-generation overlap.
                assert_eq!(accepted_used[1], accepted_used[0],
                    "successor at accept charges exactly the first generation's level");
            }
        }
    })
    .await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = window(Duration::from_secs(15), service.run()).await.unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.phase(), Phase::Complete);
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
            let completion = service.completion.as_ref().unwrap();
            let old = completion._retired.as_ref().unwrap();
            old.node.assert_service_clean_for_test();
            assert_eq!(old.control.state(), SessionDeviceState::BusinessSettled);
            assert!(!old.control.ledger().quarantined);
            let current = &completion.control;
            {
                let ledger = current.ledger();
                assert!(ledger.first_failure.is_none() && !ledger.quarantined);
            }
            assert!(peers[0].close_reason().is_some(), "old generation connection closed");
        }
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
}

/// B2 rejection: the successor's real handshake is refused (foreign ALPN). The
/// original generation stays settled and clean, the successor never publishes
/// DATA, no third Node is built, and the shared budget returns to zero.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_successor_alpn_rejection_never_publishes_or_builds_third_node() {
    let profile = ServiceProfile::new(true);
    let ServiceProfile::Hard(identity) = &profile else { unreachable!() };
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = native_ordered_chunk();
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut old_control: Option<Arc<SessionControl>> = None;
    let scenario = capture(async {
        window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    serve_generation(&peer, &mut apps).await;
                    peers.push(peer);
                } => {},
                report = service.run() => panic!("Native Service ended before first DATA: {report:?}"),
            }
        })
        .await
        .expect("first generation DATA within 5s");
        old_control = Some(saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap());
        server.set_server_config(Some(identity.server_config_with_alpn(b"n1-controlled-refusal")));
        assert!(handle.request_rebuild());
        // Frozen semantics (full_rejection.rs): the refused successor is the
        // last generation; with no device gate the saved driver completes on its
        // own. The typed refusal is owned by the Node disposition, not the ledger.
        let mut denied = None;
        let report = window(Duration::from_secs(5), async {
            let accept = async { server.accept().await.unwrap().await };
            tokio::pin!(accept);
            let mut run = std::pin::pin!(service.run());
            loop {
                tokio::select! {
                    result = &mut accept, if denied.is_none() => denied = Some(result.is_err()),
                    report = &mut run => break report,
                }
            }
        })
        .await
        .expect("refused successor settles within 5s");
        assert_eq!(denied, Some(true), "real successor handshake refused");
        report
    })
    .await;
    let cleanup = capture(async {
        let report = scenario.as_ref().ok().copied();
        let report = match report {
            Some(report) => report,
            None => {
                handle.request_stop();
                window(Duration::from_secs(15), service.run()).await.unwrap()
            }
        };
        assert!(report.device_settled() && !report.device_failed(), "{report:?}");
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::FailedSettled));
        assert_eq!(report.rebuilds, 1, "budget spent at old Allowed→Retired");
        assert_eq!(handle.phase(), Phase::Complete);
        assert!(!handle.request_rebuild(), "no third generation");
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        let completion = service.completion.as_mut().unwrap();
        let retired = completion._retired.as_ref().unwrap();
        assert!(Arc::ptr_eq(&retired.control, old_control.as_ref().unwrap()));
        retired.node.assert_service_clean_for_test();
        assert_eq!(retired.control.state(), SessionDeviceState::BusinessSettled);
        {
            let ledger = retired.control.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        }
        assert!(completion
            ._node
            .as_mut()
            .unwrap()
            .controlled_rebuild_disposition()
            .configuration_rejected_for_test());
        let current = &completion.control;
        assert!(!Arc::ptr_eq(current, old_control.as_ref().unwrap()), "successor session existed");
        {
            let ledger = current.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        }
        assert_eq!(current.flow_observation().map(|flow| flow.published).unwrap_or(0), 0,
            "refused successor never published DATA");
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
}

/// Bytes the cut Device keeps charged on the shared budget between the old
/// generation's drain and the successor's App cold (measured, self-checked).
pub(super) const DEVICE_HELD_AT_SUCCESSOR: usize = 83_255;

struct ColdGap {
    report: Report,
    successor_accepted: bool,
    held_at_successor_failure: Option<usize>,
    old: Arc<SessionControl>,
    successor: Arc<SessionControl>,
}

/// B3 pressure driver. After real first-generation DATA and the original
/// business settlement, a protocol-tier filler leaves exactly `gap` bytes of
/// App tier for everything the cut Device still holds plus the successor's App
/// cold. The filler is released as soon as the successor either records its
/// first failure or reaches the peer, so the terminal I6 check sees only the
/// Service's own resources. Then the Service is stopped and fully joined.
async fn rebuild_with_successor_cold_gap(gap: usize) -> ColdGap {
    let profile = ServiceProfile::new(true);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = native_ordered_chunk();
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let filler_bytes = budget.limit() - budget.protocol_reserve() - gap;
    let mut filler = None;
    let mut peers = Vec::new();
    let mut held_at_successor_failure = None;
    let mut successor_accepted = false;
    let scenario = capture(async {
        let peer = window(Duration::from_secs(5), async {
            tokio::select! {
                peer = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    serve_generation(&peer, &mut apps).await;
                    peer
                } => peer,
                report = service.run() => panic!("Native Service ended before first DATA: {report:?}"),
            }
        })
        .await
        .expect("first generation DATA within 5s");
        peers.push(peer);
        let old = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(handle.request_rebuild());
        let report = window(Duration::from_secs(5), async {
            let accept = async { server.accept().await.unwrap().await };
            tokio::pin!(accept);
            let mut run = std::pin::pin!(service.run());
            loop {
                tokio::select! {
                    biased;
                    connection = &mut accept, if !successor_accepted => {
                        successor_accepted = true;
                        drop(filler.take());
                        peers.push(connection.expect("admitted successor completes its handshake"));
                        handle.request_stop();
                    }
                    report = &mut run => break report,
                    () = tokio::task::yield_now() => {
                        if filler.is_none() && held_at_successor_failure.is_none() && !successor_accepted
                            && old.business_complete.load(Ordering::Acquire)
                        {
                            let meter = bytes::BackingMeter::with_protocol_budget(&budget);
                            filler = Some(meter.reserve_cold_layout(filler_bytes).unwrap()
                                .expect("hard budget charges the filler"));
                        }
                        let current = saved.session.lock().unwrap().as_ref().and_then(|w| w.upgrade());
                        let failed = current.filter(|c| !Arc::ptr_eq(c, &old))
                            .is_some_and(|c| c.ledger().first_failure.is_some());
                        if failed && filler.is_some() {
                            held_at_successor_failure = Some(budget.used() - filler_bytes);
                            drop(filler.take());
                        }
                    }
                }
            }
        })
        .await
        .expect("rebuild under App-cold pressure settles within 5s");
        let successor = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        (report, old, successor)
    })
    .await;
    drop(filler);
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let (report, old, successor) = scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
    assert!(!Arc::ptr_eq(&old, &successor), "a successor session was composed");
    assert_eq!(handle.phase(), Phase::Complete);
    assert!(!handle.request_rebuild(), "no third generation");
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    let completion = service.completion.as_ref().unwrap();
    let retired = completion._retired.as_ref().expect("old generation retired as Allowed");
    assert!(Arc::ptr_eq(&retired.control, &old));
    retired.node.assert_service_clean_for_test();
    assert_eq!(old.state(), SessionDeviceState::BusinessSettled);
    {
        let ledger = old.ledger();
        assert!(ledger.first_failure.is_none() && !ledger.quarantined);
    }
    assert!(!successor.ledger().quarantined);
    ColdGap { report, successor_accepted, held_at_successor_failure, old, successor }
}

fn assert_app_full_io(owner: &SessionControl, phase: crate::shared::errors::FailurePhase) {
    let ledger = owner.ledger();
    let facts = ledger.first_failure.as_ref().expect("typed successor admission failure").facts();
    assert_eq!(facts.class, crate::shared::errors::FailureClass::Io);
    assert_eq!(facts.io_kind, Some(std::io::ErrorKind::WouldBlock), "App-tier Full, not Q");
    assert_eq!(facts.origin.phase, phase);
}

/// B3 pressure: one byte short of the successor's App cold. The successor is
/// refused at its cold admission with a typed non-Q failure before any Node is
/// prepared; the old generation (whose protocol tier drained while the filler
/// held the App tier over its ceiling) stays Allowed and clean.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_successor_app_cold_shortfall_is_typed_and_builds_no_node() {
    let cold = crate::client_tun::compatibility_runtime::egress::deadline::layout_bytes();
    let gap = DEVICE_HELD_AT_SUCCESSOR + cold - 1;
    let outcome = rebuild_with_successor_cold_gap(gap).await;
    assert_eq!(outcome.held_at_successor_failure, Some(DEVICE_HELD_AT_SUCCESSOR),
        "exact boundary: cut Device holdings + successor cold = gap + 1");
    assert!(!outcome.successor_accepted, "no successor Node reached the peer");
    assert_eq!(outcome.report.node, None, "no successor Node was prepared");
    assert_eq!(outcome.report.rebuilds, 1);
    assert_eq!(outcome.report.session, SessionDeviceState::DeviceFailedSettled);
    assert_app_full_io(&outcome.successor, crate::shared::errors::FailurePhase::SessionLoop);
    assert_eq!(outcome.successor.flow_observation().map(|f| f.published).unwrap_or(0), 0);
    assert!(outcome.old.business_complete.load(Ordering::Acquire));
}

/// B3 boundary partner: exactly enough App tier for the successor's cold. The
/// cold is admitted and a successor Node reaches the peer on the same budget;
/// any later App-tier admission under the still-full tier stays typed non-Q.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_successor_app_cold_exact_fit_builds_successor_node() {
    let cold = crate::client_tun::compatibility_runtime::egress::deadline::layout_bytes();
    let outcome = rebuild_with_successor_cold_gap(DEVICE_HELD_AT_SUCCESSOR + cold).await;
    assert!(outcome.successor_accepted, "successor Node built and reached the peer");
    assert_eq!(outcome.report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
    assert_eq!(outcome.report.rebuilds, 1);
    if outcome.successor.ledger().first_failure.is_some() {
        assert_eq!(outcome.successor.ledger().first_failure.as_ref().unwrap().facts().class,
            crate::shared::errors::FailureClass::Io);
        let kind = outcome.successor.ledger().first_failure.as_ref().unwrap().facts().io_kind;
        assert_eq!(kind, Some(std::io::ErrorKind::WouldBlock), "later App Full stays typed non-Q");
    }
}
