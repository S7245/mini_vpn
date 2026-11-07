//! Original health Probe, saved retirement and a pending physical device expose
//! shared deadline adoption. Only the production root may settle these owners.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use device::{device, Gate};
use fixture::{auth, capture, config};
use std::sync::atomic::Ordering;

fn deadline(saved: &Control) -> Option<tokio::time::Instant> {
    *saved.deadline.lock().unwrap()
}

async fn until<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    budget: Duration,
    ready: impl Fn() -> bool,
) {
    tokio::time::timeout(budget, async {
        tokio::select! {
            biased;
            () = async { while !ready() { tokio::time::sleep(Duration::from_millis(1)).await; } } => (),
            report = service.run() => panic!("service ended before witness {report:?}"),
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn retiring_failover_stop_adopts_original_owner_limit_for_same_cancelled_observer() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let backup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (_, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let reality = RealityUpstream::new(RealityClientConfig {
        server: backup.local_addr().unwrap().to_string(),
        uuid: [0x42; 16],
        pbk: pk,
        short_id: [0; 8],
        sni: "example.com".into(),
    });
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare_with(
        device(gate.clone()),
        Configuration::failover(cfg.clone(), reality),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    // This original lazy observer is first polled only after real retirement.
    let mut observer = Box::pin(handle.observe_shutdown());
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2);
    let mut incoming = None;
    let mut owner: Option<Arc<SessionControl>> = None;
    let mut health: Option<Arc<crate::failover::HealthObservation>> = None;
    let mut generation: Option<Arc<crate::owned_upstream::generation::GenerationControl>> = None;
    let mut original_limit = None;
    let mut diagnostic_until = None;
    let mut observer_result = None;
    let mut actual_report = None;
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                () = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await; peers.push(peer);
                } => (),
                report = service.run() => panic!("unexpected startup {report:?}"),
            }
        }).await.unwrap();
        until(&mut service, Duration::from_secs(2), || saved.health_observation.lock().unwrap().is_some()).await;
        let observed = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
        health = Some(observed.clone());
        let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        until(&mut service, Duration::from_secs(2), || observed.ticks.load(Ordering::Acquire) >= 1).await;
        // Selection injection exposes the unchanged real 30s Probe schedule;
        // it is not evidence of natural failover or hysteresis.
        state.set_leg(crate::failover::TcpLeg::Reality);
        until(&mut service, Duration::from_secs(29), || observed.ticks.load(Ordering::Acquire) >= 10).await;
        assert_eq!(observed.probes_started.load(Ordering::Acquire), 0);
        peers[0].close(17u32.into(), b"original primary lost before health tick");
        until(&mut service, Duration::from_secs(5), || observed.probes_started.load(Ordering::Acquire) == 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                value = server.accept() => incoming = Some(value.unwrap()),
                report = service.run() => panic!("ended before original Probe Incoming {report:?}"),
            }
        }).await.unwrap();
        assert_eq!(observed.probes_finished.load(Ordering::Acquire), 0);
        let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        owner = Some(current.clone());
        let source = saved.udp_generation.lock().unwrap().as_ref().unwrap().clone();
        generation = Some(source.clone());
        assert!(handle.request_rebuild());
        until(&mut service, Duration::from_secs(2), || handle.phase() == Phase::Rebuilding && current.ledger().shutdown_deadline.is_some()).await;
        let limit = current.ledger().shutdown_deadline.unwrap();
        original_limit = Some(limit);
        // Fixed once at original rebuild, only an observation bound for Node's
        // separate cleanup. It never extends the parent or business deadline.
        diagnostic_until = Some(limit + Duration::from_secs(15));
        assert!(deadline(&saved).is_none());
        assert_eq!(source.diagnostic().0, 0);
        assert!(!observed.joined.load(Ordering::Acquire));
        assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0, 0));
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(500)) => (),
            report = service.run() => panic!("retirement unexpectedly ended {report:?}"),
        }
        assert!(tokio::time::timeout(Duration::from_millis(5), observer.as_mut()).await.is_err());
        let first_parent = deadline(&saved).unwrap();
        assert!(first_parent > limit, "external stop was actually requested later");
        until(&mut service, Duration::from_secs(2), || gate.polls.load(Ordering::Acquire) > 0 && handle.phase() == Phase::Stopping).await;
        let adopted = deadline(&saved);
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(5), observer.as_mut()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
            assert_eq!(deadline(&saved), adopted);
            assert_eq!(current.ledger().shutdown_deadline, Some(limit));
        }
        // Finish the SAME original Probe Incoming. It is not a new session.
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                () = async {
                    let peer = incoming.take().unwrap().await.unwrap();
                    auth(&peer, &cfg).await; peers.push(peer);
                } => (),
                report = service.run() => panic!("ended before original health completion {report:?}"),
            }
        }).await.unwrap();
        until(&mut service, Duration::from_secs(3), || observed.joined.load(Ordering::Acquire) && source.diagnostic().0 == 2 && current.business_complete.load(Ordering::Acquire)).await;
        assert_eq!((observed.probes_started.load(Ordering::Acquire), observed.probes_finished.load(Ordering::Acquire), observed.probes_ok.load(Ordering::Acquire)), (1, 1, 1));
        assert!(!observed.quarantined.load(Ordering::Acquire));
        assert_eq!(source.diagnostic().4, 0);
        let settled = current.flow_observation().unwrap();
        assert_eq!((settled.live, settled.running, settled.f, settled.w, settled.reserved, settled.escrow_bytes), (0, 0, 0, 0, 0, 0));
        assert!(settled.audit);
        assert!(!current.driver_complete.load(Ordering::Acquire));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire), gate.cuts.load(Ordering::Acquire)), (0, 0, 0));
        assert!(peers[1].close_reason().is_none());
        assert!(!handle.request_rebuild());
        assert!(service.completion.is_none());
        assert!(current.ledger().first_failure.is_none());
        eprintln!("B23 before shared-min assertion now={:?} owner={limit:?} first_parent={first_parent:?} shared={:?} phase={:?} health=1/1/1 joined source={:?} business=true driver=false Dpoll/join/drop={}/0/0 Nodeopen=true", tokio::time::Instant::now(), deadline(&saved), handle.phase(), source.diagnostic(), gate.polls.load(Ordering::Acquire));
        assert_eq!(deadline(&saved), Some(limit), "retirement stop must adopt old owner limit before physical D join");
        tokio::time::sleep_until(limit).await;
        let result = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx))).await;
        eprintln!("B23 same original observer at owner absolute limit now={:?} owner={limit:?} shared={:?} actual={result:?}", tokio::time::Instant::now(), deadline(&saved));
        let Poll::Ready(phase) = result else { panic!("same observer still Pending at original owner limit") };
        observer_result = Some(phase);
        assert_eq!(phase, Phase::Incomplete);
        assert_eq!(handle.phase(), Phase::Stopping);
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0));
        assert!(!current.driver_complete.load(Ordering::Acquire));
        assert!(peers[1].close_reason().is_none());
    }).await;
    // The original observer, Incoming, health/source and Service owners survive
    // scenario failure. No observer or deadline is reconstructed in this finally.
    handle.request_stop();
    gate.open();
    let until = diagnostic_until.unwrap_or_else(|| deadline(&saved).unwrap());
    let cleanup = capture(async {
        let (report, ()) = tokio::time::timeout_at(until, async {
            tokio::join!(service.run(), async {
                if let Some(original) = incoming.take() {
                    let peer = original.await.unwrap(); auth(&peer, &cfg).await; peers.push(peer);
                }
            })
        }).await.unwrap();
        actual_report = Some(report);
        let shared = *saved.deadline.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        eprintln!("B23 finally original report={report:?} owner_limit={original_limit:?} shared={:?} diagnostic_until={until:?} sameobserver_result={observer_result:?} Djoin/drop={}/{} healthjoined={:?} source={:?} first={:?}", shared, gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire), health.as_ref().map(|h| h.joined.load(Ordering::Acquire)), generation.as_ref().map(|g| g.diagnostic()), owner.as_ref().map(|o| o.ledger().first_failure.as_ref().map(|f| format!("{:?}", f.facts()))));
        assert!(report.device_settled());
        assert_eq!(report.rebuilds, 0);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        assert!(health.as_ref().unwrap().joined.load(Ordering::Acquire));
        assert_eq!(generation.as_ref().unwrap().diagnostic().0, 2);
        let done = service.completion.as_ref().unwrap();
        done._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(done._retired.is_none());
        assert!(done._health.as_ref().unwrap().failure().is_none());
        if scenario.is_ok() {
            assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
            assert_eq!(done.control.ledger().first_failure.as_ref().unwrap().facts().io_kind, Some(std::io::ErrorKind::TimedOut));
            assert_eq!(deadline(&saved), original_limit);
        }
    }).await;
    if cleanup.is_err() {
        eprintln!(
            "B23 secondary finally failure retained; original scenario error takes priority; returned_report={actual_report:?}"
        );
    }
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn data_phase_rebuild_stop_adopts_original_limit_before_returning_device_borrow() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = fixture::Apps::new(outbound.clone(), inbound.clone());
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare(
        device::GatedDevice {
            inner: LoopbackTunDevice::new(inbound, outbound).with_ingress_pump(4),
            gate: gate.clone(),
        },
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut observer = Box::pin(handle.observe_shutdown());
    let mut peer = None;
    let mut streams = None;
    let mut stop_code = None;
    let mut owner: Option<Arc<SessionControl>> = None;
    let mut original_limit = None;
    let mut actual_report = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                () = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let tcp = async {
                        let (mut tx, mut rx) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()]; rx.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        tx.write_all(b"tcp-reply").await.unwrap(); streams = Some((tx, rx));
                    };
                    let udp = async {
                        let actual = remote.read_datagram().await.unwrap();
                        let assoc = u16::from_be_bytes([actual[2], actual[3]]);
                        let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
                        assert_eq!(&actual[..], encode_packet(assoc, &target, b"service-udp"));
                        remote.send_datagram(encode_packet(assoc, &target, b"udp-reply").into()).unwrap();
                    };
                    let local = async {
                        while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
                            apps.turn().await; tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(tcp, udp, local);
                } => (),
                report = service.run() => panic!("unexpected DATA completion {report:?}"),
            }
        }).await.unwrap();
        let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        owner = Some(current.clone());
        assert!(handle.request_rebuild());
        until(&mut service, Duration::from_secs(2), || handle.phase() == Phase::Rebuilding && current.ledger().shutdown_deadline.is_some()).await;
        let limit = current.ledger().shutdown_deadline.unwrap(); original_limit = Some(limit);
        assert!(deadline(&saved).is_none());
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                code = streams.as_mut().unwrap().0.stopped() => stop_code = Some(code.unwrap().expect("actual original STOP")),
                report = service.run() => panic!("original carrier not retired yet {report:?}"),
            }
        }).await.unwrap();
        // Keep the same real peer's final offset outstanding through phase I.
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(500)) => (),
            report = service.run() => panic!("premature provider settlement {report:?}"),
        }
        assert!(tokio::time::timeout(Duration::from_millis(5), observer.as_mut()).await.is_err());
        let first_parent = deadline(&saved).unwrap(); assert!(first_parent > limit);
        assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
        let shared = deadline(&saved);
        let business_done = current.business_complete.load(Ordering::Acquire);
        let flow = current.flow_observation().unwrap();
        eprintln!("B23 DATA phase before min assertion owner={limit:?} first_parent={first_parent:?} shared={shared:?} phase={:?} actualSTOP={stop_code:?} business={business_done} heldstreams={} Dpoll/join/drop={}/{}/{} flow={flow:?}", handle.phase(), streams.is_some(), gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire));
        assert_eq!(handle.phase(), Phase::Stopping);
        assert!(!business_done, "DATA loop still owns the original device borrow");
        assert!(streams.is_some());
        assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0, 0));
        assert_eq!(flow.live, 1);
        assert_eq!(shared, Some(limit), "phase I stop must adopt the original owner limit");
        assert_eq!(current.ledger().shutdown_deadline, Some(limit));
        assert!(!handle.request_rebuild());
    }).await;
    handle.request_stop();
    gate.open();
    let limit = original_limit.unwrap_or_else(|| deadline(&saved).unwrap());
    let cleanup = capture(async {
        let (report, ()) = tokio::time::timeout_at(limit, async {
            tokio::join!(service.run(), async {
                if let Some((mut tx, mut rx)) = streams.take() {
                    let code = match stop_code { Some(code) => code, None => tx.stopped().await.unwrap().expect("actual STOP in finally") };
                    assert!(rx.read_to_end(0).await.unwrap().is_empty()); tx.reset(code).unwrap();
                }
            })
        }).await.unwrap();
        actual_report = Some(report);
        eprintln!("B23 DATA phase finally report={report:?}, original_limit={original_limit:?}, shared={:?}, Djoin/drop={}/{}", deadline(&saved), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire));
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.rebuilds, 0);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        let done = service.completion.as_ref().unwrap(); done._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(done._retired.is_none());
        let flow = owner.as_ref().unwrap().flow_observation().unwrap();
        assert_eq!((flow.live, flow.running, flow.released, flow.joined), (0, 0, 1, 3));
        assert!(flow.audit);
    }).await;
    if cleanup.is_err() {
        eprintln!(
            "B23 DATA phase secondary finally failure, first scenario error retained; returned_report={actual_report:?}"
        );
    }
    if let Some(peer) = peer {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}
