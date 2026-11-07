use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, config, Apps};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathProfile {
    Generic,
    NativeOrderedChunk,
    NativeChunkPump,
    NativePermit,
    NativeByteOwned,
}

#[tokio::test(flavor = "current_thread")]
async fn production_helper_carries_real_tcp_udp_and_settles_node_last() {
    run_path(false, PathProfile::Generic).await;
}

#[tokio::test(flavor = "current_thread")]
async fn production_helper_recovers_primary_for_new_tcp_and_subsequent_udp() {
    run_path(true, PathProfile::Generic).await;
}

#[tokio::test(flavor = "current_thread")]
async fn production_helper_native_permit_carries_real_tcp_udp_and_joins_before_node() {
    run_path(false, PathProfile::NativePermit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn production_helper_native_chunk_carries_real_tcp_udp_and_joins_before_node() {
    run_path(false, PathProfile::NativeOrderedChunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn production_helper_native_chunk_pump_carries_real_tcp_udp_and_joins_before_node() {
    run_path(false, PathProfile::NativeChunkPump).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_chunk_pump_recovers_primary_with_original_direct_root() {
    run_path(true, PathProfile::NativeChunkPump).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_chunk_pump_pending_read_stops_and_joins_original_four_roles() {
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
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let service_control = service.control.clone();
    let mut peer = None;
    let mut held_send = None;
    let mut backing = None;
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
                        held_send = Some(send);
                    };
                    let app_open = async {
                        loop {
                            apps.turn().await;
                            let control = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let observation = control.flow_observation().unwrap();
                            if observation.published == 1 && observation.running == 4 {
                                backing = control.flow_native_backing_meter();
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
        assert!(apps.tcp_reply.is_empty());
        assert!(backing.is_some());
    }).await;
    handle.request_stop();
    let joined_while_peer_open = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), service.run())
                        .await
                        .is_err()
                );
                let control = service_control
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let observation = control.flow_observation().unwrap();
                if observation.joined == 4 {
                    assert_eq!((observation.f, observation.released), (1, 0));
                    assert_eq!(
                        (
                            observation.w,
                            observation.reserved,
                            observation.escrow_bytes
                        ),
                        (0, 0, 0)
                    );
                    break;
                }
            }
        })
        .await
        .unwrap();
    })
    .await;
    if let Some(mut send) = held_send.take() {
        let stopped = tokio::time::timeout(Duration::from_secs(1), send.stopped())
            .await
            .unwrap()
            .unwrap();
        assert!(
            stopped.is_some(),
            "the original reader must signal STOP_SENDING"
        );
        send.finish().unwrap();
    }
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap_or_else(|elapsed| {
                let control = service
                    .control
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                eprintln!(
                    "pending stop timeout: phase={:?} flow={:?} deadline={:?}",
                    handle.phase(),
                    control.flow_observation(),
                    *service.control.deadline.lock().unwrap()
                );
                panic!("pending stop cleanup: {elapsed}");
            });
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        let observation = service
            .completion
            .as_ref()
            .unwrap()
            .control
            .flow_observation()
            .unwrap();
        assert_eq!(
            (
                observation.published,
                observation.released,
                observation.joined
            ),
            (1, 1, 4)
        );
        assert_eq!(
            (
                observation.w,
                observation.f,
                observation.reserved,
                observation.escrow_bytes
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(observation.running, 0);
        assert!(observation.audit);
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (
                physical.allocations,
                physical.payload_bytes,
                physical.physical_bytes,
                physical.quarantined
            ),
            (0, 0, 0, false)
        );
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = setup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = joined_while_peer_open {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn native_chunk_pump_remote_reset_retains_first_error_and_joins_before_node() {
    native_reset_case(PathProfile::NativeChunkPump).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_remote_reset_retains_first_error_and_joins_before_node() {
    native_reset_case(PathProfile::NativeOrderedChunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_remote_reset_retains_first_error_and_joins_before_node() {
    native_reset_case(PathProfile::NativePermit).await;
}

async fn native_reset_case(profile: PathProfile) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: (profile == PathProfile::NativeChunkPump).then_some("1"),
        tuic_native_ordered: matches!(
            profile,
            PathProfile::NativeOrderedChunk | PathProfile::NativePermit
        )
        .then_some("1"),
        native_egress_permit: (profile == PathProfile::NativePermit).then_some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        if profile == PathProfile::NativeChunkPump {
            TuicNativeOverride::NativeChunkPump
        } else {
            TuicNativeOverride::NativeOrderedPump
        }
    );
    assert_eq!(
        policy.native_engine(),
        if profile == PathProfile::NativePermit {
            NativeTcpRelayEngine::PermitPump
        } else {
            NativeTcpRelayEngine::ChunkPump
        }
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let service_control = service.control.clone();
    let mut peer = None;
    let mut stream = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let tcp = async {
                        let (mut send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        send.write_all(b"tcp-reply").await.unwrap();
                        stream = Some((send, recv));
                    };
                    let udp = async {
                        let packet = remote.read_datagram().await.unwrap();
                        let assoc = u16::from_be_bytes([packet[2], packet[3]]);
                        let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
                        assert_eq!(&packet[..], encode_packet(assoc, &target, b"service-udp"));
                        remote.send_datagram(encode_packet(assoc, &target, b"udp-reply").into()).unwrap();
                    };
                    let app = async {
                        while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
                            apps.turn().await;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(tcp, udp, app);
                } => {},
                report = service.run() => panic!("service ended before remote reset: {report:?}"),
            }
        }).await.unwrap();
        let owner = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let backing = owner.flow_native_backing_meter().expect("original Native Root meter");
        let permit_queue = (profile == PathProfile::NativePermit)
            .then(|| owner.flow_native_permit_queue().expect("original Native Permit queue"));
        let (mut send, mut recv) = stream.take().unwrap();
        send.reset(77u32.into()).unwrap();
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    while owner.flow_observation().unwrap().released != 1 {
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("remote reset ended Service: {report:?}"),
            }
        }).await.unwrap();
        let observation = owner.flow_observation().unwrap();
        assert_eq!((observation.live, observation.released, observation.joined), (0, 1, if profile == PathProfile::NativeChunkPump { 4 } else { 5 }));
        assert!(observation.audit);
        if let Some(queue) = &permit_queue {
            assert_eq!(queue.snapshot_now().owned_bytes(), 0);
        }
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().expect("original direct reader IO failure");
        let first_value = (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec());
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        assert_eq!(first.facts().origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
        assert!(first.facts().opaque);
        assert!(first.facts().source_content_omitted);
        assert!(first.text_bytes().is_empty());
        drop(ledger);
        (backing, first_value, permit_queue)
    }).await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        let owner = service.completion.as_ref().unwrap().control.clone();
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap();
        (
            first as *const _ as usize,
            format!("{:?}", first.facts()),
            first.text_bytes().to_vec(),
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
    let (backing, first_value, permit_queue) =
        scenario.unwrap_or_else(|error| std::panic::resume_unwind(error));
    let final_first = cleanup.unwrap_or_else(|error| std::panic::resume_unwind(error));
    assert_eq!(first_value, final_first);
    if let Some(queue) = permit_queue {
        assert_eq!(queue.snapshot_now().owned_bytes(), 0);
    }
    let physical = backing.snapshot();
    assert_eq!(
        (
            physical.allocations,
            physical.payload_bytes,
            physical.physical_bytes,
            physical.quarantined
        ),
        (0, 0, 0, false)
    );
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn production_helper_native_byte_owned_carries_real_tcp_udp_and_joins_before_node() {
    run_path(false, PathProfile::NativeByteOwned).await;
}

async fn run_path(recover: bool, profile: PathProfile) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    if profile != PathProfile::Generic {
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            tuic_native_ordered: matches!(
                profile,
                PathProfile::NativeOrderedChunk | PathProfile::NativePermit
            )
            .then_some("1"),
            tuic_native_chunk: (profile == PathProfile::NativeChunkPump).then_some("1"),
            native_egress_permit: (profile == PathProfile::NativePermit).then_some("1"),
            d16_byte_owned: (profile == PathProfile::NativeByteOwned).then_some("1"),
            ..Default::default()
        }));
        assert_eq!(
            policy.tuic_native_override(),
            match profile {
                PathProfile::NativeOrderedChunk | PathProfile::NativePermit => {
                    TuicNativeOverride::NativeOrderedPump
                }
                PathProfile::NativeChunkPump => TuicNativeOverride::NativeChunkPump,
                PathProfile::NativeByteOwned => TuicNativeOverride::D16DirectOrdered,
                PathProfile::Generic => unreachable!(),
            }
        );
        cfg = cfg.with_relay_policy(policy.clone());
        runtime = runtime.with_relay_policy(policy);
    }
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let metrics = Arc::new(Metrics::default());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        cfg.clone(),
        runtime,
        metrics.clone(),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peer = None;
    let mut native_backing = Vec::new();
    let scenario=capture(async {
      for round in 0..if recover { 2 } else { 1 } {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5),async {
            tokio::select! {
                _=async {
                    let remote=server.accept().await.unwrap().await.unwrap();
                    auth(&remote,&cfg).await;
                    peer=Some(remote.clone());
                    let remote_traffic=async {
                        let tcp=async {
                            let (mut tx,mut rx)=remote.accept_bi().await.unwrap();
                            let mut want=encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                            want.extend_from_slice(b"service-tcp");
                            let mut actual=vec![0;want.len()]; rx.read_exact(&mut actual).await.unwrap(); assert_eq!(actual,want);
                            tx.write_all(b"tcp-reply").await.unwrap(); tx.finish().unwrap();
                            (tx,rx)
                        };
                        let udp=async {
                            let actual=remote.read_datagram().await.unwrap();
                            let target=TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
                            let assoc=u16::from_be_bytes([actual[2],actual[3]]);
                            assert_eq!(&actual[..],encode_packet(assoc,&target,b"service-udp"));
                            remote.send_datagram(encode_packet(assoc,&target,b"udp-reply").into()).unwrap();
                        };
                        tokio::join!(tcp,udp)
                    };
                    let app_traffic=async {
                        while apps.tcp_reply!=b"tcp-reply" || apps.udp_reply!=b"udp-reply" {
                            apps.turn().await; tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    let (streams,())=tokio::join!(remote_traffic,app_traffic);
                    drop(streams);
                }=>{},
                report=service.run()=>panic!("service ended during DATA {report:?}"),
            }
        }).await.unwrap_or_else(|elapsed| {
            let control = service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("path timeout: profile={:?} flow={:?} tcp_reply={:?} udp_reply={:?}", profile, control.flow_observation(), apps.tcp_reply, apps.udp_reply);
            panic!("path business timeout: {elapsed}");
        });
        let control=service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let observation=control.flow_observation().unwrap();
        assert_eq!(observation.factory_started,round+1); assert_eq!(observation.published,round+1);
        if profile != PathProfile::Generic {
            let roles_per_flow = match profile {
                PathProfile::NativeByteOwned => 3,
                PathProfile::NativeChunkPump => 4,
                _ => 5,
            };
            assert_eq!(observation.running as u64 + observation.joined, roles_per_flow * (round + 1) as u64);
            let meters = match profile {
                PathProfile::NativeByteOwned => vec![control.flow_d16_backing_meter().expect("original D16 backing meter")],
                _ => control.flow_native_backing_meters(),
            };
            assert!(!meters.is_empty());
            if profile == PathProfile::NativeChunkPump {
                assert_eq!(meters.len(), observation.live);
            }
            for meter in meters {
                if !native_backing.iter().any(|prior: &bytes::BackingMeter| prior.same_authority(&meter)) {
                    native_backing.push(meter);
                }
            }
        }
        assert_eq!(inbound.dropped_packets(),0); assert_eq!(outbound.dropped_packets(),0);
        if recover && round==0 {
            peer.take().unwrap().close(17u32.into(),b"primary lost");
            apps.restart();
        }
      }
    }).await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        let completion = service.completion.as_ref().unwrap();
        assert!(completion
            .control
            .driver_complete
            .load(std::sync::atomic::Ordering::Acquire));
        let observation = completion.control.flow_observation().unwrap();
        assert_eq!(observation.live, 0);
        assert_eq!(observation.running, 0);
        assert_eq!(
            (
                observation.w,
                observation.f,
                observation.reserved,
                observation.escrow_bytes
            ),
            (0, 0, 0, 0)
        );
        let count = if recover { 2 } else { 1 };
        assert_eq!(
            (
                observation.published,
                observation.released,
                observation.joined
            ),
            (
                count,
                count,
                if matches!(
                    profile,
                    PathProfile::NativeOrderedChunk | PathProfile::NativePermit
                ) {
                    5 * count
                } else if profile == PathProfile::NativeChunkPump {
                    4 * count
                } else {
                    3 * count
                }
            )
        );
        assert!(observation.audit);
        if profile != PathProfile::Generic {
            assert_eq!(
                native_backing.len(),
                1,
                "one original Node endpoint backing authority"
            );
        }
        for backing in &native_backing {
            let physical = backing.snapshot();
            assert_eq!(
                (
                    physical.allocations,
                    physical.payload_bytes,
                    physical.physical_bytes,
                    physical.quarantined
                ),
                (0, 0, 0, false)
            );
        }
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    assert!(drained.unwrap());
}
