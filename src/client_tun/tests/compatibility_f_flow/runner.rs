//! Actual saved-loop F runner over the authenticated local TUIC peer.
//! Owns only the multi-case test fixture, not Root or transport lifecycle.
//! Entry: `run_saved_f`; the parent declares cases and test entrypoints.
//! Invariant: all outcomes use original session/role/physical owners.
//! Tests are declared in `compatibility_f_flow.rs`.
use super::*;

pub(super) async fn run_saved_f(case: CloseCase) {
    let foreign_phase = match case {
        CloseCase::NativePermitForeignFlush => {
            Some(crate::client_tun::legacy_event_loop::relay_events::ForeignEgressPhase::Flush)
        }
        CloseCase::NativePermitForeignAck => {
            Some(crate::client_tun::legacy_event_loop::relay_events::ForeignEgressPhase::Ack)
        }
        _ => None,
    };
    let native = matches!(
        case,
        CloseCase::NativeChunkFin
            | CloseCase::NativePermitFin
            | CloseCase::NativeChunkReset
            | CloseCase::NativePermitReset
            | CloseCase::NativePermitForeignFlush
            | CloseCase::NativePermitForeignAck
    );
    let native_permit_reset = case == CloseCase::NativePermitReset || foreign_phase.is_some();
    let role_count = if native { 5 } else { 3 };
    let pressure = !matches!(
        case,
        CloseCase::Fin
            | CloseCase::NativeChunkFin
            | CloseCase::NativePermitFin
            | CloseCase::NativeByteOwnedFin
    );
    let receipt_case = matches!(
        case,
        CloseCase::ReceiptHold | CloseCase::ReceiptWrong | CloseCase::ReceiptWindow
    );
    let mut receipt_scenario = None;
    let remote_failure = matches!(
        case,
        CloseCase::ResetAfterClose | CloseCase::ResetAfterRetire
    );
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 100, true)));
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: native.then_some("1"),
        native_egress_permit: matches!(
            case,
            CloseCase::NativePermitFin
                | CloseCase::NativePermitReset
                | CloseCase::NativePermitForeignFlush
                | CloseCase::NativePermitForeignAck
        )
        .then_some("1"),
        d16_byte_owned: matches!(
            case,
            CloseCase::NativeByteOwnedFin | CloseCase::NativeByteOwnedReset
        )
        .then_some("1"),
        ..Default::default()
    }));
    let mut config = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some(crate::tuic::tcp_fixture::test_ca_path().as_str()),
        Some("h3"),
    )
    .unwrap()
    .with_relay_policy(policy.clone());
    config.tcp_pool = 1;
    let connected = capture(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                TuicUpstream::connect(&config, Arc::new(Metrics::default())),
                async { pair.server.accept().await.unwrap().await.unwrap() }
            )
        })
        .await
        .unwrap()
    })
    .await;
    let (upstream, peer) = match connected {
        Ok((Ok(upstream), peer)) => (Arc::new(upstream), peer),
        failure => {
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("actual TUIC connection setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let endpoint_backing = endpoint.receive_backing_meter();
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    if pressure {
        runtime.downlink_backpressure = crate::client_tun::DownlinkBackpressureConfig {
            high_bytes: 4096,
            low_bytes: 2048,
        };
        runtime.bounded_global_rx_receive_window = true;
    }
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::new()),
        NoopSink,
    );
    let remote = peer.clone();
    let (finish, gate) = tokio::sync::oneshot::channel();
    let (payload_start, payload_gate) = tokio::sync::oneshot::channel();
    let mut payload_start = Some(payload_start);
    let (payload_tail, tail_gate) = tokio::sync::oneshot::channel();
    let mut payload_tail = Some(payload_tail);
    let (peer_stopped, mut stop_observed) = tokio::sync::oneshot::channel();
    let (fail_writer, writer_failure_gate) = tokio::sync::oneshot::channel();
    let mut fail_writer = Some(fail_writer);
    let (failure_sent, mut failure_observed) = tokio::sync::oneshot::channel();
    let mut finish = Some(finish);
    let mut server = Some(tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(15), async {
            let (mut send, mut recv) = remote.accept_bi().await.unwrap();
            let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
            let mut expected = encode_connect(&target);
            expected.extend_from_slice(b"f0-retained");
            let mut bytes = vec![0; expected.len()];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected, "CONNECT then first payload exactly once");
            if pressure {
                payload_gate.await.unwrap();
                send.write_all(&vec![0x63; 32_768]).await.unwrap();
                tail_gate.await.unwrap();
                let tail = send.write_all(&vec![0x63; 2 * 1024 * 1024 - 32_768]).await;
                let tail_stopped =
                    native_permit_reset && matches!(tail, Err(quinn::WriteError::Stopped(_)));
                if !tail_stopped {
                    tail.unwrap();
                }
                if native_permit_reset {
                    if foreign_phase.is_some() {
                        let _ = gate.await;
                        return;
                    }
                    if !tail_stopped {
                        assert!(send.stopped().await.unwrap().is_some());
                    }
                    peer_stopped.send(()).unwrap();
                    let _ = gate.await;
                    drop(send);
                    drop(recv);
                    let (mut successor_send, mut successor_recv) =
                        remote.accept_bi().await.unwrap();
                    let mut successor_expected = encode_connect(&target);
                    successor_expected.extend_from_slice(b"f0-retained");
                    let mut successor_bytes = vec![0; successor_expected.len()];
                    successor_recv
                        .read_exact(&mut successor_bytes)
                        .await
                        .unwrap();
                    assert_eq!(successor_bytes, successor_expected);
                    successor_send.write_all(b"f-root-successor").await.unwrap();
                    successor_send.finish().unwrap();
                    return;
                }
            } else {
                send.write_all(b"f-root-reply").await.unwrap();
            }
            if pressure {
                if remote_failure {
                    writer_failure_gate.await.unwrap();
                    recv.stop(77u32.into()).unwrap();
                    failure_sent.send(()).unwrap();
                }
                if case == CloseCase::NativeByteOwnedReset {
                    let _ = gate.await;
                    return;
                }
                assert!(
                    send.stopped().await.unwrap().is_some(),
                    "real client STOP_SENDING"
                );
                peer_stopped.send(()).unwrap();
                let _ = gate.await;
                return;
            }
            let _ = gate.await;
            send.finish().unwrap();
            assert!(
                recv.read_to_end(1).await.unwrap().is_empty(),
                "no duplicate payload"
            );
        })
        .await
    }));
    let mut auth = None;
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            auth = Some(peer.accept_uni().await.unwrap());
            let observed = auth.as_mut().unwrap().read_to_end(64).await.unwrap();
            let mut token = [0; 32];
            peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
                .unwrap();
            assert_eq!(observed, encode_authenticate(&config.uuid, &token));
        })
        .await
        .unwrap();
        if pressure {
            let mut readiness_step = "publication";
            let ready = tokio::time::timeout(Duration::from_secs(3), async {
                while session.control.flow_observation().unwrap().published == 0 {
                    apps.turn().await;
                    let _ = poll_once(session.run()).await;
                    tokio::task::yield_now().await;
                }
                if receipt_case {
                    session.control.hold_only_published_receipt();
                }
                let bridge = session.control.relay_event_test_bridge().unwrap();
                payload_start.take().unwrap().send(()).unwrap();
                readiness_step = "global queue";
                // First establish actual pending bytes and a full app window.
                // The held tail leaves capacity for DATA that must remain queued.
                while bridge.queued_len() == 0 {
                    tokio::task::yield_now().await;
                }
                readiness_step = "physical pressure and app window";
                loop {
                    apps.turn().await;
                    let _ = poll_once(session.run()).await;
                    let physical_pressure = if native_permit_reset {
                        session.control.flow_native_permit_snapshot().is_some_and(|queue| queue.leased_bytes > 0)
                    } else {
                        bridge.snapshot().is_some_and(|s| {
                            s.context.is_some_and(|c| c.pending_downlink_bytes >= 16_384)
                        })
                    };
                    if physical_pressure && apps.sockets.get::<Socket>(apps.handle).recv_queue() == 4096
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                readiness_step = "queued DATA after pressure";
                let native_registered_before_tail = endpoint_backing.as_ref().map(|meter| meter.snapshot().registered_total);
                payload_tail.take().unwrap().send(()).unwrap();
                // Keep the saved loop parked while the original reader publishes
                // real DATA; an AckDrainHint alone cannot satisfy this barrier.
                while (!native_permit_reset && bridge.queued_len() == 0)
                    || (!native && case != CloseCase::NativeByteOwnedReset
                        && !session.control.test_queued_data(apps.handle).is_some_and(|n| n > 0))
                    || (native_permit_reset
                        && !endpoint_backing.as_ref().zip(native_registered_before_tail)
                            .is_some_and(|(meter, before)| meter.snapshot().registered_total > before))
                    || (case == CloseCase::NativeByteOwnedReset
                        && !session.control.flow_d16_physical_snapshots().is_some_and(
                            |(queue, budget, same_budget)| {
                                same_budget
                                    && queue.queued_bytes + queue.leased_bytes > 0
                                    && budget.owned_bytes >= queue.queued_bytes + queue.leased_bytes
                            },
                        ))
                {
                    tokio::task::yield_now().await;
                }
                bridge
            })
            .await;
            if ready.is_err() {
                let bridge = session.control.relay_event_test_bridge();
                panic!("pressure readiness {readiness_step}: flow={:?}; app_rx={}; queue={:?}; snapshot={:?}; ordered={:?}; native_queue={:?}; backing={:?}",
                    session.control.flow_observation(),
                    apps.sockets.get::<Socket>(apps.handle).recv_queue(),
                    bridge.as_ref().map(|b| b.queued_len()),
                    bridge.as_ref().and_then(|b| b.snapshot()),
                    session.control.ordered_custody(),
                    session.control.flow_native_permit_snapshot(),
                    endpoint_backing.as_ref().map(|meter| meter.snapshot()));
            }
            let bridge = ready.unwrap();
            if matches!(case, CloseCase::NativeByteOwnedReset | CloseCase::NativeChunkReset | CloseCase::NativePermitReset | CloseCase::NativePermitForeignFlush | CloseCase::NativePermitForeignAck) {
                let root_backing = if native {
                    session.control.flow_native_backing_meter().expect("original Native Root backing authority")
                } else {
                    session.control.flow_d16_backing_meter().expect("original D16 Root backing authority")
                };
                assert!(endpoint_backing.as_ref().is_some_and(|endpoint| endpoint.same_authority(&root_backing)));
            }
            let held_lane = receipt_case || remote_failure || matches!(case, CloseCase::Reset | CloseCase::NativeByteOwnedReset);
            if held_lane { session.control.hold_test_lane(apps.handle); }
            if case == CloseCase::NativeByteOwnedReset {
                session.control.request_active_reasoned_close_probe(apps.handle);
                tokio::time::timeout(Duration::from_secs(2), async {
                    while session.control.active_reasoned_close_observation().is_none() {
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        tokio::task::yield_now().await;
                    }
                }).await.expect("original active reasoned close probe");
                let (active, before, after, budget_before, budget_after, preclear) =
                    session.control.active_reasoned_close_observation().unwrap();
                assert!(active && before > 0, "real active D16 inflight permit: {before}");
                assert_eq!(after, before, "active close must retain physical permit");
                assert_eq!(budget_after, budget_before, "active close must retain global charge");
                assert_eq!(preclear, None, "active close cannot claim terminal preclear");
            }
            if receipt_case {
                receipt_scenario = Some(
                    published_receipt::ReceiptScenario::connect(&mut session, &mut apps, &peer).await,
                );
            }
            assert!(apps.sockets.get::<Socket>(apps.handle).recv_queue() > 0);
            if remote_failure {
                fail_writer.take().unwrap().send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), &mut failure_observed)
                    .await
                    .unwrap()
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while session
                        .control
                        .flow_observation()
                        .unwrap()
                        .stopped_producers
                        == 0
                    {
                        let socket = apps.sockets.get_mut::<Socket>(apps.handle);
                        if socket.can_send() {
                            let _ = socket.send_slice(b"after-stop");
                        }
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("real writer failure reaches original producer stop");
                assert!(apps.sockets.get::<Socket>(apps.handle).is_active());
                assert!(bridge.queued_len() > 0);
            }
            if case == CloseCase::ResetAfterRetire {
                tokio::time::timeout(Duration::from_secs(1), &mut stop_observed)
                    .await.unwrap().unwrap();
                finish.take().unwrap().send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while session.control.flow_observation().unwrap().settled_providers == 0 {
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        tokio::task::yield_now().await;
                    }
                }).await.expect("actual provider Settled before RST");
                let retired = session.control.flow_observation().unwrap();
                assert_eq!((retired.live, retired.joined, retired.settled_providers), (1, 3, 1));
                assert!(bridge.queued_len() > 0);
            }
            let retained_native_permit = if native_permit_reset {
                let queue = session.control.flow_native_permit_queue()
                    .expect("original Native permit queue before RST");
                assert!(queue.snapshot_now().leased_bytes > 0);
                Some(queue)
            } else {
                None
            };
            let foreign_queue = if let Some(phase) = foreign_phase {
                let id = session.control.test_flow_identity(apps.handle);
                session.control.watch_ordered_physical(id);
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !session.control.ordered_custody()
                        .is_some_and(|custody| custody.inflight_bytes > 0)
                    {
                        let _ = poll_once(session.run()).await;
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap_or_else(|_| panic!(
                    "original cap did not reach the physical inflight ledger: flow={:?} custody={:?} queue={:?}",
                    session.control.flow_observation(),
                    session.control.ordered_custody(),
                    retained_native_permit.as_ref().map(|queue| queue.snapshot_now()),
                ));
                let before = session.control.ordered_custody().expect("original ordered custody");
                assert!(before.inflight_bytes > 0, "foreign tracer requires original inflight cap: {before:?}");
                let foreign = crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue::new_with_release_mode(
                    4,
                    crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain,
                ).unwrap();
                foreign.push(bytes::Bytes::from_static(b"fail")).await.unwrap();
                let (_, cap) = foreign.recv_up_to(4).await.unwrap().into_parts();
                bridge.arm_foreign_egress(apps.handle, id.epoch, phase, cap);
                Some(foreign)
            } else {
                None
            };
            if case == CloseCase::Shutdown {
                session.request_stop();
            } else {
                apps.sockets.get_mut::<Socket>(apps.handle).abort();
                apps.turn().await;
            }
            if let Some(phase) = foreign_phase {
                tokio::time::timeout(Duration::from_secs(3), async {
                    while bridge.foreign_fired().is_none() {
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap_or_else(|_| panic!(
                    "foreign {phase:?} entry not reached: flow={:?} custody={:?} queue={:?} poll={:?}",
                    session.control.flow_observation(), session.control.ordered_custody(),
                    retained_native_permit.as_ref().map(|queue| queue.snapshot_now()),
                    bridge.foreign_diag(),
                ));
                let fired = bridge.foreign_fired().unwrap();
                assert_eq!(fired.phase, phase);
                if phase == crate::client_tun::legacy_event_loop::relay_events::ForeignEgressPhase::Ack {
                    assert!(fired.same_cycle_dead_transition);
                }
                assert!(
                    matches!(fired.stage, "inbound_poll" | "tun_rx_drain"),
                    "foreign cap must enter the original staged ingress cycle: {fired:?}"
                );
                assert!(fired.original_bytes > 0);
                assert_eq!(fired.foreign_bytes, 4);
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        if session.control.ordered_custody().is_some_and(|s| s.fault.is_some()) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("original inflight first fault");
                let custody = session.control.ordered_custody().unwrap();
                assert_eq!(custody.fault, Some("capability does not belong to original flow queue"));
                assert!(custody.quarantined);
                assert!(custody.inflight_bytes >= fired.original_bytes + fired.foreign_bytes);
                assert!(retained_native_permit.as_ref().unwrap().snapshot_now().leased_bytes >= fired.original_bytes);
                assert_eq!(foreign_queue.as_ref().unwrap().snapshot_now().leased_bytes, 4);
                assert_eq!(session.control.flow_observation().unwrap().published, 1);
                return;
            }
            if held_lane {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !session.control.held_lane_inactive() {
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        tokio::task::yield_now().await;
                    }
                }).await.expect("original physical owner observes actual RST before DATA release");
                assert!(bridge.queued_len() > 0);
                session.control.release_test_lane();
            }
            let local = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    apps.turn().await;
                    let _ = poll_once(session.run()).await;
                    let state = session.control.flow_observation().unwrap();
                    if state.joined == role_count as u64 && bridge.queued_len() == 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(
                local.is_ok(),
                "local DATA drain: {:?}; queue={}",
                session.control.flow_debug(),
                bridge.queued_len()
            );
            if let Some(finish) = finish.take() {
                if case != CloseCase::NativeByteOwnedReset {
                    if native_permit_reset {
                        tokio::time::timeout(Duration::from_secs(2), async {
                            loop {
                                match stop_observed.try_recv() {
                                    Ok(()) => break,
                                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                                    Err(error) => panic!("peer STOP observer closed: {error:?}"),
                                }
                                apps.turn().await;
                                let _ = poll_once(session.run()).await;
                                tokio::task::yield_now().await;
                            }
                        }).await.unwrap_or_else(|_| panic!(
                            "original Native permit STOP after Root progression: flow={:?}; debug={:?}; queue={:?}; backing={:?}",
                            session.control.flow_observation(),
                            session.control.flow_debug(),
                            session.control.flow_native_permit_snapshot(),
                            endpoint_backing.as_ref().map(|meter| meter.snapshot()),
                        ));
                    } else {
                        tokio::time::timeout(Duration::from_secs(1), &mut stop_observed)
                            .await.unwrap().unwrap();
                    }
                }
                finish.send(()).unwrap();
            }
            if let Some(scenario) = &mut receipt_scenario {
                scenario.exercise(&mut session, &mut apps, case).await;
                return;
            }
            let drain = tokio::time::timeout(Duration::from_secs(2), async {
                while session.control.flow_observation().unwrap().live != 0 {
                    apps.turn().await;
                    let _ = poll_once(session.run()).await;
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(
                drain.is_ok(),
                "shutdown drain: {:?}; {:?}; queue={}; snapshot={:?}",
                session.control.flow_observation(),
                session.control.flow_debug(),
                bridge.queued_len(),
                bridge.snapshot()
            );
            let settled = session.control.flow_observation().unwrap();
            assert!(settled.audit);
            assert_eq!(
                (settled.w, settled.f, settled.reserved, settled.joined),
                (0, 0, 0, role_count as u64)
            );
            assert_eq!(bridge.queued_len(), 0);
            if case == CloseCase::NativeByteOwnedReset {
                let (queued, transferred, discarded) = session.control.flow_d16_disposition().expect("original D16 physical receipt");
                assert!(queued > 0 && discarded > 0, "real queued backing discarded: {queued} {transferred} {discarded}");
                assert_eq!(transferred + discarded, queued);
                let budget = crate::client_tun::d16_process_global_budget().snapshot();
                assert!(budget.total_bytes() <= budget.capacity_bytes);
            } else {
                let (messages, bytes, transferred, discarded, pending_discard) =
                    settled.last_data_disposition.unwrap();
                if native_permit_reset {
                    assert!(messages >= 2 && transferred > 0);
                    assert!(retained_native_permit.as_ref().is_some_and(|queue| queue.physical_zero()),
                        "original Native permit remains charged after Root join: {:?}",
                        retained_native_permit.as_ref().map(|queue| queue.snapshot_now()));
                } else {
                    assert!(messages >= 2 && transferred > 0 && discarded > 0);
                    assert!(pending_discard > 0 && pending_discard <= transferred);
                }
                assert_eq!(transferred + discarded, bytes);
            }
            if native_permit_reset {
                apps.sockets.get_mut::<Socket>(apps.handle)
                    .connect(apps.iface.context(), (IpAddress::v4(192, 0, 2, 10), 10_000), 40_000)
                    .expect("same app tuple starts unique successor after Root receipt");
                apps.sent = false;
                let mut successor_reply = Vec::new();
                tokio::time::timeout(Duration::from_secs(4), async {
                    loop {
                        apps.turn().await;
                        let _ = poll_once(session.run()).await;
                        let socket = apps.sockets.get_mut::<Socket>(apps.handle);
                        if socket.can_recv() {
                            let mut scratch = [0; 64];
                            let n = socket.recv_slice(&mut scratch).unwrap();
                            successor_reply.extend_from_slice(&scratch[..n]);
                        }
                        if successor_reply == b"f-root-successor" {
                            socket.close();
                        }
                        let state = session.control.flow_observation().unwrap();
                        if state.released == 2 && state.live == 0 {
                            assert_eq!(state.joined, 2 * role_count as u64);
                            assert_eq!((state.published, state.w, state.f, state.reserved, state.retained_capacity),
                                (2, 0, 0, 0, 0));
                            assert!(state.audit);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap_or_else(|_| panic!(
                    "same-tuple successor: flow={:?}; debug={:?}; app={:?}; reply={:?}; peer={:?}",
                    session.control.flow_observation(), session.control.flow_debug(),
                    apps.sockets.get::<Socket>(apps.handle).state(), successor_reply,
                    peer.close_reason()));
                assert_eq!(successor_reply, b"f-root-successor");
                assert!(!session.control.ledger().quarantined);
            }
            return;
        }
        let mut received = Vec::new();
        let mut saw_fin = false;
        let completed = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                let socket = apps.sockets.get_mut::<Socket>(apps.handle);
                if socket.can_recv() {
                    socket
                        .recv(|bytes| {
                            received.extend_from_slice(bytes);
                            (bytes.len(), ())
                        })
                        .unwrap();
                }
                if received == b"f-root-reply" && finish.is_some() {
                    if matches!(case, CloseCase::NativeChunkFin | CloseCase::NativePermitFin) {
                        let root_backing = session.control.flow_native_backing_meter().expect("original Native Root backing authority");
                        assert!(endpoint_backing.as_ref().is_some_and(|endpoint| endpoint.same_authority(&root_backing)));
                    }
                    if case == CloseCase::NativeByteOwnedFin {
                        let root_backing = session.control.flow_d16_backing_meter().expect("original D16 Root backing authority");
                        assert!(endpoint_backing.as_ref().is_some_and(|endpoint| endpoint.same_authority(&root_backing)));
                    }
                    let active = session.control.flow_observation().unwrap();
                    assert!(active.audit, "{active:?}");
                    assert_eq!(
                        (
                            active.live,
                            active.w,
                            active.f,
                            active.reserved,
                            active.retained_capacity
                        ),
                        (1, 0, 1, 0, 0),
                        "{active:?}"
                    );
                    assert_eq!(
                        (
                            active.running,
                            active.published,
                            active.joined,
                            active.released
                        ),
                        (role_count, 1, 0, 0),
                        "{active:?}"
                    );
                    assert!(
                        peer.close_reason().is_none(),
                        "shared carrier must remain usable"
                    );
                    finish.take().unwrap().send(()).unwrap();
                }
                if socket.state() == smoltcp::socket::tcp::State::CloseWait {
                    saw_fin = true;
                    socket.close();
                }
                if finish.is_none() && session.control.flow_observation().unwrap().live == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            completed.is_ok(),
            "F drain failed: {:?}; debug={:?}; failure={:?}",
            session.control.flow_observation(),
            session.control.flow_debug(),
            session.control.ledger().first_failure.as_ref().map(|failure| String::from_utf8_lossy(failure.text_bytes()).into_owned())
        );
        assert_eq!(received, b"f-root-reply");
        assert!(saw_fin, "actual app must receive FIN, not an RST: flow={:?} debug={:?} socket={:?}", session.control.flow_observation(), session.control.flow_debug(), apps.sockets.get::<Socket>(apps.handle).state());
        apps.turn().await;
        assert_eq!(apps.received_resets, 0, "graceful FIN must not gain a late reset after physical receipt");
        let settled = session.control.flow_observation().unwrap();
        assert!(settled.audit, "{settled:?}");
        assert_eq!(
            (
                settled.live,
                settled.w,
                settled.f,
                settled.reserved,
                settled.retained_capacity
            ),
            (0, 0, 0, 0, 0),
            "{settled:?}"
        );
        assert_eq!(
            (settled.published, settled.released, settled.joined),
            (1, 1, role_count as u64),
            "{settled:?}"
        );
        assert!(
            !session
                .control
                .device_settlement_observed
                .load(Ordering::SeqCst)
        );
        assert!(peer.close_reason().is_none());
    })
    .await;
    // Retain errors until original-session shutdown and both endpoints are cleaned.
    session.control.release_test_lane();
    drop(fail_writer.take());
    drop(payload_start.take());
    drop(payload_tail.take());
    drop(finish.take());
    drop(receipt_scenario.take());
    if receipt_case {
        session.control.release_held_receipt();
    }
    let prejoined = if receipt_case && scenario.is_ok() {
        Some(server.take().unwrap().await)
    } else {
        None
    };
    session.request_stop();
    let state = session.observe_shutdown().await;
    let server_result = match prejoined {
        Some(result) => result,
        None => server.take().unwrap().await,
    };
    let clean = !session.control.ledger().quarantined;
    let connection_alive = peer.close_reason().is_none();
    endpoint.close(0u32.into(), b"F fixture cleanup");
    peer.close(0u32.into(), b"F fixture cleanup");
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    let pair_cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(matches!(server_result, Ok(Ok(()))), "{server_result:?}");
    assert!(connection_alive);
    assert_eq!(
        clean,
        !matches!(case, CloseCase::ReceiptWrong | CloseCase::ReceiptWindow)
            && foreign_phase.is_none()
    );
    assert_eq!(
        state,
        if matches!(case, CloseCase::ReceiptWrong | CloseCase::ReceiptWindow)
            || foreign_phase.is_some()
        {
            SessionDeviceState::Incomplete
        } else if remote_failure {
            SessionDeviceState::DeviceFailedSettled
        } else {
            SessionDeviceState::DeviceSettled
        }
    );
    idle.unwrap();
    drop(endpoint);
    if matches!(
        case,
        CloseCase::NativeChunkFin
            | CloseCase::NativePermitFin
            | CloseCase::NativeChunkReset
            | CloseCase::NativePermitReset
            | CloseCase::NativeByteOwnedFin
            | CloseCase::NativeByteOwnedReset
    ) {
        let backing = endpoint_backing
            .expect("original Quinn endpoint meter")
            .snapshot();
        assert_eq!(
            (
                backing.allocations,
                backing.payload_bytes,
                backing.physical_bytes
            ),
            (0, 0, 0)
        );
        assert!(!backing.quarantined);
    }
    if let Err(panic) = pair_cleanup {
        std::panic::resume_unwind(panic);
    }
}
