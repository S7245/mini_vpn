//! Actual saved-loop F path over an authenticated loopback TUIC peer.
//! Uses real packet/socket/provider components, never direct slot fill.
use super::*;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use std::panic::{AssertUnwindSafe, catch_unwind};

async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = Box::pin(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

#[tokio::test]
async fn saved_f_root_forwards_real_tuic_data_and_settles_before_device() {
    run_saved_f(CloseCase::Fin).await;
}

#[tokio::test]
async fn saved_f_shutdown_drains_queued_data_under_zero_window_pressure() {
    run_saved_f(CloseCase::Shutdown).await;
}

#[tokio::test]
async fn saved_f_local_reset_disposes_queued_data_without_stopping_session() {
    run_saved_f(CloseCase::Reset).await;
}

#[tokio::test]
async fn saved_f_reset_after_remote_failure_rechecks_dead_pending() {
    run_saved_f(CloseCase::ResetAfterClose).await;
}

#[tokio::test]
async fn saved_f_reset_after_provider_retirement_disposes_dead_pending() {
    run_saved_f(CloseCase::ResetAfterRetire).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CloseCase {
    Fin,
    Shutdown,
    Reset,
    ResetAfterClose,
    ResetAfterRetire,
    ReceiptHold,
    ReceiptWrong,
    ReceiptWindow,
}

async fn run_saved_f(case: CloseCase) {
    let pressure = case != CloseCase::Fin;
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
    let mut config = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
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
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
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
                send.write_all(&vec![0x63; 2 * 1024 * 1024 - 32_768])
                    .await
                    .unwrap();
            } else {
                send.write_all(b"f-root-reply").await.unwrap();
            }
            if pressure {
                if remote_failure {
                    writer_failure_gate.await.unwrap();
                    recv.stop(77u32.into()).unwrap();
                    failure_sent.send(()).unwrap();
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
                    if bridge.snapshot().is_some_and(|s| {
                        s.context.is_some_and(|c| c.pending_downlink_bytes >= 16_384)
                    }) && apps.sockets.get::<Socket>(apps.handle).recv_queue() == 4096
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                readiness_step = "queued DATA after pressure";
                payload_tail.take().unwrap().send(()).unwrap();
                // Keep the saved loop parked while the original reader publishes
                // real DATA; an AckDrainHint alone cannot satisfy this barrier.
                while bridge.queued_len() == 0
                    || !session.control.test_queued_data(apps.handle).is_some_and(|n| n > 0)
                {
                    tokio::task::yield_now().await;
                }
                bridge
            })
            .await;
            if ready.is_err() {
                let bridge = session.control.relay_event_test_bridge();
                panic!("pressure readiness {readiness_step}: flow={:?}; app_rx={}; queue={:?}; snapshot={:?}",
                    session.control.flow_observation(),
                    apps.sockets.get::<Socket>(apps.handle).recv_queue(),
                    bridge.as_ref().map(|b| b.queued_len()),
                    bridge.as_ref().and_then(|b| b.snapshot()));
            }
            let bridge = ready.unwrap();
            let held_lane = receipt_case || remote_failure || case == CloseCase::Reset;
            if held_lane { session.control.hold_test_lane(apps.handle); }
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
            if case == CloseCase::Shutdown {
                session.request_stop();
            } else {
                apps.sockets.get_mut::<Socket>(apps.handle).abort();
                apps.turn().await;
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
                    if state.joined == 3 && bridge.queued_len() == 0 {
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
                tokio::time::timeout(Duration::from_secs(1), &mut stop_observed)
                    .await.unwrap().unwrap();
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
                (0, 0, 0, 3)
            );
            assert_eq!(bridge.queued_len(), 0);
            let (messages, bytes, transferred, discarded, pending_discard) =
                settled.last_data_disposition.unwrap();
            assert!(messages >= 2 && transferred > 0 && discarded > 0);
            assert_eq!(transferred + discarded, bytes);
            assert!(pending_discard > 0 && pending_discard <= transferred);
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
                        (3, 1, 0, 0),
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
            "F drain failed: {:?}",
            session.control.flow_observation()
        );
        assert_eq!(received, b"f-root-reply");
        assert!(saw_fin, "actual app must receive FIN, not an RST");
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
            (1, 1, 3),
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
    );
    assert_eq!(
        state,
        if matches!(case, CloseCase::ReceiptWrong | CloseCase::ReceiptWindow) {
            SessionDeviceState::Incomplete
        } else if remote_failure {
            SessionDeviceState::DeviceFailedSettled
        } else {
            SessionDeviceState::DeviceSettled
        }
    );
    idle.unwrap();
    if let Err(panic) = pair_cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[path = "compatibility_f_flow/published_receipt.rs"]
mod published_receipt;

#[path = "compatibility_f_reset.rs"]
pub(in crate::client_tun::compatibility_runtime::tests) mod reset;

#[path = "compatibility_f_flow/egress_failure.rs"]
mod egress_failure;

#[path = "compatibility_f_flow/panic_failure.rs"]
mod panic_failure;

#[path = "compatibility_f_flow/final_owner.rs"]
mod final_owner;

#[path = "compatibility_f_flow/isolation.rs"]
mod isolation;
