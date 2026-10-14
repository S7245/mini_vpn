use super::super::*;
use super::startup::{auth, config, finish, guarded, server};
use crate::upstream::{TcpOpenPhase, TcpOpenRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "current_thread")]
async fn published_node_joins_udp_monitor_and_drains_original_endpoint() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        assert!(host.data().is_err());
        let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.start(), async {
                for _ in 0..2 {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(ready, NodeStartup::Ready);
        let data = host.data().unwrap();
        assert!(
            data.take_udp_shutdown().is_none(),
            "node owns the unique token"
        );
        let mut receiver = data.start_udp();
        peers[0]
            .send_datagram(bytes::Bytes::from_static(b"real-downlink"))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            b"real-downlink"
        );
        let mut blocked = peers[0].open_uni().await.unwrap();
        blocked.write_all(b"unfinished-packet").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.udp_runtime.child_count.load(Ordering::Acquire) != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        host.request_stop();
        assert!(host.data().is_err());
        let cancelled =
            tokio::time::timeout(Duration::from_millis(50), host.observe_shutdown()).await;
        assert!(cancelled.is_err(), "held DATA Arc must prevent settlement");
        assert!(!host.resources.state().endpoint_drained);
        assert_eq!(data.udp_runtime.child_count.load(Ordering::Acquire), 0);
        drop(blocked);
        drop(data);
        drop(receiver);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        let state = host.resources.state();
        assert_eq!(state.attempts_started, 2);
        assert_eq!(state.attempts_drained, 2);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.monitor_started && state.monitor_joined);
        assert!(state.endpoint_drained);
        assert!(state.connections.iter().all(Option::is_none));
    })
    .await;
    finish(host, server, peers, result).await;
}

pub(super) async fn boot(
    host: &mut NodeHost,
    server: &Endpoint,
    cfg: &TuicClientConfig,
    peers: &mut Vec<Connection>,
) {
    let (ready, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(host.start(), async {
            for _ in 0..cfg.tcp_pool {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, cfg).await;
                peers.push(peer);
            }
        })
    })
    .await
    .unwrap();
    assert_eq!(ready, NodeStartup::Ready);
}

#[tokio::test(flavor = "current_thread")]
async fn real_tcp_and_stream_packet_tails_settle_at_published_root() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.udp_relay_mode = "quic".into();
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        tokio::time::timeout(Duration::from_secs(2), async {
            let ((), ()) = tokio::join!(
                async {
                    let mut stream = data.open_tcp(&target).await.unwrap();
                    stream.write_all(b"request").await.unwrap();
                    stream.shutdown().await.unwrap();
                    let mut result = Vec::new();
                    stream.read_to_end(&mut result).await.unwrap();
                    assert_eq!(result, b"response");
                },
                async {
                    let (mut send, mut recv) = peers[0].accept_bi().await.unwrap();
                    assert_eq!(
                        recv.read_to_end(1024).await.unwrap(),
                        [encode_connect(&target), b"request".to_vec()].concat()
                    );
                    send.write_all(b"response").await.unwrap();
                    send.finish().unwrap();
                }
            );
        })
        .await
        .unwrap();
        let packet = encode_packet(7, &target, &[0x5a; 128]);
        data.send_udp(packet.clone()).await;
        let mut recv = tokio::time::timeout(Duration::from_secs(2), peers[0].accept_uni())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recv.read_to_end(1024).await.unwrap(), packet);
        drop(recv);
        assert_eq!(data.udp_drop_count(), 0);
        assert_eq!(data.tcp_pool_admission.active_total(), 0);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert!(host.resources.state().uplink.is_none());
        assert!(host.resources.state().endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn stop_after_staged_ready_prevents_carrier_publication_without_losing_lease() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let mut attempt = data
            .clone()
            .staged_tcp_open(TcpOpenRequest {
                target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
                deadline: Instant::now() + Duration::from_secs(5),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while attempt.view().phase != TcpOpenPhase::Ready {
                attempt.step(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    Instant::now(),
                );
                assert!(attempt.failure().is_none(), "{:?}", attempt.failure());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(data.tcp_pool_admission.active_total(), 1);
        host.request_stop();
        assert!(attempt.take_carrier(Instant::now()).is_none());
        assert!(attempt.view().lease_held);
        let active = data.tcp_pool_admission.pool_active_total.clone();
        drop(data);
        let deadline = Instant::now() + Duration::from_secs(2);
        let (node, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.drive_shutdown(), async {
                while attempt.view().phase != TcpOpenPhase::Settled {
                    attempt.settle(
                        &mut Context::from_waker(std::task::Waker::noop()),
                        Instant::now(),
                        deadline,
                    );
                    assert_ne!(attempt.view().phase, TcpOpenPhase::Quarantined);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(attempt.failure().is_some());
        assert_eq!(node, NodeShutdown::RunningResourcesSettled);
        drop(attempt);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn retained_data_arc_keeps_original_deadline_and_cleanup_after_incomplete() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let identity = Arc::as_ptr(&data);
        host.request_stop();
        let deadline = host.deadline;
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Incomplete);
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Incomplete);
        assert_eq!(host.deadline, deadline);
        assert_eq!(Arc::as_ptr(&data), identity);
        assert!(host.cleanup.is_some());
        assert!(!host.resources.state().endpoint_drained);
        assert_eq!(host.resources.state().attempts_started, 1);
        assert!(data.live_conn().await.is_err());
        let before = data.udp_drop_count();
        data.send_udp(vec![1]).await;
        assert_eq!(data.udp_drop_count(), before + 1);
        assert!(data.start_udp().recv().await.is_none());
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert_eq!(host.deadline, deadline);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_carrier_without_data_arc_keeps_original_lease_charged() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let stream = data.open_tcp(&target).await.unwrap();
        let (send, mut recv) = tokio::time::timeout(Duration::from_secs(2), peers[0].accept_bi())
            .await
            .unwrap()
            .unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        drop(send);
        drop(recv);
        let active = data.tcp_pool_admission.pool_active_total.clone();
        drop(data);
        host.request_stop();
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), host.drive_shutdown())
                .await
                .is_err(),
            "a live original lease forbids settlement even without a DATA Arc"
        );
        assert_eq!(active.load(Ordering::Acquire), 1);
        drop(stream);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert_eq!(active.load(Ordering::Acquire), 0);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn transferred_carrier_returns_strong_progress_before_closed_connection_retirement() {
    for mode in 0..3 {
        let server = server(8);
        let mut cfg = config(server.local_addr().unwrap());
        cfg.relay_policy = Arc::new(RelayFamilyPolicy::from_sources(
            crate::upstream::RelayFamilyPolicySources {
                tuic_ordered_chunk: (mode == 1).then_some("1"),
                tuic_unordered_reassembly: (mode == 2).then_some("1"),
                ..Default::default()
            },
        ));
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
            let mut attempt = data
                .clone()
                .staged_tcp_open(TcpOpenRequest {
                    target: target.clone(),
                    deadline: Instant::now() + Duration::from_secs(5),
                })
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while attempt.view().phase != TcpOpenPhase::Ready {
                    attempt.step(
                        &mut Context::from_waker(std::task::Waker::noop()),
                        Instant::now(),
                    );
                    assert!(attempt.failure().is_none(), "{:?}", attempt.failure());
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            let OpenedTcpRelay::Generic(mut carrier) =
                attempt.take_carrier(Instant::now()).unwrap()
            else {
                panic!("Generic");
            };
            carrier.write_all(b"payload").await.unwrap();
            let (send, mut recv) =
                tokio::time::timeout(Duration::from_secs(2), peers[0].accept_bi())
                    .await
                    .unwrap()
                    .unwrap();
            let mut received = vec![0; encode_connect(&target).len() + 7];
            recv.read_exact(&mut received).await.unwrap();
            assert_eq!(
                received,
                [encode_connect(&target), b"payload".to_vec()].concat()
            );
            let active = data.tcp_pool_admission.pool_active_total.clone();
            host.request_stop();
            assert_eq!(active.load(Ordering::Acquire), 1);
            drop(carrier);
            drop(send);
            drop(recv);
            drop(data);
            let deadline = Instant::now() + Duration::from_secs(2);
            let (node, ()) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(host.drive_shutdown(), async {
                    while attempt.view().phase != TcpOpenPhase::Settled {
                        attempt.settle(
                            &mut Context::from_waker(std::task::Waker::noop()),
                            Instant::now(),
                            deadline,
                        );
                        assert_ne!(
                            attempt.view().phase,
                            TcpOpenPhase::Quarantined,
                            "{:?}",
                            attempt.failure()
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
            })
            .await
            .unwrap();
            assert_eq!(active.load(Ordering::Acquire), 0);
            assert_eq!(node, NodeShutdown::RunningResourcesSettled);
            assert!(host.resources.state().terminal.is_none());
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_data_publication_fails_closed_after_poison_or_udp_stop() {
    for poison in [true, false] {
        let server = server(8);
        let cfg = config(server.local_addr().unwrap());
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let data = host.data().unwrap();
            if poison {
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    let _state = host.resources.0.lock().unwrap();
                    panic!("injected real node ledger poison");
                }));
            } else {
                data.udp_runtime.stop_node();
            }
            assert!(host.data().is_err());
            assert!(host.data().is_err());
            assert_eq!(host.resources.state().attempts_started, 1);
            drop(data);
            let result = tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap();
            assert_eq!(
                result,
                if poison {
                    NodeShutdown::Quarantined
                } else {
                    NodeShutdown::RunningResourcesSettled
                }
            );
            assert!(host.resources.state().endpoint_drained);
            if poison {
                assert!(host.resources.state().first.is_some());
            }
        })
        .await;
        finish(host, server, peers, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn monitor_panic_preserves_first_error_and_cleans_known_resources() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        host.resources
            .record(ClientError::Io(io::Error::from_raw_os_error(5)));
        // Control fault: actual registered JoinHandle, not a natural monitor panic.
        host.resources.spawn_monitor(|| {
            tokio::spawn(async {
                panic!("injected monitor role panic");
            })
        });
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        let state = host.resources.state();
        assert_eq!(state.first.as_ref().unwrap().facts().raw_os_code, Some(5));
        assert_eq!(state.additional, 1);
        assert!(state.monitor_started && state.monitor_joined);
        assert!(state.endpoint_drained);
        assert_eq!(state.attempts_drained, 1);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_udp_parent_failure_transfers_typed_error_to_node_and_joins_child() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        data.udp_runtime.fault.store(1, Ordering::Release);
        let receiver = data.start_udp();
        let mut uni = peers[0].open_uni().await.unwrap();
        uni.write_all(b"blocked").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !data.udp_runtime.node_stopping()
                || data.udp_runtime.child_count.load(Ordering::Acquire) != 0
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(host.data().is_err());
        drop(uni);
        drop(receiver);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        let state = host.resources.state();
        assert_eq!(
            state.first.as_ref().unwrap().facts().origin.phase,
            FailurePhase::UdpJoin
        );
        assert!(state.endpoint_drained);
        assert_eq!(state.attempts_drained, 1);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_identity_failure_preserves_original_inventory_after_zero_and_repair() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        // Control fault in the actual admission owner. Both counters are zero;
        // the different activity identity must still remain a nonzero debt.
        let original = std::mem::replace(
            &mut *data.tcp_pool_admission.current_generation_activity[0]
                .lock()
                .unwrap(),
            Arc::new(TcpPoolGenerationActivity::new()),
        );
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        {
            let mut state = host.resources.state();
            assert!(state.endpoint_drained);
            assert_eq!(state.attempts_drained, 1);
            let terminal = state.terminal.as_mut().unwrap();
            assert!(terminal.invalid);
            *terminal.admission.current_generation_activity[0]
                .lock()
                .unwrap() = original;
            assert!(
                terminal.settled().unwrap(),
                "zero is insufficient to erase a prior identity failure"
            );
        }
        assert_eq!(host.observe_shutdown().await, NodeShutdown::Quarantined);
        assert!(host.resources.state().terminal.as_ref().unwrap().invalid);
        assert!(host.resources.state().first.is_some());
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn publication_missing_original_witness_stays_quarantined_after_return() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let original = host.resources.state().connections[0].take();
        assert!(host.data().is_err());
        host.resources.state().connections[0] = original;
        assert!(host.data().is_err());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        let state = host.resources.state();
        assert!(state.first.is_some());
        assert!(state.endpoint_drained);
        assert_eq!(state.attempts_started, state.attempts_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn unsupported_native_profiles_reject_before_data_publication_and_settle_original_node() {
    for mode in 0..3 {
        let server = server(8);
        let mut cfg = config(server.local_addr().unwrap());
        cfg.relay_policy = Arc::new(RelayFamilyPolicy::from_sources(
            crate::upstream::RelayFamilyPolicySources {
                tuic_native_chunk: (mode == 0).then_some("1"),
                tuic_native_ordered: (mode == 1).then_some("1"),
                d16_byte_owned: (mode == 2).then_some("1"),
                ..Default::default()
            },
        ));
        assert_ne!(
            cfg.relay_policy.tuic_native_override(),
            TuicNativeOverride::None
        );
        let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
        let mut peers = Vec::new();
        let result = guarded(async {
            boot(&mut host, &server, &cfg, &mut peers).await;
            let runtime = Arc::clone(&host.ready.as_ref().unwrap().udp_runtime);
            let task_count = host.resources.state().tasks_started;
            assert!(host.data().is_err(), "unowned Native DATA must not escape");
            let deadline = host.deadline.unwrap();
            assert!(host.data().is_err());
            assert_eq!(host.deadline, Some(deadline));
            assert!(runtime.owner.get().is_none());
            assert_eq!(runtime.child_count.load(Ordering::Acquire), 0);
            {
                let state = host.resources.state();
                assert!(!state.published && !state.monitor_started);
                assert!(state.udp.is_none());
                assert!(
                    state
                        .connections
                        .iter()
                        .flatten()
                        .all(|entry| !entry.published)
                );
                assert_eq!(state.attempts_started, 1);
                assert_eq!(state.tasks_started, task_count);
                assert!(state.first.is_some());
                assert_eq!(state.additional, 0);
            }
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                    .await
                    .unwrap(),
                NodeShutdown::FailedSettled
            );
            assert!(
                tokio::time::timeout(Duration::from_secs(2), peers[0].accept_bi())
                    .await
                    .unwrap()
                    .is_err(),
                "no CONNECT was published"
            );
            let state = host.resources.state();
            assert!(state.endpoint_drained);
            assert_eq!(state.attempts_drained, 1);
            assert_eq!(state.tasks_started, state.tasks_joined);
            assert!(state.connections.iter().all(Option::is_none));
            assert_eq!(state.additional, 0);
        })
        .await;
        finish(host, server, peers, result).await;
    }
}
