use super::super::*;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};
use crate::owned_upstream::UdpDownlinkEvent;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "current_thread")]
async fn primary_reauth_seals_old_udp_and_preserves_auxiliary() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let mut source = data.start_udp_source().unwrap();
        let clock = Instant::now();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary_hold = data.open_tcp(&target).await.unwrap();
        let mut healthy_aux = data.open_tcp(&target).await.unwrap();
        let (mut aux_send, mut aux_recv) = peers[1].accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        aux_recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        let aux_id = data.tcp_pool_slots[1].state.lock().await.current.identity();
        let old = data.tcp_pool_slots[0].state.lock().await.current.identity();
        let mut prefix = encode_packet(7, &target, b"old-prefix");
        prefix[6] = 2;
        peers[0].send_datagram(prefix.into()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), source.recv(&clock))
                .await
                .unwrap(),
            Some(UdpDownlinkEvent::Ignored)
        );
        peers[0]
            .send_datagram(encode_packet(7, &target, b"old-queued").into())
            .unwrap();
        let mut partial = peers[0].open_uni().await.unwrap();
        partial.write_all(&[5]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while source.generation_pending() != Some(1)
                || data.udp_runtime.child_count.load(Ordering::Relaxed) != 1
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        peers[0].close(17u32.into(), b"primary lost");
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = data.udp_runtime.generation.wait_frozen(old.generation) => {},
                r = host.run() => panic!("node stopped before freeze {r:?}"),
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), host.run())
                .await
                .is_err()
        );
        assert_eq!(
            host.resources.state().attempts_started,
            2,
            "consumer seal must precede new handshake"
        );

        tokio::time::timeout(Duration::from_secs(4), async {
            let recover = async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peers.push(peer);
                while data.tcp_pool_slots[0].state.lock().await.current.identity() == old {
                    tokio::task::yield_now().await;
                }
            };
            let drain = async {
                loop {
                    assert!(!matches!(
                        source.recv(&clock).await,
                        Some(UdpDownlinkEvent::Deliver(_))
                    ));
                }
            };
            tokio::pin!(recover, drain);
            tokio::select! {
                _ = &mut recover => {},
                _ = &mut drain => panic!("source ended"),
                r = host.run() => panic!("node stopped {r:?}"),
            }
        })
        .await
        .unwrap();
        assert_eq!(
            data.tcp_pool_slots[1].state.lock().await.current.identity(),
            aux_id
        );
        assert_eq!(host.resources.state().attempts_started, 3);
        let mut suffix = encode_packet(7, &target, b"new-suffix");
        suffix[6] = 2;
        suffix[7] = 1;
        peers[2].send_datagram(suffix.into()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), source.recv(&clock))
                .await
                .unwrap(),
            Some(UdpDownlinkEvent::Ignored)
        );
        peers[2]
            .send_datagram(encode_packet(7, &target, b"fresh-downlink").into())
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), source.recv(&clock))
            .await
            .unwrap()
            .unwrap();
        let UdpDownlinkEvent::Deliver(packet) = received else {
            panic!("fresh UDP not delivered")
        };
        assert_eq!(packet.association().get(), 7);
        assert_eq!(packet.payload().as_ref(), b"fresh-downlink");
        data.send_udp(encode_packet(7, &target, b"fresh-uplink"))
            .await;
        assert_eq!(
            peers[2].read_datagram().await.unwrap().as_ref(),
            encode_packet(7, &target, b"fresh-uplink")
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut stream = data.open_tcp(&target).await.unwrap();
            stream.write_all(b"new TCP").await.unwrap();
            stream.shutdown().await.unwrap();
            let (mut remote_send, mut remote_recv) = peers[2].accept_bi().await.unwrap();
            assert_eq!(
                remote_recv.read_to_end(1024).await.unwrap(),
                [encode_connect(&target), b"new TCP".to_vec()].concat()
            );
            remote_send.write_all(b"reply").await.unwrap();
            remote_send.finish().unwrap();
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"reply");
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            healthy_aux.write_all(b"healthy").await.unwrap();
            let mut bytes = [0; 7];
            aux_recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"healthy");
            aux_send.write_all(b"alive").await.unwrap();
            let mut reply = [0; 5];
            healthy_aux.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"alive");
        })
        .await
        .unwrap();
        drop(primary_hold);
        drop(healthy_aux);
        drop(aux_send);
        drop(aux_recv);
        drop(partial);
        drop(source);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn primary_closed_before_udp_pump_first_poll_still_seals() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let mut source = data.start_udp_source().unwrap();
        // No await since start: the original UDP task cannot have been polled.
        data.tcp_pool_slots[0]
            .state
            .try_lock()
            .unwrap()
            .current
            .transport
            .close(17u32.into(), b"closed before pump first poll");
        let clock = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), async {
            let work = async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peers.push(peer);
                while data.tcp_pool_slots[0]
                    .state
                    .lock()
                    .await
                    .current
                    .identity()
                    .generation
                    == 1
                {
                    tokio::task::yield_now().await;
                }
            };
            tokio::pin!(work);
            tokio::select! {
                _ = &mut work => {},
                _ = async { loop { source.recv(&clock).await; } } => unreachable!(),
                r = host.run() => panic!("node ended: {r:?}"),
            }
        })
        .await
        .unwrap();
        drop(source);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn stop_seals_full_original_queue_and_child_without_dropping_source() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        assert!(
            host.ready.as_ref().unwrap().start_udp_source().is_err(),
            "typed API cannot bypass NodeHost attachment"
        );
        let data = host.data().unwrap();
        let mut source = data.start_udp_source().unwrap();
        let mut partial = peers[0].open_uni().await.unwrap();
        partial.write_all(b"partial").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.udp_runtime.child_count.load(Ordering::Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for batch in 1..=16 {
            for _ in 0..64 {
                peers[0]
                    .send_datagram(bytes::Bytes::from_static(b"queued"))
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                while source.generation_pending() != Some(batch * 64) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        peers[0]
            .send_datagram(bytes::Bytes::from_static(b"blocked"))
            .unwrap();
        peers[0].close(17u32.into(), b"full queue primary loss");
        tokio::time::timeout(
            Duration::from_secs(2),
            data.udp_runtime.generation.wait_frozen(1),
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), host.run())
                .await
                .is_err()
        );
        assert_eq!(host.resources.state().attempts_started, 1);
        host.request_stop();
        drop(data);
        drop(partial);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), host.drive_shutdown())
                .await
                .is_err(),
            "unpolled original source still owns queue debt"
        );
        assert_eq!(source.generation_pending(), Some(1024));
        let clock = Instant::now();
        let (shutdown, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(host.drive_shutdown(), async {
                while let Some(event) = source.recv(&clock).await {
                    assert_eq!(event, UdpDownlinkEvent::Ignored);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(shutdown, NodeShutdown::RunningResourcesSettled);
        assert_eq!(source.generation_pending(), Some(0));
        let state = host.resources.state();
        assert!(state.endpoint_drained);
        assert_eq!(state.attempts_started, state.attempts_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn primary_and_auxiliary_keep_two_original_predecessor_permits() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let primary = data.open_tcp(&target).await.unwrap();
        let auxiliary = data.open_tcp(&target).await.unwrap();
        // No UDP start: explicit NotStarted custody, not a fabricated consumer ACK.
        for slot in 0..2 {
            data.tcp_pool_slots[slot].state.lock().await.current.transport.close(17u32.into(), b"retained predecessor");
            let ticket = data.request_aux_recovery(slot, data.tcp_pool_slots[slot].state.lock().await).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = async {
                        tokio::join!(async { data.await_aux_recovery(ticket).await.unwrap(); }, async {
                            let peer = server.accept().await.unwrap().await.unwrap(); auth(&peer, &cfg).await; peers.push(peer);
                        });
                    } => {},
                    r = host.run() => panic!("node ended: {r:?}"),
                }
            }).await.unwrap();
        }
        {
            let state = host.resources.state();
            assert!(state.recovery.primary_predecessor.is_some());
            assert!(state.recovery.predecessor.is_some());
            assert!(state.recovery.primary_permit.load(Ordering::Acquire));
            assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
            assert!(state.connections.iter().flatten().count() <= 5);
            assert_eq!(state.attempts_started, 4);
        }
        drop(data);
        assert!(tokio::time::timeout(Duration::from_millis(30), host.drive_shutdown()).await.is_err());
        drop(primary);
        assert!(tokio::time::timeout(Duration::from_millis(30), host.drive_shutdown()).await.is_err());
        drop(auxiliary);
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
        let state = host.resources.state();
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert!(state.endpoint_drained);
    }).await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn missing_consumer_seal_keeps_first_deadline_and_prevents_handshake() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let source = data.start_udp_source().unwrap();
        data.tcp_pool_slots[0]
            .state
            .lock()
            .await
            .current
            .transport
            .close(17u32.into(), b"missing consumer poll");
        let ticket = data
            .request_aux_recovery(0, data.tcp_pool_slots[0].state.lock().await)
            .unwrap();
        let start = tokio::time::Instant::now();
        let failure = tokio::time::timeout(Duration::from_secs(16), async {
            tokio::select! {
                result = data.await_aux_recovery(ticket) => result,
                r = host.run() => panic!("owner ended before original demand: {r:?}"),
            }
        })
        .await
        .unwrap();
        assert!(failure.is_err());
        assert!(start.elapsed() >= Duration::from_secs(15));
        assert_eq!(host.resources.state().attempts_started, 1);
        assert!(data.udp_runtime.generation.frozen(1));
        assert!(data.await_aux_recovery(ticket).await.is_err());
        assert_eq!(host.resources.state().attempts_started, 1);
        drop(source);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn raw_receiver_refuses_primary_recovery_without_minting_candidate() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let raw = data.start_udp();
        data.tcp_pool_slots[0]
            .state
            .lock()
            .await
            .current
            .transport
            .close(17u32.into(), b"raw unsupported");
        assert!(
            data.request_aux_recovery(0, data.tcp_pool_slots[0].state.lock().await)
                .is_err()
        );
        assert_eq!(host.resources.state().attempts_started, 1);
        assert!(!data.udp_runtime.generation.frozen(1));
        drop(raw);
        drop(data);
        let result = tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
            .await
            .unwrap();
        assert!(matches!(
            result,
            NodeShutdown::RunningResourcesSettled | NodeShutdown::FailedSettled
        ));
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_only_udp_inventory_failure_retains_first_error() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let source = data.start_udp_source().unwrap();
        crate::owned_upstream::generation::tests::poison_publication(&data.udp_runtime.generation);
        drop(source);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        let state = host.resources.state();
        assert!(
            state.first.is_some(),
            "Q must preserve its original UDP inventory error"
        );
        assert!(state.endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn primary_freeze_wakes_only_after_original_root_and_slot_unlock() {
    struct Probe {
        owner: Arc<StartupResources>,
        slot: Arc<TcpPoolGenerationSlot<Connection>>,
        called: AtomicBool,
        unlocked: AtomicBool,
    }
    impl std::task::Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.called.store(true, Ordering::Release);
            self.unlocked.store(
                self.owner.0.try_lock().is_ok() && self.slot.state.try_lock().is_ok(),
                Ordering::Release,
            );
        }
    }
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        {
            let probe = Arc::new(Probe {
                owner: host.resources.clone(),
                slot: data.tcp_pool_slots[0].clone(),
                called: AtomicBool::new(false),
                unlocked: AtomicBool::new(false),
            });
            let waker = std::task::Waker::from(probe.clone());
            let mut cx = Context::from_waker(&waker);
            let notification = data.udp_runtime.generation.changed.notified();
            tokio::pin!(notification);
            assert!(notification.as_mut().poll(&mut cx).is_pending());
            let slot = data.tcp_pool_slots[0].state.lock().await;
            slot.current
                .transport
                .close(17u32.into(), b"freeze notification");
            data.request_aux_recovery(0, slot).unwrap();
            assert!(probe.called.load(Ordering::Acquire));
            assert!(probe.unlocked.load(Ordering::Acquire));
        }
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
    })
    .await;
    finish(host, server, peers, result).await;
}
