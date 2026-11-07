//! Live primary/old auxiliary TCP and original UDP while one actual staged
//! Healthy demand owns three slow candidates under its original shared 5s.
use super::super::*;
use super::running::boot;
use super::startup::{auth, config, finish, guarded, server};
use crate::owned_upstream::{LegacyOwnedUpstream, OpenedOwnedTcp, OwnedUpstream};
use crate::upstream::{TcpOpenAttempt, TcpOpenPhase, TcpOpenRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Open = Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>;
fn step(open: &mut Open) {
    open.step(
        &mut Context::from_waker(std::task::Waker::noop()),
        Instant::now(),
    );
}
async fn roundtrip(
    stream: &mut crate::upstream::RelayStream,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) {
    stream.write_all(b"live").await.unwrap();
    let mut bytes = [0; 4];
    recv.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"live");
    send.write_all(b"back").await.unwrap();
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"back");
}
fn first(owner: &StartupResources) -> Option<(usize, String, Vec<u8>)> {
    let root = owner.state();
    root.first.as_ref().map(|failure| {
        (
            failure as *const _ as usize,
            format!("{:?}", failure.facts()),
            failure.text_bytes().to_vec(),
        )
    })
}

#[tokio::test(flavor = "current_thread")]
async fn three_slow_staged_candidates_keep_live_primary_udp_and_original_five_before_settled_fallback()
 {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let scenario = async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let mut primary = data.open_tcp(&target).await.unwrap();
        let mut old = data.open_tcp(&target).await.unwrap();
        let (mut primary_send, mut primary_recv) = peers[0].accept_bi().await.unwrap();
        let (mut old_send, mut old_recv) = peers[1].accept_bi().await.unwrap();
        let header = encode_connect(&target);
        let mut actual = vec![0; header.len()];
        primary_recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        old_recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        let mut downlink = data.start_udp();
        // Retain the real clock and real age gate, as in the existing staged path.
        tokio::select! { _=tokio::time::sleep(Duration::from_secs(TUIC_TCP_POOL_STALE_RECONNECT_SECS))=>{}, result=host.run()=>panic!("node ended {result:?}") }
        let original = {
            let mut slot = data.tcp_pool_slots[1].state.lock().await;
            let identity = slot.current.identity();
            slot.current.successor_service_certificate = Some(TcpPoolSuccessorServiceCertificate {
                identity,
                path_generation: slot.current.transport.current_path_generation() + 1,
                proved_cwnd_floor: 1,
            });
            identity
        };
        let mut open = Arc::new(LegacyOwnedUpstream::new(data.clone()))
            .staged_tcp_open(TcpOpenRequest {
                target: target.clone(),
                deadline: Instant::now() + Duration::from_secs(20),
            })
            .unwrap();
        let resources = host.resources.clone();
        let mut baseline = None;
        let mut earliest = None;
        let mut timings = Vec::new();
        let mut payload_rounds = 0;
        let work = async {
            let opening = async {
                let admission_started = tokio::time::Instant::now();
                loop {
                    step(&mut open);
                    let after = tokio::time::Instant::now();
                    assert!(open.failure().is_none(), "{:?}", open.failure());
                    let phase = open.view().phase;
                    {
                        let root = resources.state();
                        if let Some(snapshot) = root.recovery.healthy_snapshot_for_test(1) {
                            if let Some((ticket, deadline)) = baseline {
                                assert_eq!(
                                    (snapshot.ticket, snapshot.deadline),
                                    (ticket, deadline)
                                );
                            } else {
                                assert!(
                                    snapshot.deadline >= admission_started + Duration::from_secs(5)
                                        && snapshot.deadline <= after + Duration::from_secs(5)
                                );
                                baseline = Some((snapshot.ticket, snapshot.deadline));
                            }
                        }
                        if root.attempts_drained < 3 {
                            assert_ne!(phase, TcpOpenPhase::Ready);
                            assert_eq!(data.tcp_pool_admission.active_total(), 2);
                        }
                        if phase == TcpOpenPhase::Ready {
                            assert_eq!(root.attempts_started, 5);
                            assert_eq!(root.attempts_drained, 3);
                            assert!(root.recovery.healthy_snapshot_for_test(1).unwrap().settled);
                            assert!(
                                root.recovery.preparation.is_none()
                                    && root.recovery.permit.is_none()
                                    && root.recovery.predecessor.is_none()
                            );
                        }
                    }
                    if let Some(observed) = first(&resources) {
                        if let Some(original) = &earliest {
                            assert_eq!(&observed, original);
                        } else {
                            earliest = Some(observed);
                        }
                    }
                    if phase == TcpOpenPhase::Ready {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let candidates = async {
                for attempt in 1..=3 {
                    let incoming = server.accept().await.unwrap();
                    let accepted = tokio::time::Instant::now();
                    let snapshot = resources
                        .state()
                        .recovery
                        .healthy_snapshot_for_test(1)
                        .unwrap();
                    assert_eq!(snapshot.attempts, attempt);
                    assert_eq!(resources.state().attempts_started, 2 + u64::from(attempt));
                    assert!(!snapshot.settled);
                    assert!(data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
                    assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
                    roundtrip(&mut primary, &mut primary_send, &mut primary_recv).await;
                    roundtrip(&mut old, &mut old_send, &mut old_recv).await;
                    data.send_udp(b"live-udp".to_vec()).await;
                    assert_eq!(
                        peers[0].read_datagram().await.unwrap().as_ref(),
                        b"live-udp"
                    );
                    peers[0]
                        .send_datagram(bytes::Bytes::from_static(b"back-udp"))
                        .unwrap();
                    assert_eq!(downlink.recv().await.unwrap(), b"back-udp");
                    payload_rounds += 1;
                    if attempt < 3 {
                        tokio::time::sleep_until(accepted + Duration::from_millis(600)).await;
                    } else {
                        // Keep the real third Incoming unresolved through the
                        // ORIGINAL deadline, until its client witness really drained.
                        while resources.state().attempts_drained < 3 {
                            assert_eq!(
                                resources
                                    .state()
                                    .recovery
                                    .healthy_snapshot_for_test(1)
                                    .unwrap()
                                    .deadline,
                                snapshot.deadline
                            );
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        assert!(tokio::time::Instant::now() >= snapshot.deadline);
                    }
                    let refused = tokio::time::Instant::now();
                    incoming.refuse();
                    timings.push((accepted, refused));
                }
            };
            tokio::join!(opening, candidates);
        };
        let mut work = Box::pin(work);
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(40),async {tokio::select! { _=&mut work=>panic!("slow work finished early"),result=host.run()=>panic!("node ended {result:?}") }}).await.is_err());
        }
        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::select! { _=&mut work=>{},result=host.run()=>panic!("node ended {result:?}") }
        })
        .await
        .unwrap();
        drop(work);
        assert_eq!(payload_rounds, 3);
        assert_eq!(timings.len(), 3);
        assert!(timings[1].0 - timings[0].1 >= Duration::from_millis(250));
        assert!(timings[2].0 - timings[1].1 >= Duration::from_millis(500));
        let (ticket, deadline) = baseline.unwrap();
        assert!(tokio::time::Instant::now() >= deadline);
        assert!(data.healthy_fallback_allowed(ticket));
        assert_eq!(
            data.tcp_pool_slots[1].state.lock().await.current.identity(),
            original
        );
        assert!(!data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
        assert!(!data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert_eq!(data.tcp_pool_admission.active_total(), 3);
        let OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(mut fallback)) =
            open.take_carrier(Instant::now()).unwrap()
        else {
            panic!("family changed")
        };
        let (mut send, mut recv) = peers[0].accept_bi().await.unwrap();
        recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        roundtrip(&mut fallback, &mut send, &mut recv).await;
        roundtrip(&mut primary, &mut primary_send, &mut primary_recv).await;
        roundtrip(&mut old, &mut old_send, &mut old_recv).await;
        assert!(peers.iter().all(|peer| peer.close_reason().is_none()));
        assert_eq!(first(&resources), earliest);
        assert!(resources.state().additional >= 2);
        drop((fallback, send, recv));
        let settle_deadline = Instant::now() + Duration::from_secs(3);
        while open.view().phase != TcpOpenPhase::Settled {
            assert!(Instant::now() < settle_deadline);
            open.settle(
                &mut Context::from_waker(std::task::Waker::noop()),
                Instant::now(),
                settle_deadline,
            );
            assert_ne!(open.view().phase, TcpOpenPhase::Quarantined);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(open.failure().is_none());
        drop(open);
        drop((
            primary,
            primary_send,
            primary_recv,
            old,
            old_send,
            old_recv,
            downlink,
            data,
        ));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
        let root = resources.state();
        assert_eq!(root.attempts_started, 5);
        assert_eq!(root.attempts_drained, 5);
        assert_eq!(root.tasks_started, root.tasks_joined);
        assert!(root.endpoint_drained);
        drop(root);
        assert_eq!(first(&resources), earliest);
        eprintln!(
            "actual slow3 shared5/250+500, liveprimary+oldauxTCP/UDP3, failedcandidate drain3 beforeReady, fallbackTCP, final5drains/alljoins/FailedSettled; originalRetainedFailure VALUE only"
        );
    };
    let result = guarded(scenario).await;
    finish(host, server, peers, result).await;
}

async fn app_datagram(peer: &Connection, expected: &[u8]) {
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..4 {
            let packet = peer.read_datagram().await.unwrap();
            if packet.as_ref() == expected {
                return;
            }
            assert_eq!(
                packet.as_ref(),
                encode_heartbeat(),
                "unexpected application datagram"
            );
        }
        panic!("application payload missing within bounded control datagrams");
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn third_slow_staged_candidate_proves_new_service_before_old_carrier_retires() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let mut retained_open: Option<Open> = None;
    let mut fixed_settle = None;
    let scenario = async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let mut primary = data.open_tcp(&target).await.unwrap();
        let mut old = data.open_tcp(&target).await.unwrap();
        let (mut primary_send, mut primary_recv) = peers[0].accept_bi().await.unwrap();
        let (mut old_send, mut old_recv) = peers[1].accept_bi().await.unwrap();
        let header = encode_connect(&target);
        let mut actual = vec![0; header.len()];
        primary_recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        old_recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        let mut downlink = data.start_udp();
        tokio::select! { _=tokio::time::sleep(Duration::from_secs(TUIC_TCP_POOL_STALE_RECONNECT_SECS))=>{}, result=host.run()=>panic!("node ended {result:?}") }
        let original = {
            let mut slot = data.tcp_pool_slots[1].state.lock().await;
            let identity = slot.current.identity();
            slot.current.successor_service_certificate = Some(TcpPoolSuccessorServiceCertificate {
                identity,
                path_generation: slot.current.transport.current_path_generation() + 1,
                proved_cwnd_floor: 1,
            });
            identity
        };
        retained_open = Some(
            Arc::new(LegacyOwnedUpstream::new(data.clone()))
                .staged_tcp_open(TcpOpenRequest {
                    target: target.clone(),
                    deadline: Instant::now() + Duration::from_secs(20),
                })
                .unwrap(),
        );
        let open = retained_open.as_mut().unwrap();
        let resources = host.resources.clone();
        let mut baseline = None;
        let mut earliest = None;
        let mut timings = Vec::new();
        let mut new_peer = None;
        let mut payload_rounds = 0;
        let work = async {
            let opening = async {
                let admission_started = tokio::time::Instant::now();
                loop {
                    step(open);
                    let after = tokio::time::Instant::now();
                    assert!(open.failure().is_none(), "{:?}", open.failure());
                    let phase = open.view().phase;
                    {
                        let root = resources.state();
                        if let Some(snapshot) = root.recovery.healthy_snapshot_for_test(1) {
                            if let Some((ticket, deadline)) = baseline {
                                assert_eq!(
                                    (snapshot.ticket, snapshot.deadline),
                                    (ticket, deadline)
                                );
                            } else {
                                assert!(
                                    snapshot.deadline >= admission_started + Duration::from_secs(5)
                                        && snapshot.deadline <= after + Duration::from_secs(5)
                                );
                                baseline = Some((snapshot.ticket, snapshot.deadline));
                            }
                            if snapshot.attempts < 3 {
                                assert_ne!(phase, TcpOpenPhase::Ready);
                            }
                        }
                        if phase == TcpOpenPhase::Ready {
                            assert_eq!(root.attempts_started, 5);
                            assert_eq!(root.attempts_drained, 2);
                            let snapshot = root.recovery.healthy_snapshot_for_test(1).unwrap();
                            assert_eq!(snapshot.attempts, 3);
                            assert!(!snapshot.failed);
                            assert!(tokio::time::Instant::now() < snapshot.deadline);
                            assert_eq!(
                                root.recovery
                                    .predecessor
                                    .as_ref()
                                    .unwrap()
                                    .generation
                                    .identity,
                                original
                            );
                            assert!(root.recovery.preparation.is_none());
                        }
                    }
                    if let Some(observed) = first(&resources) {
                        if let Some(original) = &earliest {
                            assert_eq!(&observed, original);
                        } else {
                            earliest = Some(observed);
                        }
                    }
                    if phase == TcpOpenPhase::Ready {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let candidates = async {
                for attempt in 1..=3 {
                    let incoming = server.accept().await.unwrap();
                    let accepted = tokio::time::Instant::now();
                    let snapshot = resources
                        .state()
                        .recovery
                        .healthy_snapshot_for_test(1)
                        .unwrap();
                    assert_eq!(snapshot.attempts, attempt);
                    assert_eq!(resources.state().attempts_started, 2 + u64::from(attempt));
                    assert!(data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
                    assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
                    roundtrip(&mut primary, &mut primary_send, &mut primary_recv).await;
                    roundtrip(&mut old, &mut old_send, &mut old_recv).await;
                    data.send_udp(b"live-udp".to_vec()).await;
                    app_datagram(&peers[0], b"live-udp").await;
                    peers[0]
                        .send_datagram(bytes::Bytes::from_static(b"back-udp"))
                        .unwrap();
                    assert_eq!(downlink.recv().await.unwrap(), b"back-udp");
                    payload_rounds += 1;
                    tokio::time::sleep_until(accepted + Duration::from_millis(600)).await;
                    let resolved = tokio::time::Instant::now();
                    if attempt < 3 {
                        incoming.refuse();
                    } else {
                        let peer = incoming.await.unwrap();
                        auth(&peer, &cfg).await;
                        new_peer = Some(peer);
                    }
                    timings.push((accepted, resolved));
                }
            };
            tokio::join!(opening, candidates);
        };
        let mut work = Box::pin(work);
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(40),async {tokio::select! { _=&mut work=>panic!("slow work finished early"),result=host.run()=>panic!("node ended {result:?}") }}).await.is_err());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! { _=&mut work=>{},result=host.run()=>panic!("node ended {result:?}") }
        })
        .await
        .unwrap();
        drop(work);
        peers.push(new_peer.unwrap());
        assert_eq!(payload_rounds, 3);
        assert!(timings[1].0 - timings[0].1 >= Duration::from_millis(250));
        assert!(timings[2].0 - timings[1].1 >= Duration::from_millis(500));
        let (ticket, deadline) = baseline.unwrap();
        assert!(tokio::time::Instant::now() < deadline);
        assert!(!data.healthy_fallback_allowed(ticket));
        let current = data.tcp_pool_slots[1].state.lock().await.current.identity();
        assert_eq!(current.generation, original.generation + 1);
        assert_ne!(current.stable_id, original.stable_id);
        assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert!(!data.tcp_pool_admission.preparing_slots[1].load(Ordering::Acquire));
        assert!(peers[1].close_reason().is_none());
        let OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(mut carrier)) =
            open.take_carrier(Instant::now()).unwrap()
        else {
            panic!("family changed")
        };
        let (mut send, mut recv) = peers[2].accept_bi().await.unwrap();
        recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        roundtrip(&mut carrier, &mut send, &mut recv).await;
        roundtrip(&mut primary, &mut primary_send, &mut primary_recv).await;
        roundtrip(&mut old, &mut old_send, &mut old_recv).await;
        data.send_udp(b"after-ready".to_vec()).await;
        app_datagram(&peers[0], b"after-ready").await;
        peers[0]
            .send_datagram(bytes::Bytes::from_static(b"after-return"))
            .unwrap();
        assert_eq!(downlink.recv().await.unwrap(), b"after-return");
        assert_eq!(resources.state().attempts_drained, 2);
        assert!(data.tcp_pool_draining_predecessor.load(Ordering::Acquire));
        assert!(peers[1].close_reason().is_none());
        drop((old, old_send, old_recv));
        tokio::time::timeout(Duration::from_secs(3),async {
            tokio::select! { _=async {while data.tcp_pool_draining_predecessor.load(Ordering::Acquire) {tokio::time::sleep(Duration::from_millis(1)).await;}}=>{},result=host.run()=>panic!("node ended {result:?}") }
        }).await.unwrap();
        {
            let root = resources.state();
            assert_eq!(root.attempts_drained, 3);
            assert!(root.recovery.predecessor.is_none());
            assert!(
                root.tasks
                    .iter()
                    .all(|task| task.stable_id != Some(original.stable_id))
            );
        }
        assert!(peers[1].close_reason().is_some());
        roundtrip(&mut carrier, &mut send, &mut recv).await;
        roundtrip(&mut primary, &mut primary_send, &mut primary_recv).await;
        assert_eq!(first(&resources), earliest);
        drop((carrier, send, recv));
        let settle_deadline =
            *fixed_settle.get_or_insert_with(|| Instant::now() + Duration::from_secs(3));
        while open.view().phase != TcpOpenPhase::Settled {
            assert!(Instant::now() < settle_deadline);
            open.settle(
                &mut Context::from_waker(std::task::Waker::noop()),
                Instant::now(),
                settle_deadline,
            );
            assert_ne!(open.view().phase, TcpOpenPhase::Quarantined);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(open.failure().is_none());
        drop(retained_open.take());
        drop((primary, primary_send, primary_recv, downlink, data));
        let shutdown_started = tokio::time::Instant::now();
        let observed = tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown()).await;
        let original_deadline = host.deadline.unwrap();
        assert!(original_deadline >= shutdown_started + Duration::from_secs(15));
        let final_state = match observed {
            Ok(state) => state,
            Err(_) => {
                {
                    let root = resources.state();
                    let entries = root
                        .connections
                        .iter()
                        .flatten()
                        .map(|entry| {
                            let retirement = match &entry.retirement {
                                Retirement::Ready(_) => "Ready",
                                Retirement::Waiting(_) => "Waiting",
                                Retirement::Failed => "Failed",
                            };
                            (entry.stable_id, entry.published, retirement)
                        })
                        .collect::<Vec<_>>();
                    let tasks = root
                        .tasks
                        .iter()
                        .map(|task| (task.stable_id, task.handle.is_finished()))
                        .collect::<Vec<_>>();
                    let terminal = root.terminal.as_ref().map(|pool| {
                        (
                            pool.invalid,
                            pool.admission.active_total(),
                            pool.generations
                                .iter()
                                .map(|generation| {
                                    (
                                        generation.slot,
                                        generation.identity,
                                        generation.physical_drained,
                                        generation.activity.active.load(Ordering::Acquire),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        )
                    });
                    eprintln!(
                        "originalNode3s Pending: now={:?} first15={original_deadline:?} attempts={}/{} entries={entries:?} tasks={}/{} handles={tasks:?} udpPresent={} uplinkPresent={} monitorPresent={} monitorJoined={} endpointDrained={} terminal={terminal:?}",
                        tokio::time::Instant::now(),
                        root.attempts_started,
                        root.attempts_drained,
                        root.tasks_started,
                        root.tasks_joined,
                        root.udp.is_some(),
                        !root.uplink.is_empty(),
                        root.monitor.is_some(),
                        root.monitor_joined,
                        root.endpoint_drained
                    );
                }
                // Same original cleanup, bounded by its already fixed first15.
                // No new deadline, no unbounded drive and no abort-as-join.
                host.observe_shutdown().await
            }
        };
        assert_eq!(host.deadline, Some(original_deadline));
        assert!(tokio::time::Instant::now() <= original_deadline);
        assert_eq!(final_state, NodeShutdown::FailedSettled);
        {
            let root = resources.state();
            assert_eq!(root.attempts_started, 5);
            assert_eq!(root.attempts_drained, 5);
            assert_eq!(root.tasks_started, root.tasks_joined);
            assert!(root.endpoint_drained);
        }
        assert_eq!(first(&resources), earliest);
        eprintln!(
            "actual slow3 successor viaoriginalserviceproof/newauxCONNECTDATA; oldTCPusefuluntilreturned, oldwitnessdrain+statsjoin thenpermitrelease; final5drains/FailedSettled/firstRetainedFailureVALUEstable"
        );
    };
    let result = guarded(scenario).await;
    // Original provider remains outside the scenario unwind boundary. All actual
    // carrier users have now returned/dropped, so finally can settle that owner.
    let cleanup = guarded(async {
        if let Some(open) = retained_open.as_mut() {
            let deadline =
                *fixed_settle.get_or_insert_with(|| Instant::now() + Duration::from_secs(3));
            while open.view().phase != TcpOpenPhase::Settled {
                assert!(
                    Instant::now() < deadline,
                    "original provider cleanup {:?}",
                    open.view()
                );
                open.settle(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    Instant::now(),
                    deadline,
                );
                assert_ne!(open.view().phase, TcpOpenPhase::Quarantined);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        drop(retained_open.take());
    })
    .await;
    if cleanup.is_err() {
        eprintln!(
            "original provider finally failed; preserve charged Open, scenario error remains first"
        );
        std::mem::forget(retained_open.take());
    }
    finish(host, server, peers, result.and(cleanup)).await;
}
