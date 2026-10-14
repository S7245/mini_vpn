//! Mixed full F pressure through the saved driver and authenticated Quinn peer.
//! Owns this test's actual peer halves and cleanup, never provider or role state.
//! The last 128 streams remain unread until stop; all offsets come from the root.
use super::f_full::{FillTiming, measured_turn};
use super::f_pressure::credit_turn;
use super::*;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint, upstream_ready_connection};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use crate::upstream::TcpOpenPhase;

const PARTIAL: usize = 128;

fn header() -> Vec<u8> {
    encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()))
}

async fn hold_streams(
    peer: quinn::Connection,
    accepted: Arc<AtomicUsize>,
    mut drain: tokio::sync::oneshot::Receiver<()>,
    prefixes: tokio::sync::oneshot::Sender<usize>,
    published: usize,
    reading_index: Arc<AtomicUsize>,
) -> (usize, usize) {
    let full = published + PARTIAL;
    let mut streams = Vec::with_capacity(full);
    loop {
        tokio::select! {
            biased;
            _ = &mut drain => break,
            next = peer.accept_bi(), if streams.len() < full => {
                let Ok((send, mut recv)) = next else { return (0, 0); };
                let index = streams.len();
                assert_eq!(recv.id().index(), index as u64);
                if index < published {
                    let mut bytes = vec![0; header().len() + 1];
                    reading_index.store(index + 1, Ordering::Release);
                    recv.read_exact(&mut bytes).await.unwrap();
                    reading_index.store(0, Ordering::Release);
                    assert_eq!(&bytes[..header().len()], &header());
                    assert!(bytes[header().len()] < 251);
                }
                // In particular, do NOT read the four partial CONNECT bytes yet.
                streams.push((send, recv));
                accepted.store(streams.len(), Ordering::Release);
            }
        }
    }
    let mut observed = 0;
    for (_, recv) in streams.iter_mut().skip(published) {
        let mut bytes = [0; 4];
        recv.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, &header()[..4]);
        observed += 1;
    }
    let _ = prefixes.send(observed);
    let mut retired = (0, 0);
    for (index, (send, mut recv)) in streams.into_iter().enumerate() {
        let mut unexpected = [0; 16];
        let (read, stopped) = tokio::join!(recv.read(&mut unexpected), send.stopped());
        assert!(stopped.unwrap().is_some(), "actual peer STOP_SENDING");
        if index < published {
            assert!(matches!(read, Ok(None) | Err(quinn::ReadError::Reset(_))));
            retired.0 += 1;
        } else {
            assert!(
                matches!(read, Err(quinn::ReadError::Reset(_))),
                "partial CONNECT cannot finish or publish DATA after stop: {read:?}"
            );
            retired.1 += 1;
        }
    }
    retired
}

async fn partial_pressure_case(published: usize) {
    let full = published + PARTIAL;
    let pair = UdpPair::new(8, 4).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 4, full as u32, true)));
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
                async {
                    let peer = pair.server.accept().await.unwrap().await.unwrap();
                    // AUTH also exceeds four bytes, so consume it during connect.
                    let mut auth = peer.accept_uni().await.unwrap();
                    let bytes = auth.read_to_end(64).await.unwrap();
                    let mut token = [0; 32];
                    peer.export_keying_material(
                        &mut token,
                        &config.uuid,
                        config.password.as_bytes(),
                    )
                    .unwrap();
                    assert_eq!(bytes, encode_authenticate(&config.uuid, &token));
                    (peer, auth)
                }
            )
        })
        .await
        .unwrap()
    })
    .await;
    let (upstream, peer, auth) = match connected {
        Ok((Ok(upstream), (peer, auth))) => (Arc::new(upstream), peer, auth),
        failure => {
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("actual TUIC setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let carrier = upstream_ready_connection(&upstream).unwrap().stable_id();
    let inbound = PacketLink::bounded(16_384);
    let outbound = PacketLink::bounded(16_384);
    let mut apps = RealApps::unconnected(outbound.clone(), inbound.clone(), full + 1);
    apps.targets.fill(0);
    let links = (inbound.clone(), outbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let accepted = Arc::new(AtomicUsize::new(0));
    let reading_index = Arc::new(AtomicUsize::new(0));
    let (drain, drain_rx) = tokio::sync::oneshot::channel();
    let mut drain = Some(drain);
    let (prefixes, prefix_rx) = tokio::sync::oneshot::channel();
    let mut prefix_rx = Some(prefix_rx);
    let mut server = Some(tokio::spawn(hold_streams(
        peer.clone(),
        accepted.clone(),
        drain_rx,
        prefixes,
        published,
        reading_index.clone(),
    )));
    let mut phase = "fill";
    let scenario = capture(async {
        for end in (128..=published).step_by(128) {
            apps.connect_range(end - 128..end);
            apps.targets[end - 128..end].fill(1);
            let started = std::time::Instant::now();
            let mut timing = FillTiming::default();
            let mut observation = (0usize, Duration::ZERO, Duration::ZERO);
            let filled = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    measured_turn(&mut session, &mut apps, &mut timing).await;
                    let observed = std::time::Instant::now();
                    let published = session.control.published_flow_count().unwrap();
                    let elapsed = observed.elapsed();
                    observation.0 += 1;
                    observation.1 += elapsed;
                    observation.2 = observation.2.max(elapsed);
                    if published == end as u64
                        && accepted.load(Ordering::Acquire) == end { break; }
                }
            }).await;
            eprintln!("FULL_PARTIAL fill={end} elapsed={:?} accepted={} reading_ordinal={} header={} cursor={} peer_rtt={:?} timing={timing:?} observation_count_total_max={observation:?}",
                started.elapsed(), accepted.load(Ordering::Acquire), reading_index.load(Ordering::Acquire),
                header().len(), session.control.opens().cursor, peer.rtt());
            if filled.is_err() {
                let rows: Vec<_> = session.control.flow_debug().into_iter()
                    .filter(|row| !row.starts_with("stage=Published ")).take(128).collect();
                let (quarantined, stop_deadline) = {
                    let ledger = session.control.ledger();
                    (ledger.quarantined, ledger.shutdown_deadline)
                };
                eprintln!("FULL_PARTIAL stalled quarantined={} stop_deadline={:?} peer_finished={} rows={rows:?}",
                    quarantined, stop_deadline,
                    server.as_ref().is_some_and(|task| task.is_finished()));
            }
            assert!(filled.is_ok(), "real fill {end}: {:?}", session.control.flow_observation());
            let seen = session.control.flow_observation().unwrap();
            assert_eq!(session.control.published_flow_count(), Some(seen.published));
            assert_eq!((seen.live, seen.f, seen.w, seen.running), (end, end, 0, end * 3));
            assert_eq!((seen.released, seen.joined), (0, 0));
            assert!(seen.audit && !session.control.ledger().quarantined);
            assert_eq!((links.0.dropped_packets(), links.1.dropped_packets()), (0, 0));
        }
        phase = "partial128";
        let admitted_after = tokio::time::Instant::now();
        apps.connect_range(published..full);
        apps.targets[published..full].fill(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, true).await;
                let rows = session.control.partial_connect_observations();
                if rows.len() == PARTIAL && rows.iter().all(|r| r.view.connect_written == 4)
                    && accepted.load(Ordering::Acquire) == full { break; }
            }
        }).await.expect("128 real unconsumed four-byte CONNECT streams");
        let frozen_at = tokio::time::Instant::now();
        let partials = session.control.partial_connect_observations();
        let slots = || session.control.opens().slots.iter().flatten()
            .map(|s| (s.handle, s.epoch, s.deadline)).collect::<Vec<_>>();
        let frozen_slots = slots();
        assert_eq!(frozen_slots.len(), full);
        let identities: std::collections::HashSet<_> = partials.iter()
            .map(|r| (r.id.index, r.id.handle, r.id.epoch, r.id.generation)).collect();
        assert_eq!(identities.len(), PARTIAL);
        let providers: std::collections::HashSet<_> = partials.iter().map(|r| r.provider).collect();
        assert_eq!(providers.len(), PARTIAL);
        for row in &partials {
            assert!(row.physical && row.w && row.f && !row.roles && !row.carrier);
            assert_eq!((row.escrow.len(), row.reserved), (1, crate::client_tun::MAX_UPLINK_BUFFER));
            assert_eq!(row.view.phase, TcpOpenPhase::Connecting);
            assert_eq!((row.view.connect_written, row.view.connect_bytes), (4, header().len()));
            // Connecting already owns the bi-retirement witness; it is not retired.
            assert!(row.view.lease_held && row.view.retirement_pending);
            assert_eq!(row.open_deadline, row.deadline.into_std());
            assert!(row.deadline >= admitted_after + Duration::from_secs(20)
                && row.deadline <= frozen_at + Duration::from_secs(20));
        }
        phase = "unadmittedC4097";
        apps.connect_range(full..full + 1);
        apps.targets[full] = 8192;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, true).await;
                if apps.sent[full] == 8192 && apps.queued(full) == 4096
                    && apps.windows[full] == Some(0) { break; }
            }
        }).await.expect("C4097 stays outside root admission with real TCP backpressure");
        assert_eq!(apps.established(), full + 1);
        for _ in 0..8 {
            // credit_turn drops only the run observer; the host retains the loop.
            credit_turn(&mut session, &mut apps, true).await;
            assert_eq!(session.control.partial_connect_observations(), partials);
            assert_eq!(session.control.opens().slots.iter().flatten()
                .map(|s| (s.handle, s.epoch, s.deadline)).collect::<Vec<_>>(), frozen_slots);
            let seen = session.control.flow_observation().unwrap();
            assert_eq!((seen.live, seen.f, seen.w, seen.waiting), (full, full, PARTIAL, 0));
            assert_eq!((seen.factory_started, seen.attempts, seen.published, seen.running),
                (full as u64, full, published as u64, published * 3));
            assert_eq!((seen.escrow_bytes, seen.reserved), (PARTIAL, PARTIAL * crate::client_tun::MAX_UPLINK_BUFFER));
            assert_eq!((seen.released, seen.joined, seen.retry), (0, 0, 0));
            assert_eq!((apps.queued(full), apps.windows[full]), (4096, Some(0)));
            assert!(seen.audit && !session.control.ledger().quarantined);
        }
        phase = "stop-prefix";
        let started = tokio::time::Instant::now();
        session.request_stop();
        let stop_deadline = session.control.ledger().shutdown_deadline.unwrap();
        assert!(stop_deadline >= started + Duration::from_secs(15));
        assert!(stop_deadline <= tokio::time::Instant::now() + Duration::from_secs(15));
        drain.take().unwrap().send(()).unwrap();
        // Do not poll the saved host until the real peer has captured all prefixes.
        let prefix_deadline = stop_deadline.min(tokio::time::Instant::now() + Duration::from_secs(3));
        assert_eq!(tokio::time::timeout_at(prefix_deadline, prefix_rx.take().unwrap())
            .await.unwrap().unwrap(), PARTIAL);
        assert_eq!(session.control.partial_connect_observations(), partials);
        phase = "stop-join";
        let mut retained = None;
        let stopped = tokio::time::timeout_at(stop_deadline, async {
            loop {
                apps.turn(true).await;
                if session.state() != SessionDeviceState::DeviceFailedSettled {
                    tokio::select! {
                        result = session.run() => assert_eq!(result, SessionDeviceState::DeviceFailedSettled),
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    }
                }
                if let Some(source) = session.control.retained_provider_failure() {
                    let identity = (source.0, source.2);
                    if let Some(previous) = &retained { assert_eq!(previous, &identity); }
                    else { retained = Some(identity); }
                }
                if session.state() == SessionDeviceState::DeviceFailedSettled
                    && server.as_ref().unwrap().is_finished() { break; }
                tokio::task::yield_now().await;
            }
        }).await;
        eprintln!("FULL_PARTIAL stop complete={} elapsed={:?} flow={:?}",
            stopped.is_ok(), started.elapsed(), session.control.flow_observation());
        assert!(stopped.is_ok(), "original15s shared stop budget");
        assert_eq!(server.take().unwrap().await.unwrap(), (published, PARTIAL));
        apps.turn(true).await;
        let not_closed: Vec<_> = apps.handles.iter().enumerate().filter_map(|(index, h)| {
            let socket = apps.sockets.get::<Socket>(*h);
            (socket.state() != smoltcp::socket::tcp::State::Closed)
                .then_some((index, socket.state(), socket.send_queue(), socket.recv_queue()))
        }).collect();
        assert!(not_closed.is_empty(), "published={published} unclosed actual apps: {not_closed:?}");
        let done = session.control.flow_observation().unwrap();
        assert_eq!((done.live, done.w, done.f, done.escrow_bytes, done.reserved, done.attempts, done.running), (0, 0, 0, 0, 0, 0, 0));
        assert_eq!((done.factory_started, done.published, done.released, done.joined),
            (full as u64, published as u64, full as u64, (published * 3) as u64));
        assert!(done.audit && !session.control.ledger().quarantined);
        assert_eq!(session.control.retained_provider_io_kind(), Some(std::io::ErrorKind::Interrupted));
        let source = session.control.retained_provider_failure().unwrap();
        assert_eq!(source.1.phase, TcpOpenPhase::Settled);
        assert_eq!((source.1.connect_written, source.1.connect_bytes), (4, header().len()));
        assert!(!source.1.lease_held && !source.1.retirement_pending);
        assert_eq!(Some((source.0, source.2)), retained);
        let failure = {
            let ledger = session.control.ledger();
            let failure = ledger.first_failure.as_ref().unwrap();
            (failure as *const _ as usize, format!("{:?}", failure.facts()))
        };
        for _ in 0..2 {
            assert_eq!(session.observe_shutdown().await, SessionDeviceState::DeviceFailedSettled);
            assert_eq!(session.control.ledger().shutdown_deadline, Some(stop_deadline));
            let source = session.control.retained_provider_failure().unwrap();
            assert_eq!(Some((source.0, source.2)), retained);
            let ledger = session.control.ledger();
            let observed = ledger.first_failure.as_ref().unwrap();
            assert_eq!((observed as *const _ as usize, format!("{:?}", observed.facts())), failure);
        }
        let current = upstream_ready_connection(&upstream).unwrap();
        assert_eq!(current.stable_id(), carrier);
        assert_eq!(current.bi_stream_open_snapshot().0, full as u64);
        assert!(peer.close_reason().is_none(), "carrier cleanup cannot settle flows");
        assert_eq!((links.0.dropped_packets(), links.1.dropped_packets()), (0, 0));
    }).await;
    eprintln!(
        "FULL_PARTIAL scenario complete={} phase={phase}",
        scenario.is_ok()
    );
    session.request_stop();
    if let Some(drain) = drain.take() {
        let _ = drain.send(());
    }
    // Preserve the first panic. Release the peer first, then drain the actual owner.
    if let Some(prefix_rx) = prefix_rx.take() {
        let _ = tokio::time::timeout(Duration::from_secs(3), prefix_rx).await;
    }
    let state = tokio::time::timeout(Duration::from_secs(20), session.observe_shutdown()).await;
    let (joined, pending_server) = if let Some(mut server) = server.take() {
        match tokio::time::timeout(Duration::from_secs(5), &mut server).await {
            Ok(joined) => (Some(joined), None),
            Err(_) => (None, Some(server)),
        }
    } else {
        (None, None)
    };
    let forced_peer_cleanup = pending_server.is_some();
    endpoint.close(0u32.into(), b"full partial fixture cleanup");
    peer.close(0u32.into(), b"full partial fixture cleanup");
    pair.client
        .close(0u32.into(), b"full partial fixture cleanup");
    pair.server
        .close(0u32.into(), b"full partial fixture cleanup");
    if let Some(mut server) = pending_server {
        // Only a failed scenario may need endpoint cancellation; never detach it.
        if tokio::time::timeout(Duration::from_secs(5), &mut server)
            .await
            .is_err()
        {
            server.abort();
            let _ = server.await;
        }
    }
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            endpoint.wait_idle(),
            pair.client.wait_idle(),
            pair.server.wait_idle()
        );
    })
    .await;
    let remaining = (
        endpoint.open_connections(),
        pair.client.open_connections(),
        pair.server.open_connections(),
    );
    eprintln!(
        "FULL_PARTIAL endpoints complete={} remaining={remaining:?}",
        idle.is_ok()
    );
    let cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state.unwrap(), SessionDeviceState::DeviceFailedSettled);
    assert!(
        !forced_peer_cleanup,
        "peer retirement must precede endpoint cleanup"
    );
    if let Some(joined) = joined {
        joined.unwrap();
    }
    idle.unwrap();
    assert_eq!(remaining, (0, 0, 0));
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_f_partial_connect_observation_cancel_and_stop_preserve_exact_custody() {
    partial_pressure_case(3968).await;
}

#[tokio::test(flavor = "current_thread")]
async fn w_full_partial_stop_closes_unadmitted_tcp_without_forging_flow_release() {
    partial_pressure_case(0).await;
}
