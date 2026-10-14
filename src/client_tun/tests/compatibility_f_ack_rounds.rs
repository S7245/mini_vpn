//! Real F4096/W128 ACK rounds with one pressured A, healthy B and retained C.
//! Owns only bounded peer halves/tasks and test observations. The saved root
//! owns every factory, role, deadline and release; no FIFO assumption is used.
use super::*;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint, upstream_ready_connection};
use crate::tuic::{decode_packet_meta, encode_authenticate, encode_connect, encode_packet};
use smoltcp::socket::udp::{PacketBuffer, PacketMetadata, Socket as UdpSocket};
use std::collections::HashSet;

const DNS: &[u8] =
    b"\x72\x32\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x06rounds\x07example\x00\x00\x01\x00\x01";
const UDP: &[u8] = b"real-ack-rounds";
const REPLY: &[u8] = b"healthy-through-held-C";

enum Command {
    Close(std::ops::Range<usize>, tokio::sync::oneshot::Sender<()>),
    Health(tokio::sync::oneshot::Sender<()>),
    Drain,
}

async fn peer_owner(
    peer: quinn::Connection,
    mut commands: mpsc::Receiver<Command>,
    observed: Arc<Mutex<Vec<(u64, u8)>>>,
    a_send: tokio::sync::oneshot::Sender<quinn::SendStream>,
) -> usize {
    let header = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
    let mut halves = Vec::with_capacity(4224);
    let mut a_send = Some(a_send);
    let mut retired = 0;
    loop {
        tokio::select! {
            stream = peer.accept_bi() => {
                let (send, mut recv) = stream.unwrap();
                let mut bytes = vec![0; header.len() + 1];
                recv.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes[..header.len()], &header);
                let index = recv.id().index();
                assert_eq!(index as usize, halves.len());
                assert!(halves.len() < 4224);
                let send = if index == 0 {
                    assert_eq!(bytes[header.len()], 0);
                    a_send.take().unwrap().send(send).unwrap();
                    None
                } else { Some(send) };
                halves.push(Some((send, recv)));
                observed.lock().unwrap().push((index, bytes[header.len()]));
            }
            command = commands.recv() => match command {
                Some(Command::Close(range, done)) => {
                    assert!(range.len() <= 64 && range.start >= 2);
                    for index in range {
                        let (send, mut recv) = halves[index].take().unwrap();
                        send.unwrap().reset(77u32.into()).unwrap();
                        recv.stop(78u32.into()).unwrap();
                        retired += 1;
                    }
                    done.send(()).unwrap();
                }
                Some(Command::Health(done)) => {
                    let (send, recv) = halves[1].as_mut().unwrap();
                    let mut bytes = [0; 32];
                    recv.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(bytes, [0x42; 32]);
                    send.as_mut().unwrap().write_all(REPLY).await.unwrap();
                    done.send(()).unwrap();
                }
                Some(Command::Drain) => {
                    for (send, mut recv) in halves.into_iter().flatten() {
                        if let Some(send) = send {
                            assert!(send.stopped().await.unwrap().is_some());
                        }
                        let mut byte = [0];
                        assert!(matches!(recv.read(&mut byte).await,
                            Ok(None) | Err(quinn::ReadError::Reset(_))));
                        retired += 1;
                    }
                    return retired;
                }
                None => return retired,
            }
        }
    }
}

async fn pressure_writer(
    mut send: quinn::SendStream,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> usize {
    let payload = vec![0x63; 2 * 1024 * 1024];
    let mut written = 0;
    let mut signaled = false;
    while written < payload.len() {
        tokio::select! {
            biased;
            _ = &mut stop => { signaled = true; break; },
            result = send.write(&payload[written..]) => match result {
                Ok(n) => { assert!(n > 0); written += n; }
                other => panic!("actual A write: {other:?}"),
            },
        }
    }
    if !signaled {
        let _ = stop.await;
    }
    assert!(send.stopped().await.unwrap().is_some());
    written
}

async fn pulse<F: Future<Output = ()>>(host: &mut ClientTunSession<F>, apps: &mut RealApps) {
    apps.turn(true).await;
    tokio::select! {
        state = host.drive_until_settled() => panic!("premature driver completion: {state:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
}

async fn until<F: Future<Output = ()>>(
    host: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    mut ready: impl FnMut(&RealApps, &SessionControl) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            pulse(host, apps).await;
            if ready(apps, &host.control) {
                break;
            }
        }
    })
    .await
    .expect("actual ACK/health transition within original operation budget");
}

async fn remote<F: Future<Output = ()>, G: Future>(
    host: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    work: G,
) -> G::Output {
    let mut work = std::pin::pin!(work);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            apps.turn(true).await;
            tokio::select! {
                value = &mut work => return value,
                state = host.drive_until_settled() => panic!("premature driver completion: {state:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    }).await.expect("actual peer operation within original budget")
}

fn udp_socket(apps: &mut RealApps, port: u16) -> SocketHandle {
    let mut socket = UdpSocket::new(
        PacketBuffer::new(vec![PacketMetadata::EMPTY; 4], vec![0; 4096]),
        PacketBuffer::new(vec![PacketMetadata::EMPTY; 4], vec![0; 4096]),
    );
    socket.bind(port).unwrap();
    apps.sockets.add(socket)
}

async fn health<F: Future<Output = ()>>(
    host: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    commands: &mpsc::Sender<Command>,
    peer: &quinn::Connection,
    upstream: &TuicUpstream,
    downlink: &mpsc::Sender<Vec<u8>>,
    handles: (SocketHandle, SocketHandle),
) {
    assert_eq!(
        apps.sockets
            .get_mut::<Socket>(apps.handles[1])
            .send_slice(&[0x42; 32])
            .unwrap(),
        32
    );
    let (done, completed) = tokio::sync::oneshot::channel();
    commands.send(Command::Health(done)).await.unwrap();
    remote(host, apps, completed).await.unwrap();
    until(host, apps, |apps, _| {
        apps.sockets.get::<Socket>(apps.handles[1]).recv_queue() == REPLY.len()
    })
    .await;
    let mut bytes = [0; 1536];
    let n = apps
        .sockets
        .get_mut::<Socket>(apps.handles[1])
        .recv_slice(&mut bytes)
        .unwrap();
    assert_eq!(&bytes[..n], REPLY);
    let (dns, udp) = handles;
    apps.sockets
        .get_mut::<UdpSocket>(dns)
        .send_slice(DNS, (IpAddress::v4(8, 8, 8, 8), 53))
        .unwrap();
    until(host, apps, |apps, _| {
        apps.sockets.get::<UdpSocket>(dns).can_recv()
    })
    .await;
    let (n, meta) = apps
        .sockets
        .get_mut::<UdpSocket>(dns)
        .recv_slice(&mut bytes)
        .unwrap();
    assert_eq!(meta.endpoint, (IpAddress::v4(8, 8, 8, 8), 53).into());
    assert_eq!(&bytes[..2], &DNS[..2]);
    assert_eq!(&bytes[12..DNS.len()], &DNS[12..]);
    assert_eq!(bytes[2] & 0x80, 0x80);
    assert_eq!(&bytes[6..8], &[0, 1]);
    assert_eq!(&bytes[n - 4..n - 2], &[198, 18]);
    let target = TargetAddr::IpPort("192.0.2.12:53000".parse().unwrap());
    apps.sockets
        .get_mut::<UdpSocket>(udp)
        .send_slice(UDP, (IpAddress::v4(192, 0, 2, 12), 53000))
        .unwrap();
    let connection = upstream_ready_connection(upstream).unwrap();
    remote(host, apps, async {
        let packet = peer.read_datagram().await.unwrap();
        let meta = decode_packet_meta(&packet).unwrap();
        assert_eq!(packet.as_ref(), encode_packet(meta.assoc_id, &target, UDP));
        peer.send_datagram(encode_packet(meta.assoc_id, &target, REPLY).into())
            .unwrap();
        let actual = connection.read_datagram().await.unwrap();
        assert_eq!(
            actual.as_ref(),
            encode_packet(meta.assoc_id, &target, REPLY)
        );
        downlink.send(actual.to_vec()).await.unwrap();
    })
    .await;
    until(host, apps, |apps, _| {
        apps.sockets.get::<UdpSocket>(udp).can_recv()
    })
    .await;
    let (n, meta) = apps
        .sockets
        .get_mut::<UdpSocket>(udp)
        .recv_slice(&mut bytes)
        .unwrap();
    assert_eq!(meta.endpoint, (IpAddress::v4(192, 0, 2, 12), 53000).into());
    assert_eq!(&bytes[..n], REPLY);
    assert_eq!(
        upstream_ready_connection(upstream).unwrap().stable_id(),
        connection.stable_id()
    );
}

async fn ack_rounds(bad: bool) {
    let pressured = |owner: &SessionControl, id| {
        owner
            .flow_contracts()
            .iter()
            .any(|row| row.id == id && row.active && row.outstanding.is_some_and(|n| n > 0))
    };
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 4224, true)));
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
    config.udp_relay_mode = "native".into();
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
            let cleanup = capture(pair.close()).await;
            eprintln!("ACK_ROUNDS setup cleanup={cleanup:?}");
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
    let links = (inbound.clone(), outbound.clone());
    let mut apps = RealApps::unconnected(outbound.clone(), inbound.clone(), 4224);
    apps.targets.fill(0);
    let (downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    runtime.select_f4096_for_test();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.downlink_backpressure = crate::client_tun::DownlinkBackpressureConfig {
        high_bytes: 4096,
        low_bytes: 2048,
    };
    runtime.bounded_global_rx_receive_window = true;
    let mut host = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let observed = Arc::new(Mutex::new(Vec::with_capacity(4224)));
    let (commands, peer_commands) = mpsc::channel(2);
    let (a_send, a_recv) = tokio::sync::oneshot::channel();
    let mut a_recv = Some(a_recv);
    let mut server = Some(tokio::spawn(peer_owner(
        peer.clone(),
        peer_commands,
        observed.clone(),
        a_send,
    )));
    let mut writer = None;
    let mut writer_stop = None;
    let mut auth = None;
    let mut phase = "fill";
    let scenario = capture(async {
        auth = Some(
            tokio::time::timeout(Duration::from_secs(2), async {
                let mut auth = peer.accept_uni().await.unwrap();
                let bytes = auth.read_to_end(64).await.unwrap();
                let mut token = [0; 32];
                peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
                    .unwrap();
                assert_eq!(bytes, encode_authenticate(&config.uuid, &token));
                auth
            })
            .await
            .unwrap(),
        );
        let mut abc = Vec::with_capacity(3);
        for index in 0..3 {
            apps.connect_range(index..index + 1);
            apps.targets[index] = 1;
            until(&mut host, &mut apps, |_, owner| {
                owner.published_flow_count() == Some(index as u64 + 1)
                    && observed.lock().unwrap().len() == index + 1
            })
            .await;
            let rows = host.control.flow_contracts();
            let id = rows.iter().find(|row| !abc.contains(&row.id)).unwrap().id;
            abc.push(id);
            assert_eq!(observed.lock().unwrap()[index], (index as u64, index as u8));
            if index == 0 {
                let send = a_recv.take().unwrap().await.unwrap();
                let (stop, stopped) = tokio::sync::oneshot::channel();
                writer_stop = Some(stop);
                writer = Some(tokio::spawn(pressure_writer(send, stopped)));
                until(&mut host, &mut apps, |apps, owner| {
                    apps.sockets.get::<Socket>(apps.handles[0]).recv_queue() == 4096
                        && pressured(owner, id)
                })
                .await;
            }
        }
        let health_handles = (udp_socket(&mut apps, 45000), udp_socket(&mut apps, 45001));
        for end in (128..=4096).step_by(128) {
            let start = if end == 128 { 3 } else { end - 128 };
            apps.connect_range(start..end);
            apps.targets[start..end].fill(1);
            let filled = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    pulse(&mut host, &mut apps).await;
                    if host.control.published_flow_count() == Some(end as u64)
                        && observed.lock().unwrap().len() == end
                    {
                        break;
                    }
                }
            })
            .await;
            if filled.is_err() {
                eprintln!(
                    "ACK_ROUNDS fill={end} accepted={} established={} owner={:?}",
                    observed.lock().unwrap().len(),
                    apps.established(),
                    host.control.flow_observation()
                );
                eprintln!("ACK_ROUNDS phases={:?}", host.control.flow_debug());
                eprintln!(
                    "ACK_ROUNDS app={:?} drops={:?}",
                    apps.snapshot(0..end),
                    (links.0.dropped_packets(), links.1.dropped_packets())
                );
            }
            filled.expect("actual full F before original Waiting admission");
            let got: HashSet<_> = observed.lock().unwrap()[start..end]
                .iter()
                .map(|r| r.1)
                .collect();
            let expected: HashSet<_> = (start..end).map(|i| (i % 251) as u8).collect();
            assert_eq!(got, expected, "actual per-wave stream/tag bijection");
            assert_eq!(got.len(), end - start);
            let seen = host.control.flow_observation().unwrap();
            assert_eq!(
                (
                    seen.live,
                    seen.f,
                    seen.w,
                    seen.factory_started,
                    seen.running
                ),
                (end, end, 0, end as u64, end * 3)
            );
            assert!(seen.audit && !host.control.ledger().quarantined);
            assert_eq!(
                (links.0.dropped_packets(), links.1.dropped_packets()),
                (0, 0)
            );
        }
        health(
            &mut host,
            &mut apps,
            &commands,
            &peer,
            &upstream,
            &downlink,
            health_handles,
        )
        .await;
        phase = "original Waiting128";
        apps.connect_range(4096..4224);
        apps.targets[4096..4224].fill(1);
        until(&mut host, &mut apps, |_, owner| {
            owner.flow_observation().unwrap().waiting == 128
        })
        .await;
        let frozen: Vec<_> = host
            .control
            .flow_contracts()
            .into_iter()
            .filter(|r| r.waiting)
            .collect();
        assert_eq!(frozen.len(), 128);
        let expiry = frozen.iter().map(|r| r.slot_deadline).min().unwrap();
        let flow_expiry = frozen.iter().map(|r| r.open_deadline).min().unwrap();
        assert_eq!(
            frozen
                .iter()
                .map(|r| (r.id.index, r.id.handle, r.id.epoch, r.id.generation))
                .collect::<HashSet<_>>()
                .len(),
            128
        );
        assert!(
            frozen
                .iter()
                .all(|r| r.w && !r.f && !r.attempt && !r.published)
        );
        host.control.hold_published_receipt(abc[2]);
        let (done, closed) = tokio::sync::oneshot::channel();
        commands.send(Command::Close(2..3, done)).await.unwrap();
        remote(&mut host, &mut apps, closed).await.unwrap();
        until(&mut host, &mut apps, |_, owner| {
            owner.held_exact_receipt().is_some()
        })
        .await;
        let held = host.control.held_exact_receipt().unwrap();
        assert_eq!(held.id, abc[2]);
        assert!(
            held.row_held
                && held.rearming
                && held.original_matches
                && held.producer_stopped
                && held.provider_settled
        );
        assert_eq!(held.role_joins, 3);
        phase = "held receipt health";
        for _ in 0..3 {
            pulse(&mut host, &mut apps).await;
            let seen = host.control.flow_observation().unwrap();
            assert_eq!(
                (
                    seen.live,
                    seen.f,
                    seen.w,
                    seen.factory_started,
                    seen.released,
                    seen.retry
                ),
                (4224, 4096, 128, 4096, 0, 0)
            );
            assert_eq!(
                host.control
                    .flow_contracts()
                    .into_iter()
                    .filter(|r| r.waiting)
                    .collect::<Vec<_>>(),
                frozen
            );
        }
        health(
            &mut host,
            &mut apps,
            &commands,
            &peer,
            &upstream,
            &downlink,
            health_handles,
        )
        .await;
        assert_eq!(
            apps.sockets.get::<Socket>(apps.handles[0]).recv_queue(),
            4096
        );
        assert!(pressured(&host.control, abc[0]));
        if bad {
            host.control.corrupt_held_receipt();
            until(&mut host, &mut apps, |_, owner| {
                owner.held_exact_receipt().unwrap().quarantined
            })
            .await;
            let calls = host.control.held_exact_receipt().unwrap().calls;
            host.control.release_held_receipt();
            for _ in 0..3 {
                pulse(&mut host, &mut apps).await;
                let same = host.control.held_exact_receipt().unwrap();
                assert!(same.row_held && same.quarantined && same.original_matches);
                assert_eq!(
                    (same.id, same.disposition, same.calls),
                    (held.id, held.disposition, calls)
                );
            }
        } else {
            host.control.release_held_receipt();
        }
        phase = "two successful ACK rounds";
        for round in 0..2 {
            let range = match (bad, round) {
                (false, 0) => 3..66,
                (false, _) => 66..130,
                (true, 0) => 3..67,
                (true, _) => 67..131,
            };
            let (done, closed) = tokio::sync::oneshot::channel();
            commands.send(Command::Close(range, done)).await.unwrap();
            remote(&mut host, &mut apps, closed).await.unwrap();
            let added = (round + 1) * 64;
            until(&mut host, &mut apps, |_, owner| {
                owner.published_flow_count() == Some((4096 + added) as u64)
                    && observed.lock().unwrap().len() == 4096 + added
            })
            .await;
            let now = tokio::time::Instant::now();
            assert!(
                now < expiry && now.into_std() < flow_expiry,
                "never extend either original Waiting deadline"
            );
            let seen = host.control.flow_observation().unwrap();
            assert_eq!(
                (
                    seen.live,
                    seen.f,
                    seen.w,
                    seen.waiting,
                    seen.factory_started,
                    seen.released,
                    seen.retry
                ),
                (
                    4224 - added,
                    4096,
                    128 - added,
                    128 - added,
                    (4096 + added) as u64,
                    added as u64,
                    0
                )
            );
            assert_eq!(seen.joined, ((added + usize::from(bad)) * 3) as u64);
            let rows = host.control.flow_contracts();
            let mut started = 0;
            for old in &frozen {
                let row = rows
                    .iter()
                    .find(|r| r.id == old.id)
                    .expect("original W identity retained");
                assert_eq!(
                    (row.slot_deadline, row.open_deadline),
                    (old.slot_deadline, old.open_deadline)
                );
                if row.published {
                    assert!(row.attempt && row.f && !row.w);
                    started += 1;
                } else {
                    assert_eq!(row, old);
                }
            }
            assert_eq!(started, added);
            assert!(seen.audit);
            assert_eq!(host.control.ledger().quarantined, bad);
            assert_eq!(
                apps.sockets.get::<Socket>(apps.handles[0]).recv_queue(),
                4096
            );
            assert!(pressured(&host.control, abc[0]));
            health(
                &mut host,
                &mut apps,
                &commands,
                &peer,
                &upstream,
                &downlink,
                health_handles,
            )
            .await;
        }
        let tags: HashSet<_> = observed.lock().unwrap()[4096..]
            .iter()
            .map(|r| r.1)
            .collect();
        assert_eq!(tags, (4096..4224).map(|i| (i % 251) as u8).collect());
        assert_eq!(tags.len(), 128);
        assert_eq!(
            upstream_ready_connection(&upstream).unwrap().stable_id(),
            carrier
        );
        phase = "original shutdown";
        host.request_stop();
        let deadline = host.control.ledger().shutdown_deadline.unwrap();
        let expected = 4224 - u64::from(bad);
        tokio::time::timeout_at(deadline, async {
            commands.send(Command::Drain).await.unwrap();
            writer_stop.take().unwrap().send(()).unwrap();
            loop {
                apps.turn(true).await;
                tokio::select! {
                    _ = host.drive_until_settled() => {},
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
                let seen = host.control.flow_observation().unwrap();
                if seen.released == expected && seen.joined == 12672 && seen.running == 0 {
                    break;
                }
            }
            let peer_join = server.as_mut().unwrap().await;
            server.take();
            assert_eq!(peer_join.unwrap(), 4224);
            let writer_join = writer.as_mut().unwrap().await;
            writer.take();
            assert!(writer_join.unwrap() >= 65_536);
        })
        .await
        .expect("actual root settlement and both peer joins by original shutdown deadline");
        assert_eq!(host.control.ledger().shutdown_deadline, Some(deadline));
        apps.turn(true).await;
        assert!(
            apps.handles
                .iter()
                .all(|h| apps.sockets.get::<Socket>(*h).state()
                    == smoltcp::socket::tcp::State::Closed)
        );
        let settled = host.control.flow_observation().unwrap();
        assert_eq!(
            (
                settled.live,
                settled.w,
                settled.f,
                settled.attempts,
                settled.reserved,
                settled.escrow_bytes
            ),
            (
                usize::from(bad),
                0,
                usize::from(bad),
                usize::from(bad),
                0,
                0
            )
        );
        assert!(settled.audit);
        assert_eq!(settled.factory_started, 4224);
        let state = host.observe_shutdown().await;
        assert_eq!(
            state,
            if bad {
                SessionDeviceState::Incomplete
            } else {
                SessionDeviceState::DeviceFailedSettled
            }
        );
        if bad {
            let same = host.control.held_exact_receipt().unwrap();
            assert_eq!(same.id, abc[2]);
            assert!(
                same.quarantined && same.row_held && same.original_matches && same.provider_settled
            );
            assert_eq!(same.role_joins, 3);
            assert_eq!(host.control.flow_contracts().len(), 1);
        }
        assert_eq!(
            (links.0.dropped_packets(), links.1.dropped_packets()),
            (0, 0)
        );
        assert!(
            peer.close_reason().is_none(),
            "endpoint cleanup cannot manufacture retirement"
        );
        eprintln!("ACK_ROUNDS bad={bad} final={settled:?} state={state:?}");
    })
    .await;
    eprintln!(
        "ACK_ROUNDS bad={bad} phase={phase} complete={}",
        scenario.is_ok()
    );
    host.request_stop();
    if let Some(stop) = writer_stop.take() {
        let _ = stop.send(());
    }
    let _ = commands.try_send(Command::Drain);
    let deadline = host.control.ledger().shutdown_deadline.unwrap();
    let failed_cleanup = if scenario.is_err() {
        Some(
            tokio::time::timeout_at(deadline, async {
                tokio::join!(host.drive_until_settled(), async {
                    if let Some(task) = server.as_mut() {
                        let joined = task.await;
                        server.take();
                        eprintln!("ACK_ROUNDS failed peer join={joined:?}");
                    }
                    if let Some(task) = writer.as_mut() {
                        let joined = task.await;
                        writer.take();
                        eprintln!("ACK_ROUNDS failed writer join={joined:?}");
                    }
                });
            })
            .await,
        )
    } else {
        None
    };
    endpoint.close(0u32.into(), b"ACK rounds fixture cleanup");
    peer.close(0u32.into(), b"ACK rounds fixture cleanup");
    pair.client
        .close(0u32.into(), b"ACK rounds fixture cleanup");
    pair.server
        .close(0u32.into(), b"ACK rounds fixture cleanup");
    let all_joined = server.is_none() && writer.is_none();
    for task in [server.take(), writer.take()].into_iter().flatten() {
        if !task.is_finished() {
            task.abort();
        }
        eprintln!("ACK_ROUNDS forced peer join: {:?}", task.await);
    }
    eprintln!("ACK_ROUNDS failure cleanup={failed_cleanup:?}");
    drop(host);
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
    let pair_cleanup = capture(pair.close()).await;
    eprintln!("ACK_ROUNDS endpoint remaining={remaining:?}");
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(all_joined);
    idle.unwrap();
    assert_eq!(remaining, (0, 0, 0));
    if let Err(panic) = pair_cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_f_ack_rounds_preserve_original_waiting_and_healthy_pressure() {
    ack_rounds(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn full_f_ack_rounds_reject_bad_c_without_poisoning_original_waiting() {
    ack_rounds(true).await;
}
