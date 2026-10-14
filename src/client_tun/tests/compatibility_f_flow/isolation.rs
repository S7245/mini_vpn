//! Real TCP/DNS/native-UDP progress while another published TCP flow is full.
//! Only the fixture owns the borrowed QUIC datagram receive adapter; production
//! start_udp/recovery task custody is deliberately not certified by this test.
use super::*;
use crate::tuic::tcp_fixture::upstream_ready_connection;
use crate::tuic::{decode_packet_meta, encode_packet};
use smoltcp::socket::udp::{PacketBuffer, PacketMetadata, Socket as UdpSocket};
use smoltcp::wire::IpProtocol;

const DNS_QUERY: &[u8] =
    b"\x72\x31\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x09isolation\x07example\x00\x00\x01\x00\x01";
const UDP_QUERY: &[u8] = b"native-udp-under-A-pressure";
const UDP_REPLY: &[u8] = b"native-udp-reply-under-A-pressure";
const B_REPLY: &[u8] = b"healthy-B-under-A-pressure";

fn udp_socket(apps: &mut FlowRoleApps, port: u16) -> SocketHandle {
    let mut socket = UdpSocket::new(
        PacketBuffer::new(vec![PacketMetadata::EMPTY; 4], vec![0; 4096]),
        PacketBuffer::new(vec![PacketMetadata::EMPTY; 4], vec![0; 4096]),
    );
    socket.bind(port).unwrap();
    apps.sockets.add(socket)
}

async fn turn(apps: &mut FlowRoleApps) {
    while apps.device.try_recv_rx().unwrap() {
        let ip = Ipv4Packet::new_checked(apps.device.rx_peek().unwrap()).unwrap();
        match ip.next_header() {
            IpProtocol::Tcp => {
                let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
                apps.received_resets += usize::from(tcp.rst());
            }
            IpProtocol::Udp => {}
            protocol => panic!("unexpected app protocol: {protocol:?}"),
        }
        assert!(apps.device.stage_rx_for_iface());
    }
    apps.iface.poll(
        smoltcp::time::Instant::now(),
        &mut apps.device,
        &mut apps.sockets,
    );
    let socket = apps.sockets.get_mut::<Socket>(apps.handle);
    if socket.can_send() && !apps.sent {
        assert_eq!(socket.send_slice(b"f0-retained").unwrap(), 11);
        apps.sent = true;
    }
    apps.iface.poll(
        smoltcp::time::Instant::now(),
        &mut apps.device,
        &mut apps.sockets,
    );
    apps.device.flush_tx().await.unwrap();
}

async fn work<F: Future<Output = ()>, G: Future>(
    host: &mut ClientTunSession<F>,
    apps: &mut FlowRoleApps,
    operation: G,
) -> G::Output {
    let mut operation = std::pin::pin!(operation);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            turn(apps).await;
            tokio::select! {
                value = &mut operation => return value,
                state = host.drive_until_settled() => panic!("unexpected host completion: {state:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    }).await.expect("real peer operation under A pressure")
}

async fn until<F: Future<Output = ()>>(
    host: &mut ClientTunSession<F>,
    apps: &mut FlowRoleApps,
    mut ready: impl FnMut(&FlowRoleApps) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            turn(apps).await;
            tokio::select! {
                state = host.drive_until_settled() => panic!("unexpected host completion: {state:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
            if ready(apps) { return; }
        }
    }).await.expect("healthy application must progress while A remains pressured");
}

#[tokio::test(flavor = "current_thread")]
async fn saved_f_zero_window_keeps_sibling_tcp_dns_and_native_udp_progress() {
    run_isolation(IsolationEnd::Shutdown).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_window_failure_closes_only_a_after_real_role_and_provider_retirement() {
    run_isolation(IsolationEnd::WindowFailure).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_f_local_reset_under_pressure_keeps_sibling_and_datagrams_healthy() {
    run_isolation(IsolationEnd::LocalReset).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_f_remote_writer_failure_preserves_pressure_and_sibling_progress() {
    run_isolation(IsolationEnd::RemoteWriterFailure).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IsolationEnd {
    Shutdown,
    WindowFailure,
    LocalReset,
    RemoteWriterFailure,
}

async fn run_isolation(end: IsolationEnd) {
    let window_failure = end == IsolationEnd::WindowFailure;
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
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("actual TUIC connection setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let connection = upstream_ready_connection(&upstream).unwrap();
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let (downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
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
        Arc::new(Metrics::new()),
        NoopSink,
    );
    let mut handle = Some(host.take_handle().unwrap());
    let control = host.control.clone();
    let remote = peer.clone();
    let (payload_start, payload_gate) = tokio::sync::oneshot::channel::<()>();
    let mut payload_start = Some(payload_start);
    let (finish, gate) = tokio::sync::oneshot::channel::<()>();
    let mut finish = Some(finish);
    let (fail_writer, writer_failure_gate) = tokio::sync::oneshot::channel::<()>();
    let mut fail_writer = Some(fail_writer);
    let (failure_sent, failure_observed) = tokio::sync::oneshot::channel::<()>();
    let peer_stop = Arc::new(AtomicBool::new(false));
    let stopped = peer_stop.clone();
    let server = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(15), async {
            let (mut send, mut recv) = remote.accept_bi().await.unwrap();
            let mut expected =
                encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
            expected.extend_from_slice(b"f0-retained");
            let mut bytes = vec![0; expected.len()];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
            if payload_gate.await.is_err() {
                return;
            }
            send.write_all(&vec![0x63; 2 * 1024 * 1024]).await.unwrap();
            if end == IsolationEnd::RemoteWriterFailure && writer_failure_gate.await.is_ok() {
                recv.stop(77u32.into()).unwrap();
                failure_sent.send(()).unwrap();
            }
            assert!(send.stopped().await.unwrap().is_some());
            stopped.store(true, Ordering::SeqCst);
            let _ = gate.await;
        })
        .await
    });
    let mut auth = None;
    let mut sibling = None;
    let mut retained_failure = None;
    let mut step = "initial pressure/healthy sibling";
    let scenario = capture(async {
        auth = Some(
            work(&mut host, &mut apps, async {
                let mut auth = peer.accept_uni().await.unwrap();
                let observed = auth.read_to_end(64).await.unwrap();
                let mut token = [0; 32];
                peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
                    .unwrap();
                assert_eq!(observed, encode_authenticate(&config.uuid, &token));
                auth
            })
            .await,
        );
        until(&mut host, &mut apps, |_| {
            control.flow_observation().unwrap().published == 1
        })
        .await;
        let bridge = control.relay_event_test_bridge().unwrap();
        payload_start.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while bridge.queued_len() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        until(&mut host, &mut apps, |apps| {
            bridge.snapshot().is_some_and(|s| {
                s.context
                    .is_some_and(|c| c.pending_downlink_bytes == 65_536)
            }) && apps.sockets.get::<Socket>(apps.handle).recv_queue() == 4096
        })
        .await;
        let mut b = Socket::new(
            SocketBuffer::new(vec![0; 4096]),
            SocketBuffer::new(vec![0; 4096]),
        );
        b.connect(
            apps.iface.context(),
            (IpAddress::v4(192, 0, 2, 11), 10000),
            40001,
        )
        .unwrap();
        let b = apps.sockets.add(b);
        until(&mut host, &mut apps, |apps| {
            apps.sockets.get::<Socket>(b).can_send()
        })
        .await;
        assert_eq!(
            apps.sockets
                .get_mut::<Socket>(b)
                .send_slice(b"b-under-pressure")
                .unwrap(),
            16
        );
        sibling = Some(
            work(&mut host, &mut apps, async {
                let (send, mut recv) = peer.accept_bi().await.unwrap();
                assert_eq!(recv.id().index(), 1);
                let mut expected =
                    encode_connect(&TargetAddr::IpPort("192.0.2.11:10000".parse().unwrap()));
                expected.extend_from_slice(b"b-under-pressure");
                let mut bytes = vec![0; expected.len()];
                recv.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, expected);
                (send, recv)
            })
            .await,
        );
        until(&mut host, &mut apps, |_| {
            control.flow_observation().unwrap().published == 2
        })
        .await;
        let dns = udp_socket(&mut apps, 42000);
        let udp = udp_socket(&mut apps, 42001);
        apps.sockets
            .get_mut::<UdpSocket>(dns)
            .send_slice(DNS_QUERY, (IpAddress::v4(8, 8, 8, 8), 53))
            .unwrap();
        until(&mut host, &mut apps, |apps| {
            apps.sockets.get::<UdpSocket>(dns).can_recv()
        })
        .await;
        let mut bytes = [0; 1536];
        let (n, meta) = apps
            .sockets
            .get_mut::<UdpSocket>(dns)
            .recv_slice(&mut bytes)
            .unwrap();
        assert_eq!(meta.endpoint, (IpAddress::v4(8, 8, 8, 8), 53).into());
        assert_eq!(&bytes[..2], &DNS_QUERY[..2]);
        assert_eq!(&bytes[12..DNS_QUERY.len()], &DNS_QUERY[12..]);
        assert_eq!(bytes[2] & 0x80, 0x80);
        assert_eq!(&bytes[6..8], &[0, 1]);
        assert_eq!(&bytes[n - 4..n - 2], &[198, 18]);
        eprintln!(
            "isolation DNS passed while A appRX=4096, global_queue={}",
            bridge.queued_len()
        );
        let target = TargetAddr::IpPort("192.0.2.12:53000".parse().unwrap());
        apps.sockets
            .get_mut::<UdpSocket>(udp)
            .send_slice(UDP_QUERY, (IpAddress::v4(192, 0, 2, 12), 53000))
            .unwrap();
        work(&mut host, &mut apps, async {
            let received = peer.read_datagram().await.unwrap();
            let meta = decode_packet_meta(&received).unwrap();
            assert_eq!(
                received.as_ref(),
                encode_packet(meta.assoc_id, &target, UDP_QUERY)
            );
            peer.send_datagram(encode_packet(meta.assoc_id, &target, UDP_REPLY).into())
                .unwrap();
            let actual = connection.read_datagram().await.unwrap();
            assert_eq!(
                actual.as_ref(),
                encode_packet(meta.assoc_id, &target, UDP_REPLY)
            );
            // Borrowed, one-shot fixture adapter. No task or fabricated packet.
            downlink.send(actual.to_vec()).await.unwrap();
        })
        .await;
        until(&mut host, &mut apps, |apps| {
            apps.sockets.get::<UdpSocket>(udp).can_recv()
        })
        .await;
        let (n, meta) = apps
            .sockets
            .get_mut::<UdpSocket>(udp)
            .recv_slice(&mut bytes)
            .unwrap();
        assert_eq!(meta.endpoint, (IpAddress::v4(192, 0, 2, 12), 53000).into());
        assert_eq!(&bytes[..n], UDP_REPLY);
        assert_eq!(
            upstream_ready_connection(&upstream).unwrap().stable_id(),
            connection.stable_id()
        );
        eprintln!(
            "isolation native UDP passed on original carrier; before B DATA: {:?}, queue={}",
            control.flow_observation(),
            bridge.queued_len()
        );
        work(
            &mut host,
            &mut apps,
            sibling.as_mut().unwrap().0.write_all(B_REPLY),
        )
        .await
        .unwrap();
        assert_eq!(apps.sockets.get::<Socket>(apps.handle).recv_queue(), 4096);
        assert!(!peer_stop.load(Ordering::SeqCst));
        assert_eq!(control.flow_observation().unwrap().released, 0);
        until(&mut host, &mut apps, |apps| {
            apps.sockets.get::<Socket>(b).can_recv()
        })
        .await;
        assert_eq!(
            apps.sockets
                .get_mut::<Socket>(b)
                .recv_slice(&mut bytes)
                .unwrap(),
            B_REPLY.len()
        );
        assert_eq!(&bytes[..B_REPLY.len()], B_REPLY);
        assert_eq!(apps.sockets.get::<Socket>(apps.handle).recv_queue(), 4096);
        assert_eq!(control.flow_observation().unwrap().released, 0);
        assert!(peer.close_reason().is_none() && !control.ledger().quarantined);
        if window_failure {
            step = "A fault and role/provider quarantine";
            control.fail_test_window(apps.handle);
            until(&mut host, &mut apps, |_| {
                control.flow_observation().unwrap().joined == 3 && peer_stop.load(Ordering::SeqCst)
            })
            .await;
            assert_eq!(control.flow_observation().unwrap().settled_providers, 0);
            assert!(control.flow_debug()[0].contains("phase: Retiring"));
            finish.take().unwrap().send(()).unwrap();
            until(&mut host, &mut apps, |_| {
                let seen = control.flow_observation().unwrap();
                seen.joined == 3 && seen.settled_providers == 1 && control.ledger().quarantined
            })
            .await;
            let seen = control.flow_observation().unwrap();
            assert_eq!(
                (seen.live, seen.f, seen.released, seen.running),
                (2, 2, 0, 3)
            );
            assert!(seen.audit && peer_stop.load(Ordering::SeqCst));
            assert_eq!(
                control
                    .ledger()
                    .first_failure
                    .as_ref()
                    .unwrap()
                    .facts()
                    .io_kind,
                Some(std::io::ErrorKind::InvalidData)
            );
            step = "B DATA after A quarantine";
            work(
                &mut host,
                &mut apps,
                sibling
                    .as_mut()
                    .unwrap()
                    .0
                    .write_all(b"after-a-window-failure"),
            )
            .await
            .unwrap();
            until(&mut host, &mut apps, |apps| {
                apps.sockets.get::<Socket>(b).can_recv()
            })
            .await;
            let n = apps
                .sockets
                .get_mut::<Socket>(b)
                .recv_slice(&mut bytes)
                .unwrap();
            assert_eq!(&bytes[..n], b"after-a-window-failure");
            assert_eq!(control.flow_observation().unwrap().released, 0);
            assert!(peer.close_reason().is_none());
        }
        if matches!(
            end,
            IsolationEnd::LocalReset | IsolationEnd::RemoteWriterFailure
        ) {
            step = "A real local RST or remote writer failure";
            if end == IsolationEnd::LocalReset {
                apps.sockets.get_mut::<Socket>(apps.handle).abort();
            } else {
                fail_writer.take().unwrap().send(()).unwrap();
                work(&mut host, &mut apps, failure_observed).await.unwrap();
                // The peer's local STOP call is not client-side receipt of STOP.
                // Keep issuing bounded real writes until the original writer fails.
                tokio::time::timeout(Duration::from_secs(4), async {
                    while control.flow_observation().unwrap().stopped_producers == 0 {
                        let socket = apps.sockets.get_mut::<Socket>(apps.handle);
                        if socket.can_send() {
                            assert!(socket.send_slice(b"after-stop").unwrap() > 0);
                        }
                        turn(&mut apps).await;
                        tokio::select! {
                            state = host.drive_until_settled() => panic!("early host completion: {state:?}"),
                            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                        }
                    }
                }).await.expect("actual writer observes peer STOP while A is pressured");
            }
            until(&mut host, &mut apps, |_| {
                control.flow_observation().unwrap().joined == 3 && peer_stop.load(Ordering::SeqCst)
            })
            .await;
            assert_eq!(control.flow_observation().unwrap().settled_providers, 0);
            finish.take().unwrap().send(()).unwrap();
            until(&mut host, &mut apps, |_| {
                let seen = control.flow_observation().unwrap();
                if end == IsolationEnd::LocalReset {
                    seen.released == 1
                } else {
                    seen.settled_providers == 1
                }
            })
            .await;
            let seen = control.flow_observation().unwrap();
            assert!(seen.audit && !control.ledger().quarantined);
            assert_eq!((seen.published, seen.joined, seen.running), (2, 3, 3));
            if end == IsolationEnd::RemoteWriterFailure {
                assert_eq!((seen.live, seen.f, seen.released), (2, 2, 0));
                assert_eq!(apps.sockets.get::<Socket>(apps.handle).recv_queue(), 4096);
                assert!(control.ledger().first_failure.is_none(), "original roles retain error until physical ACK");
            } else {
                assert_eq!((seen.live, seen.f, seen.released), (1, 1, 1));
            }
            step = "B and datagrams after A failure";
            assert_eq!(
                apps.sockets
                    .get_mut::<Socket>(b)
                    .send_slice(b"after-a-fault")
                    .unwrap(),
                13
            );
            let mut uplink = [0; 13];
            work(
                &mut host,
                &mut apps,
                sibling.as_mut().unwrap().1.read_exact(&mut uplink),
            )
            .await
            .unwrap();
            assert_eq!(&uplink, b"after-a-fault");
            work(
                &mut host,
                &mut apps,
                sibling.as_mut().unwrap().0.write_all(B_REPLY),
            )
            .await
            .unwrap();
            until(&mut host, &mut apps, |apps| {
                apps.sockets.get::<Socket>(b).can_recv()
            })
            .await;
            let n = apps
                .sockets
                .get_mut::<Socket>(b)
                .recv_slice(&mut bytes)
                .unwrap();
            assert_eq!(&bytes[..n], B_REPLY);
            apps.sockets
                .get_mut::<UdpSocket>(dns)
                .send_slice(DNS_QUERY, (IpAddress::v4(8, 8, 8, 8), 53))
                .unwrap();
            until(&mut host, &mut apps, |apps| {
                apps.sockets.get::<UdpSocket>(dns).can_recv()
            })
            .await;
            let (n, meta) = apps
                .sockets
                .get_mut::<UdpSocket>(dns)
                .recv_slice(&mut bytes)
                .unwrap();
            assert_eq!(meta.endpoint, (IpAddress::v4(8, 8, 8, 8), 53).into());
            assert_eq!(&bytes[..2], &DNS_QUERY[..2]);
            assert_eq!(&bytes[12..DNS_QUERY.len()], &DNS_QUERY[12..]);
            assert_eq!(bytes[2] & 0x80, 0x80);
            assert_eq!(&bytes[6..8], &[0, 1]);
            assert_eq!(&bytes[n - 4..n - 2], &[198, 18]);
            apps.sockets
                .get_mut::<UdpSocket>(udp)
                .send_slice(UDP_QUERY, (IpAddress::v4(192, 0, 2, 12), 53000))
                .unwrap();
            work(&mut host, &mut apps, async {
                let received = peer.read_datagram().await.unwrap();
                let meta = decode_packet_meta(&received).unwrap();
                assert_eq!(
                    received.as_ref(),
                    encode_packet(meta.assoc_id, &target, UDP_QUERY)
                );
                peer.send_datagram(encode_packet(meta.assoc_id, &target, UDP_REPLY).into())
                    .unwrap();
                let actual = connection.read_datagram().await.unwrap();
                assert_eq!(
                    actual.as_ref(),
                    encode_packet(meta.assoc_id, &target, UDP_REPLY)
                );
                downlink.send(actual.to_vec()).await.unwrap();
            })
            .await;
            until(&mut host, &mut apps, |apps| {
                apps.sockets.get::<UdpSocket>(udp).can_recv()
            })
            .await;
            let (n, meta) = apps
                .sockets
                .get_mut::<UdpSocket>(udp)
                .recv_slice(&mut bytes)
                .unwrap();
            assert_eq!(meta.endpoint, (IpAddress::v4(192, 0, 2, 12), 53000).into());
            assert_eq!(&bytes[..n], UDP_REPLY);
            assert_eq!(
                upstream_ready_connection(&upstream).unwrap().stable_id(),
                connection.stable_id()
            );
            assert!(peer.close_reason().is_none() && !control.ledger().quarantined);
            if end == IsolationEnd::RemoteWriterFailure {
                assert_eq!(control.flow_observation().unwrap().released, 0);
                assert_eq!(apps.sockets.get::<Socket>(apps.handle).recv_queue(), 4096);
                apps.sockets.get_mut::<Socket>(apps.handle).abort();
                until(&mut host, &mut apps, |_| {
                    control.flow_observation().unwrap().released == 1
                })
                .await;
            }
            let settled = control.flow_observation().unwrap();
            assert_eq!(settled.factory_started, 2, "B stays on the original flow");
            assert_eq!(
                (
                    settled.live,
                    settled.f,
                    settled.joined,
                    settled.running,
                    settled.released
                ),
                (1, 1, 3, 3, 1)
            );
            let (messages, total, transferred, discarded, pending_discard) =
                settled.last_data_disposition.unwrap();
            assert!(messages > 0 && transferred >= 65_536 && pending_discard >= 65_536);
            assert_eq!(transferred + discarded, total);
            assert!(pending_discard <= transferred && settled.audit);
            if end == IsolationEnd::RemoteWriterFailure {
                let ledger = control.ledger();
                let facts = ledger.first_failure.as_ref().unwrap().facts();
                assert_eq!(facts.origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
                assert_eq!(facts.class, crate::shared::errors::FailureClass::Io);
                assert_eq!(facts.io_kind, Some(std::io::ErrorKind::ConnectionReset));
                retained_failure = Some(format!("{facts:?}"));
            }
        }
    })
    .await;
    if scenario.is_err() {
        let ledger = control.ledger();
        eprintln!(
            "isolation failure step={step}, flow={:?}, debug={:?}, Q={}, first={:?}",
            control.flow_observation(),
            control.flow_debug(),
            ledger.quarantined,
            ledger.first_failure.as_ref().map(|e| e.facts())
        );
    }
    // End every borrowed peer operation before stopping the original host.
    drop(sibling.take());
    drop(payload_start.take());
    drop(fail_writer.take());
    drop(finish.take());
    drop(handle.take());
    let cleanup_roles = if window_failure {
        capture(until(&mut host, &mut apps, |_| {
            let seen = control.flow_observation().unwrap();
            seen.joined == 6 && seen.released == 1
        }))
        .await
    } else {
        Ok(())
    };
    let state = host.observe_shutdown().await;
    let server_result = server.await;
    let settled = control.flow_observation().unwrap();
    eprintln!("isolation final actual host: {state:?}, {settled:?}");
    endpoint.close(0u32.into(), b"isolation fixture cleanup");
    peer.close(0u32.into(), b"isolation fixture cleanup");
    drop(host);
    drop(upstream);
    drop(auth);
    drop(peer);
    drop(connection);
    let idle = tokio::time::timeout(Duration::from_secs(3), endpoint.wait_idle()).await;
    let cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup_roles {
        std::panic::resume_unwind(panic);
    }
    assert!(matches!(server_result, Ok(Ok(()))), "{server_result:?}");
    if end == IsolationEnd::RemoteWriterFailure {
        assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            retained_failure.as_deref(),
            Some(
                format!(
                    "{:?}",
                    control.ledger().first_failure.as_ref().unwrap().facts()
                )
                .as_str()
            )
        );
    }
    if window_failure {
        assert_eq!(state, SessionDeviceState::Incomplete);
    } else {
        assert!(matches!(
            state,
            SessionDeviceState::DeviceSettled | SessionDeviceState::DeviceFailedSettled
        ));
    }
    assert_eq!(
        (
            settled.live,
            settled.w,
            settled.f,
            settled.joined,
            settled.released
        ),
        if window_failure {
            (1, 0, 1, 6, 1)
        } else {
            (0, 0, 0, 6, 2)
        }
    );
    assert!(settled.audit && peer_stop.load(Ordering::SeqCst));
    idle.unwrap();
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
