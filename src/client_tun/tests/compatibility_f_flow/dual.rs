//! Two actual saved-loop Flow transactions on one authenticated TUIC connection.
//! Owns only the dual-flow fixture; original Root, Quinn meter, and roles own settlement.
//! Entry: `saved_f_two_native_flows_join_independently_on_one_meter`.
//! Invariant: both F receipts coexist before either stream finishes; tests live here.

use super::*;

#[tokio::test]
async fn saved_f_two_native_flows_join_independently_on_one_meter() {
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 100, true)));
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: Some("1"),
        ..Default::default()
    }));
    let mut config = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap()
    .with_relay_policy(policy.clone());
    config.tcp_pool = 1;
    let (upstream, peer) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            TuicUpstream::connect(&config, Arc::new(Metrics::default())),
            async { pair.server.accept().await.unwrap().await.unwrap() }
        )
    })
    .await
    .expect("original TUIC connection deadline");
    let upstream = Arc::new(upstream.expect("original TUIC connection"));
    let endpoint = upstream_endpoint(&upstream);
    let meter = endpoint
        .receive_backing_meter()
        .expect("original endpoint meter");
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let mut second_socket = Socket::new(
        SocketBuffer::new(vec![0; 4096]),
        SocketBuffer::new(vec![0; 8192]),
    );
    second_socket
        .connect(
            apps.iface.context(),
            (IpAddress::v4(192, 0, 2, 10), 10_000),
            40_001,
        )
        .expect("second actual app socket");
    let second_handle = apps.sockets.add(second_socket);
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::new()),
        NoopSink,
    );
    let (finish, gate) = tokio::sync::oneshot::channel();
    let remote = peer.clone();
    let server = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(8), async {
            let (mut first_tx, mut first_rx) = remote.accept_bi().await.unwrap();
            let (mut second_tx, mut second_rx) = remote.accept_bi().await.unwrap();
            let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
            let connect = encode_connect(&target);
            let mut first = vec![0; connect.len() + 11];
            let mut second = vec![0; connect.len() + 11];
            first_rx.read_exact(&mut first).await.unwrap();
            second_rx.read_exact(&mut second).await.unwrap();
            assert_eq!(&first[..connect.len()], connect);
            assert_eq!(&second[..connect.len()], connect);
            let payloads = (&first[connect.len()..], &second[connect.len()..]);
            assert!(
                payloads == (b"f0-retained", b"f1-retained")
                    || payloads == (b"f1-retained", b"f0-retained")
            );
            first_tx
                .write_all(if payloads.0 == b"f0-retained" {
                    b"first-reply"
                } else {
                    b"second-reply"
                })
                .await
                .unwrap();
            second_tx
                .write_all(if payloads.1 == b"f0-retained" {
                    b"first-reply"
                } else {
                    b"second-reply"
                })
                .await
                .unwrap();
            gate.await.unwrap();
            first_tx.finish().unwrap();
            second_tx.finish().unwrap();
        })
        .await
        .expect("two-stream peer deadline");
    });
    let mut auth = peer_authenticate(&peer, &config).await;
    let mut sent_second = false;
    let mut first_reply = Vec::new();
    let mut second_reply = Vec::new();
    let mut finish = Some(finish);
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                let second = apps.sockets.get_mut::<Socket>(second_handle);
                if second.can_send() && !sent_second {
                    assert_eq!(second.send_slice(b"f1-retained").unwrap(), 11);
                    sent_second = true;
                }
                for (handle, reply) in [
                    (apps.handle, &mut first_reply),
                    (second_handle, &mut second_reply),
                ] {
                    let socket = apps.sockets.get_mut::<Socket>(handle);
                    if socket.can_recv() {
                        let mut scratch = [0; 64];
                        let n = socket.recv_slice(&mut scratch).unwrap();
                        reply.extend_from_slice(&scratch[..n]);
                    }
                }
                if first_reply == b"first-reply"
                    && second_reply == b"second-reply"
                    && finish.is_some()
                {
                    assert_eq!(session.control.test_native_meter_bindings(&meter), (2, 2));
                    let active = session.control.flow_observation().unwrap();
                    assert!(active.audit, "{active:?}");
                    assert_eq!(
                        (active.live, active.published, active.running, active.joined),
                        (2, 2, 10, 0),
                        "{active:?}"
                    );
                    assert_eq!((active.w, active.f, active.reserved), (0, 2, 0));
                    assert!(meter.snapshot().registered_total > 0);
                    finish.take().unwrap().send(()).unwrap();
                }
                for handle in [apps.handle, second_handle] {
                    let socket = apps.sockets.get_mut::<Socket>(handle);
                    if socket.state() == smoltcp::socket::tcp::State::CloseWait {
                        socket.close();
                    }
                }
                let settled = session.control.flow_observation().unwrap();
                if finish.is_none() && settled.live == 0 {
                    assert!(settled.audit, "{settled:?}");
                    assert_eq!(
                        (
                            settled.published,
                            settled.released,
                            settled.joined,
                            settled.w,
                            settled.f,
                            settled.reserved,
                            settled.retained_capacity,
                        ),
                        (2, 2, 10, 0, 0, 0, 0),
                        "{settled:?}"
                    );
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two Root Flows terminal deadline");
    })
    .await;
    drop(finish.take());
    session.request_stop();
    let state = session.observe_shutdown().await;
    let server_result = server.await;
    let clean = !session.control.ledger().quarantined;
    let no_first_failure = session.control.ledger().first_failure.is_none();
    endpoint.close(0u32.into(), b"dual fixture cleanup");
    peer.close(0u32.into(), b"dual fixture cleanup");
    drop(session);
    drop(upstream);
    drop(auth.take());
    drop(peer);
    endpoint.wait_idle().await;
    pair.close().await;
    drop(endpoint);
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    server_result.expect("actual peer task");
    assert_eq!(state, SessionDeviceState::DeviceSettled);
    assert!(clean);
    assert!(no_first_failure);
    let backing = meter.snapshot();
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

async fn peer_authenticate(
    peer: &quinn::Connection,
    config: &TuicClientConfig,
) -> Option<quinn::RecvStream> {
    let mut auth = Some(peer.accept_uni().await.unwrap());
    let observed = auth.as_mut().unwrap().read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
        .unwrap();
    assert_eq!(observed, encode_authenticate(&config.uuid, &token));
    auth
}
