//! N1 §8.1 first complete path through the public Service lifecycle (hard
//! Native, D104/D105). Owns no production authority: the Service is built by
//! the same `prepare_factory` → `rebuild::drive` chain the CLI `serve` uses. A
//! real loopback TUIC peer serves TCP and UDP, then kills the live QUIC
//! connection while staying reachable. The same session owner must restore new
//! TCP and the same local UDP 4-tuple through Node local recovery, isolate
//! old-generation late events (old fragment, foreign association) and settle
//! messages/bytes/flows/connections/budget exactly at shutdown. The old fragment
//! is proven delivered before the fault: it is bracketed by two whole controls
//! on the live old connection and the fault waits for the second (D107 F1).
use super::*;
use fixture::{auth, capture, config, window, Apps, ServiceProfile};
use native_rebuild::native_ordered_chunk;

const TCP: u16 = 10000;
const UDP: u16 = 10001;

/// `encode_packet` with an explicit fragment header (same wire layout).
fn fragment(assoc: u16, target: &TargetAddr, packet: u16, total: u8, id: u8, data: &[u8]) -> Vec<u8> {
    let mut wire = encode_packet(assoc, target, data);
    wire[4..6].copy_from_slice(&packet.to_be_bytes());
    wire[6] = total;
    wire[7] = id;
    wire
}

/// One generation: real TCP Connect echo and one UDP association round trip.
/// Returns the client-chosen association id seen on this connection.
async fn serve(peer: &quinn::Connection, apps: &mut Apps, round: u8) -> u16 {
    let target = |port| TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, port)));
    let (tcp_target, udp_target) = (target(TCP), target(UDP));
    let reply: &'static [u8] = if round == 0 { b"udp-reply-0" } else { b"udp-reply-1" };
    let remote = async {
        let tcp = async {
            let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
            let mut want = encode_connect(&tcp_target);
            want.extend_from_slice(b"service-tcp");
            let mut actual = vec![0; want.len()];
            rx.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, want, "round {round}: exact Connect + payload, no replayed bytes");
            tx.write_all(if round == 0 { b"tcp-reply-0" } else { b"tcp-reply-1" }).await.unwrap();
            tx.finish().unwrap();
            (tx, rx)
        };
        let udp = async {
            let actual = peer.read_datagram().await.unwrap();
            let assoc = u16::from_be_bytes([actual[2], actual[3]]);
            assert_eq!(&actual[..], encode_packet(assoc, &udp_target, b"service-udp"));
            if round == 1 {
                // Late/foreign events first, then a positive fragment control.
                let send = |wire: Vec<u8>| peer.send_datagram(wire.into()).unwrap();
                send(encode_packet(assoc ^ 0x5555, &udp_target, b"late-foreign"));
                send(fragment(assoc, &udp_target, 7, 2, 1, b"late-new"));
                send(fragment(assoc, &udp_target, 9, 2, 0, b"pos-1-"));
                send(fragment(assoc, &udp_target, 9, 2, 1, b"pos-2"));
            }
            peer.send_datagram(encode_packet(assoc, &udp_target, reply).into()).unwrap();
            assoc
        };
        tokio::join!(tcp, udp)
    };
    let tcp_reply: &[u8] = if round == 0 { b"tcp-reply-0" } else { b"tcp-reply-1" };
    let app = async {
        let done = |apps: &Apps| {
            let udp = &apps.udp_reply;
            apps.tcp_reply == tcp_reply
                && udp.windows(reply.len()).any(|w| w == reply)
                && (round == 0 || udp.windows(11).any(|w| w == b"pos-1-pos-2"))
        };
        while !done(apps) {
            apps.turn().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let ((streams, assoc), ()) = tokio::join!(remote, app);
    drop(streams);
    assoc
}

fn has(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle)
}

/// D107 F1: on the still-live old connection send whole control 1, the late
/// old-generation fragment 0 (pkt 7) and whole control 2. Datagrams of one
/// connection are read and handed to the fragment owner in order, so control 2
/// reaching the app proves fragment 0 is inside the old generation's owner
/// before the transport fault is injected.
async fn old_generation_tail(peer: &quinn::Connection, apps: &mut Apps, assoc: u16) {
    let target = TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, UDP)));
    for wire in [
        encode_packet(assoc, &target, b"old-ctl1"),
        fragment(assoc, &target, 7, 2, 0, b"late-old-"),
        encode_packet(assoc, &target, b"old-ctl2"),
    ] {
        peer.send_datagram(wire.into()).unwrap();
    }
    while !has(&apps.udp_reply, b"old-ctl2") {
        apps.turn().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(has(&apps.udp_reply, b"old-ctl1"), "old control 1 delivered first");
    assert!(!has(&apps.udp_reply, b"late"), "a lone fragment 0 must stay partial");
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_primary_loss_recovers_locally_in_same_session() {
    let profile = ServiceProfile::new(true);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = native_ordered_chunk();
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let udp_socket = apps.udp;
    let mut service = profile.service(inbound.clone(), outbound.clone(), cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut assocs = Vec::new();
    let mut first: Option<Arc<SessionControl>> = None;
    let started = tokio::time::Instant::now();
    let mut fault_at = None;
    let mut marks = Vec::new();
    let scenario = capture(async {
        for round in 0..2u8 {
            window(Duration::from_secs(10), async {
                tokio::select! {
                    _ = async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        let accepted = tokio::time::Instant::now();
                        auth(&peer, &cfg).await;
                        marks.push((accepted, tokio::time::Instant::now()));
                        let assoc = serve(&peer, &mut apps, round).await;
                        if round == 0 {
                            old_generation_tail(&peer, &mut apps, assoc).await;
                        }
                        assocs.push(assoc);
                        peers.push(peer);
                    } => {},
                    report = service.run() => panic!("Native Service ended in round {round}: {report:?}"),
                }
            })
            .await
            .unwrap_or_else(|_| panic!("round {round} DATA within window"));
            let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert!(current.ledger().first_failure.is_none(), "round {round}: no session failure");
            assert!(!budget.quarantined());
            assert_eq!(handle.phase(), Phase::Running);
            println!("N1P round={round} assoc={:#06x} used={} t={:?}",
                assocs[usize::from(round)], budget.used(), started.elapsed());
            if round == 0 {
                first = Some(current);
                // Fragment 0 is already inside the old owner; now the transport fault.
                peers[0].close(17u32.into(), b"primary lost");
                fault_at = Some(tokio::time::Instant::now());
                apps.restart();
            } else {
                let fault = fault_at.unwrap();
                // t_reconnect bounds t_detect: the Node opens the new connection
                // only after it detected the loss; t_ready = new connection AUTHed.
                println!("N1P recovery t_reconnect={:?} t_ready={:?} t_udp={:?}",
                    marks[1].0 - fault, marks[1].1 - fault, fault.elapsed());
                assert!(Arc::ptr_eq(first.as_ref().unwrap(), &current), "local recovery keeps session");
                // Same UDP 4-tuple: the app socket is the original 10.0.0.1:40001
                // binding, the new generation reuses the old association id (a
                // fresh association would get a different id) and every reply in
                // both generations came back from the original target endpoint.
                assert_eq!(apps.udp, udp_socket);
                let local = apps.sockets.get::<smoltcp::socket::udp::Socket>(udp_socket).endpoint();
                assert_eq!(local.port, 40001, "app UDP socket still bound to 40001");
                assert_eq!(assocs[0], assocs[1], "new generation reuses the same association");
                let remote = smoltcp::wire::IpEndpoint::new(
                    smoltcp::wire::Ipv4Address::from(apps.target).into(), UDP);
                assert!(apps.udp_from.len() >= 4, "replies from both generations: {:?}", apps.udp_from);
                assert!(apps.udp_from.iter().all(|from| *from == remote), "{:?}", apps.udp_from);
                let late = has(&apps.udp_reply, b"late");
                assert!(!late, "old/foreign generation event reached the app: {:?}",
                    String::from_utf8_lossy(&apps.udp_reply));
                assert!(peers[0].close_reason().is_some(), "old connection closed");
            }
        }
    })
    .await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = window(Duration::from_secs(15), service.run()).await.unwrap();
        println!("N1P report={report:?} used={} q={}", budget.used(), budget.quarantined());
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 0, "local recovery consumes no session rebuild");
        assert_eq!(handle.phase(), Phase::Complete);
        let completion = service.completion.as_ref().unwrap();
        if let Some(first) = &first {
            assert!(Arc::ptr_eq(first, &completion.control), "one session for the whole run");
        }
        assert!(completion._retired.is_none());
        let ledger = completion.control.ledger();
        assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        drop(ledger);
        let flows = completion.control.flow_observation().unwrap();
        assert_eq!((flows.live, flows.running), (0, 0));
        assert_eq!((flows.w, flows.f, flows.reserved, flows.escrow_bytes), (0, 0, 0, 0));
        assert_eq!((flows.published, flows.released), (2, 2));
        // Task/open inventory zero (`retry` is release credit, not live work).
        assert_eq!((flows.opening_providers, flows.waiting), (0, 0));
        assert!(flows.audit);
        println!("N1P tasks joined={} running={}", flows.joined, flows.running);
        assert_eq!((inbound.dropped_packets(), outbound.dropped_packets()), (0, 0));
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "shared budget returned exactly");
    })
    .await;
    println!("N1P peers={} assocs={assocs:x?}", peers.len());
    // Connection inventory: exactly two generations, the client closed the
    // second one itself at shutdown, and the server holds no connection.
    assert_eq!(peers.len(), 2, "one connection per generation");
    let closed_by_client = tokio::time::timeout(Duration::from_secs(10), peers[1].closed()).await;
    assert!(matches!(closed_by_client, Ok(quinn::ConnectionError::ApplicationClosed(_))),
        "client closed generation 2: {closed_by_client:?}");
    let zero = tokio::time::timeout(Duration::from_secs(10), async {
        while server.open_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(zero.is_ok(), "server connections {} != 0", server.open_connections());
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
}
