//! True QUIC + original pumped memory TUN through the production service root.
use super::*;
use fixture::{auth, capture, config};
use std::net::Ipv4Addr;

pub(super) async fn dns(inbound: &PacketLink, outbound: &PacketLink, name: &str) -> Ipv4Addr {
    let mut query = vec![0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    if !name.is_empty() {
        for label in name.split('.') {
            query.push(label.len() as u8);
            query.extend_from_slice(label.as_bytes());
        }
    }
    query.extend_from_slice(&[0, 0, 1, 0, 1]);
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [8, 8, 8, 8], 64).udp(50000, 53);
    let mut packet = Vec::new();
    builder.write(&mut packet, &query).unwrap();
    assert!(inbound.push(bytes::BytesMut::from(&packet[..])));
    let mut reader = LoopbackTunDevice::new(outbound.clone(), PacketLink::bounded(1));
    loop {
        if reader.try_recv_rx().unwrap() {
            let reply = reader.rx_take().unwrap();
            let headers = etherparse::PacketHeaders::from_ip_slice(&reply).unwrap();
            assert!(
                matches!(headers.transport, Some(etherparse::TransportHeader::Udp(ref udp)) if udp.source_port == 53 && udp.destination_port == 50000)
            );
            assert!(headers.payload.len() >= 12);
            assert_eq!(&headers.payload[..2], &[0, 1]);
            let p = headers.payload;
            assert_eq!(p[2] & 0x80, 0x80);
            assert_eq!(p[3] & 0x0f, 0);
            assert_eq!(&p[6..8], &[0, 1]);
            return Ipv4Addr::new(
                p[p.len() - 4],
                p[p.len() - 3],
                p[p.len() - 2],
                p[p.len() - 1],
            );
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn controlled_rebuild_keeps_tun_fake_ip_and_restores_new_tcp_and_udp() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut service = prepare(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(4),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peers = Vec::new();
    let mut old_control = None;
    let mut fake = None;
    let mut apps = None;
    let scenario = capture(async {
        for round in 0..2 {
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5),async {
                tokio::select! {
                    _ = async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer,&cfg).await;
                        if round == 1 {
                            let pending_old = {
                                let probe = handle.0.old_retirement_probe.lock().unwrap();
                                probe.as_ref().and_then(|probe| {
                                    probe.lock().unwrap().iter().flatten().copied().next()
                                })
                            };
                            assert!(
                                pending_old.is_some(),
                                "candidate AUTH must precede original published ConnectionDrain settlement after a real Pending poll"
                            );
                            assert_eq!(
                                handle.phase(),
                                Phase::Rebuilding,
                                "candidate AUTH must overlap original Node drain before DATA publication"
                            );
                        }
                        if round==0 { dns(&inbound,&outbound,"first.example").await; }
                        let ip = if round == 0 {
                            let ip = dns(&inbound,&outbound,"persist.example").await;
                            fake = Some(ip); apps = Some(fixture::Apps::with_target(outbound.clone(),inbound.clone(),ip)); ip
                        } else { apps.as_mut().unwrap().restart(); fake.unwrap() };
                        let apps = apps.as_mut().unwrap();
                        let remote=async {
                            let tcp=async {
                                let (mut tx,mut rx)=peer.accept_bi().await.unwrap();
                                let mut want=encode_connect(&TargetAddr::DomainPort { host: "persist.example".into(), port: 10000 }); want.extend_from_slice(b"service-tcp");
                                let mut actual=vec![0;want.len()];rx.read_exact(&mut actual).await.unwrap();assert_eq!(actual,want);
                                tx.write_all(b"tcp-reply").await.unwrap();tx.finish().unwrap();(tx,rx)
                            };
                            let udp=async {
                                let actual=peer.read_datagram().await.unwrap();let assoc=u16::from_be_bytes([actual[2],actual[3]]);
                                let target=TargetAddr::DomainPort { host: "persist.example".into(), port: 10001 };
                                assert_eq!(&actual[..],encode_packet(assoc,&target,b"service-udp"));
                                peer.send_datagram(encode_packet(assoc,&TargetAddr::IpPort(std::net::SocketAddr::from((ip,10001))),b"udp-reply").into()).unwrap();
                            };
                            tokio::join!(tcp,udp)
                        };
                        let app=async { while apps.tcp_reply!=b"tcp-reply" || apps.udp_reply!=b"udp-reply" { apps.turn().await;tokio::time::sleep(Duration::from_millis(1)).await; } };
                        let (streams,())=tokio::join!(remote,app);drop(streams);peers.push(peer);
                    } => {},
                    report=service.run()=>panic!("service ended before successor DATA {report:?}"),
                }
            }).await.expect("same production helper must publish the one real successor");
            let current=service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            if round==0 { old_control=Some(current); assert!(handle.request_rebuild()); }
            else {
                let old=old_control.as_ref().unwrap();assert!(old.business_complete.load(Ordering::Acquire)); assert!(!old.driver_complete.load(Ordering::Acquire));
                assert!(!Arc::ptr_eq(old,&current));assert_eq!(old.flow_observation().unwrap().live,0);
                assert!(!handle.request_rebuild(),"one successor budget is spent");
            }
        }
    }).await;
    if scenario.is_ok() {
        let clocks = service.control.namespace_clocks.lock().unwrap();
        assert!(clocks[0].is_some());
        assert_eq!(clocks[0], clocks[1]);
    }
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
            let completion = service.completion.as_ref().unwrap();
            let old = completion._retired.as_ref().unwrap();
            old.node.assert_service_clean_for_test();
            assert_eq!(old.control.state(), SessionDeviceState::BusinessSettled);
            assert!(
                !old.control
                    .device_settlement_observed
                    .load(Ordering::Acquire)
            );
            assert!(
                completion
                    .control
                    .device_settlement_observed
                    .load(Ordering::Acquire)
            );
            assert!(peers[0].close_reason().is_some());
        }
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    assert!(drained.unwrap());
}

#[tokio::test]
async fn unsupported_device_cut_never_builds_successor_and_keeps_node_until_real_device_join() {
    use device::{Gate, device};
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let gate = Arc::new(Gate::default());
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peer = None;
    let scenario=capture(async {
        tokio::time::timeout(Duration::from_secs(3),async {
            tokio::select! {
                remote=async{let p=server.accept().await.unwrap().await.unwrap();auth(&p,&cfg).await;p}=>peer=Some(remote),
                report=service.run()=>panic!("startup ended {report:?}"),
            }
        }).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(10),service.run()).await.is_err());
        assert!(handle.request_rebuild()); let saved=service.control.clone();
        tokio::time::timeout(Duration::from_secs(3),async {
            tokio::select! {
                _=async {while gate.polls.load(Ordering::Acquire)==0 {tokio::task::yield_now().await;}}=>{},
                report=service.run()=>panic!("physical device gate not joined {report:?}"),
            }
        }).await.unwrap();
        assert_eq!(*saved.phase.lock().unwrap(),Phase::Stopping);
        assert!(peer.as_ref().unwrap().close_reason().is_none());
        assert_eq!(gate.joins.load(Ordering::Acquire),0);assert_eq!(gate.drops.load(Ordering::Acquire),0);
        assert!(!handle.request_rebuild());
        assert!(tokio::time::timeout(Duration::from_millis(20),server.accept()).await.is_err());
    }).await;
    handle.request_stop();
    gate.open();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(report.rebuilds, 0);
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(gate.drops.load(Ordering::Acquire), 1);
        let completion = service.completion.as_ref().unwrap();
        assert!(completion._retired.is_none());
        assert_eq!(
            completion
                .control
                .ledger()
                .first_failure
                .as_ref()
                .unwrap()
                .facts()
                .io_kind,
            Some(std::io::ErrorKind::Unsupported)
        );
    })
    .await;
    if let Some(peer) = peer {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    assert!(drained.unwrap());
}
