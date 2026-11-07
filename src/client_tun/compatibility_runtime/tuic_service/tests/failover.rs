//! Real production composition tracer; policy selection injection is explicit.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn same_root_carries_tuic_then_reality_tcp_with_original_typed_udp() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let original_policy = Arc::new(crate::upstream::RelayFamilyPolicy::from_sources(
        crate::upstream::RelayFamilyPolicySources {
            d16_byte_owned: Some("1"),
            native_egress_permit: Some("1"),
            native_read_floor: Some("1"),
            ..Default::default()
        },
    ));
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(original_policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(original_policy.clone());
    assert!(Arc::ptr_eq(runtime.relay_policy(), &original_policy));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let reality = RealityUpstream::new(RealityClientConfig {
        server: listener.local_addr().unwrap().to_string(),
        uuid: [0x42; 16],
        pbk: pk,
        short_id: [0; 8],
        sni: "example.com".into(),
    });
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    apps.tcp_payload = b"ping";
    let mut service = prepare_with(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        Configuration::failover(cfg.clone(), reality),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peer = None;
    let scenario=capture(async {
        for backup in [false,true] {
            if backup {
                let state=service.control.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                state.set_leg(crate::failover::TcpLeg::Reality); // selection only, not failure/hysteresis evidence
                apps.restart();
            }
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5),async {
                tokio::select! {
                    _=async {
                        if !backup {let remote=server.accept().await.unwrap().await.unwrap();auth(&remote,&cfg).await;peer=Some(remote);}
                        let remote=peer.as_ref().unwrap();
                        let tcp=async {
                            if backup {
                                let (io,_)=listener.accept().await.unwrap();
                                crate::reality_upstream::reality_server_sim(io,sk,[0x42;16]).await;
                            } else {
                                let (mut tx,mut rx)=remote.accept_bi().await.unwrap();
                                let mut want=encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));want.extend_from_slice(b"ping");
                                let mut actual=vec![0;want.len()];rx.read_exact(&mut actual).await.unwrap();assert_eq!(actual,want);
                                tx.write_all(b"pong").await.unwrap();tx.finish().unwrap();
                            }
                        };
                        let udp=async {
                            let actual=remote.read_datagram().await.unwrap();
                            let target=TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());let assoc=u16::from_be_bytes([actual[2],actual[3]]);
                            assert_eq!(&actual[..],encode_packet(assoc,&target,b"service-udp"));
                            remote.send_datagram(encode_packet(assoc,&target,b"udp-reply").into()).unwrap();
                        };
                        let local=async {while apps.tcp_reply!=b"pong"||apps.udp_reply!=b"udp-reply" {apps.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}};
                        tokio::join!(tcp,udp,local);
                    }=>{},
                    report=service.run()=>panic!("service ended {report:?}"),
                }
            }).await.unwrap();
        }
        let observation=service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap().flow_observation().unwrap();
        assert_eq!((observation.factory_started,observation.published),(2,2));
        assert_eq!((inbound.dropped_packets(),outbound.dropped_packets()),(0,0));
    }).await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        let done = service.completion.as_ref().unwrap();
        assert!(done._health.as_ref().unwrap().failure().is_none());
        let o = done.control.flow_observation().unwrap();
        assert_eq!(
            (o.live, o.running, o.w, o.f, o.reserved, o.escrow_bytes),
            (0, 0, 0, 0, 0, 0)
        );
        assert_eq!((o.published, o.released, o.joined), (2, 2, 6));
        assert!(o.audit);
    })
    .await;
    if let Some(remote) = peer {
        remote.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
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
