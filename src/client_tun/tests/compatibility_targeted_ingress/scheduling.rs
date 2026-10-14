//! Actual saved loop's full timer services B during continuously ready A input.
use super::*;

#[tokio::test]
async fn original_full_timer_retransmits_unselected_b_during_continuous_targeted_a() {
    let up = PacketLink::bounded(128);
    let down = PacketLink::bounded(128);
    let mut apps = RealApps::unconnected(down.clone(), up.clone(), 2);
    apps.targets.fill(0);
    apps.connect_range(0..2);
    apps.turn(true).await;
    let mut device = LoopbackTunDevice::new(up.clone(), down.clone());
    // Preserve one actual application SYN for bounded retransmission injection.
    assert!(device.try_recv_rx().unwrap());
    let first = device.rx_peek().unwrap().to_vec();
    let ip = Ipv4Packet::new_checked(first.as_slice()).unwrap();
    assert_eq!(
        TcpPacket::new_checked(ip.payload()).unwrap().src_port(),
        40000
    );
    let probe = Arc::new(Probe::default());
    *probe.replay.lock().unwrap() = Some((up.clone(), first.clone()));
    let upstream = Arc::new(PendingOpens::new());
    let (_udp, rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    config.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        WireDevice(device, probe.clone()),
        upstream.clone(),
        rx,
        config,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario=capture(async {
        tokio::time::timeout(Duration::from_secs(2),async {
            while probe.b_synacks.lock().unwrap().is_empty() {
                tokio::select! { v=session.run()=>panic!("premature {v:?}"), _=tokio::time::sleep(Duration::from_millis(1))=>{} }
            }
        }).await.unwrap();
        assert_eq!(probe.received.load(Ordering::SeqCst),2);
        // Apps never poll again: B sends neither duplicate SYN nor ACK. Each
        // staged A replay replenishes exactly one packet in the original link.
        probe.flood.store(true,Ordering::SeqCst);assert!(up.push(bytes::BytesMut::from(first.as_slice())));
        tokio::time::timeout(Duration::from_secs(2),async {
            while probe.b_synacks.lock().unwrap().len()<2 {
                tokio::select! { v=session.run()=>panic!("premature {v:?}"), _=tokio::time::sleep(Duration::from_millis(1))=>{} }
            }
        }).await.unwrap();
        let b=probe.b_synacks.lock().unwrap();assert_eq!(b[0].0,b[1].0);
        assert!(b[1].1.duration_since(b[0].1)>=Duration::from_millis(100),"real TCP timer, not immediate duplicate");
        assert!(probe.received.load(Ordering::SeqCst)>=34);
        assert_eq!(probe.coverage.load(Ordering::SeqCst),probe.received.load(Ordering::SeqCst));
        assert_eq!((up.dropped_packets(),down.dropped_packets()),(0,0));
        let view=session.control.flow_observation().unwrap();assert_eq!((view.live,view.w,view.f,view.factory_started),(0,0,0,0));assert!(view.audit);
        assert_eq!(upstream.entered.load(Ordering::SeqCst),0);
    }).await;
    probe.flood.store(false, Ordering::SeqCst);
    session.request_stop();
    let cleanup = capture(async {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), session.run())
                .await
                .unwrap(),
            SessionDeviceState::DeviceSettled
        );
    })
    .await;
    drop(session);
    if let Err(p) = scenario {
        std::panic::resume_unwind(p);
    }
    if let Err(p) = cleanup {
        std::panic::resume_unwind(p);
    }
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
}
