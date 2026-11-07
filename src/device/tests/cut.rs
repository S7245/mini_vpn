use super::*;

#[tokio::test]
async fn cancelled_cut_keeps_original_read_actor_and_producer_until_real_ack() {
    let (mut writer, reader) = tokio::io::duplex(4096);
    let mut pump = spawn_tun_ingress_pump(reader, 1200, 1);
    let original = match &pump.task {
        PumpTask::Running(t) => t.id(),
        _ => panic!(),
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while pump.control.producer_backing.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let cut = pump.cut();
        tokio::pin!(cut);
        std::future::poll_fn(|cx| {
            assert!(cut.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert!(pump.control.pause.requested.load(Ordering::Acquire));
    let discarded = tokio::time::timeout(Duration::from_secs(1), pump.cut())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(discarded, (1, rx_buffer_capacity_for_mtu(1200) as u64));
    assert_eq!(pump.control.producer_backing.load(Ordering::Acquire), 0);
    assert_eq!(pump.control.queued_backing.load(Ordering::Acquire), 0);
    match &pump.task {
        PumpTask::Running(t) => assert_eq!(t.id(), original),
        _ => panic!(),
    }
    pump.validate_resume().unwrap();
    pump.resume();
    writer.write_all(b"new").await.unwrap();
    assert_eq!(pump.recv().await.unwrap().unwrap().as_ref(), b"new");
    pump.request_stop();
    assert_eq!(pump.observe_join().await, PumpExit::StopRequested);
}

struct UnsupportedSource;
impl IngressPacketSource for UnsupportedSource {
    async fn read_packet(&mut self, _slot: &mut PumpPacket) -> std::io::Result<SourceRead> {
        std::future::pending().await
    }
}
#[tokio::test]
async fn unsupported_source_cannot_mint_cut_or_replace_original_actor() {
    let mut pump = TunIngressPump::spawn_source(UnsupportedSource, 1);
    assert_eq!(pump.cut().await.unwrap_err().kind(), ErrorKind::Unsupported);
    assert!(!pump.control.pause.requested.load(Ordering::Acquire));
    pump.request_stop();
    assert_eq!(pump.observe_join().await, PumpExit::StopRequested);
}
