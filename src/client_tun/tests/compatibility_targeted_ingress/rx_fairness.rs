//! D116 b2 tracer: the saved legacy driver yields to the executor while its
//! device stays ready. Owns only the always-ready test device below; the
//! session, driver and smoltcp state are the original ones.
//! Device: RX is served through synchronous `try_recv_rx` (ready, spends no
//! tokio coop budget, so b1's harness budget does not apply) and replays a
//! finite count of real application SYN copies through the original link.
use super::*;

const REPLAYS: usize = 1000;
const BUDGET: usize = 128;

struct AlwaysReady(WireDevice, PacketLink, Vec<u8>, Arc<AtomicUsize>);
impl Device for AlwaysReady {
    type RxToken<'a> = <WireDevice as Device>::RxToken<'a>;
    type TxToken<'a> = <WireDevice as Device>::TxToken<'a>;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.0.receive(now)
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.0.transmit(now)
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
impl TunIo for AlwaysReady {
    fn request_stop(&mut self) {
        self.0.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.0.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.0.try_recv_rx()? {
            return Ok(());
        }
        self.0.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        self.0.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.0.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.0.rx_take()
    }
    fn rx_take_packet(&mut self) -> Option<crate::device::RxPacket> {
        self.0.rx_take_packet()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.0.can_stage_single_iface_rx()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        let staged = self.0.stage_rx_for_iface();
        let left = &self.3;
        if staged && left.load(Ordering::SeqCst) > 0 {
            left.fetch_sub(1, Ordering::SeqCst);
            assert!(self.1.push(bytes::BytesMut::from(self.2.as_slice())));
        }
        staged
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.0.flush_tx().await
    }
    fn queued_tx_bytes(&self) -> usize {
        self.0.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        self.0.take_flushed_tcp_packets()
    }
    fn inject_ip_packet(&mut self, packet: &[u8]) {
        self.0.inject_ip_packet(packet);
    }
}

#[tokio::test]
async fn original_driver_yields_within_coop_budget_while_device_stays_ready() {
    let up = PacketLink::bounded(128);
    let down = PacketLink::bounded(128);
    let mut apps = RealApps::unconnected(down.clone(), up.clone(), 2);
    apps.targets.fill(0);
    apps.connect_range(0..2);
    apps.turn(true).await;
    let mut device = LoopbackTunDevice::new(up.clone(), down.clone());
    assert!(device.try_recv_rx().unwrap());
    let first = device.rx_peek().unwrap().to_vec();
    let probe = Arc::new(Probe::default());
    let left = Arc::new(AtomicUsize::new(0));
    let device = AlwaysReady(
        WireDevice(device, probe.clone()),
        up.clone(),
        first.clone(),
        left.clone(),
    );
    let upstream = Arc::new(PendingOpens::new());
    let (_udp, rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    config.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        device,
        upstream.clone(),
        rx,
        config,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let ticks = Arc::new(AtomicUsize::new(0));
    let sibling = tokio::spawn({
        let ticks = ticks.clone();
        async move {
            loop {
                ticks.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        }
    });
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while probe.received.load(Ordering::SeqCst) < 2 {
                tokio::select! { v = session.run() => panic!("premature {v:?}"), _ = tokio::time::sleep(Duration::from_millis(1)) => {} }
            }
        })
        .await
        .unwrap();
        let (staged0, received0) = (
            probe.staged.load(Ordering::SeqCst),
            probe.received.load(Ordering::SeqCst),
        );
        left.store(REPLAYS, Ordering::SeqCst);
        assert!(up.push(bytes::BytesMut::from(first.as_slice())));
        let (mut polls, mut max_per_poll, mut sibling_during_flood) = (0usize, 0usize, 0usize);
        {
            let mut run = std::pin::pin!(session.run());
            while left.load(Ordering::SeqCst) > 0 || probe.staged.load(Ordering::SeqCst) < staged0 + REPLAYS + 1 {
                assert!(polls < 10 * REPLAYS, "flood never drained");
                let before = probe.staged.load(Ordering::SeqCst);
                std::future::poll_fn(|cx| {
                    if let Poll::Ready(v) = run.as_mut().poll(cx) {
                        panic!("premature {v:?}");
                    }
                    Poll::Ready(())
                })
                .await;
                polls += 1;
                max_per_poll = max_per_poll.max(probe.staged.load(Ordering::SeqCst) - before);
                let ticks_before = ticks.load(Ordering::SeqCst);
                tokio::task::yield_now().await;
                if left.load(Ordering::SeqCst) > 0 && ticks.load(Ordering::SeqCst) > ticks_before {
                    sibling_during_flood += 1;
                }
            }
        }
        eprintln!("D116_RX_FAIRNESS polls={polls} max_per_poll={max_per_poll} sibling_during_flood={sibling_during_flood}");
        assert!(max_per_poll <= BUDGET, "one executor poll staged {max_per_poll} > {BUDGET}");
        assert!(sibling_during_flood > 0, "sibling task starved during the flood");
        assert_eq!(probe.staged.load(Ordering::SeqCst) - staged0, REPLAYS + 1);
        assert_eq!(probe.received.load(Ordering::SeqCst) - received0, REPLAYS + 1);
        assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
        let view = session.control.flow_observation().unwrap();
        assert_eq!((view.live, view.w, view.f, view.factory_started), (0, 0, 0, 0));
        assert!(view.audit);
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
    })
    .await;
    sibling.abort();
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
