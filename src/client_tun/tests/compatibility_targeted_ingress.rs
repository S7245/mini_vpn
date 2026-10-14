//! Saved original owner, real SYN/SYNACK and pending flush cancellation/error.
//! No successful upstream carrier: these cases prove local ingress custody only.
use super::*;
use crate::device::{DeviceShutdown, TunFlushedTcpPacket};
use crate::shared::errors::{FailureClass, FailurePhase};
use smoltcp::phy::{Device, TxToken};
use std::task::Context;

#[path = "compatibility_targeted_ingress/drain.rs"]
mod drain;
#[path = "compatibility_targeted_ingress/finite_tx.rs"]
mod finite_tx;
#[path = "compatibility_targeted_ingress/scheduling.rs"]
mod scheduling;

#[derive(Default)]
struct Probe {
    staged: AtomicUsize,
    flood: AtomicBool,
    replay: Mutex<Option<(PacketLink, Vec<u8>)>>,
    b_synacks: Mutex<Vec<(i32, std::time::Instant)>>,
    received: AtomicUsize,
    syn_ack: AtomicUsize,
    resets: AtomicUsize,
    held: AtomicBool,
    tx_blocked: AtomicBool,
    waiting: AtomicBool,
    fail_read: AtomicBool,
    try_fail_read: AtomicBool,
    try_fail_after: AtomicUsize,
    fail_write: AtomicBool,
    coverage: AtomicUsize,
    shutdown: AtomicUsize,
    dropped: AtomicUsize,
    wake: Notify,
}
impl Probe {
    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        self.wake.notify_waiters();
    }
}
struct WireDevice(LoopbackTunDevice, Arc<Probe>);
struct WireTx<T>(T, Arc<Probe>);
impl<T: TxToken> TxToken for WireTx<T> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        self.0.consume(len, |packet| {
            let result = f(packet);
            if let Ok(ip) = Ipv4Packet::new_checked(&*packet) {
                if TcpPacket::new_checked(ip.payload())
                    .is_ok_and(|tcp| tcp.rst() && tcp.dst_port() == 40000)
                {
                    self.1.resets.fetch_add(1, Ordering::SeqCst);
                }
                if TcpPacket::new_checked(ip.payload()).is_ok_and(|tcp| tcp.syn() && tcp.ack()) {
                    self.1.syn_ack.fetch_add(1, Ordering::SeqCst);
                    let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
                    if tcp.dst_port() == 40001 {
                        self.1
                            .b_synacks
                            .lock()
                            .unwrap()
                            .push((tcp.seq_number().0, std::time::Instant::now()));
                    }
                }
            }
            result
        })
    }
}
impl Device for WireDevice {
    type RxToken<'a> = <LoopbackTunDevice as Device>::RxToken<'a>;
    type TxToken<'a> = WireTx<<LoopbackTunDevice as Device>::TxToken<'a>>;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.1.tx_blocked.load(Ordering::SeqCst) {
            return None;
        }
        self.0.receive(now).map(|(rx, tx)| {
            self.1.received.fetch_add(1, Ordering::SeqCst);
            (rx, WireTx(tx, self.1.clone()))
        })
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        if self.1.tx_blocked.load(Ordering::SeqCst) {
            return None;
        }
        self.0.transmit(now).map(|tx| WireTx(tx, self.1.clone()))
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
impl TunIo for WireDevice {
    fn request_stop(&mut self) {
        self.0.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.1.shutdown.fetch_add(1, Ordering::SeqCst);
        self.0.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.1.received.load(Ordering::SeqCst) > 0
            && self.1.fail_read.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::from_raw_os_error(5));
        }
        self.0.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.1.received.load(Ordering::SeqCst) >= self.1.try_fail_after.load(Ordering::SeqCst)
            && self.1.try_fail_read.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::from_raw_os_error(5));
        }
        self.0.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.0.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.0.rx_take()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.1.coverage.fetch_add(1, Ordering::SeqCst);
        self.0.can_stage_single_iface_rx()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        let staged = self.0.stage_rx_for_iface();
        if staged {
            self.1.staged.fetch_add(1, Ordering::SeqCst);
            if self.1.flood.load(Ordering::SeqCst) {
                let replay = self.1.replay.lock().unwrap();
                let (link, packet) = replay.as_ref().unwrap();
                assert!(link.push(bytes::BytesMut::from(packet.as_slice())));
            }
        }
        staged
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        while self.1.syn_ack.load(Ordering::SeqCst) > 0 {
            let wake = self.1.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            if !self.1.held.load(Ordering::SeqCst) {
                break;
            }
            self.1.waiting.store(true, Ordering::SeqCst);
            wake.await;
        }
        if self.1.syn_ack.load(Ordering::SeqCst) > 0
            && self.1.fail_write.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::from_raw_os_error(5));
        }
        self.0.flush_tx().await
    }
    fn queued_tx_bytes(&self) -> usize {
        self.0.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        self.0.take_flushed_tcp_packets()
    }
    fn inject_ip_packet(&mut self, packet: &[u8]) {
        self.0.inject_ip_packet(packet);
    }
}
impl Drop for WireDevice {
    fn drop(&mut self) {
        self.1.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

async fn saved_single(mode: u8) {
    let failed = mode != 0;
    let up = PacketLink::bounded(128);
    let down = PacketLink::bounded(128);
    let mut apps = RealApps::unconnected(down.clone(), up.clone(), 33);
    apps.targets.fill(0);
    apps.connect_range(0..33);
    apps.turn(true).await;
    assert_eq!(up.high_water_packets(), 33);
    let probe = Arc::new(Probe::default());
    probe.held.store(!failed, Ordering::SeqCst);
    probe.fail_read.store(mode == 1, Ordering::SeqCst);
    probe.fail_write.store(mode == 2, Ordering::SeqCst);
    let upstream = Arc::new(PendingOpens::new());
    let (_udp, rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    config.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        WireDevice(
            LoopbackTunDevice::new(up.clone(), down.clone()),
            probe.clone(),
        ),
        upstream.clone(),
        rx,
        config,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        if failed {
            let expected = if mode == 1 {
                SessionDeviceState::Incomplete
            } else {
                SessionDeviceState::DeviceFailedSettled
            };
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), session.run())
                    .await
                    .unwrap(),
                expected
            );
            let deadline = session.control.ledger().shutdown_deadline.unwrap();
            let facts = {
                let ledger = session.control.ledger();
                let facts = ledger.first_failure.as_ref().unwrap().facts();
                assert_eq!(
                    facts.origin.phase,
                    if mode == 1 {
                        FailurePhase::DeviceRead
                    } else {
                        FailurePhase::DeviceWrite
                    }
                );
                assert_eq!(facts.class, FailureClass::Io);
                assert_eq!(facts.raw_os_code, Some(5));
                format!("{facts:?}")
            };
            for _ in 0..3 {
                assert_eq!(session.observe_shutdown().await, expected);
                assert_eq!(session.control.ledger().shutdown_deadline, Some(deadline));
                assert_eq!(
                    format!(
                        "{:?}",
                        session
                            .control
                            .ledger()
                            .first_failure
                            .as_ref()
                            .unwrap()
                            .facts()
                    ),
                    facts
                );
                assert_eq!(session.control.ledger().quarantined, mode == 1);
                assert_eq!(
                    session.control.driver_complete.load(Ordering::Acquire),
                    mode == 2
                );
                assert_eq!(
                    session
                        .control
                        .device_settlement_observed
                        .load(Ordering::Acquire),
                    mode == 2
                );
                assert_eq!(probe.coverage.load(Ordering::SeqCst), 1);
                assert_eq!(probe.staged.load(Ordering::SeqCst), 1);
                assert_eq!(probe.received.load(Ordering::SeqCst), 1);
                assert_eq!(probe.syn_ack.load(Ordering::SeqCst), 1);
                assert_eq!(probe.shutdown.load(Ordering::SeqCst) > 0, mode == 2);
                assert_eq!(probe.dropped.load(Ordering::SeqCst), usize::from(mode == 2));
                assert_eq!(probe.resets.load(Ordering::SeqCst), usize::from(mode == 2));
                assert_eq!(down.high_water_packets(), if mode == 2 { 2 } else { 1 });
            }
            if mode == 2 {
                apps.turn(true).await;
                assert_eq!(
                    apps.sockets.get::<Socket>(apps.handles[0]).state(),
                    smoltcp::socket::tcp::State::Closed
                );
                assert!(
                    apps.handles[1..]
                        .iter()
                        .all(|h| apps.sockets.get::<Socket>(*h).state()
                            == smoltcp::socket::tcp::State::SynSent)
                );
                assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
            }
        } else {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !probe.waiting.load(Ordering::SeqCst) {
                    tokio::select! {
                        value = session.run() => panic!("premature {value:?}"),
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                    }
                }
            })
            .await
            .unwrap();
            for _ in 0..3 {
                assert!(
                    tokio::time::timeout(Duration::from_millis(1), session.run())
                        .await
                        .is_err()
                );
                assert_eq!(probe.coverage.load(Ordering::SeqCst), 1);
                assert_eq!(probe.staged.load(Ordering::SeqCst), 1);
                assert_eq!(probe.received.load(Ordering::SeqCst), 1);
                assert_eq!(probe.syn_ack.load(Ordering::SeqCst), 1);
                assert_eq!(down.high_water_packets(), 0);
                assert_eq!(probe.shutdown.load(Ordering::SeqCst), 0);
                assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
            }
            probe.release();
            tokio::time::timeout(Duration::from_secs(2), async {
                while apps.established() != 33 {
                    super::f_pressure::credit_turn(&mut session, &mut apps, true).await;
                }
            })
            .await
            .unwrap();
            assert!(probe.staged.load(Ordering::SeqCst) >= 33);
            assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
        }
        let seen = session.control.flow_observation().unwrap();
        assert_eq!(
            (seen.live, seen.w, seen.f, seen.factory_started),
            (0, 0, 0, 0)
        );
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
    })
    .await;
    probe.release();
    session.request_stop();
    let cleanup = capture(async {
        if !failed {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), session.run())
                    .await
                    .unwrap(),
                SessionDeviceState::DeviceSettled
            );
        }
    })
    .await;
    // A quarantined owner's forced destruction is cleanup, never clean settlement.
    drop(session);
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn targeted_real_synack_flush_survives_observer_cancellation() {
    saved_single(0).await;
}

#[tokio::test]
async fn targeted_wait_read_error_retains_original_fatal_owner() {
    saved_single(1).await;
}

#[tokio::test]
async fn targeted_synack_write_error_preserves_failure_after_empty_flow_settlement() {
    saved_single(2).await;
}
