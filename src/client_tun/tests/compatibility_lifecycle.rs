//! Compatibility ingress/event-loop characterization helpers and tests.
//! Owns compatibility fixtures and deterministic real-device lifecycle tracers.
//! Shared fixtures are visible only to parent tests; all test entries live below.

use super::*;
use crate::device::{DeviceShutdown, device_closed};

/// 最小 TunIo mock（刀11 T4）：只支持 `handle_dns_hijack` 用到的 `inject_ip_packet`（记录注入回包）
/// + `flush_tx`（no-op）；`Device` 超 trait 的 receive/transmit 在本测试路径不被调用 → 返 None。
#[derive(Default)]
pub(super) struct DnsInjectRecorder {
    pub(super) injected: Vec<Vec<u8>>,
    stopping: bool,
}
pub(super) struct DeadToken;
impl smoltcp::phy::RxToken for DeadToken {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _f: F) -> R {
        unreachable!("mock 不产生 rx")
    }
}
impl smoltcp::phy::TxToken for DeadToken {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _len: usize, _f: F) -> R {
        unreachable!("mock 不经 smoltcp transmit")
    }
}
impl smoltcp::phy::Device for DnsInjectRecorder {
    type RxToken<'a> = DeadToken;
    type TxToken<'a> = DeadToken;
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        smoltcp::phy::DeviceCapabilities::default()
    }
    fn receive(
        &mut self,
        _t: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        None
    }
    fn transmit(&mut self, _t: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        None
    }
}
impl TunIo for DnsInjectRecorder {
    fn request_stop(&mut self) {
        self.stopping = true;
    }
    fn poll_shutdown(&mut self, _: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        drop(std::mem::take(&mut self.injected));
        Poll::Ready(DeviceShutdown::settled())
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        unreachable!("测试直调 handle_dns_hijack，不进 wait_for_rx")
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.stopping {
            return Err(device_closed());
        }
        Ok(false)
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        None
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        None
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        false
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        Ok(())
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        if self.stopping {
            return;
        }
        self.injected.push(pkt.to_vec());
    }
}

pub(super) struct NoopTestUpstream;

#[async_trait::async_trait]
impl ProxyUpstream for NoopTestUpstream {
    async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
        Ok(Box::new(tokio::io::duplex(64).0))
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for NoopTestUpstream {
    async fn send_udp(&self, _datagram: Vec<u8>) {}
}

#[derive(Default)]
struct BrokenTunDevice {
    stopping: bool,
}

impl smoltcp::phy::Device for BrokenTunDevice {
    type RxToken<'a> = DeadToken;
    type TxToken<'a> = DeadToken;

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        let mut capabilities = smoltcp::phy::DeviceCapabilities::default();
        capabilities.medium = smoltcp::phy::Medium::Ip;
        capabilities.max_transmission_unit = 1200;
        capabilities
    }

    fn receive(
        &mut self,
        _timestamp: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        None
    }

    fn transmit(&mut self, _timestamp: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        None
    }
}

impl TunIo for BrokenTunDevice {
    fn request_stop(&mut self) {
        self.stopping = true;
    }
    fn poll_shutdown(&mut self, _: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        Poll::Ready(DeviceShutdown::settled())
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "synthetic terminal TUN failure",
        ))
    }

    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.stopping {
            return Err(device_closed());
        }
        Ok(false)
    }

    fn rx_peek(&self) -> Option<&[u8]> {
        None
    }

    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        None
    }

    fn stage_rx_for_iface(&mut self) -> bool {
        false
    }

    async fn flush_tx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        Ok(())
    }

    fn inject_ip_packet(&mut self, _packet: &[u8]) {}
}

#[tokio::test]
async fn event_loop_stops_after_terminal_tun_failure() {
    let (_downlink_tx, downlink_rx) = tokio::sync::mpsc::channel(1);
    let config = TunRuntimeConfig::from_sources(Some("2")).expect("valid test profile");
    let run = run_event_loop(
        BrokenTunDevice::default(),
        Arc::new(NoopTestUpstream),
        downlink_rx,
        config,
        Arc::new(Metrics::new()),
        NoopSink,
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), run)
        .await
        .expect("terminal TUN failure must stop the event loop instead of busy-waking");
}

struct BatchTestRxToken {
    packet: bytes::BytesMut,
}

impl smoltcp::phy::RxToken for BatchTestRxToken {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(mut self, f: F) -> R {
        f(&mut self.packet)
    }
}

struct BatchTestTxToken<'a> {
    queue: &'a mut std::collections::VecDeque<bytes::BytesMut>,
}

impl smoltcp::phy::TxToken for BatchTestTxToken<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut packet = bytes::BytesMut::zeroed(len);
        let result = f(&mut packet);
        self.queue.push_back(packet);
        result
    }
}

struct BatchTestTunDevice {
    stopping: bool,
    ready: std::collections::VecDeque<bytes::BytesMut>,
    current: Option<bytes::BytesMut>,
    iface_ready: std::collections::VecDeque<bytes::BytesMut>,
    tx: std::collections::VecDeque<bytes::BytesMut>,
}

impl BatchTestTunDevice {
    fn new(packets: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            stopping: false,
            ready: packets
                .into_iter()
                .map(|packet| bytes::Bytes::from(packet).into())
                .collect(),
            current: None,
            iface_ready: std::collections::VecDeque::new(),
            tx: std::collections::VecDeque::new(),
        }
    }
}

impl smoltcp::phy::Device for BatchTestTunDevice {
    type RxToken<'a> = BatchTestRxToken;
    type TxToken<'a> = BatchTestTxToken<'a>;

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        let mut capabilities = smoltcp::phy::DeviceCapabilities::default();
        capabilities.medium = smoltcp::phy::Medium::Ip;
        capabilities.max_transmission_unit = 1200;
        capabilities
    }

    fn receive(
        &mut self,
        _timestamp: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.stopping {
            return None;
        }
        self.iface_ready.pop_front().map(|packet| {
            (
                BatchTestRxToken { packet },
                BatchTestTxToken {
                    queue: &mut self.tx,
                },
            )
        })
    }

    fn transmit(&mut self, _timestamp: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        if self.stopping {
            return None;
        }
        Some(BatchTestTxToken {
            queue: &mut self.tx,
        })
    }
}

impl TunIo for BatchTestTunDevice {
    fn request_stop(&mut self) {
        self.stopping = true;
    }
    fn poll_shutdown(&mut self, _: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        self.current.take();
        drop(std::mem::take(&mut self.ready));
        drop(std::mem::take(&mut self.iface_ready));
        drop(std::mem::take(&mut self.tx));
        Poll::Ready(DeviceShutdown::settled())
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        if self.current.is_none() {
            self.current = self.ready.pop_front();
        }
        Ok(())
    }

    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.stopping {
            return Err(device_closed());
        }
        if self.current.is_none() {
            self.current = self.ready.pop_front();
        }
        Ok(self.current.is_some())
    }

    fn rx_peek(&self) -> Option<&[u8]> {
        if self.stopping {
            return None;
        }
        self.current.as_deref()
    }

    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        if self.stopping {
            return None;
        }
        self.current.take()
    }

    fn stage_rx_for_iface(&mut self) -> bool {
        if self.stopping {
            return false;
        }
        let Some(packet) = self.current.take() else {
            return false;
        };
        self.iface_ready.push_back(packet);
        true
    }

    async fn flush_tx(&mut self) -> std::io::Result<()> {
        if self.stopping {
            return Err(device_closed());
        }
        self.tx.clear();
        Ok(())
    }

    fn inject_ip_packet(&mut self, packet: &[u8]) {
        if self.stopping {
            return;
        }
        self.tx.push_back(bytes::BytesMut::from(packet));
    }
}

struct BatchDrainTestReport {
    drained: usize,
    diag: TunRxDrainDiag,
    poll_enters: usize,
    flush_calls: u64,
    relay_enters: usize,
    remaining_ready: usize,
    current_ready: bool,
    iface_ready: usize,
}

async fn run_batch_drain_test(packets: Vec<Vec<u8>>) -> BatchDrainTestReport {
    let budget = packets.len();
    let mut device = BatchTestTunDevice::new(packets);
    let mut iface = Interface::new(
        SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    let upstream = Arc::new(LegacyOwnedUpstream::new(Arc::new(NoopTestUpstream)));
    let metrics = Metrics::new();
    let mut assoc_table = AssocTable::new();
    let mut fake_pool = FakeIpPool::new();
    let mut registry = ListenerRegistry::new(1);
    let mut sockets = SocketSet::new(vec![]);
    let mut socket_ctxs = HashMap::new();
    registry
        .ensure_port(443, &mut sockets, &mut socket_ctxs)
        .unwrap();
    let mut dirty = HashSet::new();
    let (handshake_done_tx, _handshake_done_rx) = OpenDispatch::channel(1, None);
    let (global_tx, _global_rx) = mpsc::channel(1);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(1);
    let mut sink = CountingSink::default();
    let cfg = default_downlink_backpressure_for_tun_egress(1200, 500);
    let mut drop_debt = DownlinkEgressDropDebt::default();
    let mut tcp_loop_flush_tx_calls = 0;
    let mut tcp_loop_flush_tx_failures = 0;
    let mut diag = TunRxDrainDiag::default();

    let drained = drain_ready_tun_rx(
        &mut device,
        &mut assoc_table,
        &mut fake_pool,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut socket_ctxs,
        &mut iface,
        &mut dirty,
        &handshake_done_tx,
        &global_tx,
        &owned_action_tx,
        &mut sink,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        cfg,
        1200,
        &mut drop_debt,
        &mut tcp_loop_flush_tx_calls,
        &mut tcp_loop_flush_tx_failures,
        &mut diag,
        budget,
        TUN_RX_DRAIN_SOURCE_LOCAL_EGRESS_SERVICE,
        false,
        BufferedDownlinkConfig::disabled(cfg, 1),
    )
    .await
    .unwrap();

    BatchDrainTestReport {
        drained,
        diag,
        poll_enters: sink.poll_enters,
        flush_calls: tcp_loop_flush_tx_calls,
        relay_enters: sink.relay_enters,
        remaining_ready: device.ready.len(),
        current_ready: device.current.is_some(),
        iface_ready: device.iface_ready.len(),
    }
}

#[tokio::test]
async fn bounded_tun_drain_services_dirty_relay_once_per_tcp_batch() {
    const PACKETS: usize = 8;
    let packets = (0..PACKETS).map(|index| {
        build_ipv4_tcp(
            [10, 0, 0, 1],
            [93, 184, 216, 34],
            40_000 + index as u16,
            443,
            false,
            true,
        )
    });
    let report = run_batch_drain_test(packets.collect()).await;

    assert_eq!(report.drained, PACKETS);
    assert_eq!(report.diag.tcp_packets, PACKETS as u64);
    assert_eq!(report.diag.tcp_batches, 1);
    assert_eq!(report.diag.batch_dirty_relay_passes, 1);
    assert_eq!(report.diag.batch_iface_polls, 1);
    assert_eq!(report.diag.batch_flushes, 1);
    assert_eq!(report.diag.tcp_batch_packets_high_water, PACKETS as u64);
    assert_eq!(
        report.diag.avoided_dirty_relay_passes(),
        (PACKETS - 1) as u64
    );
    assert_eq!(report.diag.avoided_iface_polls(), (PACKETS - 1) as u64);
    assert_eq!(report.remaining_ready, 0);
    assert!(!report.current_ready);
    assert_eq!(report.iface_ready, 0);
    assert_eq!(
        report.poll_enters, 1,
        "one bounded TCP batch must enter smoltcp poll once"
    );
    assert_eq!(
        report.flush_calls, 1,
        "one bounded TCP batch must flush TUN once"
    );
    assert_eq!(
        report.relay_enters, 1,
        "one bounded TUN batch must not amplify dirty relay service per packet"
    );
}

#[tokio::test]
async fn bounded_tun_drain_skips_tcp_relay_for_udp_only_batch() {
    const PACKETS: usize = 4;
    let packets = (0..PACKETS)
        .map(|index| {
            build_udp_ip_packet(
                (Ipv4Addr::new(10, 0, 0, 1), 40_000 + index as u16),
                (Ipv4Addr::new(93, 184, 216, 34), 443),
                &[index as u8],
            )
            .expect("valid UDP packet")
        })
        .collect();
    let report = run_batch_drain_test(packets).await;

    assert_eq!(report.drained, PACKETS);
    assert_eq!(report.diag.tcp_packets, 0);
    assert_eq!(report.diag.udp_packets, PACKETS as u64);
    assert_eq!(report.diag.tcp_batches, 0);
    assert_eq!(report.diag.batch_dirty_relay_passes, 0);
    assert_eq!(report.diag.batch_iface_polls, 0);
    assert_eq!(report.diag.batch_flushes, 0);
    assert_eq!(report.diag.avoided_dirty_relay_passes(), 0);
    assert_eq!(report.relay_enters, 0);
    assert_eq!(report.remaining_ready, 0);
    assert!(!report.current_ready);
    assert_eq!(report.iface_ready, 0);
}

#[tokio::test]
async fn bounded_tun_drain_services_mixed_batch_once_when_tcp_is_present() {
    let packets = vec![
        build_udp_ip_packet(
            (Ipv4Addr::new(10, 0, 0, 1), 40_000),
            (Ipv4Addr::new(93, 184, 216, 34), 443),
            &[0x11],
        )
        .expect("valid UDP packet"),
        build_ipv4_tcp([10, 0, 0, 1], [93, 184, 216, 34], 40_001, 443, false, true),
        build_udp_ip_packet(
            (Ipv4Addr::new(10, 0, 0, 1), 40_002),
            (Ipv4Addr::new(93, 184, 216, 34), 443),
            &[0x22],
        )
        .expect("valid UDP packet"),
        build_ipv4_tcp([10, 0, 0, 1], [93, 184, 216, 34], 40_003, 443, false, true),
    ];
    let report = run_batch_drain_test(packets).await;

    assert_eq!(report.drained, 4);
    assert_eq!(report.diag.tcp_packets, 2);
    assert_eq!(report.diag.udp_packets, 2);
    assert_eq!(report.diag.tcp_batches, 1);
    assert_eq!(report.diag.batch_dirty_relay_passes, 1);
    assert_eq!(report.diag.batch_iface_polls, 1);
    assert_eq!(report.diag.batch_flushes, 1);
    assert_eq!(report.diag.tcp_batch_packets_high_water, 2);
    assert_eq!(report.diag.avoided_dirty_relay_passes(), 1);
    assert_eq!(report.diag.avoided_iface_polls(), 1);
    assert_eq!(report.relay_enters, 1);
    assert_eq!(report.remaining_ready, 0);
    assert!(!report.current_ready);
    assert_eq!(report.iface_ready, 0);
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn loopback_stop_observes_join_after_whole_packet_handoff() {
    use crate::device::DeviceShutdownState;
    use crate::harness::{LoopbackTunDevice, PacketLink};

    let inbound = PacketLink::bounded(1);
    let outbound = PacketLink::bounded(1);
    let mut device = LoopbackTunDevice::new(inbound.clone(), outbound).with_ingress_pump(1);
    for (capacity, content) in [(128, &b"whole packet"[..]), (192, &b""[..])] {
        let mut packet = bytes::BytesMut::with_capacity(capacity);
        packet.extend_from_slice(content);
        let pointer = packet.as_ptr();
        let backing = packet.capacity();
        assert!(inbound.push(packet));
        tokio::time::timeout(std::time::Duration::from_secs(1), device.wait_for_rx())
            .await
            .expect("whole packet must arrive")
            .expect("source remains live");
        let packet = device.rx_take().expect("empty packet is still a packet");
        assert_eq!(packet.as_ptr(), pointer);
        assert_eq!(packet.capacity(), backing);
        assert_eq!(&packet[..], content);
        drop(packet);
    }

    device.request_stop();
    std::future::poll_fn(|cx| {
        assert!(
            device.poll_shutdown(cx).is_pending(),
            "stop request cannot substitute for observing the live pump join"
        );
        // End this observation in the same poll turn without allowing the child to run.
        std::task::Poll::Ready(())
    })
    .await;
    let shutdown = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        std::future::poll_fn(|cx| device.poll_shutdown(cx)),
    )
    .await
    .expect("stop must wake the live packet source without another packet");
    assert_eq!(shutdown.state(), DeviceShutdownState::Settled);
    let repeated = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
    assert_eq!(repeated.state(), DeviceShutdownState::Settled);
    drop(inbound);
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn loopback_pressure_shutdown_closes_entry_points_without_clearing_peer_links() {
    use crate::device::DeviceShutdownState;
    use crate::harness::{LoopbackTunDevice, PacketLink};

    let inbound = PacketLink::bounded(4);
    let outbound = PacketLink::bounded(4);
    let mut device = LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(1);
    for byte in [1, 2] {
        assert!(inbound.push(bytes::BytesMut::from(&[byte; 8][..])));
        tokio::time::timeout(std::time::Duration::from_secs(1), device.wait_for_rx())
            .await
            .expect("packet must reach the live device")
            .unwrap();
        if byte == 1 {
            assert!(device.stage_rx_for_iface());
        }
    }
    assert!(inbound.push(bytes::BytesMut::from(&[3; 8][..])));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while device.tun_ingress_pump_snapshot().packets != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("third packet must fill the single channel slot");
    let full_waits = device.tun_ingress_pump_snapshot().full_waits;
    assert!(inbound.push(bytes::BytesMut::from(&[4; 8][..])));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while device.tun_ingress_pump_snapshot().full_waits == full_waits {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fourth packet must be retained by the producer under pressure");
    assert!(inbound.push(bytes::BytesMut::from(&[5; 8][..])));
    assert!(outbound.push(bytes::BytesMut::from(&b"peer-owned"[..])));
    device.inject_ip_packet(&[9; 8]);
    let queued = device.queued_tx_bytes();
    assert!(queued > 0);

    device.request_stop();
    assert_eq!(
        device.wait_for_rx().await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        device.try_recv_rx().unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        device.flush_tx().await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert!(device.rx_peek().is_none());
    assert!(device.rx_take().is_none());
    assert!(!device.stage_rx_for_iface());
    let now = smoltcp::time::Instant::from_millis(0);
    assert!(smoltcp::phy::Device::receive(&mut device, now).is_none());
    assert!(smoltcp::phy::Device::transmit(&mut device, now).is_none());
    device.inject_ip_packet(&[10; 8]);
    assert_eq!(device.queued_tx_bytes(), queued);
    let shutdown = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        std::future::poll_fn(|cx| device.poll_shutdown(cx)),
    )
    .await
    .expect("reserve pressure must stop and join without consumer progress");
    assert_eq!(shutdown.state(), DeviceShutdownState::Settled);
    assert_eq!(device.queued_tx_bytes(), 0);
    assert!(device.take_flushed_tcp_packets().is_empty());
    assert_eq!(
        std::future::poll_fn(|cx| device.poll_shutdown(cx))
            .await
            .state(),
        DeviceShutdownState::Settled
    );

    for (link, expected) in [(inbound, &[5; 8][..]), (outbound, &b"peer-owned"[..])] {
        let mut peer = LoopbackTunDevice::new(link, PacketLink::bounded(1));
        assert!(peer.try_recv_rx().unwrap());
        assert_eq!(&peer.rx_take().unwrap()[..], expected);
        let result = std::future::poll_fn(|cx| peer.poll_shutdown(cx)).await;
        assert_eq!(result.state(), DeviceShutdownState::Settled);
    }
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn loopback_repeated_pump_assembly_preserves_the_original_owner_after_stop() {
    use crate::device::DeviceShutdownState;
    use crate::harness::{LoopbackTunDevice, PacketLink};

    let inbound = PacketLink::bounded(1);
    let mut device =
        LoopbackTunDevice::new(inbound.clone(), PacketLink::bounded(1)).with_ingress_pump(1);
    assert!(inbound.push(bytes::BytesMut::from(&b"one owner"[..])));
    tokio::time::timeout(std::time::Duration::from_secs(1), device.wait_for_rx())
        .await
        .expect("original pump must deliver a packet")
        .unwrap();
    assert_eq!(&device.rx_take().unwrap()[..], b"one owner");
    let original = device.tun_ingress_pump_snapshot();
    assert_eq!(original.packets, 1);
    device = device.with_ingress_pump(2);
    let same_owner_before_stop = device.tun_ingress_pump_snapshot() == original;
    device.request_stop();
    device = device.with_ingress_pump(3);
    let same_owner_after_stop = device.tun_ingress_pump_snapshot() == original;
    let shutdown = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        std::future::poll_fn(|cx| device.poll_shutdown(cx)),
    )
    .await
    .expect("the retained owner must join");
    assert_eq!(shutdown.state(), DeviceShutdownState::Settled);
    assert!(
        same_owner_before_stop && same_owner_after_stop,
        "repeated assembly must preserve the original pump owner before and after stop"
    );
}
