//! Test-only wire fixture using the original Interface and TcpSocket algorithms.
//! No root binding, socket field edits, synthetic ACK progress or success stub.
use super::*;
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};

#[derive(Default)]
struct Wire {
    capture_meter: Option<bytes::BackingMeter>,
    rx: VecDeque<Vec<u8>>,
    tx: Vec<Vec<u8>>,
    mtu: usize,
    outbound: Vec<Vec<u8>>,
    flushed: Vec<TunFlushedTcpPacket>,
    stopped: bool,
    hold_flush: bool,
    panic_flush: Option<Arc<()>>,
    delay_flush: Option<std::time::Duration>,
    fail_after_packets: Option<usize>,
}
struct Rx(Vec<u8>);
struct Tx<'a>(&'a mut Vec<Vec<u8>>);
impl RxToken for Rx {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(mut self, f: F) -> R {
        f(&mut self.0)
    }
}
impl TxToken for Tx<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut packet = vec![0; len];
        let result = f(&mut packet);
        self.0.push(packet);
        result
    }
}
impl Device for Wire {
    type RxToken<'a> = Rx;
    type TxToken<'a> = Tx<'a>;
    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = self.mtu;
        caps
    }
    fn receive(&mut self, _: smoltcp::time::Instant) -> Option<(Rx, Tx<'_>)> {
        self.rx
            .pop_front()
            .map(|packet| (Rx(packet), Tx(&mut self.tx)))
    }
    fn transmit(&mut self, _: smoltcp::time::Instant) -> Option<Tx<'_>> {
        Some(Tx(&mut self.tx))
    }
}

impl TunIo for Wire {
    fn main_capture_meter(&self) -> Option<&bytes::BackingMeter> {
        self.capture_meter.as_ref()
    }
    fn request_stop(&mut self) {
        self.stopped = true;
    }
    fn poll_shutdown(&mut self, _: &mut Context<'_>) -> Poll<crate::device::DeviceShutdown> {
        self.request_stop();
        self.rx.clear();
        self.tx.clear();
        self.outbound.clear();
        self.flushed.clear();
        Poll::Ready(crate::device::DeviceShutdown::settled())
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        Ok(!self.rx.is_empty())
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.rx.front().map(Vec::as_slice)
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.rx
            .pop_front()
            .map(|v| bytes::BytesMut::from(v.as_slice()))
    }
    fn rx_take_packet(&mut self) -> Option<crate::device::RxPacket> {
        self.rx_take().map(Into::into)
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        !self.rx.is_empty()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        if let Some(payload) = self.panic_flush.take() {
            std::panic::panic_any(payload);
        }
        if self.hold_flush {
            std::future::pending::<()>().await;
        }
        if let Some(delay) = self.delay_flush {
            tokio::time::sleep(delay).await;
        }
        if self.stopped {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        let mut delivered = 0;
        while !self.tx.is_empty() {
            if self.fail_after_packets == Some(delivered) {
                return Err(std::io::Error::other("controlled local partial flush"));
            }
            let packet = self.tx.remove(0);
            // Actual handoff to the local byte endpoint precedes its summary.
            self.outbound.push(packet);
            if let Some(summary) =
                crate::device::summarize_flushed_ip_tcp_packet(self.outbound.last().unwrap())
            {
                self.flushed.push(summary);
            }
            delivered += 1;
        }
        Ok(())
    }
    fn queued_tx_bytes(&self) -> usize {
        self.tx.iter().map(Vec::len).sum()
    }
    fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        std::mem::take(&mut self.flushed).into()
    }
    fn inject_ip_packet(&mut self, p: &[u8]) {
        self.tx.push(p.to_vec());
    }
}

fn handshake_same_tuple(
    device: &mut Wire,
    iface: &mut Interface,
    sockets: &mut SocketSet<'static>,
    handle: SocketHandle,
    sequence: u32,
    now: i64,
) -> i64 {
    handshake_tuple(device, iface, sockets, handle, sequence, now, 53124, 443)
}

fn handshake_tuple(
    device: &mut Wire,
    iface: &mut Interface,
    sockets: &mut SocketSet<'static>,
    handle: SocketHandle,
    sequence: u32,
    _now: i64,
    source_port: u16,
    local_port: u16,
) -> i64 {
    let mut syn = Vec::new();
    etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
        .tcp(source_port, local_port, sequence, u16::MAX)
        .syn()
        .options(&[etherparse::TcpOptionElement::MaximumSegmentSize(
            (device.mtu - 40) as u16,
        )])
        .unwrap()
        .write(&mut syn, b"")
        .unwrap();
    device.rx.push_back(syn);
    iface.poll(
        smoltcp::time::Instant::now(),
        device,
        sockets,
    );
    let synack = device.tx.pop().expect("real SYNACK");
    let headers = etherparse::PacketHeaders::from_ip_slice(&synack).unwrap();
    let etherparse::TransportHeader::Tcp(tcp) = headers.transport.unwrap() else {
        panic!("TCP SYNACK")
    };
    assert!(tcp.syn && tcp.ack);
    let mut ack = Vec::new();
    etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
        .tcp(source_port, local_port, sequence.wrapping_add(1), u16::MAX)
        .ack(tcp.sequence_number.wrapping_add(1))
        .write(&mut ack, b"")
        .unwrap();
    device.rx.push_back(ack);
    iface.poll(
        smoltcp::time::Instant::now(),
        device,
        sockets,
    );
    assert_eq!(
        sockets.get::<TcpSocket>(handle).state(),
        TcpState::Established
    );
    device.tx.clear();
    smoltcp::time::Instant::now().total_millis()
}

pub(super) struct Flow {
    device: Wire,
    iface: Interface,
    pub(super) sockets: SocketSet<'static>,
    pub(super) handle: SocketHandle,
    pub(super) ctxs: HashMap<SocketHandle, SocketCtx>,
    pub(super) queue: AsyncLeasedByteFlowQueue,
    now: i64,
    client_sequence: u32,
}
impl Flow {
    pub(super) fn new() -> Self {
        Self::with_limits(4096, 1500)
    }
    pub(super) fn with_limits(capacity: usize, mtu: usize) -> Self {
        Self::with_seeded_limits(capacity, mtu, 0)
    }
    pub(super) fn with_seed(seed: u64) -> Self {
        Self::with_seeded_limits(4096, 1500, seed)
    }
    fn with_seeded_limits(capacity: usize, mtu: usize, seed: u64) -> Self {
        let mut device = Wire {
            mtu,
            ..Wire::default()
        };
        let mut config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
        config.random_seed = seed;
        let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::now());
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::v4(203, 0, 113, 9), 24))
                .unwrap();
        });
        iface
            .routes_mut()
            .add_default_ipv4_route(smoltcp::wire::Ipv4Address::new(203, 0, 113, 1))
            .unwrap();
        let mut socket = TcpSocket::new(
            TcpSocketBuffer::new(vec![0; capacity]),
            TcpSocketBuffer::new(vec![0; capacity]),
        );
        configure_local_tcp_socket(&mut socket);
        socket.listen(443).unwrap();
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(socket);
        let now = handshake_same_tuple(&mut device, &mut iface, &mut sockets, handle, 1000, 0);
        Self {
            device,
            iface,
            sockets,
            handle,
            ctxs: HashMap::from([(handle, SocketCtx::new(443))]),
            queue: AsyncLeasedByteFlowQueue::new_with_release_mode(
                capacity * 2,
                DownstreamPermitReleaseMode::OnEgressDrain,
            )
            .unwrap(),
            now,
            client_sequence: 1001,
        }
    }
    pub(super) fn reconnect(&mut self) {
        assert_eq!(
            self.sockets.get::<TcpSocket>(self.handle).state(),
            TcpState::Listen
        );
        self.now = handshake_same_tuple(
            &mut self.device,
            &mut self.iface,
            &mut self.sockets,
            self.handle,
            2000,
            self.now,
        );
        self.client_sequence = 2001;
        let capacity = self.queue.snapshot_now().capacity_bytes;
        assert_eq!(self.queue.snapshot_now().owned_bytes(), 0);
        self.queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
            capacity,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap();
    }
    pub(super) async fn admit(&mut self, payload: &'static [u8]) {
        self.admit_prefix(payload, payload.len()).await;
    }
    pub(super) async fn admit_prefix(&mut self, payload: &'static [u8], limit: usize) {
        self.queue.push(Bytes::from_static(payload)).await.unwrap();
        let (bytes, permit) = self
            .queue
            .recv_up_to(payload.len())
            .await
            .unwrap()
            .into_parts();
        let ctx = self.ctxs.get_mut(&self.handle).unwrap();
        ctx.append_downlink_pending_with_permit(&bytes, &mut Some(permit));
        let outcome = flush_downlink(
            self.handle,
            self.sockets.get_mut::<TcpSocket>(self.handle),
            ctx,
            limit,
            DownlinkBackpressureConfig {
                high_bytes: 128 * 1024,
                low_bytes: 32 * 1024,
            },
            1500,
            &mut DownlinkEgressDropDebt::default(),
        );
        assert_eq!(outcome.accepted_bytes, limit);
    }
    pub(super) fn dispatch_payload(&mut self) -> TunFlushedTcpPacket {
        let packets = self.dispatch_packets();
        assert_eq!(packets.len(), 1, "one actual DATA packet");
        packets[0]
    }
    pub(super) fn dispatch_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        self.poll_queued();
        self.device
            .tx
            .drain(..)
            .filter_map(|p| crate::device::summarize_flushed_ip_tcp_packet(&p))
            .collect()
    }
    pub(super) fn poll_queued(&mut self) {
        self.now = smoltcp::time::Instant::now().total_millis();
        self.iface.poll(
            smoltcp::time::Instant::from_millis(self.now),
            &mut self.device,
            &mut self.sockets,
        );
    }
    pub(super) async fn flush_original(&mut self) -> (std::io::Result<()>, TunFlushPermitRelease) {
        flush_tx_and_release_downlink_permits(
            &mut self.device,
            &self.sockets,
            &mut self.ctxs,
            "actual_wire_reset",
        )
        .await
    }
    pub(super) fn capture_pending_flush(
        &mut self,
    ) -> crate::client_tun::downlink_custody::FlushBatch {
        crate::client_tun::downlink_custody::begin_flush_batch(
            &self.sockets,
            &mut self.ctxs,
            self.device.main_capture_meter(),
        )
        .unwrap()
    }
    pub(super) async fn finish_pending_flush(
        &mut self,
        captured: crate::client_tun::downlink_custody::FlushBatch,
    ) -> TunFlushPermitRelease {
        // Test-only successful-IO split of the original batch boundary. The
        // original non-Copy capture is consumed exactly once, never forged.
        self.device.flush_tx().await.unwrap();
        crate::client_tun::downlink_custody::finish_flush_batch(
            self.device.take_flushed_tcp_packets(),
            &self.sockets,
            &mut self.ctxs,
            captured,
            true,
        )
    }
    pub(super) fn last_delivered_payload(&self) -> TunFlushedTcpPacket {
        self.device
            .outbound
            .iter()
            .rev()
            .find_map(|packet| crate::device::summarize_flushed_ip_tcp_packet(packet))
            .expect("actual delivered payload")
    }
    pub(super) fn reset_delivered(&self) -> bool {
        self.device.outbound.iter().any(|p| {
            etherparse::PacketHeaders::from_ip_slice(p)
                .ok()
                .and_then(|h| h.transport)
                .is_some_and(|t| matches!(t,etherparse::TransportHeader::Tcp(h) if h.rst))
        })
    }
    pub(super) fn observe(&mut self, packet: TunFlushedTcpPacket) -> usize {
        let captured = self.capture_pending_flush();
        crate::client_tun::downlink_custody::finish_flush_batch(
            vec![packet],
            &self.sockets,
            &mut self.ctxs,
            captured,
            true,
        )
        .released_inflight_permit_bytes
    }
    pub(super) fn hold_flush(&mut self) {
        self.device.hold_flush = true;
    }
    pub(super) fn fail_flush(&mut self) {
        self.device.stopped = true;
    }
    pub(super) fn partial_flush_after(&mut self, packets: usize) {
        self.device.fail_after_packets = Some(packets);
    }
    pub(super) fn delay_flush(&mut self, delay: std::time::Duration) {
        self.device.delay_flush = Some(delay);
    }
    pub(super) fn acknowledge(&mut self, sequence: u32) -> usize {
        use crate::client_tun::downlink_custody::{
            capture_dirty_send_queues, release_dirty_downlink_inflight_permits_for_send_queue_drain,
        };
        let before =
            capture_dirty_send_queues(&HashSet::from([self.handle]), &self.sockets, &self.ctxs);
        let mut ack = Vec::new();
        etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
            .tcp(53124, 443, self.client_sequence, u16::MAX)
            .ack(sequence)
            .write(&mut ack, b"")
            .unwrap();
        self.device.rx.push_back(ack);
        self.poll_queued();
        release_dirty_downlink_inflight_permits_for_send_queue_drain(
            &before,
            &self.sockets,
            &mut self.ctxs,
        )
    }
}

#[tokio::test]
async fn one_flow_ack_refunds_while_another_flow_payload_flushes() {
    let mut flow = Flow::new();
    flow.admit(b"AAAA").await;
    let a = flow.dispatch_payload();
    assert_eq!(a.payload_bytes, 4);
    assert_eq!(flow.queue.snapshot().await.leased_bytes, 4);

    let mut socket_b = TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 4096]),
        TcpSocketBuffer::new(vec![0; 4096]),
    );
    configure_local_tcp_socket(&mut socket_b);
    socket_b.listen(444).unwrap();
    let handle_b = flow.sockets.add(socket_b);
    flow.now = handshake_tuple(
        &mut flow.device,
        &mut flow.iface,
        &mut flow.sockets,
        handle_b,
        2000,
        flow.now,
        53125,
        444,
    );
    flow.ctxs.insert(handle_b, SocketCtx::new(444));
    let queue_b = AsyncLeasedByteFlowQueue::new_with_release_mode(
        8,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    queue_b.push(Bytes::from_static(b"BBBB")).await.unwrap();
    let (bytes_b, cap_b) = queue_b.recv_up_to(4).await.unwrap().into_parts();
    let ctx_b = flow.ctxs.get_mut(&handle_b).unwrap();
    ctx_b.append_downlink_pending_with_permit(&bytes_b, &mut Some(cap_b));
    assert_eq!(
        flush_downlink(
            handle_b,
            flow.sockets.get_mut::<TcpSocket>(handle_b),
            ctx_b,
            4,
            DownlinkBackpressureConfig {
                high_bytes: 128 * 1024,
                low_bytes: 32 * 1024,
            },
            1500,
            &mut DownlinkEgressDropDebt::default(),
        )
        .accepted_bytes,
        4
    );

    let mut ack_a = Vec::new();
    etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
        .tcp(53124, 443, flow.client_sequence, u16::MAX)
        .ack(a.payload_start_seq.wrapping_add(a.payload_bytes as u32))
        .write(&mut ack_a, b"")
        .unwrap();
    flow.device.rx.push_back(ack_a);
    let (result, release) = poll_egress_cycle(
        &mut flow.device,
        &mut flow.iface,
        &mut flow.sockets,
        &mut flow.ctxs,
        None,
        "mixed_flow_wire",
        #[cfg(all(test, feature = "harness"))]
        None,
    )
    .await;
    result.unwrap();
    assert!(
        release.flushed_tcp_payload_bytes >= 4,
        "B actual payload flush: {release:?}"
    );
    assert_eq!(
        release.released_inflight_permit_bytes, 8,
        "A ACK plus B flush: {release:?}"
    );
    assert_eq!(flow.queue.snapshot().await.leased_bytes, 0);
    assert_eq!(queue_b.snapshot().await.leased_bytes, 0);
    assert!(
        !flow.ctxs[&flow.handle]
            .downlink_inflight_permits
            .quarantined()
    );
    assert!(!flow.ctxs[&handle_b].downlink_inflight_permits.quarantined());
}

#[tokio::test]
async fn same_flow_prior_ack_and_new_payload_flush_refund_only_new_cap() {
    let mut flow = Flow::new();
    flow.admit_prefix(b"AAAABBBB", 4).await;
    let (first_result, first_release) = poll_egress_cycle(
        &mut flow.device,
        &mut flow.iface,
        &mut flow.sockets,
        &mut flow.ctxs,
        None,
        "same_flow_first_wire",
        #[cfg(all(test, feature = "harness"))]
        None,
    )
    .await;
    first_result.unwrap();
    assert_eq!(first_release.flushed_tcp_payload_bytes, 4);
    assert_eq!(first_release.released_inflight_permit_bytes, 4);
    let a = flow.last_delivered_payload();
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);

    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    assert_eq!(
        flush_downlink(
            flow.handle,
            flow.sockets.get_mut::<TcpSocket>(flow.handle),
            ctx,
            4,
            DownlinkBackpressureConfig {
                high_bytes: 128 * 1024,
                low_bytes: 32 * 1024,
            },
            1500,
            &mut DownlinkEgressDropDebt::default(),
        )
        .accepted_bytes,
        4
    );
    let mut ack_a = Vec::new();
    etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
        .tcp(53124, 443, flow.client_sequence, u16::MAX)
        .ack(a.payload_start_seq.wrapping_add(4))
        .write(&mut ack_a, b"")
        .unwrap();
    flow.device.rx.push_back(ack_a.clone());
    flow.device.rx.push_back(ack_a.clone());
    let (second_result, second_release) = poll_egress_cycle(
        &mut flow.device,
        &mut flow.iface,
        &mut flow.sockets,
        &mut flow.ctxs,
        None,
        "same_flow_ack_and_payload_wire",
        #[cfg(all(test, feature = "harness"))]
        None,
    )
    .await;
    second_result.unwrap();
    assert!(
        flow.device.rx.is_empty(),
        "both old wire ACKs were consumed"
    );
    assert_eq!(second_release.flushed_tcp_payload_bytes, 4);
    assert_eq!(second_release.released_inflight_permit_bytes, 4);
    assert_eq!(
        flow.sockets
            .get::<TcpSocket>(flow.handle)
            .send_payload_range(),
        Some((a.payload_start_seq.wrapping_add(4), 4)),
        "the real ACK advanced A while B remains outstanding"
    );
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 0);
    assert!(flow.ctxs[&flow.handle].downlink_inflight_permits.is_empty());
    assert!(
        !flow.ctxs[&flow.handle]
            .downlink_inflight_permits
            .quarantined()
    );
    flow.device.rx.push_back(ack_a);
    let (duplicate_result, duplicate_release) = poll_egress_cycle(
        &mut flow.device,
        &mut flow.iface,
        &mut flow.sockets,
        &mut flow.ctxs,
        None,
        "same_flow_duplicate_old_ack_wire",
        #[cfg(all(test, feature = "harness"))]
        None,
    )
    .await;
    duplicate_result.unwrap();
    assert!(
        flow.device.rx.is_empty(),
        "replayed old wire ACK was consumed"
    );
    assert_eq!(duplicate_release.released_inflight_permit_bytes, 0);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 0);
    assert!(flow.ctxs[&flow.handle].downlink_inflight_permits.is_empty());
    assert!(
        !flow.ctxs[&flow.handle]
            .downlink_inflight_permits
            .quarantined()
    );
}

mod native_summary;

impl Flow {
    pub(super) fn native_capture(&mut self, meter: &bytes::BackingMeter) {
        self.device.capture_meter = Some(meter.clone());
    }
}

impl Flow {
    pub(super) fn panic_next_flush(&mut self, payload: Arc<()>) {
        self.device.panic_flush = Some(payload);
    }
    pub(super) fn enqueue_ack(&mut self, sequence: u32) {
        let mut ack = Vec::new();
        etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
            .tcp(53124, 443, self.client_sequence, u16::MAX)
            .ack(sequence)
            .write(&mut ack, b"")
            .unwrap();
        self.device.rx.push_back(ack);
    }
    pub(super) async fn poll_original(&mut self) -> (std::io::Result<()>, TunFlushPermitRelease) {
        poll_egress_cycle(
            &mut self.device,
            &mut self.iface,
            &mut self.sockets,
            &mut self.ctxs,
            None,
            "native_capture_wire",
            #[cfg(all(test, feature = "harness"))]
            None,
        )
        .await
    }
}
