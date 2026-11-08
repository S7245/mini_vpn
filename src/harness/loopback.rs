//! Owns the real in-memory TUN device, pump and packet queues. It implements
//! the same TunIo/Device semantics as the physical device; no protocol, session
//! or retry authority lives here. Public type path remains harness::LoopbackTunDevice.
//! Existing characterization tests remain at harness::tests.
use super::*;
pub(crate) mod native;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct LoopbackReceiveWatch {
    replay: Option<BytesMut>,
    replay_count: usize,
    ack_origin: Option<(TunFlushedTcpPacket, u32)>,
    ack_packet: Option<BytesMut>,
}

#[cfg(test)]
impl LoopbackReceiveWatch {
    pub(crate) fn arm_ack(&mut self, origin: TunFlushedTcpPacket, max_extra: u32) {
        assert!(self.ack_origin.is_none());
        self.ack_origin = Some((origin, max_extra));
    }

    pub(crate) fn captured_ack(&self) -> Option<BytesMut> {
        self.ack_packet.clone()
    }

    pub(crate) fn arm_replay(&mut self, packet: BytesMut) -> usize {
        self.replay = Some(packet);
        self.replay_count
    }

    pub(crate) fn replay_count(&self) -> usize {
        self.replay_count
    }
}

#[cfg(test)]
impl PacketLink {
    /// Passive bounded wire snapshot for real Service/App tests; leaves packet order intact.
    pub(crate) fn packets_for_test(&self) -> Vec<BytesMut> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }
}

/// 被测主循环（SUT）用的内存回环 TUN 设备：结构镜像 `VirtualTunDevice`
/// （单槽 rx_buffer + tx_queue），只是数据来自 [`PacketLink`] 而非真 utun。
pub struct LoopbackTunDevice {
    cut: crate::device::cut::CutBoundary,
    rx: crate::device::rx::RxStock,
    egress: crate::device::egress::Egress,
    inbound: Option<PacketLink>,  // 发生器 → SUT
    outbound: Option<PacketLink>, // SUT → 发生器
    ingress_pump: crate::device::pump_native::Startup<native::PacketSource>,
    stopping: bool,
    shutdown_failure: Option<std::io::ErrorKind>,
    shutdown_failure_transferred: bool,
    shutdown_callbacks: Option<crate::device::egress::CallbackStatus>,
    /// 刀12：每次 `flush_tx` 注入的合成 on-loop CPU 成本（busy-spin）。默认 ZERO。
    /// `flush_tx` 在主循环 poll 段内（enter_poll/leave_poll 括起）→ 该 burn 计入 poll_time / loop-active，
    /// 用于验证 profiler 能侦测「主循环被 on-loop CPU 拖满」的饱和信号（T4 spike）。
    cpu_burn_per_flush: Duration,
    suppress_wait_after_tcp_payload_flush: bool,
    wait_suppressed: bool,
    max_tcp_payload_packets_per_flush: Arc<AtomicU64>,
    mtu: usize,
    #[cfg(test)]
    receive_watch: Option<Arc<Mutex<LoopbackReceiveWatch>>>,
    #[cfg(test)]
    partial_flush: Option<Arc<PartialFlushProbe>>,
}

/// One test-only device failure after a real bounded-link payload handoff.
/// The original flush packet summary remains with the device; queued tail
/// packets stay in its original tx_queue for the production custody path.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct PartialFlushProbe {
    armed: AtomicBool,
    witness: Mutex<Option<(TunFlushedTcpPacket, usize, Option<usize>)>>,
}

#[cfg(test)]
impl PartialFlushProbe {
    pub(crate) fn arm(&self) {
        assert!(self.witness.lock().unwrap().is_none());
        self.armed.store(true, Ordering::Release);
    }

    pub(super) fn native_plan<'a>(
        &self,
        packets: impl Iterator<Item = &'a BytesMut>,
    ) -> Option<(TunFlushedTcpPacket, usize)> {
        if !self.armed.load(Ordering::Acquire) {
            return None;
        }
        let mut summaries = packets.filter_map(|p| summarize_flushed_ip_tcp_packet(p));
        let first = summaries.next()?;
        let mut count = 0;
        let mut same_flow = false;
        for next in summaries {
            count += 1;
            same_flow |= (
                first.src_addr,
                first.dst_addr,
                first.src_port,
                first.dst_port,
            ) == (next.src_addr, next.dst_addr, next.src_port, next.dst_port);
        }
        same_flow.then_some((first, count))
    }
    pub(super) fn native_handoff(&self, summary: TunFlushedTcpPacket, remaining: usize, index: usize) {
        self.armed.store(false, Ordering::Release);
        *self.witness.lock().unwrap() = Some((summary, remaining, Some(index)));
    }
    pub(crate) fn native_cut_index(&self) -> Option<usize> {
        self.witness.lock().unwrap().and_then(|(_, _, index)| index)
    }
    pub(crate) fn witness(&self) -> Option<(TunFlushedTcpPacket, usize)> {
        self.witness.lock().unwrap().map(|(summary, remaining, _)| (summary, remaining))
    }
}

impl LoopbackTunDevice {
    fn unavailable(&self) -> std::io::Error {
        self.shutdown_callbacks.as_ref().map_or_else(
            || self.egress.unavailable(),
            crate::device::egress::CallbackStatus::error,
        )
    }
    fn discard_cut_buffers(&mut self, discarded: &mut (u64, u64)) {
        use crate::device::cut::discard_packets;
        self.rx.discard(discarded);
        self.egress.discard(discarded);
        if let Some(link) = &self.inbound {
            link.discard(discarded, self.egress.native_meter());
        }
    }
    /// Two real packet devices for local contract tests: pumped service side
    /// and direct application side. Capacities retain the original min-one policy.
    /// Construct inside a live Tokio runtime; no OS interface is opened.
    pub fn bounded_pair(capacity_packets: usize, pump_capacity: usize) -> (Self, Self) {
        let inbound = PacketLink::bounded(capacity_packets);
        let outbound = PacketLink::bounded(capacity_packets);
        (
            Self::new(inbound.clone(), outbound.clone()).with_ingress_pump(pump_capacity),
            Self::new(outbound, inbound),
        )
    }

    pub(crate) fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self::with_options(inbound, outbound, Duration::ZERO, false)
    }

    #[cfg(test)]
    pub(crate) fn with_partial_flush_probe(mut self) -> (Self, Arc<PartialFlushProbe>) {
        let probe = Arc::new(PartialFlushProbe::default());
        self.partial_flush = Some(probe.clone());
        (self, probe)
    }

    /// 带合成 on-loop CPU 成本的回环设备（T4：multi_thread 饱和 spike 用）。
    pub(super) fn with_burn(
        inbound: PacketLink,
        outbound: PacketLink,
        cpu_burn_per_flush: Duration,
    ) -> Self {
        Self::with_options(inbound, outbound, cpu_burn_per_flush, false)
    }

    pub(super) fn with_suppressed_wait_after_tcp_payload_flush(
        inbound: PacketLink,
        outbound: PacketLink,
    ) -> Self {
        Self::with_options(inbound, outbound, Duration::ZERO, true)
    }

    fn with_options(
        inbound: PacketLink,
        outbound: PacketLink,
        cpu_burn_per_flush: Duration,
        suppress_wait_after_tcp_payload_flush: bool,
    ) -> Self {
        Self {
            cut: crate::device::cut::CutBoundary::default(),
            rx: crate::device::rx::RxStock::default(),
            egress: crate::device::egress::Egress::default(),
            inbound: Some(inbound),
            outbound: Some(outbound),
            stopping: false,
            shutdown_failure: None,
            shutdown_failure_transferred: false,
            shutdown_callbacks: None,
            ingress_pump: crate::device::pump_native::Startup::Empty,
            cpu_burn_per_flush,
            suppress_wait_after_tcp_payload_flush,
            wait_suppressed: false,
            max_tcp_payload_packets_per_flush: Arc::new(AtomicU64::new(0)),
            mtu: 1500,
            #[cfg(test)]
            receive_watch: None,
            #[cfg(test)]
            partial_flush: None,
        }
    }

    pub(super) fn with_tcp_payload_flush_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.max_tcp_payload_packets_per_flush = counter;
        self
    }

    #[cfg(test)]
    pub(super) fn with_mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu;
        self
    }
    #[cfg(test)]
    pub(crate) fn with_receive_watch(mut self, watch: Arc<Mutex<LoopbackReceiveWatch>>) -> Self {
        self.receive_watch = Some(watch);
        self
    }
    pub(crate) fn with_ingress_pump(mut self, capacity_packets: usize) -> Self {
        if !self.stopping && self.ingress_pump.is_empty() {
            if let Some(inbound) = &self.inbound {
                self.ingress_pump = crate::device::pump_native::Startup::Running(
                    TunIngressPump::spawn_source(inbound.clone(), capacity_packets),
                );
            }
        }
        self
    }
}
impl Drop for LoopbackTunDevice {
    fn drop(&mut self) {
        self.request_stop();
    }
}
impl TunIo for LoopbackTunDevice {
    fn main_capture_meter(&self) -> Option<&bytes::BackingMeter> {
        self.egress.native_meter()
    }

    fn ingress_failure_observer(&self) -> Option<crate::device::DeviceIngressFailure> {
        Some(self.ingress_pump.as_ref()?.ingress_failure_observer())
    }
    fn bind_backing_meter(
        &mut self,
        binding: crate::device::DeviceBinding<'_>,
    ) -> std::io::Result<()> {
        self.bind_native(binding)
    }
    async fn cut_ingress(&mut self) -> std::io::Result<crate::device::DeviceCutReceipt> {
        if self.stopping || self.ingress_pump.pending() {
            return Err(self.unavailable());
        }
        if self
            .inbound
            .as_ref()
            .is_none_or(|link| link.capacity_packets.is_none())
        {
            return Err(std::io::Error::from(std::io::ErrorKind::Unsupported));
        }
        if let Some(pump) = self.ingress_pump.as_ref() {
            pump.check_cut()?;
        }
        self.cut.begin()?;
        let mut discarded = if let Some(pump) = self.ingress_pump.as_mut() {
            pump.cut().await?
        } else {
            (0, 0)
        };
        self.discard_cut_buffers(&mut discarded);
        Ok(self.cut.issue(discarded))
    }
    fn resume_ingress(
        &mut self,
        receipt: &mut crate::device::DeviceCutReceipt,
    ) -> std::io::Result<()> {
        self.cut.validate(receipt)?;
        if self.stopping || self.ingress_pump.pending() {
            return Err(self.unavailable());
        }
        if let Some(pump) = self.ingress_pump.as_ref() {
            pump.validate_resume()?;
        }
        let mut discarded = (receipt.discarded_packets, receipt.discarded_backing);
        self.discard_cut_buffers(&mut discarded);
        receipt.discarded_packets = discarded.0;
        receipt.discarded_backing = discarded.1;
        if let Some(pump) = self.ingress_pump.as_ref() {
            pump.resume();
        }
        self.cut.resumed();
        Ok(())
    }
    fn request_stop(&mut self) {
        self.stopping = true;
        if let Some(pump) = self.ingress_pump.as_ref() {
            pump.request_stop();
        }
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.request_stop();
        let mut result =
            std::task::ready!(self.ingress_pump.poll_shutdown(cx, self.shutdown_failure));
        result.mark_previous_failure_transfer(self.shutdown_failure_transferred);
        self.shutdown_failure_transferred |= result.transfers_failure();
        self.shutdown_failure = result.io_kind();
        self.ingress_pump = crate::device::pump_native::Startup::Empty;
        self.inbound.take();
        self.outbound.take();
        self.rx.discard(&mut (0, 0));
        let rx_stock = self.rx.take_stock();
        drop(std::mem::take(&mut self.rx));
        self.egress.discard(&mut (0, 0));
        if self.shutdown_callbacks.is_none() {
            self.shutdown_callbacks = self.egress.callback_custody().map(|c| c.status());
        }
        let result =
            result.with_callbacks(self.egress.shutdown_tail(rx_stock), self.shutdown_callbacks.clone());
        drop(std::mem::take(&mut self.egress));
        Poll::Ready(result)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return Err(self.unavailable());
        }
        self.rx.admit()?;
        if self.rx.has_current() {
            return Ok(());
        }
        if self.wait_suppressed {
            return std::future::pending::<std::io::Result<()>>().await;
        }
        if let Some(pump) = self.ingress_pump.as_mut() {
            let mut packet = Some(pump.recv().await.ok_or_else(device_closed)??);
            self.rx.install(&mut packet, 0)?;
            return Ok(());
        }
        let inbound = self.inbound.as_ref().ok_or_else(device_closed)?;
        loop {
            if let Some(pkt) = inbound.pop() {
                self.rx.install(&mut Some(pkt.into()), 0)?;
                // D116 b1: a ready pop spends tokio's coop budget like real
                // tokio IO, so a backlog cannot keep this task from yielding.
                // The packet is already installed (owned by `rx`); a Pending
                // here leaves it `has_current`, so the retry returns it first.
                tokio::task::consume_budget().await;
                return Ok(());
            }
            // notify_one 在无 waiter 时保留 1 个 permit，不丢唤醒（pop→None 与 notified 之间的竞态安全）。
            inbound.notify.notified().await;
        }
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return Err(self.unavailable());
        }
        self.rx.admit()?;
        if self.rx.has_current() {
            return Ok(true);
        }
        let mut packet = if let Some(pump) = self.ingress_pump.as_mut() {
            match pump.try_recv() {
                Ok(packet) => Some(packet?),
                Err(mpsc::error::TryRecvError::Empty) => None,
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(self.unavailable()),
            }
        } else {
            self.inbound.as_ref().and_then(PacketLink::pop).map(Into::into)
        };
        if packet.is_some() {
            self.rx.install(&mut packet, 0)?;
        }
        Ok(self.rx.has_current())
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return None;
        }
        self.rx.peek()
    }
    fn rx_take(&mut self) -> Option<BytesMut> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return None;
        }
        self.rx.take()
    }
    fn rx_take_packet(&mut self) -> Option<crate::device::RxPacket> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() { return None; }
        self.rx.take_packet()
    }

    fn can_stage_single_iface_rx(&self) -> bool {
        !self.stopping
            && !self.cut.active()
            && !self.ingress_pump.pending()
            && self.rx.staged_empty()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return false;
        }
        self.rx.stage()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return Err(self.unavailable());
        }
        let outbound = self.outbound.as_ref().ok_or_else(device_closed)?;
        if self.egress.native_meter().is_some() {
            let mut report = packet_link::native_report();
            #[cfg(test)]
            if let Some(probe) = &self.partial_flush {
                report.partial = probe
                    .native_plan(self.egress.packets_for_probe())
                    .map(|(summary, remaining)| (probe.clone(), summary, remaining));
            }
            let result = packet_link::flush(&mut self.egress, outbound, &mut report).await;
            self.max_tcp_payload_packets_per_flush
                .fetch_max(report.tcp_packets, Ordering::Relaxed);
            if self.suppress_wait_after_tcp_payload_flush && report.payload {
                self.wait_suppressed = true;
            }
            return result;
        }
        busy_spin(self.cpu_burn_per_flush); // 刀12：合成 on-loop CPU（默认 ZERO 即 no-op）。
        #[cfg(test)]
        if let Some(probe) = self
            .partial_flush
            .as_ref()
            .filter(|probe| probe.armed.load(Ordering::Acquire))
        {
            let mut first = None;
            let mut has_second = false;
            for packet in &self.egress.queue {
                let Some(summary) = summarize_flushed_ip_tcp_packet(packet)
                    .filter(|summary| summary.payload_bytes > 0)
                else {
                    continue;
                };
                match first {
                    None => first = Some(summary),
                    Some(head)
                        if (
                            summary.src_addr,
                            summary.dst_addr,
                            summary.src_port,
                            summary.dst_port,
                        ) == (head.src_addr, head.dst_addr, head.src_port, head.dst_port) =>
                    {
                        has_second = true;
                        break;
                    }
                    _ => {}
                }
            }
            if has_second {
                let first = first.expect("two original TCP payload packets");
                probe.armed.store(false, Ordering::Release);
                while let Some(packet) = self.egress.queue.pop_front() {
                    let summary = summarize_flushed_ip_tcp_packet(&packet);
                    if !outbound.push(packet) {
                        return Err(std::io::Error::other("partial flush outbound link full"));
                    }
                    if let Some(summary) = summary {
                        self.egress.summaries.push(summary);
                        if summary == first {
                            let remaining = self
                                .egress
                                .queue
                                .iter()
                                .filter_map(|packet| summarize_flushed_ip_tcp_packet(packet))
                                .filter(|packet| packet.payload_bytes > 0)
                                .count();
                            assert!(remaining > 0);
                            *probe.witness.lock().unwrap() = Some((summary, remaining, None));
                            self.max_tcp_payload_packets_per_flush
                                .fetch_max(1, Ordering::Relaxed);
                            return Err(std::io::Error::from_raw_os_error(5));
                        }
                    }
                }
            }
        }
        let mut tcp_payload_packets = 0u64;
        while let Some(pkt) = self.egress.queue.pop_front() {
            if let Some(packet) = summarize_flushed_ip_tcp_packet(&pkt) {
                tcp_payload_packets = tcp_payload_packets.saturating_add(1);
                if self.suppress_wait_after_tcp_payload_flush && packet.payload_bytes > 0 {
                    self.wait_suppressed = true;
                }
                self.egress.summaries.push(packet);
            }
            outbound.push(pkt);
        }
        self.max_tcp_payload_packets_per_flush
            .fetch_max(tcp_payload_packets, Ordering::Relaxed);
        Ok(())
    }
    fn queued_tx_bytes(&self) -> usize {
        self.egress.owned_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        self.egress.summaries()
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        self.ingress_pump
            .as_ref()
            .map(TunIngressPump::snapshot)
            .unwrap_or_default()
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return;
        }
        if self.egress.raw_injection_allowed() {
            self.egress.queue.push_back(BytesMut::from(pkt));
        }
    }
    fn inject_ip_packet_with(
        &mut self,
        max_len: usize,
        encode: impl FnOnce(&mut [u8]) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return Err(self.unavailable());
        }
        self.egress.inject(max_len, 0, encode)
    }
}

impl Device for LoopbackTunDevice {
    type RxToken<'a> = RawRxToken;
    type TxToken<'a> = crate::device::egress::PacketToken<'a>;
    fn capabilities(&self) -> DeviceCapabilities {
        loopback_caps_with_mtu(self.mtu)
    }
    fn receive(&mut self, _t: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return None;
        }
        if self.rx.staged_empty() {
            return None;
        }
        let tx = self.egress.token(self.mtu, 0)?;
        self.rx.take_staged().map(|buffer| {
            #[cfg(test)]
            if let Some(watch) = &self.receive_watch {
                let mut watched = watch
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if watched
                    .replay
                    .as_ref()
                    .is_some_and(|packet| packet == &buffer)
                {
                    watched.replay_count += 1;
                }
                if watched.ack_packet.is_none() {
                    if let Some((origin, max_extra)) = watched.ack_origin {
                        if let Ok(parsed) = etherparse::PacketHeaders::from_ip_slice(&buffer) {
                            if let (
                                Some(etherparse::IpHeader::Version4(ipv4, _)),
                                Some(etherparse::TransportHeader::Tcp(tcp)),
                            ) = (parsed.ip, parsed.transport)
                            {
                                let target = origin
                                    .payload_start_seq
                                    .wrapping_add(origin.payload_bytes as u32);
                                if std::net::Ipv4Addr::from(ipv4.source) == origin.dst_addr
                                    && std::net::Ipv4Addr::from(ipv4.destination) == origin.src_addr
                                    && tcp.source_port == origin.dst_port
                                    && tcp.destination_port == origin.src_port
                                    && tcp.ack
                                    && tcp.acknowledgment_number.wrapping_sub(target) <= max_extra
                                {
                                    watched.ack_packet = Some(BytesMut::from(&buffer[..]));
                                }
                            }
                        }
                    }
                }
            }
            (RawRxToken { buffer }, tx)
        })
    }
    fn transmit(&mut self, _t: SmolInstant) -> Option<Self::TxToken<'_>> {
        if self.stopping || self.cut.active() || self.ingress_pump.pending() {
            return None;
        }
        self.egress.token(self.mtu, 0)
    }
}

#[cfg(test)]
mod tests;
