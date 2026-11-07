//! Test-only actual packet observer and single controlled RST-flush failure.
//! Delegates all original TunIo ownership, tokens, packet bytes and summaries.
//! Only the real fixed fixture tuple's emitted RST can arm the one IO failure.
use super::*;
use crate::device::{
    DeviceCutReceipt, DeviceShutdown, TunFlushedTcpPacket, TunIngressPumpSnapshot,
};
use smoltcp::phy::{Device, DeviceCapabilities, TxToken};
use smoltcp::wire::{Ipv4Address, Ipv4Packet, TcpPacket};
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};

#[derive(Default)]
pub(super) struct RstIo {
    pub(super) fail: AtomicBool,
    pending: AtomicBool,
    pub(super) emitted: AtomicUsize,
    pub(super) flushed: AtomicUsize,
    pub(super) failed: AtomicUsize,
    // 0 unobserved, 1 actual first success, 2 controlled first failure; never reset.
    pub(super) first: AtomicUsize,
}
impl RstIo {
    fn observe(&self, bytes: &[u8]) {
        let Ok(ip) = Ipv4Packet::new_checked(bytes) else {
            return;
        };
        if ip.src_addr() != Ipv4Address::new(192, 0, 2, 10)
            || ip.dst_addr() != Ipv4Address::new(10, 0, 0, 1)
            || ip.next_header() != smoltcp::wire::IpProtocol::Tcp
        {
            return;
        }
        let Ok(tcp) = TcpPacket::new_checked(ip.payload()) else {
            return;
        };
        if tcp.src_port() == 10000 && tcp.dst_port() == 20000 && tcp.rst() {
            self.emitted.fetch_add(1, Ordering::AcqRel);
            self.pending.store(true, Ordering::Release);
        }
    }
}
pub(super) struct ObservedTx<T> {
    inner: T,
    io: Arc<RstIo>,
}
impl<T: TxToken> TxToken for ObservedTx<T> {
    fn set_meta(&mut self, meta: smoltcp::phy::PacketMeta) {
        self.inner.set_meta(meta);
    }
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        self.inner.consume(len, |bytes| {
            let value = f(bytes);
            self.io.observe(bytes);
            value
        })
    }
}
pub(super) struct ObservedDevice {
    pub(super) inner: GatedDevice,
    pub(super) io: Arc<RstIo>,
}
impl Device for ObservedDevice {
    type RxToken<'a> = <GatedDevice as Device>::RxToken<'a>;
    type TxToken<'a> = ObservedTx<<GatedDevice as Device>::TxToken<'a>>;
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let io = self.io.clone();
        self.inner
            .receive(now)
            .map(|(rx, tx)| (rx, ObservedTx { inner: tx, io }))
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        let io = self.io.clone();
        self.inner
            .transmit(now)
            .map(|inner| ObservedTx { inner, io })
    }
}
impl TunIo for ObservedDevice {
    async fn cut_ingress(&mut self) -> std::io::Result<DeviceCutReceipt> {
        self.inner.cut_ingress().await
    }
    fn resume_ingress(&mut self, receipt: &mut DeviceCutReceipt) -> std::io::Result<()> {
        self.inner.resume_ingress(receipt)
    }
    fn request_stop(&mut self) {
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.inner.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        self.inner.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        self.inner.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.inner.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.inner.rx_take()
    }
    fn rx_take_packet(&mut self) -> Option<crate::device::RxPacket> {
        self.inner.rx_take_packet()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        self.inner.stage_rx_for_iface()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.inner.can_stage_single_iface_rx()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        let rst = self.io.pending.load(Ordering::Acquire);
        if rst && self.io.fail.swap(false, Ordering::AcqRel) {
            self.io.failed.fetch_add(1, Ordering::AcqRel);
            let _ = self
                .io
                .first
                .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
            return Err(std::io::Error::from_raw_os_error(5));
        }
        self.inner.flush_tx().await?;
        if rst {
            self.io.pending.store(false, Ordering::Release);
            self.io.flushed.fetch_add(1, Ordering::AcqRel);
            let _ = self
                .io
                .first
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
        }
        Ok(())
    }
    fn queued_tx_bytes(&self) -> usize {
        self.inner.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> crate::device::FlushedTcpBatch<'_> {
        self.inner.take_flushed_tcp_packets()
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        self.inner.tun_ingress_pump_snapshot()
    }
    fn inject_ip_packet(&mut self, bytes: &[u8]) {
        self.inner.inject_ip_packet(bytes);
    }
}
