//! Fault/gate wrapper around the actual memory TunIo, forwarding its packet proof.
use super::*;
use crate::device::{DeviceShutdown, TunFlushedTcpPacket, TunIngressPumpSnapshot};
use smoltcp::phy::{Device, DeviceCapabilities};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
pub(super) struct Gate {
    pub(super) release: AtomicBool,
    pub(super) stops: AtomicUsize,
    pub(super) polls: AtomicUsize,
    pub(super) joins: AtomicUsize,
    pub(super) drops: AtomicUsize,
    pub(super) read_error: AtomicBool,
    pub(super) bind_panic: AtomicBool,
    pub(super) observer_payload: Mutex<Option<Arc<AtomicUsize>>>,
    pub(super) bind_payload: Mutex<Option<Arc<AtomicUsize>>>,
    pub(super) drop_payload: Mutex<Option<Arc<AtomicUsize>>>,
    pub(super) write_error: AtomicBool,
    pub(super) shutdown_panic: AtomicBool,
    pub(super) drop_secondary: AtomicBool,
    pub(super) cut_enabled: AtomicBool,
    pub(super) cuts: AtomicUsize,
    pub(super) resumes: AtomicUsize,
    pub(super) resumed: AtomicUsize,
    pub(super) hold_cut: AtomicBool,
    pub(super) release_cut: AtomicBool,
    pub(super) resume_error: AtomicBool,
    pub(super) resume_panic: AtomicBool,
    pub(super) cut_wake: tokio::sync::Notify,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    pub(super) fn open(&self) {
        self.release.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

pub(super) struct GatedDevice {
    pub(super) inner: LoopbackTunDevice,
    pub(super) gate: Arc<Gate>,
}

impl Device for GatedDevice {
    type RxToken<'a> = <LoopbackTunDevice as Device>::RxToken<'a>;
    type TxToken<'a> = <LoopbackTunDevice as Device>::TxToken<'a>;
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner.receive(now)
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.inner.transmit(now)
    }
}

impl TunIo for GatedDevice {
    fn main_capture_meter(&self) -> Option<&bytes::BackingMeter> {
        self.inner.main_capture_meter()
    }

    fn ingress_failure_observer(&self) -> Option<crate::device::DeviceIngressFailure> {
        let payload = self.gate.observer_payload.lock().unwrap().take();
        if let Some(payload) = payload {
            std::panic::panic_any(payload);
        }
        self.inner.ingress_failure_observer()
    }
    fn bind_backing_meter(
        &mut self,
        binding: crate::device::DeviceBinding<'_>,
    ) -> std::io::Result<()> {
        if self.gate.bind_panic.swap(false, Ordering::SeqCst) {
            let payload = self.gate.bind_payload.lock().unwrap().take();
            if let Some(payload) = payload {
                std::panic::panic_any(payload);
            }
            std::panic::panic_any(self.gate.clone());
        }
        self.inner.bind_backing_meter(binding)
    }

    async fn cut_ingress(&mut self) -> std::io::Result<crate::device::DeviceCutReceipt> {
        if !self.gate.cut_enabled.load(Ordering::Acquire) {
            return Err(std::io::Error::from(std::io::ErrorKind::Unsupported));
        }
        let receipt = self.inner.cut_ingress().await?;
        self.gate.cuts.fetch_add(1, Ordering::Release);
        while self.gate.hold_cut.load(Ordering::Acquire)
            && !self.gate.release_cut.load(Ordering::Acquire)
        {
            self.gate.cut_wake.notified().await;
        }
        Ok(receipt)
    }
    fn resume_ingress(
        &mut self,
        receipt: &mut crate::device::DeviceCutReceipt,
    ) -> std::io::Result<()> {
        self.gate.resumes.fetch_add(1, Ordering::Release);
        if self.gate.resume_panic.swap(false, Ordering::AcqRel) {
            std::panic::panic_any(DeviceDropPayload);
        }
        if self.gate.resume_error.swap(false, Ordering::AcqRel) {
            return Err(std::io::Error::from_raw_os_error(13));
        }
        self.inner.resume_ingress(receipt)?;
        self.gate.resumed.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn request_stop(&mut self) {
        self.gate.stops.fetch_add(1, Ordering::SeqCst);
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.gate.polls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.gate.shutdown_panic.load(Ordering::SeqCst),
            "unknown physical device join"
        );
        if !self.gate.release.load(Ordering::SeqCst) {
            *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let result = std::task::ready!(self.inner.poll_shutdown(cx));
        assert_eq!(self.gate.joins.fetch_add(1, Ordering::SeqCst), 0);
        Poll::Ready(result)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.gate.read_error.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::from_raw_os_error(5));
        }
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
        if self.gate.write_error.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::from_raw_os_error(5));
        }
        self.inner.flush_tx().await
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
    fn inject_ip_packet_with(
        &mut self,
        max_len: usize,
        encode: impl FnOnce(&mut [u8]) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        self.inner.inject_ip_packet_with(max_len, encode)
    }
    fn inject_ip_packet(&mut self, bytes: &[u8]) {
        self.inner.inject_ip_packet(bytes)
    }
}
impl Drop for GatedDevice {
    fn drop(&mut self) {
        self.gate.drops.fetch_add(1, Ordering::SeqCst);
        let payload = self.gate.drop_payload.lock().unwrap().take();
        if let Some(payload) = payload {
            std::panic::panic_any(payload);
        }
        if self.gate.drop_secondary.load(Ordering::SeqCst) {
            std::panic::panic_any(DeviceDropPayload);
        }
    }
}

pub(super) fn device(gate: Arc<Gate>) -> GatedDevice {
    let mut inner = LoopbackTunDevice::new(PacketLink::bounded(64), PacketLink::bounded(64));
    if gate.cut_enabled.load(Ordering::Acquire) {
        inner = inner.with_ingress_pump(4);
    }
    GatedDevice { inner, gate }
}

struct DeviceDropPayload;
impl Drop for DeviceDropPayload {
    fn drop(&mut self) {
        panic!("device panic payload destructor");
    }
}
