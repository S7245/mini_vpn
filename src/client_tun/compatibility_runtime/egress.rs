//! Saved session's device-write failure boundary, including direct DNS flushes.
//! The existing session ledger owns the first raw source and stop authority.
//! This adapter owns no extra task, timer, queue or flow ledger. Successful IO
//! delegates without locks or allocation; actual flow disposal stays in Closing
//! before the physical receipt gate seals uncertain custody. Tests live in
//! client_tun/tests/compatibility_f_flow/egress_failure.rs and local_reset/tests.
use super::{FailurePhase, RetainedFailure, SessionControl, TunIo};
use crate::device::{DeviceShutdown, TunFlushedTcpPacket, TunIngressPumpSnapshot};
use smoltcp::phy::{Device, DeviceCapabilities};
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
struct SharedEgressIo(Arc<io::Error>);

impl std::fmt::Display for SharedEgressIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for SharedEgressIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Existing reset owners retain the identical original source, including OS
/// code and custom payload. Other device callers retain their old error shape.
pub(super) fn original_source(error: io::Error) -> Arc<io::Error> {
    match error
        .get_ref()
        .and_then(|source| source.downcast_ref::<SharedEgressIo>())
    {
        Some(shared) => Arc::clone(&shared.0),
        None => Arc::new(error),
    }
}

pub(super) struct SessionEgress<D> {
    inner: D,
    owner: Arc<SessionControl>,
}

impl<D> SessionEgress<D> {
    pub(super) fn new(inner: D, owner: Arc<SessionControl>) -> Self {
        Self { inner, owner }
    }

    fn failed(&self, error: io::Error) -> io::Error {
        let source = original_source(error);
        let mut ledger = self.owner.ledger();
        ledger.egress_failures = ledger.egress_failures.saturating_add(1);
        if ledger.first_failure.is_none() {
            ledger.first_failure = Some(RetainedFailure::from_io_ref(
                SessionControl::origin(FailurePhase::DeviceWrite),
                &source,
            ));
        } else {
            ledger.additional_failures = ledger.additional_failures.saturating_add(1);
        }
        if ledger.first_egress_io.is_none() {
            ledger.first_egress_io = Some(Arc::clone(&source));
        }
        drop(ledger);
        self.owner.request_stop();
        io::Error::new(source.kind(), SharedEgressIo(source))
    }
}

impl<D: TunIo> Device for SessionEgress<D> {
    type RxToken<'a>
        = D::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = D::TxToken<'a>
    where
        Self: 'a;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner.receive(now)
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.inner.transmit(now)
    }
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
}

impl<D: TunIo> TunIo for SessionEgress<D> {
    fn request_stop(&mut self) {
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.inner.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> io::Result<()> {
        self.inner.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> io::Result<bool> {
        self.inner.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.inner.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.inner.rx_take()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.inner.can_stage_single_iface_rx()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        self.inner.stage_rx_for_iface()
    }
    async fn flush_tx(&mut self) -> io::Result<()> {
        self.inner
            .flush_tx()
            .await
            .map_err(|error| self.failed(error))
    }
    fn queued_tx_bytes(&self) -> usize {
        self.inner.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        self.inner.take_flushed_tcp_packets()
    }
    fn tun_ingress_pump_snapshot(&self) -> TunIngressPumpSnapshot {
        self.inner.tun_ingress_pump_snapshot()
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        self.inner.inject_ip_packet(pkt);
    }
    fn interface_name(&self) -> Option<&str> {
        self.inner.interface_name()
    }
}
