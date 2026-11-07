//! Saved session's device-write failure boundary, including direct DNS flushes.
//! The existing session ledger owns the first raw source and stop authority.
//! One cold Native callback role observes the original absolute stop deadline.
//! No task, queue or flow ledger is added; actual flow disposal stays in Closing
//! before the physical receipt gate seals uncertain custody. Tests live in
//! client_tun/tests/compatibility_f_flow/egress_failure.rs and local_reset/tests.
use super::{FailurePhase, RetainedFailure, SessionControl, TunIo};
pub(super) mod deadline;
use crate::device::{DeviceShutdown, TunFlushedTcpPacket, TunIngressPumpSnapshot};
use smoltcp::phy::{Device, DeviceCapabilities};
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
struct SharedEgressIo(Arc<io::Error>, Option<std::sync::Weak<SessionControl>>);

/// The exact admission rejection was handled by this Session's quiet boundary;
/// no source IO was recorded. Keep that distinction through the loop caller.
#[derive(Debug)]
struct HandledRefusal(Arc<io::Error>, std::sync::Weak<SessionControl>);
impl std::fmt::Display for HandledRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for HandledRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub(super) fn is_handled_refusal(error: &io::Error, owner: &SessionControl) -> bool {
    error
        .get_ref()
        .and_then(|e| e.downcast_ref::<HandledRefusal>())
        .is_some_and(|e| e.1.as_ptr() == std::ptr::from_ref(owner))
}

impl std::fmt::Display for SharedEgressIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
/// Only this private carrier proves its source was already recorded here.
/// Compare the original Weak address without upgrading or releasing an owner.
pub(super) fn is_recorded_error(error: &io::Error, owner: &SessionControl) -> bool {
    error
        .get_ref()
        .and_then(|error| error.downcast_ref::<SharedEgressIo>())
        .and_then(|error| error.1.as_ref())
        .is_some_and(|root| root.as_ptr() == std::ptr::from_ref(owner))
}

impl std::error::Error for SharedEgressIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Existing reset owners retain the identical original source, including OS
/// code and custom payload. Other device callers retain their old error shape.
pub(super) fn original_source(error: io::Error) -> Arc<io::Error> {
    if let Some(refused) = error
        .get_ref()
        .and_then(|e| e.downcast_ref::<HandledRefusal>())
    {
        return refused.0.clone();
    }
    if let Some(source) = crate::device::egress::callback_original_io(&error) {
        return source;
    }
    match error
        .get_ref()
        .and_then(|source| source.downcast_ref::<SharedEgressIo>())
    {
        Some(shared) => Arc::clone(&shared.0),
        None => Arc::new(error),
    }
}

/// Keep the adapter's original error object/identity while reset owners share
/// its source. A raw Legacy error is wrapped only when it needs shared custody.
pub(super) fn retain_reset_error(error: io::Error) -> (Arc<io::Error>, io::Error) {
    if let Some(refused) = error
        .get_ref()
        .and_then(|e| e.downcast_ref::<HandledRefusal>())
    {
        return (refused.0.clone(), error);
    }
    if let Some(shared) = error
        .get_ref()
        .and_then(|e| e.downcast_ref::<SharedEgressIo>())
    {
        return (shared.0.clone(), error);
    }
    let source = original_source(error);
    let retained = io::Error::new(source.kind(), SharedEgressIo(source.clone(), None));
    (source, retained)
}

pub(super) struct SessionEgress<'a, D> {
    inner: &'a mut D,
    owner: Arc<SessionControl>,
    native: Option<deadline::Native>,
    admission: Option<bytes::MeteredCopyError>,
}

impl<'a, D> SessionEgress<'a, D> {
    pub(super) fn new(inner: &'a mut D, owner: Arc<SessionControl>) -> Self {
        let (native, admission) = match deadline::claim(&owner) {
            Ok(native) => (native, None),
            Err(error) => (None, Some(error)),
        };
        Self {
            inner,
            owner,
            native,
            admission,
        }
    }

    fn failed(&self, error: io::Error) -> io::Error {
        failed(&self.owner, error)
    }
}

fn failed(owner: &Arc<SessionControl>, error: io::Error) -> io::Error {
    let callback = crate::device::egress::callback_report(
        &error,
        SessionControl::origin(FailurePhase::DeviceWrite),
    );
    let callback_owned = callback.is_some();
    if let Some(report) = callback {
        owner.accept_device_report(report);
    }
    let source = original_source(error);
    let mut ledger = owner.ledger();
    ledger.egress_failures = ledger.egress_failures.saturating_add(1);
    if !callback_owned {
        if ledger.first_failure.is_none() {
            ledger.first_failure = Some(RetainedFailure::from_io_ref(
                SessionControl::origin(FailurePhase::DeviceWrite),
                &source,
            ));
        } else {
            ledger.additional_failures = ledger.additional_failures.saturating_add(1);
        }
        if ledger.first_egress_io.is_none() {
            ledger.first_egress_io = Some(source.clone());
        }
    }
    drop(ledger);
    owner.request_stop();
    io::Error::new(
        source.kind(),
        SharedEgressIo(source, Some(Arc::downgrade(owner))),
    )
}

impl<D> Drop for SessionEgress<'_, D> {
    fn drop(&mut self) {
        if let Some(native) = self.native.take() {
            deadline::release(&self.owner, native);
        }
    }
}

impl<D: TunIo> Device for SessionEgress<'_, D> {
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

impl<D: TunIo> TunIo for SessionEgress<'_, D> {
    fn main_capture_failure(&mut self, prior: Option<io::Error>) -> io::Error {
        let refused = prior
            .as_ref()
            .and_then(|error| error.get_ref())
            .and_then(|error| {
                error.downcast_ref::<crate::client_tun::downlink_custody::CaptureRefusal>()
            })
            .is_some_and(|error| {
                matches!(
                    error,
                    crate::client_tun::downlink_custody::CaptureRefusal::Admission(
                        bytes::MeteredCopyError::Quarantined
                    )
                )
            });
        let prior = match (refused, prior) {
            (true, Some(error)) => {
                // This owner must publish its own quiet even when another meter
                // first marked the shared budget. Refusal is not a new source IO.
                self.owner.quarantine_quiet();
                return io::Error::other(HandledRefusal(
                    Arc::new(error),
                    Arc::downgrade(&self.owner),
                ));
            }
            (_, prior) => prior,
        };
        if let Some(meter) = &self.owner.app_meter {
            meter.quarantine();
        }
        self.owner.ledger().quarantined = true;
        let reported_here = prior.as_ref().is_some_and(|error| {
            is_recorded_error(error, self.owner.as_ref())
                || is_handled_refusal(error, self.owner.as_ref())
        });
        match prior {
            Some(error) if reported_here => {
                self.owner.request_stop();
                error
            }
            error => self.failed(
                error.unwrap_or_else(|| io::Error::other("Main capture custody is uncertain")),
            ),
        }
    }

    fn main_capture_meter(&self) -> Option<&bytes::BackingMeter> {
        self.owner.app_meter.as_ref()
    }

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
    fn rx_take_packet(&mut self) -> Option<crate::device::RxPacket> {
        self.inner.rx_take_packet()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.inner.can_stage_single_iface_rx()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        self.inner.stage_rx_for_iface()
    }
    async fn flush_tx(&mut self) -> io::Result<()> {
        if let Some(error) = self.admission {
            if error == bytes::MeteredCopyError::Quarantined {
                self.owner.quarantine_quiet();
                return Err(io::Error::other(HandledRefusal(
                    Arc::new(deadline::admission_io(error)),
                    Arc::downgrade(&self.owner),
                )));
            }
            return Err(self.failed(deadline::admission_io(error)));
        }
        match &mut self.native {
            Some(native) => native.flush(&self.owner, self.inner.flush_tx()).await,
            None => self
                .inner
                .flush_tx()
                .await
                .map_err(|error| self.failed(error)),
        }
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
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        self.inner.inject_ip_packet(pkt);
    }
    fn inject_ip_packet_with(
        &mut self,
        max_len: usize,
        encode: impl FnOnce(&mut [u8]) -> io::Result<usize>,
    ) -> io::Result<()> {
        self.inner
            .inject_ip_packet_with(max_len, encode)
            .map_err(|error| match error.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::InvalidInput => error,
                _ => self.failed(error),
            })
    }
    fn interface_name(&self) -> Option<&str> {
        self.inner.interface_name()
    }
}

#[cfg(test)]
mod tests;
