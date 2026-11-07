//! Session business borrows the service's original device, namespace and sink.
//! Owns saved DATA and source/Open retirement; never certifies device shutdown.
//! The final physical finisher is shared with the original owning constructor.
//! Q retains original poisoned work. Tests: tuic_service/tests and original custody.
use super::super::legacy_event_loop::namespace::Namespace;
use super::*;
pub(super) mod ingress;

pub(super) fn prepare<'a, D, U, M>(
    device: &'a mut D,
    ingress: Option<SessionIngress<U>>,
    runtime_config: TunRuntimeConfig,
    metrics_handle: Arc<Metrics>,
    metrics: &'a mut M,
    namespace: &'a mut Namespace,
) -> ClientTunSession<impl Future<Output = ()> + 'a>
where
    D: TunIo + 'a,
    U: OwnedUpstream + 'static,
    M: MetricsSink + 'a,
{
    let control = control(&runtime_config, None);
    let retained = control.clone();
    let driver = Box::pin(async move {
        run_loop(
            device,
            ingress,
            runtime_config,
            metrics_handle,
            metrics,
            namespace,
            retained,
        )
        .await
        .settle()
        .await;
    });
    ClientTunSession {
        driver,
        control,
        last_reported: SessionDeviceState::Running,
        handle_issued: false,
    }
}

pub(super) fn control(
    runtime_config: &TunRuntimeConfig,
    app_meter: Option<bytes::BackingMeter>,
) -> Arc<SessionControl> {
    Arc::new({ let mut control = SessionControl::default(); control.app_meter = app_meter; control.business_only = true; control.opens = flow_custody::OpenOwner::new(
            if runtime_config.listener_capacity_profile == ListenerCapacityProfile::F4096 {
                OpenCustody::with_flows()
            } else {
                OpenCustody::default()
            },
        ); control.control_remaining = AtomicUsize::new(32); control })
}

/// Returning the DATA borrow is not settlement. This value retains the actual
/// source, health receipt and original panic custody until its one saved tail ends.
pub(super) struct Retirement {
    retained: Arc<SessionControl>,
    source_retirement: Option<(UdpDownlinkSource, crate::failover::HealthJoinObservation)>,
    inner_poll_custody: Option<PanicCustody>,
    inner_drop_custody: Option<PanicCustody>,
    source_early_custody: Option<PanicCustody>,
}

pub(super) async fn run_loop<D, U, M>(
    device: &mut D,
    ingress: Option<SessionIngress<U>>,
    runtime_config: TunRuntimeConfig,
    metrics_handle: Arc<Metrics>,
    metrics: &mut M,
    namespace: &mut Namespace,
    retained: Arc<SessionControl>,
) -> Retirement
where
    D: TunIo,
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    let mut source_retirement = None;
    let mut inner_poll_custody = None;
    let mut inner_drop_custody = None;
    let mut source_early_custody = None;
    if let Some(SessionIngress {
        upstream,
        mut downlink,
        health_join,
    }) = ingress
    {
        let mut device = egress::SessionEgress::new(device, retained.clone());
        let mut inner = Box::pin(crate::client_tun::legacy_event_loop::run_with_namespace(
            &mut device,
            upstream,
            &mut downlink,
            runtime_config,
            metrics_handle,
            metrics,
            crate::client_tun::legacy_event_loop::namespace::LoopContext {
                namespace,
                control: Some(Arc::clone(&retained)),
            },
        ));
        #[cfg(test)]
        eprintln!("[N1-session-main-frame] bytes={} device={}", std::mem::size_of_val(inner.as_ref().get_ref()), std::any::type_name::<D>());
        let outcome = tokio::select! {
            biased;
            _ = retained.stopped(), if !retained.flow_mode() => None,
            result = std::future::poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
                    Err(payload) => Poll::Ready(Err(payload)),
                }
            }) => Some(result),
        };
        match outcome {
            Some(Ok(Err(error))) => retained.record_device_read(error),
            Some(Err(payload)) => {
                inner_poll_custody =
                    retained.retain_panic(FailurePhase::SessionLoop, payload, false)
            }
            _ => {}
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
            inner_drop_custody =
                retained.retain_panic(FailurePhase::SessionLoopDrop, payload, false);
        }
        if let Some(receipt) = health_join {
            source_retirement = Some((downlink, receipt));
        } else {
            source_early_custody = session_udp::dispose(downlink, &retained);
        }
    }
    Retirement {
        retained,
        source_retirement,
        inner_poll_custody,
        inner_drop_custody,
        source_early_custody,
    }
}

impl Retirement {
    pub(super) async fn settle(self) {
        let Self {
            retained,
            source_retirement,
            inner_poll_custody,
            inner_drop_custody,
            source_early_custody,
        } = self;
        retained.request_stop();
        let (_, source_custody) = tokio::join!(retained.settle_opens(), async {
            if let Some((source, receipt)) = source_retirement {
                session_udp::settle(source, receipt, &retained).await
            } else {
                None
            }
        });
        retained.settle_relay_lane().await;
        let panic_custody = (
            inner_poll_custody,
            inner_drop_custody,
            source_early_custody,
            source_custody,
        );
        if retained.ledger().quarantined {
            std::future::pending::<()>().await;
        }
        drop(panic_custody);
    }
}

pub(super) async fn finish_device<D: TunIo>(
    mut device: D,
    source_retirement: Option<(UdpDownlinkSource, crate::failover::HealthJoinObservation)>,
    retained: Arc<SessionControl>,
) {
    retained.request_stop();
    let shutdown = shutdown_device(&mut device, &retained);
    let (_, device_custody, source_custody) =
        tokio::join!(retained.settle_opens(), shutdown, async {
            if let Some((source, receipt)) = source_retirement {
                session_udp::settle(source, receipt, &retained).await
            } else {
                None
            }
        });
    retained.settle_relay_lane().await;
    let panic_custody = (device_custody, source_custody);
    drop_device(device, &retained).await;
    drop(panic_custody);
}

/// Synchronously stops once before returning the original polling future. The
/// result is custody, not permission to drop a device while business is pending.
#[derive(Default)]
pub(super) struct ShutdownCustody {
    poll: Option<PanicCustody>,
    disposal: Option<PanicCustody>,
}
impl ShutdownCustody {
    fn failed(&self) -> bool {
        self.poll.is_some() || self.disposal.is_some()
    }
    fn capture(
        slot: &mut Option<PanicCustody>,
        retained: &SessionControl,
        payload: Box<dyn std::any::Any + Send>,
        phase: FailurePhase,
    ) {
        // Each original site runs once. Retain the actual Box before facts/Q;
        // the older quarantine helper intentionally disposes its primary Box.
        *slot = Some(PanicCustody {
            _payload: std::mem::ManuallyDrop::new(payload),
        });
        retained.record(RetainedFailure::opaque_panic(SessionControl::origin(phase)));
        let mut ledger = retained.ledger();
        ledger.unknown_panic_payloads = ledger.unknown_panic_payloads.saturating_add(1);
        ledger.quarantined = true;
        if let Some(meter) = &retained.app_meter {
            meter.quarantine();
        }
    }
}

/// The original borrowed tail stays pinned on the shutdown stack. Both normal
/// and cancellation disposal happen here, before the caller can destroy D.
struct CallbackWork<'a, F: Future>
where
    F::Output: ingress::BorrowedReport,
{
    work: std::mem::ManuallyDrop<F>,
    retained: &'a SessionControl,
    custody: ShutdownCustody,
    ingress_observer: Option<&'a crate::device::DeviceIngressFailure>,
    live: bool,
}
impl<'a, F: Future> CallbackWork<'a, F>
where
    F::Output: ingress::BorrowedReport,
{
    fn new(work: F, retained: &'a SessionControl) -> Self {
        Self {
            work: std::mem::ManuallyDrop::new(work),
            retained,
            custody: ShutdownCustody::default(),
            ingress_observer: None,
            live: true,
        }
    }
    fn with_ingress(mut self, observer: &'a crate::device::DeviceIngressFailure) -> Self {
        self.ingress_observer = Some(observer);
        self
    }
    fn retain_ingress(&self) {
        if let Some(observer) = self.ingress_observer {
            ingress::retain_observer(observer, self.retained);
        }
    }
    fn dispose(&mut self) {
        if !std::mem::replace(&mut self.live, false) {
            return;
        }
        // Drop in place, exactly once, including cancellation of this wrapper.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            std::mem::ManuallyDrop::drop(&mut self.work)
        })) {
            self.retain_ingress();
            ShutdownCustody::capture(
                &mut self.custody.disposal,
                self.retained,
                payload,
                <F::Output as ingress::BorrowedReport>::phase(),
            );
        }
    }
}
impl<F: Future> Future for CallbackWork<'_, F>
where
    F::Output: ingress::BorrowedReport,
{
    type Output = (
        Option<<F::Output as ingress::BorrowedReport>::Stored>,
        ShutdownCustody,
    );
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // Structural pin projection: work never moves and Drop disposes in place.
        let this = unsafe { self.get_unchecked_mut() };
        if !this.live {
            return Poll::Pending;
        }
        let report = match catch_unwind(AssertUnwindSafe(|| unsafe {
            Pin::new_unchecked(&mut *this.work).poll(cx)
        })) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(report)) => Some(ingress::BorrowedReport::retain(report, this.retained)),
            Err(payload) => {
                this.retain_ingress();
                ShutdownCustody::capture(
                    &mut this.custody.poll,
                    this.retained,
                    payload,
                    <F::Output as ingress::BorrowedReport>::phase(),
                );
                None
            }
        };
        this.dispose();
        Poll::Ready((report, std::mem::take(&mut this.custody)))
    }
}
impl<F: Future> Drop for CallbackWork<'_, F>
where
    F::Output: ingress::BorrowedReport,
{
    fn drop(&mut self) {
        self.dispose();
    }
}

pub(super) fn shutdown_device<'a, D: TunIo + 'a>(
    device: &'a mut D,
    retained: &'a SessionControl,
) -> impl Future<Output = (Option<PanicCustody>, Option<ShutdownCustody>)> + 'a {
    let mut stop_failed = false;
    let mut stop_custody = None;
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| device.request_stop())) {
        stop_custody = retained.quarantine(FailurePhase::DeviceStop, payload);
        stop_failed = true;
    }
    async move {
        if stop_failed {
            return (stop_custody, None);
        }
        let mut custody = None;
        let shutdown = std::future::poll_fn(|cx| {
            match catch_unwind(AssertUnwindSafe(|| device.poll_shutdown(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(result)) => Poll::Ready(Some(result)),
                Err(payload) => {
                    custody = retained
                        .quarantine(FailurePhase::DeviceShutdown, payload)
                        .map(|poll| ShutdownCustody {
                            poll: Some(poll),
                            disposal: None,
                        });
                    Poll::Ready(None)
                }
            }
        })
        .await;
        if let Some(mut shutdown) = shutdown {
            if let Some(failure) = shutdown.take_failure() {
                retained.record(failure);
            } else if shutdown.io_kind().is_some() && !shutdown.failure_was_transferred() {
                retained.record(RetainedFailure::from_io(
                    SessionControl::origin(FailurePhase::DeviceShutdown),
                    std::io::Error::from(shutdown.io_kind().unwrap_or(std::io::ErrorKind::Other)),
                ));
            }
            {
                let mut ledger = retained.ledger();
                ledger.additional_failures = ledger
                    .additional_failures
                    .saturating_add(shutdown.take_additional_failures());
            }
            let (report, callback_custody) = CallbackWork::new(
                shutdown.settle_callbacks(SessionControl::origin(FailurePhase::DeviceWrite)),
                retained,
            )
            .await;
            if callback_custody.failed() {
                custody = Some(callback_custody);
            }
            if let Some(report) = report.flatten() { retained.accept_device_report(report); }
            #[cfg(test)]
            if custody.is_none()
                && shutdown.state() != crate::device::DeviceShutdownState::PendingCallbacks
            {
                retained.note_device_settled();
            }
        }
        (stop_custody, custody)
    }
}

/// Only the original business/source/open tail and physical shutdown together
/// license this guarded destruction. Unknown custody keeps the actual device.
pub(super) async fn drop_device<D: TunIo>(device: D, retained: &SessionControl) {
    if retained.ledger().quarantined {
        std::future::pending::<()>().await;
    }
    let device_drop_custody = match catch_unwind(AssertUnwindSafe(|| drop(device))) {
        Err(payload) => retained.retain_panic(FailurePhase::DeviceDrop, payload, false),
        Ok(()) => None,
    };
    // Late destructor failure must not fall through to driver_complete.
    if retained.ledger().quarantined {
        std::future::pending::<()>().await;
    }
    retained.retire_device_sources().await;
    if retained.ledger().quarantined { std::future::pending::<()>().await; }
    drop(device_drop_custody);
}

#[cfg(test)]
mod tests;
