//! Startup observation borrows the real Device and transfers its original
//! failure into the existing Session before disposing the observer future.
//! It never consumes packets, joins, refunds, or replaces Service stop clocks.
//! The original CallbackWork supplies poll/cancellation/Drop custody.
use super::*;
use crate::device::DeviceIngressFailure;

pub(super) trait BorrowedReport {
    type Stored;
    fn phase() -> FailurePhase;
    fn retain(self, retained: &SessionControl) -> Self::Stored;
}
impl BorrowedReport for Option<crate::device::egress::CallbackReport> {
    type Stored = Self;
    fn phase() -> FailurePhase {
        FailurePhase::DeviceShutdown
    }
    fn retain(self, _: &SessionControl) -> Self {
        self
    }
}
impl BorrowedReport for Option<DeviceIngressFailure> {
    type Stored = bool;
    fn phase() -> FailurePhase {
        FailurePhase::DeviceRead
    }
    fn retain(self, retained: &SessionControl) -> bool {
        retain_claim(self.and_then(|observer| observer.claim()), retained)
    }
}
fn retain_claim(claim: Option<crate::device::egress::CallbackReport>, retained: &SessionControl) -> bool {
    claim.is_some_and(|report| retained.accept_device_report(report))
}
pub(super) fn retain_observer(observer: &DeviceIngressFailure, retained: &SessionControl) -> bool {
    retain_claim(observer.claim(), retained)
}

/// Typed result returned to the sole Service root. A helper must never park
/// unknown custody before that root commits its stop clock and public phase.
pub(in crate::client_tun::compatibility_runtime) struct Observation {
    pub(in crate::client_tun::compatibility_runtime) reported: bool,
    custody: ShutdownCustody,
}
impl Observation {
    pub(in crate::client_tun::compatibility_runtime) fn unknown(&self) -> bool {
        self.custody.failed()
    }
}
pub(in crate::client_tun::compatibility_runtime) async fn wait<D: TunIo>(
    device: &mut D,
    owner: &SessionControl,
) -> Observation {
    let observer = match catch_unwind(AssertUnwindSafe(|| device.ingress_failure_observer())) {
        Ok(observer) => observer,
        Err(payload) => {
            let mut custody = ShutdownCustody::default();
            ShutdownCustody::capture(&mut custody.poll, owner, payload, FailurePhase::DeviceRead);
            return Observation {
                reported: false,
                custody,
            };
        }
    };
    let Some(observer) = observer else {
        return std::future::pending().await;
    };
    let (reported, custody) = CallbackWork::new(observer.wait(), owner)
        .with_ingress(&observer)
        .await;
    Observation {
        reported: reported.unwrap_or(false),
        custody,
    }
}
pub(in crate::client_tun::compatibility_runtime) async fn take<D: TunIo>(
    device: &mut D,
    owner: &SessionControl,
) -> Observation {
    let (reported, custody) = CallbackWork::new(
        std::future::poll_fn(|_| Poll::Ready(device.ingress_failure_observer())),
        owner,
    )
    .await;
    Observation {
        reported: reported.unwrap_or(false),
        custody,
    }
}
