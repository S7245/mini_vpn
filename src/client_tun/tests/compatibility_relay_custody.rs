//! Persistent compatibility session custody through the original DATA loop.

use super::*;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

#[path = "compatibility_flow_custody.rs"]
mod flow_custody;
#[path = "compatibility_listener_capacity.rs"]
mod listener_capacity;
#[cfg(feature = "harness")]
#[path = "compatibility_open_custody.rs"]
mod open_custody;
#[cfg(all(test, feature = "harness"))]
pub(super) use open_custody::PressureProbe;

impl SessionControl {
    pub(super) fn note_device_settled(&self) {
        self.device_settlement_observed
            .store(true, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct DeviceProbe {
    dropped: AtomicUsize,
    stops: AtomicUsize,
    joins: AtomicUsize,
}

#[derive(Default)]
struct JoinGate {
    released: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

/// No packet path; optional original pump custody exposes real shutdown error transfer.
struct FaultDevice {
    probe: Arc<DeviceProbe>,
    panic_on_drop: bool,
    read_error: Option<std::io::Error>,
    shutdown_kind: Option<std::io::ErrorKind>,
    callback_panic: Option<FailurePhase>,
    join_gate: Option<Arc<JoinGate>>,
    pump: Option<crate::device::TunIngressPump>,
}

impl FaultDevice {
    fn new(probe: Arc<DeviceProbe>) -> Self {
        Self {
            probe,
            panic_on_drop: false,
            read_error: None,
            shutdown_kind: None,
            callback_panic: None,
            join_gate: None,
            pump: None,
        }
    }
}

struct NoPacket;

impl smoltcp::phy::RxToken for NoPacket {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _: F) -> R {
        unreachable!("control fixture has no packets")
    }
}

impl smoltcp::phy::TxToken for NoPacket {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _: usize, _: F) -> R {
        unreachable!("control fixture has no packets")
    }
}

impl smoltcp::phy::Device for FaultDevice {
    type RxToken<'a> = NoPacket;
    type TxToken<'a> = NoPacket;

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        let mut capabilities = smoltcp::phy::DeviceCapabilities::default();
        capabilities.medium = smoltcp::phy::Medium::Ip;
        capabilities.max_transmission_unit = 1200;
        capabilities
    }

    fn receive(&mut self, _: smoltcp::time::Instant) -> Option<(NoPacket, NoPacket)> {
        None
    }

    fn transmit(&mut self, _: smoltcp::time::Instant) -> Option<NoPacket> {
        None
    }
}

impl TunIo for FaultDevice {
    fn request_stop(&mut self) {
        self.probe.stops.fetch_add(1, Ordering::SeqCst);
        assert_ne!(
            self.callback_panic.as_ref(),
            Some(&FailurePhase::DeviceStop),
            "unknown stop custody"
        );
        if let Some(pump) = &self.pump {
            pump.request_stop();
        }
    }

    fn poll_shutdown(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<crate::device::DeviceShutdown> {
        self.probe.joins.fetch_add(1, Ordering::SeqCst);
        assert_ne!(
            self.callback_panic.as_ref(),
            Some(&FailurePhase::DeviceShutdown),
            "unknown shutdown custody"
        );
        if let Some(gate) = &self.join_gate {
            if !gate.released.load(Ordering::SeqCst) {
                *gate.waker.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }
        if let Some(pump) = &mut self.pump {
            let result = std::task::ready!(pump.poll_shutdown(cx));
            drop(self.pump.take());
            return Poll::Ready(result);
        }
        Poll::Ready(match self.shutdown_kind {
            Some(kind) => crate::device::DeviceShutdown::failed(kind),
            None => crate::device::DeviceShutdown::settled(),
        })
    }

    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if let Some(error) = self.read_error.take() {
            return Err(error);
        }
        std::future::pending().await
    }

    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
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
        Ok(())
    }
    fn inject_ip_packet(&mut self, _: &[u8]) {
        unreachable!("control fixture has no packets")
    }
}

impl Drop for FaultDevice {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("settled diagnostic shell destructor");
        }
    }
}

struct NoFlows;

#[async_trait::async_trait]
impl ProxyUpstream for NoFlows {
    async fn open_tcp(
        &self,
        _: &crate::shared::TargetAddr,
    ) -> Result<crate::upstream::RelayStream, crate::shared::ClientError> {
        panic!("device custody tracer must not open a flow")
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for NoFlows {
    async fn send_udp(&self, _: Vec<u8>) {
        panic!("device custody tracer must not send a datagram")
    }
}

#[derive(Default)]
struct LoopProbe {
    entered: AtomicUsize,
    dropped: AtomicUsize,
    panic_payload_dropped: AtomicUsize,
}

struct ObserveLoop {
    probe: Arc<LoopProbe>,
    panic_on_poll: bool,
    panic_on_drop: bool,
}

struct LoopPanic(Arc<LoopProbe>);

impl Drop for LoopPanic {
    fn drop(&mut self) {
        self.0.panic_payload_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl MetricsSink for ObserveLoop {
    fn enter_poll(&mut self) {
        self.probe.entered.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_poll {
            std::panic::panic_any(LoopPanic(Arc::clone(&self.probe)));
        }
    }
}

impl Drop for ObserveLoop {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            std::panic::panic_any(LoopPanic(Arc::clone(&self.probe)));
        }
    }
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn session_observer_cancellation_retains_original_loop_and_device_join() {
    use crate::harness::{LoopbackTunDevice, PacketLink};

    let inbound = PacketLink::bounded(1);
    let outbound = PacketLink::bounded(1);
    let device = LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(1);
    let (_downlink, rx) = mpsc::channel(1);
    let probe = Arc::new(LoopProbe::default());
    let mut session = prepare_client_tun_session(
        device,
        Arc::new(NoFlows),
        rx,
        TunRuntimeConfig::from_sources(Some("2")).unwrap(),
        Arc::new(Metrics::new()),
        ObserveLoop {
            probe: Arc::clone(&probe),
            panic_on_poll: false,
            panic_on_drop: false,
        },
    );
    {
        let mut observer = Box::pin(session.run());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                assert!(observer.as_mut().poll(cx).is_pending());
                if probe.entered.load(Ordering::SeqCst) > 0 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("original loop must enter smoltcp poll");
    }
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    {
        let mut observer = Box::pin(session.run());
        std::future::poll_fn(|cx| {
            assert!(observer.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    session.request_stop();
    {
        let mut observer = Box::pin(session.observe_shutdown());
        std::future::poll_fn(|cx| {
            assert!(
                observer.as_mut().poll(cx).is_pending(),
                "stop is not a pump join"
            );
            Poll::Ready(())
        })
        .await;
    }
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    let state = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        session.observe_shutdown(),
    )
    .await
    .expect("the original pump must actually join without another packet");
    assert_eq!(state, SessionDeviceState::DeviceSettled);
    assert_eq!(session.observe_shutdown().await, state);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    drop((inbound, outbound));
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn session_inner_poll_panic_retains_device_until_actual_join() {
    use crate::harness::{LoopbackTunDevice, PacketLink};
    use crate::shared::errors::FailureClass;

    let inbound = PacketLink::bounded(1);
    let device =
        LoopbackTunDevice::new(inbound.clone(), PacketLink::bounded(1)).with_ingress_pump(1);
    let (_downlink, rx) = mpsc::channel(1);
    let probe = Arc::new(LoopProbe::default());
    let mut session = prepare_client_tun_session(
        device,
        Arc::new(NoFlows),
        rx,
        TunRuntimeConfig::from_sources(Some("2")).unwrap(),
        Arc::new(Metrics::new()),
        ObserveLoop {
            probe: Arc::clone(&probe),
            panic_on_poll: true,
            panic_on_drop: false,
        },
    );
    {
        let mut observer = Box::pin(session.run());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                assert!(
                    observer.as_mut().poll(cx).is_pending(),
                    "a caught panic is not a device join"
                );
                if probe.entered.load(Ordering::SeqCst) == 1 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("panic must come from the original DATA loop");
    }
    assert_eq!(session.state(), SessionDeviceState::Stopping);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(probe.panic_payload_dropped.load(Ordering::SeqCst), 1);
    {
        let ledger = session.control.ledger();
        let failure = ledger
            .first_failure
            .as_ref()
            .expect("bounded panic retained");
        assert_eq!(failure.facts().class, FailureClass::Panic);
        assert_eq!(failure.facts().origin.phase, FailurePhase::SessionLoop);
        assert!(failure.facts().opaque);
        assert_eq!(ledger.additional_failures, 0);
    }
    let state = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        session.observe_shutdown(),
    )
    .await
    .expect("the original pump must join after inner unwind");
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(session.observe_shutdown().await, state);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(probe.panic_payload_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(session.control.ledger().additional_failures, 0);
    drop(inbound);
}

#[tokio::test(flavor = "current_thread")]
async fn session_settled_device_drop_panic_is_retained_once() {
    let probe = Arc::new(DeviceProbe::default());
    let (_downlink, rx) = mpsc::channel(1);
    let mut device = FaultDevice::new(Arc::clone(&probe));
    device.panic_on_drop = true;
    let mut session = prepare_client_tun_session(
        device,
        Arc::new(NoFlows),
        rx,
        TunRuntimeConfig::from_sources(Some("2")).unwrap(),
        Arc::new(Metrics::new()),
        NoopSink,
    );
    let observed = {
        let mut observer = Box::pin(session.observe_shutdown());
        std::future::poll_fn(|cx| {
            match catch_unwind(AssertUnwindSafe(|| observer.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(state)) => Poll::Ready(Ok(state)),
                Err(payload) => Poll::Ready(Err(payload)),
            }
        })
        .await
    };
    assert_eq!(probe.joins.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    let state = match observed {
        Ok(state) => state,
        Err(payload) => {
            drop(payload);
            panic!("settled device destructor must not unwind the observer");
        }
    };
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(session.observe_shutdown().await, state);
    assert_eq!(session.run().await, state);
    assert_eq!(probe.joins.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    let ledger = session.control.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().origin.phase,
        FailurePhase::DeviceDrop
    );
    assert_eq!(ledger.additional_failures, 0);
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn session_inner_destructor_panic_still_joins_original_device() {
    use crate::harness::{LoopbackTunDevice, PacketLink};
    let inbound = PacketLink::bounded(1);
    let device =
        LoopbackTunDevice::new(inbound.clone(), PacketLink::bounded(1)).with_ingress_pump(1);
    let (_downlink, rx) = mpsc::channel(1);
    let probe = Arc::new(LoopProbe::default());
    let mut session = prepare_client_tun_session(
        device,
        Arc::new(NoFlows),
        rx,
        TunRuntimeConfig::from_sources(Some("2")).unwrap(),
        Arc::new(Metrics::new()),
        ObserveLoop {
            probe: Arc::clone(&probe),
            panic_on_poll: false,
            panic_on_drop: true,
        },
    );
    {
        let mut observer = Box::pin(session.run());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                assert!(observer.as_mut().poll(cx).is_pending());
                if probe.entered.load(Ordering::SeqCst) > 0 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("original DATA loop must own the live sink");
    }
    session.request_stop();
    let state = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        session.observe_shutdown(),
    )
    .await
    .expect("inner destruction cannot destroy the outer device owner");
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(probe.panic_payload_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .control
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .origin
            .phase,
        FailurePhase::SessionLoopDrop
    );
    assert_eq!(session.observe_shutdown().await, state);
    assert_eq!(session.control.ledger().additional_failures, 0);
    drop(inbound);
}

fn fault_session(device: FaultDevice) -> ClientTunSession<impl Future<Output = ()>> {
    let (_downlink, rx) = mpsc::channel(1);
    prepare_client_tun_session(
        device,
        Arc::new(NoFlows),
        rx,
        TunRuntimeConfig::from_sources(Some("2")).unwrap(),
        Arc::new(Metrics::new()),
        NoopSink,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn session_original_ingress_error_preserves_os_facts_through_device_settlement() {
    let probe = Arc::new(DeviceProbe::default());
    let mut device = FaultDevice::new(Arc::clone(&probe));
    device.read_error = Some(std::io::Error::from_raw_os_error(5));
    let mut session = fault_session(device);
    let state = tokio::time::timeout(std::time::Duration::from_secs(1), session.run())
        .await
        .expect("original DATA loop must return its actual ingress error");
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(probe.stops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.joins.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(session.observe_shutdown().await, state);
    let ledger = session.control.ledger();
    let facts = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(facts.origin.phase, FailurePhase::DeviceRead);
    assert_eq!(facts.raw_os_code, Some(5));
    assert_eq!(facts.class, crate::shared::errors::FailureClass::Io);
    assert_eq!(ledger.additional_failures, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn session_kind_only_device_failure_is_bounded_and_sticky() {
    let probe = Arc::new(DeviceProbe::default());
    let mut device = FaultDevice::new(Arc::clone(&probe));
    device.shutdown_kind = Some(std::io::ErrorKind::ConnectionReset);
    let mut session = fault_session(device);
    assert_eq!(
        session.observe_shutdown().await,
        SessionDeviceState::DeviceFailedSettled
    );
    assert_eq!(
        session.observe_shutdown().await,
        SessionDeviceState::DeviceFailedSettled
    );
    assert_eq!(probe.joins.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    let ledger = session.control.ledger();
    let facts = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(facts.origin.phase, FailurePhase::DeviceShutdown);
    assert_eq!(facts.io_kind, Some(std::io::ErrorKind::ConnectionReset));
    assert_eq!(ledger.additional_failures, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn session_unknown_device_callbacks_preserve_custody_in_sticky_quarantine() {
    for stop_panics in [true, false] {
        let probe = Arc::new(DeviceProbe::default());
        let mut device = FaultDevice::new(Arc::clone(&probe));
        device.callback_panic = Some(if stop_panics {
            FailurePhase::DeviceStop
        } else {
            FailurePhase::DeviceShutdown
        });
        let mut session = fault_session(device);
        for _ in 0..3 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::Incomplete
            );
            assert_eq!(session.run().await, SessionDeviceState::Incomplete);
            assert_eq!(probe.stops.load(Ordering::SeqCst), 1);
            assert_eq!(
                probe.joins.load(Ordering::SeqCst),
                usize::from(!stop_panics)
            );
            assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
        }
        {
            let ledger = session.control.ledger();
            assert!(ledger.quarantined);
            assert_eq!(ledger.additional_failures, 0);
            assert_eq!(
                ledger.first_failure.as_ref().unwrap().facts().origin.phase,
                if stop_panics {
                    FailurePhase::DeviceStop
                } else {
                    FailurePhase::DeviceShutdown
                }
            );
        }
        drop(session);
        assert_eq!(
            probe.dropped.load(Ordering::SeqCst),
            1,
            "only explicit owner destruction releases the quarantined shell; never a clean join"
        );
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn session_shutdown_deadline_does_not_reset_or_discard_pending_device() {
    let probe = Arc::new(DeviceProbe::default());
    let gate = Arc::new(JoinGate::default());
    let mut device = FaultDevice::new(Arc::clone(&probe));
    device.join_gate = Some(Arc::clone(&gate));
    let mut session = fault_session(device);
    let driver = std::ptr::from_ref(session.driver.as_ref().get_ref());
    session.request_stop();
    let deadline = session.control.ledger().shutdown_deadline.unwrap();
    assert_eq!(
        deadline,
        tokio::time::Instant::now() + std::time::Duration::from_secs(15)
    );
    for advance in [0, 14] {
        tokio::time::advance(std::time::Duration::from_secs(advance)).await;
        {
            let mut observer = Box::pin(session.observe_shutdown());
            std::future::poll_fn(|cx| {
                assert!(observer.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(session.control.ledger().shutdown_deadline, Some(deadline));
        assert_eq!(
            std::ptr::from_ref(session.driver.as_ref().get_ref()),
            driver
        );
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        session.observe_shutdown().await,
        SessionDeviceState::Incomplete
    );
    assert_eq!(session.control.ledger().shutdown_deadline, Some(deadline));
    assert_eq!(probe.stops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    gate.released.store(true, Ordering::SeqCst);
    gate.waker.lock().unwrap().take().unwrap().wake();
    assert_eq!(session.run().await, SessionDeviceState::DeviceSettled);
    assert_eq!(
        session.observe_shutdown().await,
        SessionDeviceState::DeviceSettled
    );
    assert_eq!(probe.stops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(session.control.ledger().first_failure.is_none());
}

struct FailingPumpSource(Arc<AtomicUsize>);

impl crate::device::IngressPacketSource for FailingPumpSource {
    fn read_packet(
        &mut self,
        _: &mut crate::device::PumpPacket,
    ) -> impl Future<Output = std::io::Result<crate::device::SourceRead>> + Send {
        async { Err(std::io::Error::from_raw_os_error(5)) }
    }
}

impl Drop for FailingPumpSource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("second real pump failure during source destruction");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn session_real_pump_failure_delta_moves_once_and_saturates() {
    for saturated in [false, true] {
        let source_drops = Arc::new(AtomicUsize::new(0));
        let pump = crate::device::TunIngressPump::spawn_source(
            FailingPumpSource(Arc::clone(&source_drops)),
            1,
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pump.snapshot().closed == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual pump must capture its read and source-drop errors");
        assert_eq!(pump.snapshot().read_errors, 1);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        let probe = Arc::new(DeviceProbe::default());
        let mut device = FaultDevice::new(Arc::clone(&probe));
        device.pump = Some(pump);
        let mut session = fault_session(device);
        if saturated {
            // Prior scalar history only; the transferred device result still comes from the real pump.
            let mut ledger = session.control.ledger();
            ledger.first_failure = Some(RetainedFailure::from_io(
                SessionControl::origin(FailurePhase::SessionLoop),
                std::io::Error::from(std::io::ErrorKind::Other),
            ));
            ledger.additional_failures = u64::MAX - 1;
        }
        assert_eq!(
            session.observe_shutdown().await,
            SessionDeviceState::DeviceFailedSettled
        );
        assert_eq!(
            session.control.ledger().additional_failures,
            if saturated { u64::MAX } else { 1 }
        );
        assert_eq!(probe.joins.load(Ordering::SeqCst), 1);
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
        if !saturated {
            let ledger = session.control.ledger();
            let facts = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(facts.origin.resource, FailureResource::TunPump);
            assert_eq!(facts.raw_os_code, Some(5));
        }
        assert_eq!(
            session.observe_shutdown().await,
            SessionDeviceState::DeviceFailedSettled
        );
        assert_eq!(
            session.control.ledger().additional_failures,
            if saturated { u64::MAX } else { 1 }
        );
        assert_eq!(probe.joins.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn session_running_data_poll_does_not_acquire_control_ledger() {
    let probe = Arc::new(DeviceProbe::default());
    let mut session = fault_session(FaultDevice::new(Arc::clone(&probe)));
    let control = Arc::clone(&session.control);
    let timed_out = Arc::new(AtomicBool::new(false));
    let thread_timeout = Arc::clone(&timed_out);
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let guard = control.ledger();
        locked_tx.send(()).unwrap();
        if release_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_err()
        {
            thread_timeout.store(true, Ordering::SeqCst);
        }
        drop(guard);
    });
    locked_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("holder must own ledger before DATA poll");
    let pending = {
        let mut observer = Box::pin(session.run());
        std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await
    };
    let _ = release_tx.send(());
    holder
        .join()
        .expect("ledger holder must be joined even on RED");
    let settled = session.observe_shutdown().await;
    assert_eq!(settled, SessionDeviceState::DeviceSettled);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(pending, "actual DATA loop remains live until explicit stop");
    assert!(
        !timed_out.load(Ordering::SeqCst),
        "Running DATA observer must not acquire control ledger"
    );
}
