//! Exclusive cold turn of the original Flow. W/F/escrow and deadlines stay in
//! OpenCustody; this token temporarily owns only its original provider/carrier.
//! Neither absence during preparation nor token loss is a release receipt.
//! Entries begin/prepare/restore; real Service lock tracer lives in tests/cold.
use super::super::super::{supervision, transaction};
use super::*;
use crate::upstream::{TcpOpenAttempt, TcpOpenPhase};
use std::{
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
};

type Panic = Box<dyn std::any::Any + Send>;
pub(super) struct Turn {
    owner: Arc<SessionControl>,
    id: FlowIdentity,
    attempt: ManuallyDrop<Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>>,
    carrier: ManuallyDrop<Option<OpenedOwnedTcp>>,
    panic: ManuallyDrop<Option<Panic>>,
    request: ManuallyDrop<Option<supervision::NativeLaunchRequest>>,
    roles: ManuallyDrop<Option<FlowRoles>>,
    channels: Option<NativeChannels>,
    retained: Option<RetainedCarrier>,
    clock: Option<supervision::FailureClock>,
    armed: bool,
    quiet: bool,
    deadline: Option<tokio::time::Instant>,
}
pub(super) type NativeChannels = (crate::client_tun::uplink::Sender, watch::Sender<RelayReadCredit>);
pub(super) struct Prepared {
    pub(super) channels: Option<NativeChannels>,
}
enum ResultState {
    Ready(Option<bytes::BackingMeter>),
    Waiting,
    Close(&'static str, bytes::MeteredCopyError),
    ProviderClosed,
    Quarantined,
}
impl Turn {
    pub(super) fn begin(
        owner: &Arc<SessionControl>,
        opens: &mut transaction::OpenCustody,
        id: FlowIdentity,
    ) -> Option<Self> {
        let flow = opens.flow_mut(id)?;
        if flow.preparing
            || flow.cold_retained.is_some()
            || flow.panic.is_some()
            || flow.stage != FlowStage::Ready
            || flow.roles.is_some()
            || flow.published
        {
            return None;
        }
        flow.preparing = true;
        Some(Self {
            owner: owner.clone(),
            id,
            attempt: ManuallyDrop::new(flow.attempt.take()),
            carrier: ManuallyDrop::new(flow.carrier.take()),
            panic: ManuallyDrop::new(None),
            request: ManuallyDrop::new(None),
            roles: ManuallyDrop::new(None),
            channels: None,
            retained: None,
            clock: None,
            armed: true,
            quiet: false,
            deadline: None,
        })
    }
    pub(super) fn prepare(
        &mut self,
        dispatch: &OpenDispatch,
        ctx: &mut SocketCtx,
        back: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
        cfg: crate::client_tun::DownlinkBackpressureConfig,
    ) -> PreparedResult {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut result = self.prepare_inner(ctx);
            self.prepare_native(dispatch, back, cfg, &mut result);
            result
        }));
        let state = match result {
            Ok(result) => result,
            Err(payload) => {
                *self.panic = Some(payload);
                // The original Box is retained before quiet; no owner lock is held.
                if let Some(clock) = &self.clock {
                    drop(clock.panic_deferred());
                } else {
                    self.owner.note_flow_unwind(self.id);
                }
                ResultState::Quarantined
            }
        };
        if let ResultState::Close(_, error) = &state {
            // Publish the original typed refusal before any later opaque Drop.
            // The ordinary caller's original clock orders the subsequent panic.
            let original = std::io::Error::from(quinn::owned_events::AdmissionError(*error));
            if let Some(clock) = &self.clock {
                clock.io(&original);
            } else {
                self.owner.record(RetainedFailure::from_io(
                    transaction::open_origin(self.id.index, self.id.epoch, FailurePhase::TcpOpen),
                    original,
                ));
            }
        }
        if matches!(
            state,
            ResultState::Quarantined | ResultState::Close(_, bytes::MeteredCopyError::Quarantined)
        ) {
            self.owner.quarantine_quiet();
        }
        self.deadline = crate::shared::errors::QuietFailureSink::deadline_quiet(&*self.owner);
        PreparedResult(state)
    }
    fn prepare_inner(&mut self, ctx: &mut SocketCtx) -> ResultState {
        #[cfg(test)]
        tests::cold::assert_owner_unlocked(&self.owner);
        self.clock = match FlowRoles::prepare_failure_owner(&self.owner, self.id) {
            Ok(clock) => Some(clock),
            Err(error) => return ResultState::Close("native_failure_clock", error),
        };
        if let Some(meter) = self
            .owner
            .app_meter
            .as_ref()
            .filter(|m| m.budget_limit().is_some())
        {
            if let Err(error) = crate::client_tun::downlink_custody::storage::prepare_for_flow(
                &mut ctx.downlink_pending,
                &mut ctx.downlink_pending_permits,
                &mut ctx.downlink_inflight_permits,
                meter,
                (
                    self.id.index,
                    self.id.handle,
                    self.id.epoch,
                    self.id.generation,
                ),
            ) {
                return ResultState::Close("main_storage", error);
            }
        }
        let Some(attempt) = self.attempt.as_mut() else {
            return ResultState::Quarantined;
        };
        if self.carrier.is_none() {
            *self.carrier = attempt.take_carrier(tokio::time::Instant::now().into_std());
        }
        if self.carrier.is_none() {
            return match attempt.view().phase {
                TcpOpenPhase::Ready => ResultState::Waiting,
                TcpOpenPhase::Retiring | TcpOpenPhase::Settled => ResultState::ProviderClosed,
                _ => ResultState::Quarantined,
            };
        }
        #[cfg(test)]
        tests::cold::decorate(&self.owner, self.id, &mut self.carrier);
        let meter = match self.carrier.as_ref() {
            Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(native))) => {
                native.reader.receive_backing_meter()
            }
            Some(OpenedOwnedTcp::Legacy(
                OpenedTcpRelay::Generic(_) | OpenedTcpRelay::NativeByteOwned(_),
            )) => None,
            _ => return ResultState::Quarantined,
        };
        ResultState::Ready(meter)
    }
    fn prepare_native(
        &mut self,
        dispatch: &OpenDispatch,
        back: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
        cfg: crate::client_tun::DownlinkBackpressureConfig,
        result: &mut ResultState,
    ) {
        let ResultState::Ready(meter) = result else {
            return;
        };
        if !matches!(
            self.carrier.as_ref(),
            Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(_)))
        ) {
            return;
        }
        // Stop observed during the opaque probe keeps the original carrier for
        // the same return gate; it never constructs a package that needs JOIN.
        if self
            .owner
            .stop
            .stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        let Some(clock) = self.clock.as_ref() else {
            *result = ResultState::Quarantined;
            return;
        };
        let (tx, rx, uplink_observer) = if let Some(app) = self.owner.app_meter.as_ref().filter(|m| m.budget_limit().is_some()) {
            match crate::shared::bounded_channel::native_with(
                crate::shared::bounded_channel::NativeCapacity { messages: RELAY_CHANNEL_CAPACITY, waiters: 1 },
                app, clock.clone(), &mut Some(()),
            ) {
                Ok((tx, rx, observer)) => (tx, rx, Some(observer)),
                Err(error) => { *result = ResultState::Close("native_uplink", error); return; }
            }
        } else {
            let (tx, rx) = crate::client_tun::uplink::channel(RELAY_CHANNEL_CAPACITY);
            (tx, rx, None)
        };
        let Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(native))) = self.carrier.take()
        else { return; };
        let (credit_tx, read_credit_rx) = watch::channel(initial_relay_read_credit(false));
        self.channels = Some((tx, credit_tx));
        *self.request = Some(supervision::NativeLaunchRequest {
            handle: self.id.handle,
            epoch: self.id.epoch,
            native,
            engine: dispatch.relay_policy().native_engine(),
            read_floor: dispatch.relay_policy().native_read_floor(),
            rx, uplink_observer,
            back_tx: back.clone(),
            read_credit_rx,
            downlink_backpressure: cfg,
        });
        let Some(request) = self.request.as_ref() else {
            *result = ResultState::Quarantined;
            return;
        };
        // All fallible cold work borrows the original request. It is consumed
        // only by the final, infallible construction after every admission.
        match FlowRoles::prepare_native(request, clock.clone(), meter.clone()) {
            Ok(prepared) => {
                if let Some(request) = self.request.take() {
                    *self.roles = Some(FlowRoles::from_native_preparation(request, prepared));
                    #[cfg(test)]
                    tests::cold::after_roles(&self.owner, self.id);
                } else {
                    *result = ResultState::Quarantined;
                }
            }
            Err(error) => *result = ResultState::Close("native_queue", error),
        }
    }
    pub(super) fn restore(
        &mut self,
        opens: &mut transaction::OpenCustody,
        result: &mut PreparedResult,
        ctx: &SocketCtx,
    ) -> Restore {
        let physical = opens
            .slots
            .get(self.id.index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| slot.matches_physical(self.id));
        let Some(flow) = opens.flow_mut(self.id).filter(|flow| {
            flow.preparing
                && flow.cold_retained.is_none()
                && flow.panic.is_none()
                && flow.attempt.is_none()
                && flow.carrier.is_none()
                && flow.roles.is_none()
                && !flow.published
        }) else {
            // Invalid return cannot transfer originals to another epoch or run Drop.
            return Restore::End;
        };
        // Unknown identity/authority or Q takes priority over ordinary stop.
        // No carrier destructor is allowed until this complete return gate passes.
        let context = ctx.conn_epoch == self.id.epoch
            && ctx.uplink_tx.is_none()
            && ctx.relay_read_credit_tx.is_none()
            && ctx.uplink_buffer.is_empty()
            && ctx.flow_mirror.as_ref().is_some_and(|mirror| {
                mirror.id == self.id
                    && mirror.matches_epoch(ctx.conn_epoch)
                    && !mirror.quarantined
                    && !mirror.event_in_flight
                    && !mirror.rearm_started
                    && mirror.receipt.is_none()
                    && mirror.published_bytes.is_none()
                    && mirror.window.is_none()
                    && mirror.permit_binding_matches(None)
                    && mirror.native_backing_matches(None)
            });
        let parent = self
            .owner
            .ledger()
            .shutdown_deadline
            .into_iter()
            .chain(self.deadline)
            .min();
        if let Some(deadline) = parent {
            let deadline = deadline.into_std();
            flow.stop_deadline.limit(deadline);
        }
        let unknown = !physical
            || !context
            || flow.stage == FlowStage::Quarantined
            || !matches!(flow.stage, FlowStage::Ready | FlowStage::Closing)
            || self.panic.is_some()
            || matches!(
                result.0,
                ResultState::Quarantined
                    | ResultState::Close(_, bytes::MeteredCopyError::Quarantined)
            )
            || flow
                .stop_deadline
                .is_some_and(|d| tokio::time::Instant::now().into_std() >= d)
            || self
                .owner
                .app_meter
                .as_ref()
                .is_some_and(|m| m.is_quarantined());
        if unknown {
            flow.stage = FlowStage::Quarantined;
            self.quiet = true;
        }
        let stopping = self
            .owner
            .stop
            .stopping
            .load(std::sync::atomic::Ordering::Acquire)
            || flow.stop_deadline.is_some()
            || flow.stage == FlowStage::Closing
            || matches!(result.0, ResultState::Close(_, _));
        if !unknown
            && stopping
            && (self.carrier.is_some()
                || self.request.is_some()
                || self.roles.is_some()
                || self.retained.is_some())
        {
            let reason = match &result.0 {
                ResultState::Close(stage, _) => (*stage, "cold_capacity_refused"),
                _ => ("root", "session_stop"),
            };
            if flow.stop_deadline.is_none() {
                flow.close(reason);
            } else if flow.close_reason == ("root", "opening") {
                flow.close_reason = reason;
            }
            // close preserves an already fixed deadline; stage still must stop
            // when the parent supplied that earlier deadline above.
            flow.stage = FlowStage::Closing;
            return Restore::Dispose;
        }
        if let Some(request) = self.request.take() {
            self.retained = Some(RetainedCarrier::from_request(request));
        }
        flow.attempt = self.attempt.take();
        flow.carrier = self.carrier.take();
        flow.roles = self.roles.take();
        flow.cold_retained = self.retained.take();
        if let Some(payload) = self.panic.take() {
            if flow.panic.is_none() {
                flow.panic = Some(payload);
            } else {
                *self.panic = Some(payload);
            }
        }
        self.armed = false;
        flow.preparing = false;
        if !unknown && stopping {
            flow.close(("root", "session_stop"));
            flow.stage = FlowStage::Closing;
        }
        match &mut result.0 {
            ResultState::Ready(_) if flow.stage == FlowStage::Ready => Restore::Ready(Prepared {
                channels: self.channels.take(),
            }),
            ResultState::Close(stage, _) => {
                flow.close((stage, "cold_capacity_refused"));
                Restore::End
            }
            ResultState::ProviderClosed => {
                flow.close(("remote_open", "carrier_failed"));
                Restore::End
            }
            ResultState::Quarantined => {
                flow.stage = FlowStage::Quarantined;
                Restore::End
            }
            _ => Restore::End,
        }
    }
}
pub(super) struct PreparedResult(ResultState);
pub(super) enum Restore {
    Ready(Prepared),
    Dispose,
    End,
}

/// A failed explicit disposal retains each not-yet-disposed original field.
/// No destructor can turn this uncertain physical inventory into a clean zero.
pub(in crate::client_tun::compatibility_runtime::flow_custody) enum RetainedCarrier {
    Native {
        reader: ManuallyDrop<Option<crate::upstream::NativeTcpReadHalf>>,
        writer: ManuallyDrop<Option<crate::upstream::NativeTcpWriteHalf>>,
        pump: ManuallyDrop<Option<crate::upstream::PreparedNativePump>>,
        channels: Option<ColdChannels>,
    },
    Generic(ManuallyDrop<Option<crate::client_tun::RelayStream>>),
    Other(ManuallyDrop<OpenedOwnedTcp>),
}
impl RetainedCarrier {
    fn new(carrier: OpenedOwnedTcp) -> Self {
        match carrier {
            OpenedOwnedTcp::Legacy(
                OpenedTcpRelay::Native(native) | OpenedTcpRelay::NativeByteOwned(native),
            ) => {
                let (reader, writer, pump) = native.into_parts();
                Self::Native {
                    reader: ManuallyDrop::new(Some(reader)),
                    writer: ManuallyDrop::new(Some(writer)),
                    pump: ManuallyDrop::new(pump),
                    channels: None,
                }
            }
            OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(stream)) => {
                Self::Generic(ManuallyDrop::new(Some(stream)))
            }
            other => Self::Other(ManuallyDrop::new(other)),
        }
    }
    fn from_request(request: supervision::NativeLaunchRequest) -> Self {
        let supervision::NativeLaunchRequest {
            native,
            rx, uplink_observer,
            back_tx,
            read_credit_rx,
            ..
        } = request;
        let mut retained = Self::new(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(native)));
        if let Self::Native { channels, .. } = &mut retained {
            *channels = Some(ColdChannels {
                rx: ManuallyDrop::new(Some(rx)),
                observer: uplink_observer,
                back: ManuallyDrop::new(Some(back_tx)),
                credit: ManuallyDrop::new(Some(read_credit_rx)),
            });
        }
        retained
    }
    fn dispose(&mut self) -> Result<bool, Panic> {
        match self {
            Self::Native {
                reader,
                writer,
                pump,
                channels,
            } => {
                // Three independently owned opaque objects. No aggregate Drop
                // runs while an earlier destructor is unwinding.
                if let Some(reader) = reader.take() {
                    catch_unwind(AssertUnwindSafe(|| drop(reader)))?;
                }
                if let Some(writer) = writer.take() {
                    catch_unwind(AssertUnwindSafe(|| drop(writer)))?;
                }
                if let Some(pump) = pump.take() {
                    catch_unwind(AssertUnwindSafe(|| drop(pump)))?;
                }
                if let Some(channels) = channels {
                    if !channels.dispose()? { return Ok(false); }
                }
                Ok(true)
            }
            Self::Generic(stream) => {
                if let Some(stream) = stream.take() {
                    catch_unwind(AssertUnwindSafe(|| drop(stream)))?;
                }
                Ok(true)
            }
            Self::Other(_) => Ok(false),
        }
    }
}
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct ColdChannels {
    observer: Option<crate::client_tun::uplink::Observer>,
    rx: ManuallyDrop<Option<crate::client_tun::uplink::Receiver>>,
    back:
        ManuallyDrop<Option<crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>>>,
    credit: ManuallyDrop<Option<watch::Receiver<RelayReadCredit>>>,
}
impl ColdChannels {
    fn dispose(&mut self) -> Result<bool, Panic> {
        if let Some(rx) = self.rx.take() {
            catch_unwind(AssertUnwindSafe(|| drop(rx)))?;
        }
        if let Some(back) = self.back.take() {
            catch_unwind(AssertUnwindSafe(|| drop(back)))?;
        }
        if let Some(credit) = self.credit.take() {
            catch_unwind(AssertUnwindSafe(|| drop(credit)))?;
        }
        if let Some(observer) = self.observer.take() {
            if let Err(observer) = observer.finish() { self.observer = Some(observer); return Ok(false); }
        }
        Ok(true)
    }
}
impl Turn {
    pub(super) fn dispose_unpublished(&mut self, result: &mut PreparedResult) {
        if let Some(roles) = self.roles.as_mut() {
            let Some(request) = roles.take_unstarted_native() else {
                result.0 = ResultState::Quarantined;
                self.owner.quarantine_quiet();
                return;
            };
            *self.request = Some(request);
            // The bidirectional cold check proved all roles/handles empty and
            // returned the entire original request. Only known cold inventory
            // remains in this object; release it outside opens.
            drop(self.roles.take());
        }
        if let Some(request) = self.request.take() {
            self.retained = Some(RetainedCarrier::from_request(request));
        } else if let Some(carrier) = self.carrier.take() {
            self.retained = Some(RetainedCarrier::new(carrier));
        }
        // Cold endpoints have never been polled. Drop each outside opens before
        // the original receiver and observer verify their physical inventory.
        if let Some((tx, credit)) = self.channels.take() {
            let credit = ManuallyDrop::new(credit);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(tx))) {
                *self.panic = Some(payload); result.0 = ResultState::Quarantined;
                self.owner.quarantine_quiet(); return;
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(credit)))) {
                *self.panic = Some(payload); result.0 = ResultState::Quarantined;
                self.owner.quarantine_quiet(); return;
            }
        }
        match self.retained.as_mut().map(RetainedCarrier::dispose) {
            Some(Ok(true)) => self.retained = None,
            Some(Err(payload)) => {
                *self.panic = Some(payload);
                result.0 = ResultState::Quarantined;
                if let Some(clock) = &self.clock {
                    drop(clock.panic_deferred());
                } else {
                    self.owner.note_flow_unwind(self.id);
                }
            }
            _ => {
                result.0 = ResultState::Quarantined;
                self.owner.quarantine_quiet();
            }
        }
    }
}
impl Drop for Turn {
    fn drop(&mut self) {
        if self.armed {
            self.owner.quarantine_quiet();
            let deadline = crate::shared::errors::QuietFailureSink::deadline_quiet(&*self.owner)
                .into_iter()
                .chain(self.deadline)
                .min();
            let mut opens = self.owner.opens();
            if let Some(flow) = opens.flow_mut(self.id).filter(|flow| {
                flow.preparing
                    && flow.cold_retained.is_none()
                    && flow.panic.is_none()
                    && flow.attempt.is_none()
                    && flow.carrier.is_none()
                    && flow.roles.is_none()
                    && !flow.published
            }) {
                if let Some(request) = self.request.take() {
                    self.retained = Some(RetainedCarrier::from_request(request));
                }
                flow.attempt = self.attempt.take();
                flow.carrier = self.carrier.take();
                flow.roles = self.roles.take();
                if flow.cold_retained.is_none() {
                    flow.cold_retained = self.retained.take();
                }
                if flow.panic.is_none() {
                    flow.panic = self.panic.take();
                }
                if let Some(deadline) = deadline {
                    let deadline = deadline.into_std();
                    flow.stop_deadline.limit(deadline);
                }
                flow.stage = FlowStage::Quarantined;
            }
            drop(opens);
        } else if self.quiet {
            self.owner.quarantine_quiet();
        }
        // Any unmatched originals remain retained; never destruct an uncertain
        // provider/carrier or opaque payload as part of token unwinding.
    }
}
