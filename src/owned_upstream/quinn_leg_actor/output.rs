//! Encoded submission and sole-FIFO flushing for the fixed actor.
//!
//! Owns complete transport/flush operations, not actor/FIFO storage, terminal
//! arbitration or lifecycle settlement. Existing barriers precede every write.
//! Entries: submit_encoded/try_flush/poll_owner_output_flush. The selected port
//! retains the original nursery outcome before observation and registers only
//! the closed Root's Output Waker. Real regressions live in tests/mod.rs and
//! owner_output_ready; the complete path is the physical Bound source tracer.

use super::{
    ActorEncodedTransport, ActorRootOutcome, Arc, CounterField, EncodedLegSubmission,
    EncodedLegTransport, EncodedLegTransportError, LegIoError, ObservedRootOutcome, OutboundRecord,
    QuinnLegActorError, QuinnLegActorFlushDisposition, QuinnLegCompositionGate, SimTime,
    StartedQuinnLegActor, TrackedPermit, WireHeader, mpsc,
};
#[cfg(test)]
use super::{Bytes, OrderedDeliveryToken, WireLane, WireRoute};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll};

impl From<Result<Result<ActorRootOutcome, Arc<QuinnLegActorError>>, tokio::task::JoinError>>
    for ObservedRootOutcome
{
    fn from(
        joined: Result<Result<ActorRootOutcome, Arc<QuinnLegActorError>>, tokio::task::JoinError>,
    ) -> Self {
        match joined {
            Ok(Ok(outcome)) => Self::Completed(outcome),
            Ok(Err(error)) => Self::ActorError(error),
            Err(error) => Self::JoinError(error),
        }
    }
}

impl ObservedRootOutcome {
    pub(super) fn output_error(&self) -> QuinnLegActorError {
        match self {
            Self::Completed(_) => QuinnLegActorError::Closing,
            Self::ActorError(error) => QuinnLegActorError::RootFailed(Arc::clone(error)),
            Self::JoinError(error) => QuinnLegActorError::TaskJoin(error.to_string()),
        }
    }
}

impl EncodedLegTransport for ActorEncodedTransport {
    fn submit_encoded(
        &mut self,
        submission: EncodedLegSubmission,
    ) -> Result<(), EncodedLegTransportError> {
        let (_now, route, lane, bytes, completion) = submission.into_parts();
        if route != self.expected_route {
            return Err(EncodedLegTransportError::ActorClosed);
        }
        let message_permit = Arc::clone(&self.message_capacity)
            .try_acquire_owned()
            .map_err(|_| EncodedLegTransportError::ActorPressure)?;
        let byte_units =
            u32::try_from(bytes.len()).map_err(|_| EncodedLegTransportError::ActorPressure)?;
        let byte_permit = Arc::clone(&self.byte_capacity)
            .try_acquire_many_owned(byte_units)
            .map_err(|_| EncodedLegTransportError::ActorPressure)?;
        let ordinal = self.next_ordinal;
        let next = ordinal
            .checked_add(1)
            .ok_or(EncodedLegTransportError::SubmissionOrdinalExhausted)?;
        let header = WireHeader::data(route, lane, ordinal, bytes.len(), completion.is_some());
        let record = OutboundRecord {
            header,
            bytes,
            completion,
            message_ownership: TrackedPermit::new(
                message_permit,
                Arc::clone(&self.counters),
                CounterField::OutboundMessages,
                1,
            ),
            byte_ownership: TrackedPermit::new(
                byte_permit,
                Arc::clone(&self.counters),
                CounterField::OutboundBytes,
                usize::try_from(byte_units).expect("u32 fits usize"),
            ),
        };
        self.outbound
            .try_send(record)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => EncodedLegTransportError::ActorPressure,
                mpsc::error::TrySendError::Closed(_) => EncodedLegTransportError::ActorClosed,
            })?;
        self.next_ordinal = next;
        Ok(())
    }

    #[cfg(test)]
    fn send_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        _bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError> {
        Err(EncodedLegTransportError::SubmissionCapabilityUnsupported)
    }

    #[cfg(test)]
    fn send_attach_ordered_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        _bytes: Bytes,
        _completion: OrderedDeliveryToken,
    ) -> Result<(), EncodedLegTransportError> {
        Err(EncodedLegTransportError::SubmissionCapabilityUnsupported)
    }
}

impl StartedQuinnLegActor {
    pub(in crate::owned_upstream) fn poll_owner_output_flush(
        &mut self,
        gate: &QuinnLegCompositionGate,
        now: SimTime,
        cx: &mut Context<'_>,
    ) -> Poll<Result<QuinnLegActorFlushDisposition, QuinnLegActorError>> {
        if let Some(error) = self.observe_port_error() {
            return Poll::Ready(Err(error));
        }
        let Some(runtime) = self.actor_incarnation.as_ref() else {
            return Poll::Ready(Err(QuinnLegActorError::MissingPhysicalIncarnation));
        };
        let (Some(io), Some(queue), Some(_)) = (
            self.io.as_ref(),
            self.queue.as_ref(),
            self.transport.as_ref(),
        ) else {
            return Poll::Ready(Err(QuinnLegActorError::AlreadyJoined));
        };
        if self.controller_parts_transferred
            || !io.belongs_to_physical_actor(runtime, gate)
            || !queue.belongs_to_established_leg(io.established_leg())
        {
            return Poll::Ready(Err(QuinnLegActorError::MissingPhysicalIncarnation));
        }
        if let Err(error) = self.readiness.register_output(cx.waker()) {
            return Poll::Ready(Err(error));
        }
        if self.observed_root.is_none() {
            match self.nursery.len() {
                0 => return Poll::Ready(Err(QuinnLegActorError::MissingRoot)),
                1 => {}
                _ => return Poll::Ready(Err(QuinnLegActorError::UnexpectedRootCount)),
            }
            // Only this existing JoinSet primitive is unconstrained, never
            // an actor transaction, flush, business poll or enclosing loop.
            let joined = {
                let mut primitive = pin!(tokio::task::unconstrained(poll_fn(|cx| {
                    self.nursery.poll_join_next(cx)
                })));
                primitive.as_mut().poll(cx)
            };
            match joined {
                Poll::Ready(Some(joined)) => self.observed_root = Some(joined.into()),
                Poll::Ready(None) => return Poll::Ready(Err(QuinnLegActorError::MissingRoot)),
                Poll::Pending => {}
            }
        } else if !self.nursery.is_empty() {
            return Poll::Ready(Err(QuinnLegActorError::UnexpectedRootCount));
        }
        // Retention above precedes first-failure precedence and any external
        // error text. Completed is not Joined and releases no physical owner.
        if let Some(error) = self.observe_port_error() {
            return Poll::Ready(Err(error));
        }
        if let Some(observed) = self.observed_root.as_ref() {
            return Poll::Ready(Err(observed.output_error()));
        }
        match self.try_flush(now) {
            Ok(QuinnLegActorFlushDisposition::OrderedDeliveryPending)
            | Err(QuinnLegActorError::LegIo(LegIoError::Transport(
                EncodedLegTransportError::ActorPressure,
            ))) => Poll::Pending,
            result => Poll::Ready(result),
        }
    }

    pub(crate) fn try_flush(
        &mut self,
        now: SimTime,
    ) -> Result<QuinnLegActorFlushDisposition, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        #[cfg(test)]
        self.port_linearization_gate.pause_if_armed();
        let first_failure = Arc::clone(&self.first_failure);
        let flush_result = {
            let (Some(queue), Some(io), Some(transport)) = (
                self.queue.as_mut(),
                self.io.as_mut(),
                self.transport.as_mut(),
            ) else {
                return Err(QuinnLegActorError::AlreadyJoined);
            };
            first_failure.run_if_clear(|| queue.try_flush(io, transport, now))
        };
        match flush_result {
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
            Ok(Ok(Some(_))) => {
                self.readiness.advance_output()?;
                Ok(QuinnLegActorFlushDisposition::Submitted)
            }
            Ok(Ok(None)) => Ok(QuinnLegActorFlushDisposition::Idle),
            Ok(Err(LegIoError::OrderedDeliveryPending)) => {
                Ok(QuinnLegActorFlushDisposition::OrderedDeliveryPending)
            }
            Ok(Err(error)) => Err(error.into()),
        }
    }
}
