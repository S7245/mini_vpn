//! Shared bounded-priority receive selection for the TCP ownership port.
//!
//! Owns mixed receive selection and separate Owner lane polling.
//! Driver storage, sealed client provenance, source admission and sink/terminal
//! settlement remain at the parent. No new queue or lifecycle owner is added.
//! An Owner caller exclusively uses lane polling, owns cross-lane fairness,
//! and installs each Ready command in a preflighted vacant cell before further
//! polling, notification or yield. The receive proof requires try-only channel
//! producers; it does not prove producer wake-unwind atomicity.
//! Mixed receive tests remain in tcp/tests/; Owner tests are in
//! quinn_leg_composition/owner/server_root/tests/ready_scheduler/ordered_source.rs.
//! Owner terminal revocation preserves queued commands for separate lane polls;
//! its original-Flow tests are in owner_target/commands/tests/bound/flow_lifecycle.rs.

use super::{CONTROL_BURST_LIMIT, DriverInput, FlowPortError, ResumableTcpDriver};
use std::future::{Future, poll_fn};
use std::task::{Context, Poll};
use tokio::sync::mpsc;

impl ResumableTcpDriver {
    /// Close only an exclusive Owner's producers under its actual terminal.
    /// The Owner keeps this unsplit pair; no async sender/closed waiter or
    /// sink delivery escapes. Counters check, but do not establish, that proof.
    /// Existing RESET/DATA/CLOSE stay queued; neither TUN nor a drain is emitted.
    pub(in crate::owned_upstream) fn revoke_owner_terminal_source(
        &mut self,
        source: &super::ResumableTcpFlow,
        factory: &super::ResumableTcpPortFactory,
        terminal: crate::resumable::TerminalGrace,
    ) -> Result<(), FlowPortError> {
        use super::{Arc, Direction, Ordering};
        if self.flow != terminal.local_flow()
            || source.flow != self.flow
            || self.flow.direction() != Direction::TargetToClient
            || !Arc::ptr_eq(&self.state, &source.state)
            || !Arc::ptr_eq(&self.state.progress, &factory.progress)
            || !matches!((&self.state.origin, &factory.origin),
                (Some(actual), Some(expected)) if actual.matches(expected))
        {
            return Err(FlowPortError::CrossFlowTerminalEffect);
        }
        let state = Arc::clone(&self.state);
        let admission = state
            .admission_gate
            .lock()
            .map_err(|_| FlowPortError::OwnerAdmissionPoisoned)?;
        if state.is_driver_admission_sealed() {
            return Err(FlowPortError::DriverAdmissionSealed);
        }
        if state.is_retired() {
            return Err(FlowPortError::TerminalAlreadyDelivered);
        }
        if state.source_reservation_active.load(Ordering::Acquire) {
            return Err(FlowPortError::UplinkSourceReservationActive);
        }
        let completions = state.reserved_completion_slots.load(Ordering::Acquire);
        let actions = state.pending_downlink_actions.load(Ordering::Acquire);
        if completions != 0 || actions != 0 {
            return Err(FlowPortError::OwnerTerminalOwnershipPending {
                completions,
                actions,
            });
        }
        // No callback-capable sender waits exist in this closed Owner route.
        // General TCP close cannot rely on this premise and stays unchanged.
        state.retired.store(true, Ordering::Release);
        self.reset_rx.close();
        self.control_rx.close();
        self.source_rx.close();
        drop(admission);
        // Root already owns the exact terminal and source if this wake unwinds.
        state.publish_progress();
        Ok(())
    }

    /// Borrow-only preflight for an original Owner's terminal disposition.
    /// A full LocalFlow match alone cannot authorize foreign factory permits.
    /// This neither revokes nor drains a lane and never publishes a TUN action.
    pub(in crate::owned_upstream) fn preflight_owner_terminal_input(
        &self,
        source: &super::ResumableTcpFlow,
        factory: &super::ResumableTcpPortFactory,
        terminal: crate::resumable::TerminalGrace,
        input: &DriverInput,
    ) -> Result<(), FlowPortError> {
        use super::{Arc, Direction, SessionEvent};
        let invalid = FlowPortError::CrossFlowTerminalEffect;
        if self.flow != terminal.local_flow()
            || source.flow != self.flow
            || self.flow.direction() != Direction::TargetToClient
            || !Arc::ptr_eq(&self.state, &source.state)
            || !Arc::ptr_eq(&self.state.progress, &factory.progress)
        {
            return Err(invalid);
        }
        let Some(origin) = factory.origin.as_ref() else {
            return Err(invalid);
        };
        if !self
            .state
            .origin
            .as_ref()
            .is_some_and(|actual| actual.matches(origin))
        {
            return Err(invalid);
        }
        match input {
            DriverInput::Data(data)
                if data.flow == self.flow
                    && data.ownership.flow == self.flow
                    && data.ownership.matches_factory_origin(origin)
                    && data.ownership.permits.is_some()
                    && data.ownership.bytes == data.payload.len()
                    && data.ownership.payload.as_ref() == Some(&data.payload) =>
            {
                Ok(())
            }
            DriverInput::Control(
                SessionEvent::LocalClose { flow } | SessionEvent::LocalReset { flow, .. },
            ) if *flow == self.flow => Ok(()),
            _ => Err(invalid),
        }
    }

    /// Nonblocking weighted-priority receive. A terminal reset always wins.
    /// Ordinary sink completions may run for a bounded burst, after which one
    /// ready ordered source fact must make progress.
    pub fn try_recv_next(&mut self) -> Result<Option<DriverInput>, FlowPortError> {
        match self.reset_rx.try_recv() {
            Ok(reset) => return Ok(Some(DriverInput::Control(reset.into_session_event()))),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
        }
        if self.consecutive_controls < CONTROL_BURST_LIMIT {
            match self.control_rx.try_recv() {
                Ok(control) => {
                    self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                    return Ok(Some(DriverInput::Control(control.into_session_event())));
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                }
            }
        }
        match self.source_rx.try_recv() {
            Ok(source) => {
                self.consecutive_controls = 0;
                return Ok(Some(source.into_driver_input()));
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
        }
        match self.control_rx.try_recv() {
            Ok(control) => {
                self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                Ok(Some(DriverInput::Control(control.into_session_event())))
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                if self.reset_rx.is_closed() && self.source_rx.is_closed() {
                    Err(FlowPortError::Closed)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Awaitable counterpart to [`Self::try_recv_next`]. The biased select
    /// uses the same bounded control burst. One closed lane never terminates
    /// the supervisor while another lane can still produce a reserved fact.
    pub async fn recv_next(&mut self) -> Option<DriverInput> {
        loop {
            match self.try_recv_next() {
                Ok(Some(input)) => return Some(input),
                Ok(None) => {}
                Err(FlowPortError::Closed) => return None,
                Err(_) => return None,
            }

            let reset_open = !self.reset_rx.is_closed();
            let control_open = !self.control_rx.is_closed();
            let source_open = !self.source_rx.is_closed();
            if !reset_open && !control_open && !source_open {
                return None;
            }
            if self.consecutive_controls >= CONTROL_BURST_LIMIT {
                tokio::select! {
                    biased;
                    reset = self.reset_rx.recv(), if reset_open => if let Some(reset) = reset {
                        return Some(DriverInput::Control(reset.into_session_event()));
                    },
                    source = self.source_rx.recv(), if source_open => if let Some(source) = source {
                        self.consecutive_controls = 0;
                        return Some(source.into_driver_input());
                    },
                    control = self.control_rx.recv(), if control_open => if let Some(control) = control {
                        self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                        return Some(DriverInput::Control(control.into_session_event()));
                    },
                    else => return None,
                }
            } else {
                tokio::select! {
                    biased;
                    reset = self.reset_rx.recv(), if reset_open => if let Some(reset) = reset {
                        return Some(DriverInput::Control(reset.into_session_event()));
                    },
                    control = self.control_rx.recv(), if control_open => if let Some(control) = control {
                        self.consecutive_controls = self.consecutive_controls.saturating_add(1);
                        return Some(DriverInput::Control(control.into_session_event()));
                    },
                    source = self.source_rx.recv(), if source_open => if let Some(source) = source {
                        self.consecutive_controls = 0;
                        return Some(source.into_driver_input());
                    },
                    else => return None,
                }
            }
        }
    }

    /// Polls RESET once, then ordinary Control once if no RESET was obtained.
    /// A Pending RESET lane cannot hide an already-ready sink completion.
    /// None proves both physical Control queues have drained, not session zero.
    pub(in crate::owned_upstream) fn poll_owner_control(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DriverInput>> {
        let reset = {
            let mut receive = std::pin::pin!(tokio::task::unconstrained(poll_fn(|cx| {
                self.reset_rx.poll_recv(cx)
            })));
            receive.as_mut().poll(cx)
        };
        let reset_drained = match reset {
            Poll::Ready(Some(reset)) => {
                return Poll::Ready(Some(DriverInput::Control(reset.into_session_event())));
            }
            Poll::Ready(None) => true,
            Poll::Pending => false,
        };
        let control = {
            let mut receive = std::pin::pin!(tokio::task::unconstrained(poll_fn(|cx| {
                self.control_rx.poll_recv(cx)
            })));
            receive.as_mut().poll(cx)
        };
        match control {
            Poll::Ready(Some(control)) => {
                Poll::Ready(Some(DriverInput::Control(control.into_session_event())))
            }
            Poll::Ready(None) if reset_drained => Poll::Ready(None),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }

    /// Polls only the DATA/CLOSE FIFO once, preserving its actual ownership.
    /// The caller must not mix this route with mixed receive on this driver.
    pub(in crate::owned_upstream) fn poll_owner_ordered_source(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DriverInput>> {
        let source = {
            let mut receive = std::pin::pin!(tokio::task::unconstrained(poll_fn(|cx| {
                self.source_rx.poll_recv(cx)
            })));
            receive.as_mut().poll(cx)
        };
        match source {
            Poll::Ready(Some(source)) => Poll::Ready(Some(source.into_driver_input())),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
