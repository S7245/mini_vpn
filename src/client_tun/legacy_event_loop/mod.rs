//! Deep implementation slices for the original event-loop owner.
//!
//! The private driver owns physical sockets, fake-IP, queues and timers; its
//! caller retains the device and session control. Activity/event turns borrow
//! that physical state. Tests remain under client_tun and compatibility_runtime.
//! This boundary exposes no public API and adds no owner, queue or task.

pub(super) mod activity;
mod driver;
pub(super) mod ingress;
mod ingress_effects;
pub(super) mod local_egress;
pub(super) use driver::run_borrowed_event_loop;
pub(super) mod read_credit;
pub(super) mod relay_events;
pub(super) mod remote_payload;

#[cfg(test)]
pub(super) use relay_events::handle_relay_closed;
pub(super) use relay_events::{
    RelayClose, RelayEvent, RelayGapAckDrainFollowup, RelayGapAckDrainState,
    arm_d4_stalled_read_service, clear_d4_stalled_read_service, d4_stalled_read_service_deadline,
    has_d4_stalled_read_service, note_ack_cadence_gap_hint_for_handle,
    remote_payload_left_local_egress_work, should_drain_tun_rx_after_remote_payload,
    tun_rx_drain_budget_before_remote_payload, tun_rx_drain_budget_for_relay_gap_followup,
    tun_rx_drain_budget_for_relay_gap_hint,
};

#[cfg(all(test, feature = "harness"))]
pub(super) use relay_events::{RelayEventTestBridge, RelayEventTestSnapshot};
