use super::{
    QuinnActorJoinedInventory, QuinnStandbyShutdownOutboundDiscard,
    QuinnStandbyShutdownOutboundObservation, observe_standby_shutdown_outbound,
    standby_terminal_is_failure,
};
use crate::owned_upstream::leg::LegTransportTerminalReason;
use crate::owned_upstream::quinn_leg_actor::QuinnLegActorShutdownWaitDrive;

#[test]
fn pending_or_residual_standby_fifo_is_observed_without_gating_structural_join() {
    assert_eq!(
        observe_standby_shutdown_outbound(QuinnLegActorShutdownWaitDrive::Pending),
        QuinnStandbyShutdownOutboundObservation {
            queued_frames: 0,
            queued_bytes: 0,
            ordered_delivery_wait: true,
        },
    );
    assert_eq!(
        observe_standby_shutdown_outbound(QuinnLegActorShutdownWaitDrive::Residual {
            queued_frames: 3,
            queued_bytes: 71,
            ordered_delivery_wait: true,
        }),
        QuinnStandbyShutdownOutboundObservation {
            queued_frames: 3,
            queued_bytes: 71,
            ordered_delivery_wait: true,
        },
    );
}

#[test]
fn standby_shutdown_discard_keeps_summed_and_stored_bytes_independent() {
    let discarded = QuinnStandbyShutdownOutboundDiscard {
        queued_frames: 3,
        summed_frame_bytes: 71,
        stored_owned_bytes: 72,
        ordered_delivery_wait: true,
        was_active: true,
    };

    assert_eq!(discarded.queued_frames(), 3);
    assert_eq!(discarded.summed_frame_bytes(), 71);
    assert_eq!(discarded.stored_owned_bytes(), 72);
    assert!(discarded.ordered_delivery_wait());
    assert!(discarded.was_active());
}

#[test]
fn standby_shutdown_failure_is_derived_from_authenticated_terminal_reason() {
    assert!(!standby_terminal_is_failure(
        LegTransportTerminalReason::LocalCancelled,
    ));
    assert!(standby_terminal_is_failure(
        LegTransportTerminalReason::PeerClosed,
    ));
    assert!(standby_terminal_is_failure(
        LegTransportTerminalReason::Reset,
    ));
    assert!(standby_terminal_is_failure(
        LegTransportTerminalReason::FatalIo,
    ));
}

#[test]
fn terminal_only_or_fatal_only_joined_inventory_is_not_clean() {
    let terminal_only = QuinnActorJoinedInventory::for_test(0, 0, 1, false);
    assert!(!terminal_only.is_clean());

    let fatal_only = QuinnActorJoinedInventory::for_test(0, 0, 0, true);
    assert!(!fatal_only.is_clean());
}

#[test]
fn shutdown_actor_inventory_checked_sum_exposes_standby_terminal_and_failure() {
    let current = QuinnActorJoinedInventory::for_test(0, 0, 0, false);
    let standby = QuinnActorJoinedInventory::for_test(0, 0, 1, true);

    let aggregate = current
        .checked_add(standby)
        .expect("bounded current plus standby inventory");

    assert_eq!(aggregate.owned_messages(), 0);
    assert_eq!(aggregate.owned_bytes(), 0);
    assert_eq!(aggregate.owned_terminals(), 1);
    assert!(aggregate.terminal_failure());
    assert!(!aggregate.is_clean());
}

#[test]
fn shutdown_actor_inventory_overflow_fails_closed() {
    let current = QuinnActorJoinedInventory::for_test(usize::MAX, 0, 0, false);
    let standby = QuinnActorJoinedInventory::for_test(1, 0, 0, false);

    assert_eq!(current.checked_add(standby), None);

    let current = QuinnActorJoinedInventory::for_test(0, usize::MAX, 0, false);
    let standby = QuinnActorJoinedInventory::for_test(0, 1, 0, false);
    assert_eq!(current.checked_add(standby), None);

    let current = QuinnActorJoinedInventory::for_test(0, 0, usize::MAX, false);
    let standby = QuinnActorJoinedInventory::for_test(0, 0, 1, false);
    assert_eq!(current.checked_add(standby), None);
}
