//! Owns ordered-write fixtures' cancel/terminal/FIFO/join cleanup.
//! It neither changes production join nor manufactures transport delivery.
//! Parent tests prove Pending or a queued fatal; fixed entries retain the real
//! terminal and stamp while settling that case's exact FIFO inventory once.

use super::super::{
    QuinnLegActorJoined, QuinnLegActorReportedTerminal, QuinnLegActorReportedTerminalPoll,
};
use super::*;
use crate::owned_upstream::leg::ExactLegTerminal;
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;

enum ExpectedTerminal {
    UndeliveredLocalCancel,
    QueuedLocalCancel { bytes: usize },
    QueuedFatal { bytes: usize },
}

pub(super) async fn cancel_and_settle_blocked_write(
    client: &mut StartedQuinnLegActor,
) -> (QuinnLegActorJoined, ExactLegTerminal) {
    settle_terminal_and_join(client, ExpectedTerminal::UndeliveredLocalCancel).await
}

pub(super) async fn settle_queued_fatal(
    client: &mut StartedQuinnLegActor,
    bytes: usize,
) -> (QuinnLegActorJoined, ExactLegTerminal) {
    assert_ne!(bytes, 0, "the queued fatal case retains one encoded frame");
    settle_terminal_and_join(client, ExpectedTerminal::QueuedFatal { bytes }).await
}

pub(super) async fn settle_queued_cancel(
    client: &mut StartedQuinnLegActor,
    bytes: usize,
) -> (QuinnLegActorJoined, ExactLegTerminal) {
    assert_ne!(bytes, 0, "the cancelled queue retains one encoded frame");
    settle_terminal_and_join(client, ExpectedTerminal::QueuedLocalCancel { bytes }).await
}

async fn settle_terminal_and_join(
    client: &mut StartedQuinnLegActor,
    expected: ExpectedTerminal,
) -> (QuinnLegActorJoined, ExactLegTerminal) {
    let (frames, bytes, ordered_wait, reason) = match expected {
        ExpectedTerminal::UndeliveredLocalCancel => {
            (0, 0, true, LegTransportTerminalReason::LocalCancelled)
        }
        ExpectedTerminal::QueuedLocalCancel { bytes } => {
            (1, bytes, false, LegTransportTerminalReason::LocalCancelled)
        }
        ExpectedTerminal::QueuedFatal { bytes } => {
            (1, bytes, false, LegTransportTerminalReason::FatalIo)
        }
    };
    let fatal = reason == LegTransportTerminalReason::FatalIo;
    client.request_cancel();
    let first_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("the actor task joins within the fixture's cancellation bound")
        .expect_err("the exact FIFO residual remains owned after the actor task joins");
    assert!(matches!(
        first_error,
        QuinnLegActorError::ResidualOwnership {
            messages,
            bytes: retained_bytes,
        } if messages == frames + usize::from(ordered_wait) && retained_bytes == bytes
    ));

    let gate = QuinnLegCompositionGate::mint_for_test();
    let observed = client
        .try_transact_reported_terminal_with_parts(&gate, |_runtime, io, queue, reported| {
            if reported.reason() != reason || reported.failure_observed_at().is_some() != fatal {
                return Err((
                    reported,
                    "the actual terminal reason and stamp must match the fixture case",
                ));
            }
            let Some(inventory) = queue.cold_inventory() else {
                return Err((reported, "the exact FIFO inventory must remain consistent"));
            };
            if inventory.queued_frames() != frames
                || inventory.summed_frame_bytes() != bytes
                || inventory.stored_owned_bytes() != bytes
                || inventory.ordered_delivery_wait() != ordered_wait
            {
                return Err((
                    reported,
                    "the exact frame/byte/wait inventory must match before cleanup",
                ));
            }
            let (terminal, stamp) = reported.into_parts();
            let Some(cleanup) = queue.begin_owner_terminal_cleanup(io.established_leg(), &terminal)
            else {
                return Err((
                    QuinnLegActorReportedTerminal::restore(terminal, stamp),
                    "the real terminal must bind the exact actor FIFO",
                ));
            };
            let discarded = cleanup.commit();
            Ok((terminal, stamp, discarded))
        })
        .expect("the joined task retains its terminal and physical parts until settlement");
    let (terminal, handoff_stamp, discarded) = match observed {
        QuinnLegActorReportedTerminalPoll::Applied(settled) => settled,
        QuinnLegActorReportedTerminalPoll::Pending => {
            panic!("the completed actor task must already have reported its terminal")
        }
        QuinnLegActorReportedTerminalPoll::Rejected(error) => {
            panic!("ordered-write terminal cleanup rejected: {error}")
        }
        QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
            panic!("the original terminal must not have been consumed before cleanup")
        }
    };
    assert_eq!(discarded.queued_frames(), frames);
    assert_eq!(discarded.summed_frame_bytes(), bytes);
    assert_eq!(discarded.stored_owned_bytes(), bytes);
    assert_eq!(discarded.ordered_delivery_wait(), ordered_wait);

    let joined = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("the settled ordered-write join is bounded")
        .expect("client root and children join cleanly after exact FIFO settlement");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert_eq!(joined.owned_terminals(), 0);
    assert_eq!(joined.terminal_failure().is_some(), fatal);
    let root_stamp = joined
        .terminal_stamp()
        .expect("the structural join retains its original root stamp");
    assert!(handoff_stamp.same_source(root_stamp));
    assert_eq!(handoff_stamp.observed_at(), root_stamp.observed_at());
    assert_eq!(root_stamp.observed_at().is_some(), fatal);
    (joined, terminal)
}
