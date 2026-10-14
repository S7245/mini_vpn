//! Existing Control completion and capacity-rearm characterizations.
//!
//! Owns their test-only helpers, not source-driver or production custody.

use super::*;

#[tokio::test]
async fn ready_open_completion_enters_real_mailbox_exactly_once() {
    let (mut model, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut root =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
            .unwrap();

    assert!(matches!(
        root.drive_one_unit(),
        OwnerReadyDrive::MailboxOwned
    ));
    let Some(SessionOwnerCommand::Event(event)) = inbox.recv_next().await else {
        panic!("the real mailbox must own the transferred completion");
    };
    assert_eq!(
        event,
        SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::TargetRefused,
        }
    );
    let effects = model.reduce(event).unwrap();
    assert!(effects.iter().any(|effect| matches!(
        effect,
        SessionEffect::Transmit(frame)
            if matches!(frame.record(), Record::OpenResult { flow_id, result }
                if *flow_id == request.flow_id() && *result == OpenResultCode::TargetRefused)
    )));
    assert!(matches!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable));
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn ready_open_completion_full_retains_exact_command_without_busy_retry() {
    let (_, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let mut root =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
            .unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    let command = full_command(&root);
    let original = command as *const SessionOwnerCommand;
    assert!(matches!(command, SessionOwnerCommand::Event(event)
    if *event == SessionEvent::PeerOpenResolved {
        request,
        result: OpenResultCode::TargetRefused,
    }));
    assert!(matches!(
        inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
            result: OpenResultCode::PolicyDenied,
            ..
        }))
    ));
    // Newly freed capacity does not justify polling the same blocked claim.
    // This test deliberately does not run the explicit rearm transaction.
    for _ in 0..10 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
        let command = full_command(&root);
        assert_eq!(command as *const SessionOwnerCommand, original);
    }
    assert!(root.custody_is_known_and_valid());
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn ready_open_completion_closed_retains_exact_command_without_retry() {
    let (_, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    inbox.close();
    let mut root =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
            .unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Closed);
    let command = closed_command(&root);
    let original = command as *const SessionOwnerCommand;
    assert!(matches!(command, SessionOwnerCommand::Event(event)
    if *event == SessionEvent::PeerOpenResolved {
        request,
        result: OpenResultCode::TargetRefused,
    }));
    for _ in 0..10 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
        let command = closed_command(&root);
        assert_eq!(command as *const SessionOwnerCommand, original);
    }
    assert!(root.custody_is_known_and_valid());
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn ready_open_completion_unknown_send_never_retries_after_real_enqueue() {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Wake, Waker};

    struct PanicOnce(AtomicBool);
    impl Wake for PanicOnce {
        fn wake(self: Arc<Self>) {
            if self.0.swap(false, Ordering::SeqCst) {
                panic!("receiver wake unwinds after the real channel enqueues");
            }
        }
    }
    let (_, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut root =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
            .unwrap();
    let wake = Arc::new(PanicOnce(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&wake));
    {
        let mut receive = std::pin::pin!(inbox.recv_next());
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    wake.0.store(true, Ordering::SeqCst);
    // Exercise the dependency's actual send -> queue push -> receiver wake,
    // not a root-local injection that could run before ownership transfers.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| root.drive_one_unit()));
    assert!(outcome.is_err());
    assert!(!wake.0.load(Ordering::SeqCst));
    assert!(matches!(
        root.cells[SendLane::Control.index()],
        SendCell::TransferringUnknown { origin: CommandOrigin::OpenCompletion(actual) } if actual == request
    ));
    assert!(!root.custody_is_known_and_valid());
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Quarantined
    );
    for _ in 0..10 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Quarantined);
        assert!(matches!(
            root.cells[SendLane::Control.index()],
            SendCell::TransferringUnknown { origin: CommandOrigin::OpenCompletion(actual) } if actual == request
        ));
        assert!(root.quarantined);
    }
    assert!(matches!(inbox.recv_next().await,
    Some(SessionOwnerCommand::Event(event))
        if event == SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::TargetRefused,
        }));
    // Receipt proves physical mailbox custody; it does not retroactively
    // authorize the root to normalize its unknown disposition to clean zero.
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Quarantined);
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn command_lane_mismatch_quarantines_without_discarding_held_completion() {
    let (_, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut root =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
            .unwrap();
    root.cells
        .swap(SendLane::Control.index(), SendLane::OrderedSource.index());
    assert!(!root.custody_is_known_and_valid());
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Quarantined
    );
    for _ in 0..10 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Quarantined);
        assert!(matches!(&root.cells[SendLane::OrderedSource.index()],
        SendCell::Held { packet: HeldCommand {
            storage: CommandStorage::Fresh(SessionOwnerCommand::Event(event)), ..
        }, .. } if *event == SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::TargetRefused,
        }));
    }
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn full_completion_rearms_after_real_control_capacity_and_transfers_once() {
    let (_, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let filler = SessionEvent::PeerOpenResolved {
        request,
        result: OpenResultCode::PolicyDenied,
    };
    ingress.try_send_event(filler.clone()).unwrap();
    let mut root =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
            .unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    let command = full_command(&root);
    let original = command as *const SessionOwnerCommand;
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(event)) if event == filler));
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    let command = full_command(&root);
    assert_eq!(command as *const SessionOwnerCommand, original);
    assert!(root.custody_is_known_and_valid());
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
            request: actual, result: OpenResultCode::TargetRefused,
        })) if actual == request));
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

fn full_completion() -> (SessionCommands, SessionOwnerIngress, SessionOwnerInbox) {
    let (_, request, _) = owner_request();
    let (ingress, inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let mut root = SessionCommands::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    (root, ingress, inbox)
}

fn full_command(root: &SessionCommands) -> &SessionOwnerCommand {
    let SendCell::Held {
        packet:
            HeldCommand {
                storage: CommandStorage::Returned(command),
                ..
            },
        ..
    } = &root.cells[SendLane::Control.index()]
    else {
        panic!("the held Control phase must own its actual Full-return Box");
    };
    command.as_ref()
}

fn closed_command(root: &SessionCommands) -> &SessionOwnerCommand {
    let SendCell::ClosedReturned(HeldCommand {
        storage: CommandStorage::Returned(command),
        ..
    }) = &root.cells[SendLane::Control.index()]
    else {
        panic!("the closed Control phase must retain its actual returned Box");
    };
    command.as_ref()
}

fn full_command_address(root: &SessionCommands) -> *const SessionOwnerCommand {
    full_command(root) as *const SessionOwnerCommand
}

#[tokio::test]
async fn cancelled_capacity_wait_retains_full_command_and_observer_for_reentry() {
    use std::future::Future;
    use std::task::{Context, Waker};

    for release_before_cancel in [false, true] {
        let (mut root, ingress, mut inbox) = full_completion();
        let original = full_command_address(&root);
        {
            let mut wait = std::pin::pin!(root.rearm_full_commands());
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            if release_before_cancel {
                assert!(inbox.recv_next().await.is_some());
            }
            // Cancel before or after the actual Notify wake, without consuming
            // the reported epoch. Neither future owns command or observer.
        }
        assert_eq!(full_command_address(&root), original);
        assert!(matches!(
            ingress.observe_capacity(),
            Err(SessionMailboxObserverBusy)
        ));
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
        if !release_before_cancel {
            assert!(inbox.recv_next().await.is_some());
        }
        assert_eq!(
            root.rearm_full_commands().await,
            OwnerCapacityRearm::Rearmed
        );
        assert_eq!(full_command_address(&root), original);
        assert_eq!(
            root.rearm_full_commands().await,
            OwnerCapacityRearm::NoBlocked
        );
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
        assert!(inbox.recv_next().await.is_some());
        inbox.close();
        assert!(inbox.recv_next().await.is_none());
    }
}

#[tokio::test]
async fn ordered_only_capacity_does_not_rearm_control_but_both_progress_does() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let (_, request, flow) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let filler = SessionEvent::PeerOpenResolved {
        request,
        result: OpenResultCode::PolicyDenied,
    };
    // Reach the real inbox's ordered-source turn without private state access.
    // The root observer is acquired afterward, so these epochs are not new.
    for _ in 0..8 {
        ingress.try_send_event(filler.clone()).unwrap();
        assert!(inbox.recv_next().await.is_some());
    }
    let mut root = SessionCommands::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    ingress.try_send_event(filler.clone()).unwrap();
    let ordered = SessionEvent::LocalClose { flow };
    ingress.try_send_event(ordered.clone()).unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    let original = full_command_address(&root);
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(event)) if event == ordered));
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::UnrelatedLane
    );
    assert_eq!(full_command_address(&root), original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    {
        let mut wait = std::pin::pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    ingress.try_send_event(ordered.clone()).unwrap();
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(event)) if event == filler));
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(event)) if event == ordered));
    // Both epochs advanced since the last report. No second root cell is
    // claimed here; this immutable completion only owns the Control lane.
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    assert_eq!(full_command_address(&root), original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(inbox.recv_next().await.is_some());
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn capacity_stolen_after_rearm_returns_exact_completion_and_blocks_again() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let (mut root, ingress, mut inbox) = full_completion();
    let Some(SessionOwnerCommand::Event(filler)) = inbox.recv_next().await else {
        panic!("the competitor must free the Control slot");
    };
    let SessionEvent::PeerOpenResolved { request, .. } = &filler else {
        panic!("fixture completion");
    };
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    // A notification is not a permit; a concurrent producer can take the slot.
    ingress.try_send_event(filler.clone()).unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    let command = full_command(&root);
    assert!(matches!(command, SessionOwnerCommand::Event(event)
    if *event == SessionEvent::PeerOpenResolved {
        request: *request,
        result: OpenResultCode::TargetRefused,
    }));
    // Do not compare Box addresses across a real send: the mailbox error may
    // rebox the same command. Only the new held/rearm interval keeps its Box.
    let returned = full_command_address(&root);
    for _ in 0..10 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    }
    {
        let mut wait = std::pin::pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(full_command_address(&root), returned);
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(event)) if event == filler));
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    assert_eq!(full_command_address(&root), returned);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(inbox.recv_next().await.is_some());
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn observer_busy_returns_constructor_inputs_without_sending_or_publishing() {
    let (_, request, _) = owner_request();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let observer = ingress.observe_capacity().unwrap();
    let Err(failure) =
        SessionCommands::from_open_completion(ingress, request, OpenResultCode::TargetRefused)
    else {
        panic!("an occupied observation must reject construction");
    };
    assert_eq!(failure.request, request);
    assert_eq!(failure.result, OpenResultCode::TargetRefused);
    assert_eq!(failure.cause, SessionMailboxObserverBusy);
    assert!(matches!(
        failure.ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    drop(observer);
    let mut root =
        SessionCommands::from_open_completion(failure.ingress, failure.request, failure.result)
            .unwrap();
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::NoBlocked
    );
    // Sending succeeds into the original one-slot mailbox: rejection neither
    // enqueued a completion nor retained a second observer. No cloned ingress
    // was supplied to the constructor or retained for rebuilding it.
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(matches!(inbox.recv_next().await,
        Some(SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
            request: actual, result: OpenResultCode::TargetRefused,
        })) if actual == request));
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn full_completion_close_observation_retains_the_same_box_without_retry() {
    let (mut root, _, mut inbox) = full_completion();
    let original = full_command_address(&root);
    inbox.close();
    assert_eq!(root.rearm_full_commands().await, OwnerCapacityRearm::Closed);
    let command = closed_command(&root);
    assert_eq!(command as *const SessionOwnerCommand, original);
    for _ in 0..10 {
        assert_eq!(
            root.rearm_full_commands().await,
            OwnerCapacityRearm::NoBlocked
        );
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    }
    assert!(root.custody_is_known_and_valid());
    // The preexisting mailbox command still needs draining; neither Closed
    // nor its notification is a zero inventory or shutdown-settlement result.
    assert!(inbox.recv_next().await.is_some());
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn capacity_epoch_exhaustion_mapping_quarantines_without_discarding_completion() {
    let (mut root, _, mut inbox) = full_completion();
    let original = full_command_address(&root);
    // Value-mapping test only. The real checked MAX->exhaustion transition is
    // locked in session/tests/progress.rs, whose private epoch is not exposed.
    assert_eq!(
        root.apply_capacity_change(SessionMailboxCapacityChange::EpochExhausted),
        OwnerCapacityRearm::Quarantined
    );
    assert_eq!(full_command_address(&root), original);
    assert!(inbox.recv_next().await.is_some());
    for _ in 0..10 {
        assert_eq!(
            root.rearm_full_commands().await,
            OwnerCapacityRearm::Quarantined
        );
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Quarantined);
        assert_eq!(full_command_address(&root), original);
    }
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn real_capacity_wake_unwind_keeps_root_completion_and_inbox_prefetch() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};

    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("real capacity progress wakes the root observer");
        }
    }
    let (mut root, _, mut inbox) = full_completion();
    let original = full_command_address(&root);
    let waker = Waker::from(Arc::new(PanicWake));
    {
        let mut wait = std::pin::pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut receive = std::pin::pin!(inbox.recv_next());
            receive
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        }));
        assert!(outcome.is_err());
        // Drop only the interrupted observer borrow. The real mailbox dequeue
        // already published its prefetch and epoch before this user wake.
    }
    assert_eq!(full_command_address(&root), original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    assert_eq!(full_command_address(&root), original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    // Prefetch is delivered before another raw dequeue; both concrete commands
    // survive the cross-owner wake unwind, in their original FIFO order.
    for expected in [OpenResultCode::PolicyDenied, OpenResultCode::TargetRefused] {
        assert!(matches!(inbox.recv_next().await,
            Some(SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { result, .. }))
                if result == expected));
    }
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}

#[tokio::test]
async fn registered_capacity_wait_commits_rearm_before_returning_ready() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let (mut root, _, mut inbox) = full_completion();
    let original = full_command_address(&root);
    {
        let mut wait = std::pin::pin!(root.rearm_full_commands());
        let mut context = Context::from_waker(Waker::noop());
        assert!(wait.as_mut().poll(&mut context).is_pending());
        assert!(inbox.recv_next().await.is_some());
        assert_eq!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(OwnerCapacityRearm::Rearmed)
        );
    }
    assert_eq!(full_command_address(&root), original);
    assert!(root.cells[SendLane::Control.index()].is_ready());
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(inbox.recv_next().await.is_some());
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
}
