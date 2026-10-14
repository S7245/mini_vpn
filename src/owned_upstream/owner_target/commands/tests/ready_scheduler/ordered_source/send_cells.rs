//! Real Owner two-cell transfer and capacity lifecycle characterizations.
//!
//! Owns private root/source fixtures and exact command/permit assertions.
//! Does not prove persistent production Driver binding or whole-root shutdown.

use super::*;

async fn source_root() -> (
    OwnerSourceFixture,
    SessionCommands,
    SessionOwnerIngress,
    SessionOwnerInbox,
) {
    let mut source = owner_source();
    // Mint the constructor's completion in this same Owner, not an unrelated
    // model. Its second flow is refused; the established source flow survives.
    let effects = source
        .supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg: source.leg,
            frame: Frame::try_new(
                source.leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(2).unwrap(),
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let request = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, .. } => Some(request),
            _ => None,
        })
        .unwrap();
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let mut root = SessionCommands::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    let effects = source
        .supervisor
        .apply_command(inbox.recv_next().await.unwrap())
        .unwrap();
    assert!(effects.iter().any(|effect| matches!(effect,
        SessionEffect::Transmit(frame) if matches!(frame.record(),
            Record::OpenResult { flow_id, result: OpenResultCode::TargetRefused }
                if *flow_id == request.flow_id()))));
    assert!(
        root.cells
            .iter()
            .all(|cell| matches!(cell, SendCell::Vacant))
    );
    (source, root, ingress, inbox)
}

fn returned_ordered(root: &SessionCommands) -> &SessionOwnerCommand {
    let SendCell::Held {
        packet:
            HeldCommand {
                storage: CommandStorage::Returned(command),
                ..
            },
        ..
    } = &root.cells[SendLane::OrderedSource.index()]
    else {
        panic!("the root must retain the actual returned ordered command");
    };
    command
}

fn assert_data(command: &SessionOwnerCommand, flow: LocalFlow, payload: &[u8]) {
    let SessionOwnerCommand::Driver(DriverInput::Data(data)) = command else {
        panic!("the mailbox/root must hold actual Driver DATA");
    };
    assert_eq!(data.flow(), flow);
    assert_eq!(data.payload(), payload);
}

async fn both_full() -> (
    OwnerSourceFixture,
    SessionCommands,
    SessionOwnerIngress,
    SessionOwnerInbox,
) {
    let (mut source, mut root, ingress, inbox) = source_root().await;
    let mut cx = Context::from_waker(Waker::noop());
    for payload in [b"one!", b"two!"] {
        source
            .flow
            .try_send_uplink_with(4, || Bytes::from_static(payload))
            .unwrap();
        assert_eq!(
            root.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
            Poll::Ready(OwnerLaneAdmission::Admitted)
        );
        assert_eq!(
            root.drive_one_unit(),
            if payload == b"one!" {
                OwnerReadyDrive::MailboxOwned
            } else {
                OwnerReadyDrive::Full
            }
        );
    }
    let (_, delivery) = pending_owner_sink(&mut source);
    assert!(delivery.complete_write(4).unwrap().is_none());
    assert_eq!(
        root.poll_driver_lane(&mut source.driver, SendLane::Control, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    assert_eq!(
        root.poll_driver_lane(&mut source.driver, SendLane::Control, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    assert!(root.cells.iter().all(SendCell::is_capacity_blocked));
    (source, root, ingress, inbox)
}

fn returned_addresses(root: &SessionCommands) -> [*const SessionOwnerCommand; 2] {
    root.cells.each_ref().map(|cell| {
        let SendCell::Held {
            packet:
                HeldCommand {
                    storage: CommandStorage::Returned(command),
                    ..
                },
            ..
        } = cell
        else {
            panic!("both cells must retain their actual returned Box");
        };
        command.as_ref() as *const SessionOwnerCommand
    })
}

#[tokio::test]
async fn both_capacity_epochs_commit_both_cells_before_ready_observer_is_dropped() {
    let (source, mut root, _, mut inbox) = both_full().await;
    let original = returned_addresses(&root);
    let control = inbox.recv_next().await.unwrap();
    assert!(matches!(&control,
        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::SinkAccepted {
            offer, bytes: 4,
        })) if offer.flow_id() == source.local_flow.flow_id()));
    let first = inbox.recv_next().await.unwrap();
    assert_data(&first, source.local_flow, b"one!");
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert_eq!(
            wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(OwnerCapacityRearm::Rearmed)
        );
        // Observation is discarded immediately after Ready; no second poll
        // or separate rearm may be needed to commit the other lane.
    }
    assert!(root.cells.iter().all(SendCell::is_ready));
    assert_eq!(returned_addresses(&root), original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(matches!(
        root.cells[SendLane::OrderedSource.index()],
        SendCell::Vacant
    ));
    assert!(root.cells[SendLane::Control.index()].is_ready());
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(
        root.cells
            .iter()
            .all(|cell| matches!(cell, SendCell::Vacant))
    );
    let reset = inbox.recv_next().await.unwrap();
    assert!(matches!(&reset,
        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
            flow, reason: ResetReason::TargetFailure,
        })) if *flow == source.local_flow));
    let second = inbox.recv_next().await.unwrap();
    assert_data(&second, source.local_flow, b"two!");
    assert_eq!(source.factory.session_uplink_owned_bytes(), 8);
    assert_eq!(source.factory.session_uplink_owned_segments(), 2);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
    drop((control, first, reset, second));
}

#[tokio::test]
async fn stolen_both_capacity_returns_exact_commands_and_waits_for_new_progress() {
    let (source, mut root, ingress, mut inbox) = both_full().await;
    let SessionOwnerCommand::Driver(control) = inbox.recv_next().await.unwrap() else {
        panic!("real sink completion");
    };
    let SessionOwnerCommand::Driver(first) = inbox.recv_next().await.unwrap() else {
        panic!("real first DATA");
    };
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    // Reinsert the same concrete commands as a competing producer; capacity
    // progress grants no reservation and must not fabricate a successful send.
    ingress.try_send_driver(control).unwrap();
    ingress.try_send_driver(first).unwrap();
    for _ in 0..2 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    }
    let returned = returned_addresses(&root);
    assert_data(returned_ordered(&root), source.local_flow, b"two!");
    assert!(root.cells.iter().all(SendCell::is_capacity_blocked));
    for _ in 0..3 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    }
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(returned_addresses(&root), returned);
    let control = inbox.recv_next().await.unwrap();
    let first = inbox.recv_next().await.unwrap();
    assert_data(&first, source.local_flow, b"one!");
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    assert_eq!(returned_addresses(&root), returned);
    for _ in 0..2 {
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    }
    let reset = inbox.recv_next().await.unwrap();
    assert!(matches!(&reset,
        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
            flow, reason: ResetReason::TargetFailure,
        })) if *flow == source.local_flow));
    let second = inbox.recv_next().await.unwrap();
    assert_data(&second, source.local_flow, b"two!");
    assert_eq!(source.factory.session_uplink_owned_bytes(), 8);
    assert_eq!(source.factory.session_uplink_owned_segments(), 2);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
    drop((control, first, reset, second));
}

fn admit(root: &mut SessionCommands, source: &mut OwnerSourceFixture, lane: SendLane) {
    assert_eq!(
        root.poll_driver_lane(
            &mut source.driver,
            lane,
            &mut Context::from_waker(Waker::noop())
        ),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
}

async fn mixed_held(blocked: SendLane) -> (OwnerSourceFixture, SessionCommands, SessionOwnerInbox) {
    let (mut source, mut root, _, inbox) = source_root().await;
    if blocked == SendLane::Control {
        let (_, delivery) = pending_owner_sink(&mut source);
        assert!(delivery.complete_write(4).unwrap().is_none());
        admit(&mut root, &mut source, SendLane::Control);
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
        source
            .flow
            .try_send_reset(ResetReason::TargetFailure)
            .unwrap();
        admit(&mut root, &mut source, SendLane::Control);
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
        source
            .flow
            .try_send_uplink_with(4, || Bytes::from_static(b"held"))
            .unwrap();
        admit(&mut root, &mut source, SendLane::OrderedSource);
    } else {
        for payload in [b"fill", b"held"] {
            source
                .flow
                .try_send_uplink_with(4, || Bytes::from_static(payload))
                .unwrap();
            admit(&mut root, &mut source, SendLane::OrderedSource);
            assert_eq!(
                root.drive_one_unit(),
                if payload == b"fill" {
                    OwnerReadyDrive::MailboxOwned
                } else {
                    OwnerReadyDrive::Full
                }
            );
        }
        source
            .flow
            .try_send_reset(ResetReason::TargetFailure)
            .unwrap();
        admit(&mut root, &mut source, SendLane::Control);
    }
    (source, root, inbox)
}

#[tokio::test]
async fn real_close_retains_both_fresh_and_returned_storage_without_retry() {
    for blocked in [SendLane::Control, SendLane::OrderedSource] {
        let (mut source, mut root, mut inbox) = mixed_held(blocked).await;
        let SendCell::Held {
            packet:
                HeldCommand {
                    storage: CommandStorage::Returned(command),
                    ..
                },
            ..
        } = &root.cells[blocked.index()]
        else {
            panic!("the selected lane is Full");
        };
        let returned = command.as_ref() as *const SessionOwnerCommand;
        let bytes = source.factory.session_uplink_owned_bytes();
        inbox.close();
        assert_eq!(root.rearm_full_commands().await, OwnerCapacityRearm::Closed);
        for lane in [SendLane::Control, SendLane::OrderedSource] {
            let SendCell::ClosedReturned(packet) = &root.cells[lane.index()] else {
                panic!("close must retain both known cells, including the never-sent one");
            };
            let command = match &packet.storage {
                CommandStorage::Returned(command) => {
                    assert_eq!(lane, blocked);
                    assert_eq!(command.as_ref() as *const SessionOwnerCommand, returned);
                    command.as_ref()
                }
                CommandStorage::Fresh(command) => {
                    assert_ne!(lane, blocked);
                    command
                }
            };
            if lane == SendLane::OrderedSource {
                assert_data(command, source.local_flow, b"held");
            } else {
                assert!(matches!(command,
                    SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
                        flow, reason: ResetReason::TargetFailure,
                    })) if *flow == source.local_flow));
            }
        }
        for _ in 0..3 {
            assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
            assert_eq!(
                root.rearm_full_commands().await,
                OwnerCapacityRearm::NoBlocked
            );
            for lane in [SendLane::Control, SendLane::OrderedSource] {
                assert_eq!(
                    root.poll_driver_lane(
                        &mut source.driver,
                        lane,
                        &mut Context::from_waker(Waker::noop())
                    ),
                    Poll::Ready(OwnerLaneAdmission::Occupied)
                );
            }
        }
        let filler = inbox.recv_next().await.unwrap();
        assert!(inbox.recv_next().await.is_none());
        assert!(root.custody_is_known_and_valid());
        assert_eq!(source.factory.session_uplink_owned_bytes(), bytes);
        drop(filler);
    }
}

#[tokio::test]
async fn real_send_wake_unwind_quarantines_both_entries_and_preserves_the_other_cell() {
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("actual mailbox enqueue wakes its receiver");
        }
    }
    for failed_lane in [SendLane::Control, SendLane::OrderedSource] {
        for retain_other in [false, true] {
            let (mut source, mut root, _, mut inbox) = source_root().await;
            // Advance the real cursor using real transfers, not private state
            // mutation, so either selected lane can exercise the wake unwind.
            source
                .flow
                .try_send_uplink_with(4, || Bytes::from_static(b"fill"))
                .unwrap();
            admit(&mut root, &mut source, SendLane::OrderedSource);
            assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
            let warmup = inbox.recv_next().await.unwrap();
            if failed_lane == SendLane::OrderedSource {
                let (_, delivery) = pending_owner_sink(&mut source);
                assert!(delivery.complete_write(4).unwrap().is_none());
                admit(&mut root, &mut source, SendLane::Control);
                assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
                assert!(matches!(
                    inbox.recv_next().await.unwrap(),
                    SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::SinkAccepted {
                        bytes: 4,
                        ..
                    }))
                ));
            }
            source
                .flow
                .try_send_uplink_with(4, || Bytes::from_static(b"held"))
                .unwrap();
            source
                .flow
                .try_send_reset(ResetReason::TargetFailure)
                .unwrap();
            admit(&mut root, &mut source, failed_lane);
            let other = if failed_lane == SendLane::Control {
                SendLane::OrderedSource
            } else {
                SendLane::Control
            };
            if retain_other {
                admit(&mut root, &mut source, other);
            }
            let waker = Waker::from(Arc::new(PanicWake));
            {
                let mut receive = pin!(inbox.recv_next());
                assert!(
                    receive
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        root.drive_one_unit()
                    }))
                    .is_err()
                );
            }
            assert!(matches!(root.cells[failed_lane.index()],
                SendCell::TransferringUnknown { origin: CommandOrigin::Driver(lane, flow) } if lane == failed_lane && flow == source.local_flow));
            let received = inbox.recv_next().await.unwrap();
            if failed_lane == SendLane::OrderedSource {
                assert_data(&received, source.local_flow, b"held");
            } else {
                assert!(matches!(&received,
                    SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { flow, .. }))
                        if *flow == source.local_flow));
            }
            for _ in 0..3 {
                for lane in [SendLane::Control, SendLane::OrderedSource] {
                    assert_eq!(
                        root.poll_driver_lane(
                            &mut source.driver,
                            lane,
                            &mut Context::from_waker(Waker::noop())
                        ),
                        Poll::Ready(OwnerLaneAdmission::Quarantined)
                    );
                }
                assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Quarantined);
                assert_eq!(
                    root.rearm_full_commands().await,
                    OwnerCapacityRearm::Quarantined
                );
            }
            inbox.close();
            assert_eq!(
                root.rearm_full_commands().await,
                OwnerCapacityRearm::Quarantined
            );
            assert!(inbox.recv_next().await.is_none());
            if retain_other {
                let SendCell::Held {
                    packet:
                        HeldCommand {
                            storage: CommandStorage::Fresh(command),
                            ..
                        },
                    ..
                } = &root.cells[other.index()]
                else {
                    panic!("the known other command must survive");
                };
                if other == SendLane::OrderedSource {
                    assert_data(command, source.local_flow, b"held");
                } else {
                    assert!(matches!(command,
                        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { flow, .. }))
                            if *flow == source.local_flow));
                }
            } else {
                assert!(matches!(root.cells[other.index()], SendCell::Vacant));
            }
            assert_eq!(source.factory.session_uplink_owned_bytes(), 8);
            assert_eq!(source.factory.session_uplink_owned_segments(), 2);
            drop((received, warmup));
        }
    }
}

#[tokio::test]
async fn corrupt_lane_custody_keeps_both_exact_returns_and_blocks_all_progress() {
    let (mut source, mut root, _, mut inbox) = both_full().await;
    let original = returned_addresses(&root);
    // Deliberate private corruption, not a production transfer API.
    root.cells.swap(0, 1);
    let swapped = [original[1], original[0]];
    assert_eq!(returned_addresses(&root), swapped);
    assert!(!root.custody_is_known_and_valid());
    for _ in 0..3 {
        for lane in [SendLane::Control, SendLane::OrderedSource] {
            assert_eq!(
                root.poll_driver_lane(
                    &mut source.driver,
                    lane,
                    &mut Context::from_waker(Waker::noop())
                ),
                Poll::Ready(OwnerLaneAdmission::Quarantined)
            );
        }
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Quarantined);
        assert_eq!(
            root.rearm_full_commands().await,
            OwnerCapacityRearm::Quarantined
        );
        assert_eq!(
            root.apply_capacity_change(SessionMailboxCapacityChange::Both),
            OwnerCapacityRearm::Quarantined
        );
        assert_eq!(returned_addresses(&root), swapped);
    }
    inbox.close();
    let completion = inbox.recv_next().await.unwrap();
    let first = inbox.recv_next().await.unwrap();
    assert_data(&first, source.local_flow, b"one!");
    assert!(inbox.recv_next().await.is_none());
    assert_eq!(
        root.apply_capacity_change(SessionMailboxCapacityChange::Closed),
        OwnerCapacityRearm::Quarantined
    );
    assert_eq!(returned_addresses(&root), swapped);
    assert_eq!(source.factory.session_uplink_owned_bytes(), 8);
    assert_eq!(source.factory.session_uplink_owned_segments(), 2);
    drop((completion, first));
}

#[tokio::test]
async fn cancelled_root_lane_poll_reenters_and_eof_does_not_settle_returned_data() {
    for lane in [SendLane::Control, SendLane::OrderedSource] {
        let (mut source, mut root, _, mut inbox) = source_root().await;
        {
            let mut poll = pin!(poll_fn(|cx| root.poll_driver_lane(
                &mut source.driver,
                lane,
                cx
            )));
            assert!(
                poll.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert!(
            root.cells
                .iter()
                .all(|cell| matches!(cell, SendCell::Vacant))
        );
        let wakes = Arc::new(SourcePollWake::default());
        let waker = Waker::from(Arc::clone(&wakes));
        {
            let mut poll = pin!(poll_fn(|cx| root.poll_driver_lane(
                &mut source.driver,
                lane,
                cx
            )));
            assert!(
                poll.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            if lane == SendLane::Control {
                source
                    .flow
                    .try_send_reset(ResetReason::TargetFailure)
                    .unwrap();
            } else {
                source
                    .flow
                    .try_send_uplink_with(4, || Bytes::from_static(b"held"))
                    .unwrap();
            }
            assert!(wakes.0.load(Ordering::SeqCst) > 0);
            assert_eq!(
                poll.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Ready(OwnerLaneAdmission::Admitted)
            );
        }
        assert!(matches!(
            root.cells[lane.index()],
            SendCell::Held {
                packet: HeldCommand {
                    storage: CommandStorage::Fresh(_),
                    ..
                },
                ..
            }
        ));
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
        let received = inbox.recv_next().await.unwrap();
        if lane == SendLane::OrderedSource {
            assert_data(&received, source.local_flow, b"held");
        } else {
            assert!(matches!(&received,
                SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { flow, .. }))
                    if *flow == source.local_flow));
        }
        drop(source.flow);
        for drained_lane in [SendLane::Control, SendLane::OrderedSource] {
            assert_eq!(
                root.poll_driver_lane(
                    &mut source.driver,
                    drained_lane,
                    &mut Context::from_waker(Waker::noop())
                ),
                Poll::Ready(OwnerLaneAdmission::Drained)
            );
        }
        assert!(
            root.cells
                .iter()
                .all(|cell| matches!(cell, SendCell::Vacant))
        );
        assert_eq!(
            source.factory.session_uplink_owned_bytes(),
            if lane == SendLane::OrderedSource {
                4
            } else {
                0
            }
        );
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
        inbox.close();
        assert!(inbox.recv_next().await.is_none());
        drop(received);
    }
}

#[tokio::test]
async fn ordered_full_retains_data_and_close_while_control_reset_progresses() {
    let (mut source, mut root, ingress, mut inbox) = source_root().await;
    let probe = source.flow.probe();
    for payload in [b"one!", b"two!"] {
        source
            .flow
            .try_send_uplink_with(4, || Bytes::from_static(payload))
            .unwrap();
    }
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        root.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert_eq!(
        root.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    assert_data(returned_ordered(&root), source.local_flow, b"two!");
    let returned = returned_ordered(&root) as *const SessionOwnerCommand;
    source.flow.try_send_close().unwrap();
    for _ in 0..3 {
        assert_eq!(
            root.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
            Poll::Ready(OwnerLaneAdmission::Occupied)
        );
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    }
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    assert_eq!(
        root.poll_driver_lane(&mut source.driver, SendLane::Control, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    let reset = inbox.recv_next().await.unwrap();
    assert!(matches!(&reset,
        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset {
            flow, reason: ResetReason::TargetFailure,
        })) if *flow == source.local_flow));
    // Retain, do not reduce RESET: it would deliberately abandon source DATA.
    // Control progress (including fixture completion) cannot rearm Ordered.
    assert_eq!(
        root.rearm_full_commands().await,
        OwnerCapacityRearm::UnrelatedLane
    );
    let wakes = Arc::new(SourcePollWake::default());
    let waker = Waker::from(Arc::clone(&wakes));
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    assert!(matches!(
        ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    assert_eq!(
        returned_ordered(&root) as *const SessionOwnerCommand,
        returned
    );
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 2);
    let first;
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        first = inbox.recv_next().await.unwrap();
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert_eq!(
            wait.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(OwnerCapacityRearm::Rearmed)
        );
    }
    assert_data(&first, source.local_flow, b"one!");
    assert_eq!(
        returned_ordered(&root) as *const SessionOwnerCommand,
        returned
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    let second = inbox.recv_next().await.unwrap();
    assert_data(&second, source.local_flow, b"two!");
    assert_eq!(
        root.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    let close = inbox.recv_next().await.unwrap();
    assert!(matches!(&close,
        SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalClose { flow }))
            if *flow == source.local_flow));
    assert!(close.is_ordered_source());
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
    assert_eq!(source.factory.session_uplink_owned_bytes(), 8);
    assert_eq!(source.factory.session_uplink_owned_segments(), 2);
    // Every concrete returned command remains owned here; empty inbox/root
    // positions do not release DATA permits or establish session settlement.
    inbox.close();
    assert!(inbox.recv_next().await.is_none());
    drop((first, second, close, reset));
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 0);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 0);
}
