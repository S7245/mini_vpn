use super::*;
use crate::owned_upstream::owner_target::OwnerTargetOutput;
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachProof,
    AttachRequest, AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction,
    FeatureOffer, FeatureSet, Frame, LegGeneration, OpenResultCode, OwnerIdentity,
    ReceiveBudgetLimits, Record, ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION,
    SessionConfig, SessionEffect, SessionEvent, SessionFlowId, SessionId, SessionModel,
    SessionRole, TcpWindowLimits, TerminalGrace, TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;
use bytes::Bytes;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

fn data_frame(generation: u64, payload: Bytes) -> Frame {
    Frame::try_new(
        LegGeneration::new(generation).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload,
        },
    )
    .unwrap()
}

#[test]
fn output_admission_reuses_constructor_reserved_length_scratch() {
    let max_outputs = 3;
    let mut drain = OwnerOutputDrain::new(max_outputs, 1).unwrap();
    let reserved_capacity = drain.admission_encoded_lengths.capacity();
    assert!(reserved_capacity >= max_outputs);
    assert!(drain.admission_encoded_lengths.is_empty());

    drain
        .admit_outputs(
            vec![
                OwnerTargetOutput::SessionExpired,
                OwnerTargetOutput::SessionExpired,
                OwnerTargetOutput::SessionExpired,
            ],
            false,
        )
        .unwrap();
    assert_eq!(
        drain.admission_encoded_lengths.capacity(),
        reserved_capacity
    );
    assert!(drain.admission_encoded_lengths.is_empty());

    let stopped = drain.stop(OwnerOutputDrainStopReason::Shutdown).unwrap();
    let (outputs, page, reason, _) = stopped.into_parts();
    assert_eq!(outputs.len(), max_outputs);
    assert_eq!(page, Some(OwnerOutputPageDrained::new(false)));
    assert_eq!(reason, OwnerOutputDrainStopReason::Shutdown);
}

#[test]
fn lifecycle_outputs_leave_in_fifo_order_and_complete_the_page() {
    let mut drain = OwnerOutputDrain::new(4, 1).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain
        .admit_outputs(
            vec![
                OwnerTargetOutput::LegActivated {
                    generation: LegGeneration::new(1).unwrap(),
                },
                OwnerTargetOutput::ResumeGraceStarted {
                    generation: LegGeneration::new(2).unwrap(),
                },
            ],
            true,
        )
        .unwrap();

    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled { page_drained: None }
    );
    let mut first_installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut first_installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled {
            page_drained: Some(OwnerOutputPageDrained::new(true)),
        }
    );
    let mut second_installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut second_installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(lifecycle.inventory().is_exact_zero());
}

#[test]
fn lifecycle_install_rejection_retains_the_exact_front_for_retry() {
    let expected = OwnerOutputLifecycleKind::LegActivated {
        generation: LegGeneration::new(7).unwrap(),
    };
    let mut drain = OwnerOutputDrain::new(1, 1).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain
        .admit_outputs(
            vec![OwnerTargetOutput::LegActivated {
                generation: LegGeneration::new(7).unwrap(),
            }],
            true,
        )
        .unwrap();
    let before = drain.inventory().unwrap();

    lifecycle
        .try_install(OwnerOutputLifecycleAction::resume_grace_started_for_test(
            LegGeneration::new(8).unwrap(),
        ))
        .unwrap();

    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstallBlocked
    );
    assert_eq!(drain.inventory().unwrap(), before);

    let mut blocking_installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut blocking_installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled {
            page_drained: Some(OwnerOutputPageDrained::new(true)),
        }
    );
    let mut expected_installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            assert_eq!(transaction.kind(), Some(expected));
            transaction.install_into(&mut expected_installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(drain.inventory().unwrap().is_exact_zero());
    assert_eq!(expected_installed.kind(), Some(expected));
}

#[test]
fn capacity_rejection_returns_the_exact_page_and_continuation_fact() {
    let mut drain = OwnerOutputDrain::new(1, 1).unwrap();
    let outputs = vec![
        OwnerTargetOutput::LegActivated {
            generation: LegGeneration::new(3).unwrap(),
        },
        OwnerTargetOutput::ResumeGraceStarted {
            generation: LegGeneration::new(4).unwrap(),
        },
    ];

    let failure = drain.admit_outputs(outputs, true).unwrap_err();
    assert_eq!(
        failure.kind(),
        &OwnerOutputAdmissionErrorKind::OutputCapacityExceeded {
            max_outputs: 1,
            incoming_outputs: 2,
        }
    );
    let inventory = failure.inventory();
    assert_eq!(inventory.pending_outputs(), 2);
    assert_eq!(inventory.known_encoded_bytes(), 0);
    assert_eq!(inventory.unencodable_frames(), 0);
    assert_eq!(inventory.raw_continuations(), 0);
    assert!(!inventory.byte_count_overflowed());
    assert!(inventory.page_open());
    assert!(inventory.continuation_needed());
    assert!(!inventory.is_exact_zero());
    let (outputs, needs_continue, _) = failure.into_parts();
    assert!(needs_continue);
    assert_eq!(
        outputs,
        vec![
            OwnerTargetOutput::LegActivated {
                generation: LegGeneration::new(3).unwrap(),
            },
            OwnerTargetOutput::ResumeGraceStarted {
                generation: LegGeneration::new(4).unwrap(),
            },
        ]
    );
    assert!(drain.inventory().unwrap().is_exact_zero());
}

#[test]
fn a_second_page_cannot_overwrite_the_retained_fifo() {
    let mut drain = OwnerOutputDrain::new(2, 1).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain
        .admit_outputs(
            vec![OwnerTargetOutput::LegActivated {
                generation: LegGeneration::new(20).unwrap(),
            }],
            false,
        )
        .unwrap();

    let failure = drain
        .admit_outputs(
            vec![OwnerTargetOutput::ResumeGraceStarted {
                generation: LegGeneration::new(21).unwrap(),
            }],
            true,
        )
        .unwrap_err();
    assert_eq!(failure.kind(), &OwnerOutputAdmissionErrorKind::PagePending);
    let (outputs, needs_continue, _) = failure.into_parts();
    assert!(needs_continue);
    assert_eq!(
        outputs,
        vec![OwnerTargetOutput::ResumeGraceStarted {
            generation: LegGeneration::new(21).unwrap(),
        }]
    );
    assert_eq!(drain.inventory().unwrap().pending_outputs(), 1);
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled {
            page_drained: Some(OwnerOutputPageDrained::new(false)),
        }
    );
    let mut installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(drain.inventory().unwrap().is_exact_zero());
}

#[test]
fn queue_pressure_retains_the_exact_head_and_fifo_page() {
    let retained_payload = Bytes::from_static(b"retained");
    let retained_frame = data_frame(2, retained_payload);
    let retained_len = retained_frame.encode().unwrap().len();
    let mut drain = OwnerOutputDrain::new(2, retained_len).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain
        .admit_outputs(
            vec![
                OwnerTargetOutput::Transmit(retained_frame),
                OwnerTargetOutput::LegActivated {
                    generation: LegGeneration::new(2).unwrap(),
                },
            ],
            true,
        )
        .unwrap();

    let mut full = LegOutboundQueue::new(1, 4_096).unwrap();
    full.push(data_frame(1, Bytes::from_static(b"already queued")))
        .unwrap();
    let blocked = drain.drive_one(Some(&mut full), &mut lifecycle).unwrap();
    assert!(matches!(
        blocked,
        OwnerOutputDrainDrive::Blocked {
            reason: OwnerOutputDrainBlock::Queue(
                LegOutboundQueueErrorKind::CapacityExceeded { .. }
            )
        }
    ));
    let OwnerOutputDrainDrive::Blocked { reason } = blocked else {
        unreachable!();
    };
    assert!(reason.is_retryable());
    assert_eq!(drain.inventory().unwrap().pending_outputs(), 2);
    assert_eq!(
        drain.inventory().unwrap().summed_encoded_bytes(),
        retained_len
    );

    let mut retry = LegOutboundQueue::new(1, retained_len).unwrap();
    assert_eq!(
        drain.drive_one(Some(&mut retry), &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::Transmitted { page_drained: None }
    );
    assert_eq!(retry.len(), 1);
    assert_eq!(retry.owned_bytes(), retained_len);
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled {
            page_drained: Some(OwnerOutputPageDrained::new(true)),
        }
    );
    let mut installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(drain.inventory().unwrap().is_exact_zero());

    // The queue owns the exact Bytes backing originally retained by the drain.
    let queued = retry.cold_inventory().unwrap();
    assert_eq!(queued.queued_frames(), 1);
}

#[test]
fn encoded_byte_rejection_preserves_the_frame_backing() {
    let payload = Bytes::from(vec![0x5a; 37]);
    let frame = data_frame(7, payload);
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!();
    };
    let payload_ptr = payload.as_ptr();
    let encoded_len = frame.encode().unwrap().len();
    let mut drain = OwnerOutputDrain::new(1, encoded_len - 1).unwrap();
    let reserved_scratch_capacity = drain.admission_encoded_lengths.capacity();

    let failure = drain
        .admit_outputs(vec![OwnerTargetOutput::Transmit(frame)], true)
        .unwrap_err();
    assert!(drain.admission_encoded_lengths.is_empty());
    assert_eq!(
        drain.admission_encoded_lengths.capacity(),
        reserved_scratch_capacity,
    );
    assert_eq!(
        failure.kind(),
        &OwnerOutputAdmissionErrorKind::EncodedByteCapacityExceeded {
            max_encoded_bytes: encoded_len - 1,
            incoming_encoded_bytes: encoded_len,
        }
    );
    let (mut outputs, needs_continue, _) = failure.into_parts();
    assert!(needs_continue);
    let OwnerTargetOutput::Transmit(frame) = outputs.pop().unwrap() else {
        panic!("exact rejected output must remain a transport frame");
    };
    let Record::Data { payload, .. } = frame.record() else {
        panic!("exact rejected frame must remain DATA");
    };
    assert_eq!(payload.as_ptr(), payload_ptr);
    assert_eq!(frame.encode().unwrap().len(), encoded_len);
    assert!(drain.inventory().unwrap().is_exact_zero());
}

#[test]
fn empty_page_keeps_continuation_sticky_until_page_drained() {
    let mut drain = OwnerOutputDrain::new(1, 1).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain.admit_outputs(Vec::new(), true).unwrap();

    let inventory = drain.inventory().unwrap();
    assert_eq!(inventory.pending_outputs(), 0);
    assert_eq!(inventory.summed_encoded_bytes(), 0);
    assert_eq!(inventory.stored_encoded_bytes(), 0);
    assert!(inventory.page_open());
    assert!(inventory.continuation_needed());
    assert!(!inventory.is_exact_zero());
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::PageDrained(OwnerOutputPageDrained::new(true))
    );
    assert!(drain.inventory().unwrap().is_exact_zero());
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::Idle
    );
}

#[test]
fn hard_queue_rejection_retains_the_exact_frame_for_fatal_inventory() {
    let frame = Frame::try_new(
        LegGeneration::new(9).unwrap(),
        Record::Attach {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            min_version: SESSION_PROTOCOL_VERSION,
            max_version: SESSION_PROTOCOL_VERSION,
            offered_features: FeatureSet::new(0),
            required_features: FeatureSet::new(0),
            proof: AttachProof::new([0x33; 32]).unwrap(),
        },
    )
    .unwrap();
    let expected_frame = frame.clone();
    let encoded_len = frame.encode().unwrap().len();
    let mut drain = OwnerOutputDrain::new(1, encoded_len).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain
        .admit_outputs(vec![OwnerTargetOutput::Transmit(frame)], false)
        .unwrap();
    let mut queue = LegOutboundQueue::new(1, encoded_len).unwrap();

    let blocked = drain.drive_one(Some(&mut queue), &mut lifecycle).unwrap();
    let OwnerOutputDrainDrive::Blocked { reason } = blocked else {
        panic!("restricted ATTACH must be rejected without consuming it");
    };
    assert_eq!(
        reason,
        OwnerOutputDrainBlock::Queue(LegOutboundQueueErrorKind::RegisteredAttachAdmissionRequired)
    );
    assert!(!reason.is_retryable());
    assert_eq!(queue.len(), 0);

    let stopped = drain.stop(OwnerOutputDrainStopReason::Fatal).unwrap();
    assert_eq!(stopped.reason(), OwnerOutputDrainStopReason::Fatal);
    assert_eq!(stopped.inventory().pending_outputs(), 1);
    assert_eq!(stopped.inventory().summed_encoded_bytes(), encoded_len);
    let (mut outputs, page, reason, inventory) = stopped.into_parts();
    assert_eq!(reason, OwnerOutputDrainStopReason::Fatal);
    assert_eq!(page, Some(OwnerOutputPageDrained::new(false)));
    assert!(!inventory.is_exact_zero());
    let OwnerTargetOutput::Transmit(returned) = outputs.pop().unwrap() else {
        unreachable!();
    };
    assert_eq!(returned, expected_frame);
}

#[test]
fn cancel_and_shutdown_return_complete_fifo_and_page_inventory() {
    for reason in [
        OwnerOutputDrainStopReason::Cancelled,
        OwnerOutputDrainStopReason::Shutdown,
    ] {
        let frame = data_frame(12, Bytes::from_static(b"undelivered"));
        let encoded_len = frame.encode().unwrap().len();
        let mut drain = OwnerOutputDrain::new(2, encoded_len).unwrap();
        drain
            .admit_outputs(
                vec![
                    OwnerTargetOutput::Transmit(frame),
                    OwnerTargetOutput::SessionExpired,
                ],
                true,
            )
            .unwrap();

        let stopped = drain.stop(reason).unwrap();
        assert_eq!(stopped.reason(), reason);
        let inventory = stopped.inventory();
        assert_eq!(inventory.pending_outputs(), 2);
        assert_eq!(inventory.summed_encoded_bytes(), encoded_len);
        assert_eq!(inventory.stored_encoded_bytes(), encoded_len);
        assert!(inventory.page_open());
        assert!(inventory.continuation_needed());
        let (outputs, page, returned_reason, returned_inventory) = stopped.into_parts();
        assert_eq!(outputs.len(), 2);
        assert!(matches!(outputs[0], OwnerTargetOutput::Transmit(_)));
        assert!(matches!(outputs[1], OwnerTargetOutput::SessionExpired));
        assert_eq!(page, Some(OwnerOutputPageDrained::new(true)));
        assert_eq!(returned_reason, reason);
        assert_eq!(returned_inventory, inventory);
    }
}

#[test]
fn lifecycle_port_requires_root_commit_before_the_next_output_can_advance() {
    let first_generation = LegGeneration::new(31).unwrap();
    let second_generation = LegGeneration::new(32).unwrap();
    let mut drain = OwnerOutputDrain::new(2, 1).unwrap();
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    drain
        .admit_outputs(
            vec![
                OwnerTargetOutput::LegActivated {
                    generation: first_generation,
                },
                OwnerTargetOutput::ResumeGraceStarted {
                    generation: second_generation,
                },
            ],
            false,
        )
        .unwrap();

    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled { page_drained: None },
    );
    assert_eq!(
        lifecycle.inventory().pending_actions(),
        OWNER_OUTPUT_LIFECYCLE_PORT_CAPACITY,
    );
    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstallBlocked,
        "without a production root consumer the one-slot lifecycle port livelocks",
    );

    let mut first_installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            assert_eq!(
                transaction.kind(),
                Some(OwnerOutputLifecycleKind::LegActivated {
                    generation: first_generation,
                }),
            );
            transaction.install_into(&mut first_installed)
        },),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(lifecycle.inventory().is_exact_zero());

    assert_eq!(
        drain.drive_one(None, &mut lifecycle).unwrap(),
        OwnerOutputDrainDrive::LifecycleInstalled {
            page_drained: Some(OwnerOutputPageDrained::new(false)),
        },
    );
    let mut second_installed = OwnerOutputLifecycleInstalledSlot::new();
    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            assert_eq!(
                transaction.kind(),
                Some(OwnerOutputLifecycleKind::ResumeGraceStarted {
                    generation: second_generation,
                }),
            );
            transaction.install_into(&mut second_installed)
        },),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(lifecycle.inventory().is_exact_zero());
    assert!(drain.inventory().unwrap().is_exact_zero());
}

#[test]
fn lifecycle_terminal_discard_is_retained_until_audited_for_each_stop_reason() {
    for reason in [
        OwnerOutputDrainStopReason::Fatal,
        OwnerOutputDrainStopReason::Cancelled,
        OwnerOutputDrainStopReason::Shutdown,
    ] {
        let mut lifecycle = OwnerOutputLifecyclePort::new();
        lifecycle
            .try_install(OwnerOutputLifecycleAction::session_expired_for_test())
            .unwrap();
        let settlement = OwnerOutputLifecycleSettlement::Discard { reason };

        assert_eq!(
            lifecycle.transact(settlement, |transaction| {
                assert_eq!(transaction.settlement(), settlement);
                assert_eq!(
                    transaction.kind(),
                    Some(OwnerOutputLifecycleKind::SessionExpired),
                );
                transaction.retain()
            }),
            OwnerOutputLifecycleDrive::Retained { settlement },
        );
        assert_eq!(lifecycle.inventory().pending_actions(), 1);
        assert!(!lifecycle.inventory().is_exact_zero());

        assert_eq!(
            lifecycle.transact(settlement, |transaction| {
                assert_eq!(transaction.settlement(), settlement);
                assert_eq!(
                    transaction.kind(),
                    Some(OwnerOutputLifecycleKind::SessionExpired),
                );
                transaction.discard()
            }),
            OwnerOutputLifecycleDrive::Committed { settlement },
        );
        let inventory = lifecycle.inventory();
        assert_eq!(inventory.pending_actions(), 0);
        assert!(inventory.is_exact_zero());
        assert!(!inventory.audit_failed());
        assert_eq!(inventory.installed_actions(), 0);
        assert_eq!(
            inventory.fatal_discards(),
            u64::from(reason == OwnerOutputDrainStopReason::Fatal),
        );
        assert_eq!(
            inventory.cancelled_discards(),
            u64::from(reason == OwnerOutputDrainStopReason::Cancelled),
        );
        assert_eq!(
            inventory.shutdown_discards(),
            u64::from(reason == OwnerOutputDrainStopReason::Shutdown),
        );
    }
}

#[test]
fn lifecycle_transaction_retain_and_unwind_restore_the_exact_action() {
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    lifecycle
        .try_install(OwnerOutputLifecycleAction::session_expired_for_test())
        .expect("empty lifecycle port accepts one exact action");
    let mut installed = OwnerOutputLifecycleInstalledSlot::new();

    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            assert_eq!(
                transaction.kind(),
                Some(OwnerOutputLifecycleKind::SessionExpired),
            );
            transaction.retain()
        }),
        OwnerOutputLifecycleDrive::Retained {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert_eq!(lifecycle.inventory().pending_actions(), 1);
    assert!(installed.inventory().is_exact_zero());

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            assert_eq!(
                transaction.kind(),
                Some(OwnerOutputLifecycleKind::SessionExpired),
            );
            panic!("declared lifecycle rollback tracer");
        });
    }));
    assert!(unwind.is_err());
    assert_eq!(lifecycle.inventory().pending_actions(), 1);
    assert!(installed.inventory().is_exact_zero());

    let prepared_unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            let _prepared = transaction.install_into(&mut installed);
            panic!("declared prepared lifecycle rollback tracer");
        });
    }));
    assert!(prepared_unwind.is_err());
    assert_eq!(lifecycle.inventory().pending_actions(), 1);
    assert!(installed.inventory().is_exact_zero());

    assert_eq!(
        lifecycle.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut installed)
        }),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert!(lifecycle.inventory().is_exact_zero());
    assert_eq!(installed.inventory().installed_actions(), 1);
    assert_eq!(
        installed.kind(),
        Some(OwnerOutputLifecycleKind::SessionExpired)
    );
}

#[test]
fn occupied_lifecycle_slot_retains_the_second_exact_action_in_its_port() {
    let first_generation = LegGeneration::new(41).unwrap();
    let second_generation = LegGeneration::new(42).unwrap();
    let mut first = OwnerOutputLifecyclePort::new();
    first
        .try_install(OwnerOutputLifecycleAction::leg_activated_for_test(
            first_generation,
        ))
        .expect("first lifecycle action enters its exact port");
    let mut installed = OwnerOutputLifecycleInstalledSlot::new();
    assert!(matches!(
        first.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut installed)
        }),
        OwnerOutputLifecycleDrive::Committed { .. }
    ));

    let mut second = OwnerOutputLifecyclePort::new();
    second
        .try_install(OwnerOutputLifecycleAction::resume_grace_started_for_test(
            second_generation,
        ))
        .expect("second lifecycle action enters its own exact port");
    assert_eq!(
        second.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut installed)
        }),
        OwnerOutputLifecycleDrive::Retained {
            settlement: OwnerOutputLifecycleSettlement::Install,
        },
    );
    assert_eq!(second.inventory().pending_actions(), 1);
    assert_eq!(
        installed.kind(),
        Some(OwnerOutputLifecycleKind::LegActivated {
            generation: first_generation,
        }),
    );
}

fn installed_lifecycle_action(
    action: OwnerOutputLifecycleAction,
) -> OwnerOutputLifecycleInstalledSlot {
    let mut port = OwnerOutputLifecyclePort::new();
    port.try_install(action)
        .expect("empty lifecycle port accepts one exact action");
    let mut installed = OwnerOutputLifecycleInstalledSlot::new();
    assert!(matches!(
        port.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(&mut installed)
        }),
        OwnerOutputLifecycleDrive::Committed { .. }
    ));
    installed
}

fn terminal_test_credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0xd1; 32]).unwrap(),
        ResumeSecret::new([0xa1; 32]).unwrap(),
    )
    .unwrap()
}

fn terminal_test_leg(session_id: SessionId) -> crate::resumable::CommittedLeg {
    let owner = OwnerIdentity::new([0x31; 32]).unwrap();
    let principal = DevicePrincipal::new([0x53; 16]).unwrap();
    let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
    let binding = AttachTransportBinding::new(
        owner,
        alpn,
        TlsExporterBinding::new([0x42; 32]).unwrap(),
        principal,
    );
    let request = AttachRequest::new(
        session_id,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x22; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let credentials = terminal_test_credentials();
    let proof = credentials.prove(&request, &binding).unwrap();
    AttachAuthority::new(
        session_id,
        LegGeneration::new(1).unwrap(),
        credentials,
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b1111).unwrap(),
    )
    .verify_and_commit(&request, &binding, &proof)
    .unwrap()
}

fn terminal_grace_for_test(session_id: SessionId, flow_id: SessionFlowId) -> TerminalGrace {
    let leg = terminal_test_leg(session_id);
    let config = SessionConfig::new(
        8,
        8,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReceiveBudgetLimits::new(128, 16).unwrap(),
        16,
    )
    .unwrap();
    let mut model = SessionModel::new(SessionRole::Owner, config, leg);
    let target = TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        4_000 + u16::try_from(flow_id.get()).unwrap(),
    )));
    let opened = model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(leg.generation(), Record::Open { flow_id, target }).unwrap(),
        })
        .unwrap();
    let request = opened
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, .. } => Some(request),
            _ => None,
        })
        .expect("peer OPEN publishes one exact request");
    model
        .reduce(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::TargetRefused,
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
            _ => None,
        })
        .expect("failed peer OPEN mints one exact terminal grace")
}

#[test]
fn physical_lifecycle_slots_apply_exact_actions_and_return_rejections() {
    let session_id = SessionId::new([0x61; 16]).unwrap();
    let generation = LegGeneration::new(51).unwrap();
    let mut slots = OwnerPhysicalLifecycleSlots::try_new(session_id, generation, 2)
        .expect("checked typed lifecycle slots");

    let installed = installed_lifecycle_action(OwnerOutputLifecycleAction::leg_activated_for_test(
        generation,
    ));
    assert_eq!(
        installed.apply_into(&mut slots).unwrap().kind(),
        OwnerOutputLifecycleKind::LegActivated { generation },
    );
    assert_eq!(slots.inventory().leg_activated_phases(), 1);

    let occupied = installed_lifecycle_action(OwnerOutputLifecycleAction::leg_activated_for_test(
        generation,
    ))
    .apply_into(&mut slots)
    .expect_err("the phase slot has logical capacity one");
    assert_eq!(
        occupied.kind(),
        OwnerPhysicalLifecycleApplyErrorKind::SlotOccupied {
            slot: OwnerPhysicalLifecycleSlotKind::LegActivated,
        }
    );
    assert_eq!(
        occupied.into_installed_slot().kind(),
        Some(OwnerOutputLifecycleKind::LegActivated { generation })
    );

    let wrong_generation = LegGeneration::new(52).unwrap();
    let rejected = installed_lifecycle_action(
        OwnerOutputLifecycleAction::resume_grace_started_for_test(wrong_generation),
    )
    .apply_into(&mut slots)
    .expect_err("a scalar cannot retarget the physical current generation");
    assert_eq!(
        rejected.kind(),
        OwnerPhysicalLifecycleApplyErrorKind::WrongGeneration {
            expected: generation,
            actual: wrong_generation,
        }
    );
    assert_eq!(
        rejected.into_installed_slot().kind(),
        Some(OwnerOutputLifecycleKind::ResumeGraceStarted {
            generation: wrong_generation,
        })
    );
    assert_eq!(slots.inventory().resume_grace_timers(), 0);
}

#[test]
fn terminal_grace_registry_rejects_n_plus_one_without_reconstructing_the_token() {
    let session_id = SessionId::new([0x62; 16]).unwrap();
    let generation = LegGeneration::new(53).unwrap();
    let mut slots = OwnerPhysicalLifecycleSlots::try_new(session_id, generation, 2)
        .expect("two exact terminal-grace slots");

    for raw in 1..=2 {
        let terminal = terminal_grace_for_test(session_id, SessionFlowId::new(raw).unwrap());
        let installed = installed_lifecycle_action(
            OwnerOutputLifecycleAction::terminal_grace_started_for_test(terminal),
        );
        assert!(matches!(
            installed.apply_into(&mut slots),
            Ok(OwnerPhysicalLifecycleApplied {
                kind: OwnerOutputLifecycleKind::TerminalGraceStarted { .. }
            })
        ));
    }
    assert_eq!(slots.inventory().terminal_grace_capacity(), 2);
    assert_eq!(slots.inventory().terminal_grace_timers(), 2);

    let third = terminal_grace_for_test(session_id, SessionFlowId::new(3).unwrap());
    let third_action = OwnerOutputLifecycleAction::terminal_grace_started_for_test(third);
    let expected_kind = third_action.kind();
    let rejected = installed_lifecycle_action(third_action)
        .apply_into(&mut slots)
        .expect_err("N+1 terminal grace remains installed outside the full registry");
    assert_eq!(
        rejected.kind(),
        OwnerPhysicalLifecycleApplyErrorKind::TerminalGraceCapacityExceeded { max: 2 }
    );
    assert_eq!(
        rejected.into_installed_slot().kind(),
        Some(expected_kind),
        "capacity rejection must return the exact pre-observed lifecycle token",
    );
    assert_eq!(slots.inventory().terminal_grace_timers(), 2);
}

#[test]
fn physical_lifecycle_stop_is_sticky_and_audits_exact_zero_for_every_reason() {
    let session_id = SessionId::new([0x63; 16]).unwrap();
    let generation = LegGeneration::new(54).unwrap();
    for reason in [
        OwnerOutputDrainStopReason::Fatal,
        OwnerOutputDrainStopReason::Cancelled,
        OwnerOutputDrainStopReason::Shutdown,
    ] {
        let mut slots = OwnerPhysicalLifecycleSlots::try_new(session_id, generation, 2).unwrap();
        for action in [
            OwnerOutputLifecycleAction::leg_activated_for_test(generation),
            OwnerOutputLifecycleAction::resume_grace_started_for_test(generation),
            OwnerOutputLifecycleAction::terminal_grace_started_for_test(terminal_grace_for_test(
                session_id,
                SessionFlowId::new(7).unwrap(),
            )),
            OwnerOutputLifecycleAction::session_expired_for_test(),
        ] {
            installed_lifecycle_action(action)
                .apply_into(&mut slots)
                .expect("each typed domain has one vacant slot");
        }
        let before = slots.inventory();
        assert_eq!(before.live_actions(), 4);
        assert!(!before.is_exact_zero());

        assert_eq!(
            slots.settle_for_stop(reason),
            OwnerPhysicalLifecycleStopDrive::Settled {
                reason,
                discarded: before,
            }
        );
        let after = slots.inventory();
        assert!(after.is_exact_zero());
        assert!(!after.audit_failed());
        assert_eq!(after.stopped_reason(), Some(reason));
        assert_eq!(
            slots.settle_for_stop(OwnerOutputDrainStopReason::Fatal),
            OwnerPhysicalLifecycleStopDrive::AlreadySettled { reason },
        );
        let history = slots.history();
        assert_eq!(history.leg_activated_applied(), 1);
        assert_eq!(history.resume_grace_applied(), 1);
        assert_eq!(history.terminal_grace_applied(), 1);
        assert_eq!(history.session_expired_applied(), 1);
        assert_eq!(
            history.fatal_discards(),
            u64::from(reason == OwnerOutputDrainStopReason::Fatal) * 4,
        );
        assert_eq!(
            history.cancelled_discards(),
            u64::from(reason == OwnerOutputDrainStopReason::Cancelled) * 4,
        );
        assert_eq!(
            history.shutdown_discards(),
            u64::from(reason == OwnerOutputDrainStopReason::Shutdown) * 4,
        );

        let retained = installed_lifecycle_action(
            OwnerOutputLifecycleAction::leg_activated_for_test(generation),
        )
        .apply_into(&mut slots)
        .expect_err("stopped slots return new exact work to the installed slot");
        assert_eq!(
            retained.kind(),
            OwnerPhysicalLifecycleApplyErrorKind::Stopped { reason }
        );
        assert_eq!(
            retained
                .into_installed_slot()
                .inventory()
                .installed_actions(),
            1
        );
    }
}
