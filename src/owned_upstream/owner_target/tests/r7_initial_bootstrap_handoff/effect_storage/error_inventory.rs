//! Defensive borrow-audit counterexamples, not natural recovery failure proofs.
//! Deliberate holders below may have invalid correlation. They own real values
//! but must not be executed as valid cursors or used to claim physical zero.
//! Parent's real registered/FIFO-pressure tracer owns protocol/retry acceptance.

use super::*;
mod remaining;
use crate::owned_upstream::owner_target::error_inventory::RecoveryPayloadInventory;

fn diagnostic_cursor(pending: usize, ready: usize) -> OwnerTargetRecoveryDrain {
    OwnerTargetRecoveryDrain {
        owner: std::sync::Weak::new(),
        attached: None,
        acceptance: None,
        pending: std::collections::VecDeque::with_capacity(pending),
        ready: Vec::with_capacity(ready),
    }
}

fn array_bytes(cursor: &OwnerTargetRecoveryDrain) -> usize {
    std::alloc::Layout::array::<OwnerTargetOutput>(cursor.pending.capacity())
        .unwrap()
        .size()
        + std::alloc::Layout::array::<OwnerTargetRecoveryOutput>(cursor.ready.capacity())
            .unwrap()
            .size()
}

#[test]
fn empty_cursor_and_retained_capacity_never_audit_as_zero() {
    for (pending, ready) in [(0, 0), (3, 5)] {
        let cursor = diagnostic_cursor(pending, ready);
        let expected = RecoveryPayloadInventory {
            cursors: 1,
            pending_capacity: cursor.pending.capacity(),
            ready_capacity: cursor.ready.capacity(),
            container_bytes: array_bytes(&cursor),
            ..RecoveryPayloadInventory::empty()
        };
        let error = OwnerTargetError::AttachedRecoveryOutputInvariant {
            drain: Box::new(cursor),
        };
        for _ in 0..2 {
            let inventory = error.cold_inventory().unwrap();
            assert_eq!(inventory.target_error_nodes, 1);
            assert_eq!(inventory.recovery, expected);
            assert!(!inventory.is_exact_zero());
        }
    }
}

#[test]
fn nested_cursor_keeps_frames_with_audited_scalar_inner_cause() {
    let mut cursor = diagnostic_cursor(2, 1);
    let frame = Frame::try_new(
        LegGeneration::new(3).unwrap(),
        Record::OpenResult {
            flow_id: SessionFlowId::new(3).unwrap(),
            result: OpenResultCode::Opened,
        },
    )
    .unwrap();
    let original_wire = frame.encode().unwrap();
    cursor.pending.push_back(OwnerTargetOutput::Transmit(frame));
    cursor.pending.push_back(OwnerTargetOutput::LegActivated {
        generation: LegGeneration::new(3).unwrap(),
    });
    cursor.ready.push(OwnerTargetRecoveryOutput::SessionExpired);
    let expected = RecoveryPayloadInventory {
        cursors: 1,
        output_values: 3,
        frames: 1,
        frame_encoded_bytes: original_wire.len(),
        pending_capacity: cursor.pending.capacity(),
        ready_capacity: cursor.ready.capacity(),
        container_bytes: array_bytes(&cursor),
        ..RecoveryPayloadInventory::empty()
    };
    let mut error = OwnerTargetError::PostCommitRecoveryRetentionInvariant {
        error: Box::new(OwnerTargetError::EffectResumeIdExhausted),
        recovery: Box::new(cursor),
    };
    let inventory = error.cold_inventory().unwrap();
    assert_eq!(
        (inventory.target_error_nodes, inventory.observation_facts),
        (2, 0)
    );
    assert_eq!(inventory.recovery, expected);
    assert_eq!(error.cold_inventory(), Ok(inventory));
    let OwnerTargetError::PostCommitRecoveryRetentionInvariant {
        error: inner,
        recovery,
    } = &mut error
    else {
        unreachable!()
    };
    let Some(OwnerTargetOutput::Transmit(frame)) = recovery.pending.front() else {
        unreachable!()
    };
    assert_eq!(frame.encode().unwrap(), original_wire);
    // Replace only a scalar diagnostic cause, retaining the exact cursor.
    *inner = Box::new(OwnerTargetError::Supervisor(
        SessionSupervisorError::LegProvenance(
            crate::owned_upstream::leg::LegProvenanceError::WrongLeg,
        ),
    ));
    assert_eq!(error.cold_inventory(), Ok(inventory));
}

#[test]
fn original_tokens_are_counted_at_their_actual_pending_and_ready_positions() {
    let (mut executor, mut initial, _io, _queue) = checked_owner(0xd1, 1);
    let mut outputs = executor
        .apply_event(SessionEvent::PeerFrame {
            leg: committed(&initial.attached),
            frame: Frame::try_new(
                initial.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target: target(443),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let Some(OwnerTargetOutput::NeedsResume { resume }) = outputs.pop() else {
        panic!("the actual E1 OPEN must produce its original resume");
    };
    assert!(outputs.is_empty());
    let error = OwnerTargetError::RejectedEffectResume {
        block: OwnerTargetInputBlock::PendingEffectTurn,
        resume: Box::new(resume),
    };
    assert_eq!(
        error.cold_inventory().unwrap().recovery,
        RecoveryPayloadInventory {
            resumes: 1,
            ..RecoveryPayloadInventory::empty()
        }
    );
    let OwnerTargetError::RejectedEffectResume { resume, .. } = error else {
        unreachable!()
    };
    // These are deliberate audit holders, NOT a claim that initial acceptance
    // and this ordinary resume form a valid recovery transaction.
    let mut cursor = diagnostic_cursor(1, 0);
    cursor.owner = Arc::downgrade(&executor.identity);
    cursor.attached = Some(initial.attached);
    cursor.acceptance = initial.acceptance.take();
    assert!(cursor.acceptance.is_some());
    cursor
        .pending
        .push_back(OwnerTargetOutput::NeedsResume { resume: *resume });
    let error = OwnerTargetError::RejectedAttachedRecoveryDrain {
        block: OwnerTargetInputBlock::WrongAcceptanceQueue,
        drain: Box::new(cursor),
    };
    let pending = error.cold_inventory().unwrap().recovery;
    assert_eq!(
        (pending.cursors, pending.output_values, pending.resumes),
        (1, 1, 1)
    );
    assert_eq!(
        (pending.attached_authorities, pending.acceptance_receipts),
        (1, 1)
    );
    let OwnerTargetError::RejectedAttachedRecoveryDrain { mut drain, .. } = error else {
        unreachable!()
    };
    let Some(OwnerTargetOutput::NeedsResume { resume }) = drain.pending.pop_front() else {
        unreachable!()
    };
    let error = OwnerTargetError::RejectedAttachedRecoveryResume {
        block: OwnerTargetInputBlock::WrongAcceptanceQueue,
        recovery: Box::new(OwnerTargetRecoveryResume {
            attached: drain.attached.take().unwrap(),
            acceptance: drain.acceptance.take().unwrap(),
            resume,
        }),
    };
    assert_eq!(
        error.cold_inventory().unwrap().recovery,
        RecoveryPayloadInventory {
            resumes: 1,
            attached_authorities: 1,
            acceptance_receipts: 1,
            ..RecoveryPayloadInventory::empty()
        }
    );
    let OwnerTargetError::RejectedAttachedRecoveryResume { recovery, .. } = error else {
        unreachable!()
    };
    drain
        .ready
        .push(OwnerTargetRecoveryOutput::NeedsResume { resume: *recovery });
    let (other, other_initial, _other_io, _other_queue) = checked_owner(0xd2, 1);
    drain
        .ready
        .push(OwnerTargetRecoveryOutput::RecoveryCompleted {
            handoff: OwnerTargetRecoveryHandoff {
                owner: Arc::downgrade(&other.identity),
                attached: other_initial.attached,
            },
        });
    // The other initial receipt stays outside this diagnostic cursor. It must
    // not be counted a second time merely because an attached token moved.
    assert_eq!(other_initial.acceptance.as_ref().map(|_| 1), Some(1));
    let capacity = (
        drain.pending.capacity(),
        drain.ready.capacity(),
        array_bytes(&drain),
    );
    let error = OwnerTargetError::AttachedRecoveryOutputInvariant { drain };
    let ready = error.cold_inventory().unwrap().recovery;
    assert_eq!(
        (ready.cursors, ready.output_values, ready.resumes),
        (1, 2, 1)
    );
    assert_eq!(
        (ready.attached_authorities, ready.acceptance_receipts),
        (2, 1)
    );
    assert_eq!((ready.frames, ready.frame_encoded_bytes), (0, 0));
    assert_eq!(
        (
            ready.pending_capacity,
            ready.ready_capacity,
            ready.container_bytes
        ),
        capacity
    );
    assert!(ready.pending_capacity > 0 && ready.ready_capacity > 0);
    assert!(!ready.is_exact_zero());
}

#[test]
fn scalar_queue_cause_keeps_complete_cursor_inventory() {
    let error = OwnerTargetError::AttachedRecoveryQueueBlocked {
        kind: crate::owned_upstream::leg_io::LegOutboundQueueErrorKind::InvalidFrame(
            crate::resumable::ProtocolError::InvalidMagic,
        ),
        drain: Box::new(diagnostic_cursor(2, 1)),
    };
    for _ in 0..2 {
        let inventory = error.cold_inventory().unwrap();
        assert_eq!(inventory.target_error_nodes, 1);
        assert_eq!(inventory.recovery.cursors, 1);
        assert_eq!(inventory.recovery.output_values, 0);
        assert_eq!(
            (
                inventory.recovery.pending_capacity,
                inventory.recovery.ready_capacity
            ),
            (2, 1)
        );
        assert!(inventory.recovery.container_bytes > 0);
        assert!(!inventory.is_exact_zero());
        assert_eq!(error.cold_inventory(), Ok(inventory));
    }
}
