//! Remaining error-payload counterexamples use original values but may assemble
//! deliberately invalid holders. They are never executed as valid recovery and
//! establish no physical-root cleanup or natural double-failure claim.
use super::*;
use crate::owned_upstream::leg::LegTransportTerminalReason;

#[test]
fn rejected_initial_standby_keeps_original_classified_control_inventory() {
    let (executor, initial, _io, _queue) = checked_owner(0xe1, 1);
    let mut candidate = owner_standby_leg(0xe2);
    let inbound = classified_standby_inbound(&initial.attached, &mut candidate, 0xe3);
    let before = inbound.cold_payload().unwrap();
    assert_eq!(before.control_frames, 1);
    assert!(before.control_encoded_bytes > 0);
    let error = executor
        .authenticate_owner_standby_from_initial(&initial, candidate.established_leg(), inbound)
        .unwrap_err();
    let OwnerTargetError::RejectedOwnerStandbyAuthentication { inbound, .. } = &error else {
        panic!("unsettled initial handoff returns the exact classified input");
    };
    let identity = std::ptr::from_ref(inbound.as_ref());
    let inventory = error.cold_inventory().unwrap();
    assert_eq!(inventory.payload, before);
    assert_eq!(inventory.owner.sealed_inbounds, 1);
    assert_eq!(inventory.target_error_nodes, 1);
    assert_eq!(error.cold_inventory(), Ok(inventory));
    let OwnerTargetError::RejectedOwnerStandbyAuthentication { inbound, .. } = &error else {
        unreachable!();
    };
    assert_eq!(std::ptr::from_ref(inbound.as_ref()), identity);
    assert_eq!(inbound.cold_payload().unwrap(), before);
}

#[test]
fn reported_terminal_and_original_cursor_are_audited_without_consumption() {
    let (_io, reporter) = LegIo::for_authenticated_transport_with_reporter(
        LegId::B,
        LegEndpointRole::Owner,
        binding(0xe0),
        LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
    );
    let terminal = Box::new(
        reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap(),
    );
    let terminal_identity = std::ptr::from_ref(terminal.as_ref());
    let mut cursor = diagnostic_cursor(3, 2);
    let frame = Frame::try_new(
        LegGeneration::new(2).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"tail"),
        },
    )
    .unwrap();
    let wire_bytes = frame.encode().unwrap();
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!()
    };
    let payload_identity = payload.as_ptr();
    cursor.pending.push_back(OwnerTargetOutput::Transmit(frame));
    let drain = Box::new(cursor);
    let drain_identity = std::ptr::from_ref(drain.as_ref());
    let error = OwnerTargetError::RejectedAttachedRecoveryDrainTerminalization {
        block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
        drain,
        terminal,
    };
    let inventory = error
        .cold_inventory()
        .expect("complete exact terminal/cursor audit");
    assert!(!inventory.is_exact_zero());
    assert_eq!(inventory.recovery.cursors, 1);
    assert_eq!(inventory.recovery.frames, 1);
    assert_eq!(inventory.recovery.frame_encoded_bytes, wire_bytes.len());
    assert_eq!(inventory.owner.exact_terminals, 1);
    assert_eq!(
        (
            inventory.payload.byte_views,
            inventory.payload.logical_bytes
        ),
        (1, 4)
    );
    assert_eq!(inventory.payload.session_encoded_bytes, wire_bytes.len());
    assert_eq!(error.cold_inventory(), Ok(inventory));
    let OwnerTargetError::RejectedAttachedRecoveryDrainTerminalization {
        drain, terminal, ..
    } = &error
    else {
        unreachable!()
    };
    assert_eq!(std::ptr::from_ref(terminal.as_ref()), terminal_identity);
    assert_eq!(std::ptr::from_ref(drain.as_ref()), drain_identity);
    let Some(OwnerTargetOutput::Transmit(frame)) = drain.pending.front() else {
        unreachable!()
    };
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!()
    };
    assert_eq!(payload.as_ptr(), payload_identity);
    assert_eq!(payload.as_ref(), b"tail");
    assert_eq!(frame.encode().unwrap(), wire_bytes);
}

#[test]
fn all_nested_axes_participate_in_root_checked_sum_and_zero() {
    use crate::owned_upstream::owner_target::error_inventory::{
        ErrorAuditFailure, ErrorColdInventory,
    };
    fn axis(value: &mut ErrorColdInventory, n: usize) -> &mut usize {
        match n {
            0 => &mut value.payload.events,
            1 => &mut value.payload.session_frames,
            2 => &mut value.payload.session_encoded_bytes,
            3 => &mut value.payload.byte_views,
            4 => &mut value.payload.logical_bytes,
            5 => &mut value.payload.targets,
            6 => &mut value.payload.domain_bytes,
            7 => &mut value.payload.domain_capacity,
            8 => &mut value.payload.capability_values,
            9 => &mut value.payload.effect_batches,
            10 => &mut value.payload.effect_vectors,
            11 => &mut value.payload.effects,
            12 => &mut value.payload.effect_capacity,
            13 => &mut value.payload.effect_array_bytes,
            14 => &mut value.payload.overflow_effects,
            15 => &mut value.payload.mutated_batches,
            16 => &mut value.payload.segment_values,
            17 => &mut value.payload.segment_capacity,
            18 => &mut value.payload.segment_array_bytes,
            19 => &mut value.payload.segment_backing_charges,
            20 => &mut value.payload.receipt_values,
            21 => &mut value.payload.control_frames,
            22 => &mut value.payload.control_encoded_bytes,
            23 => &mut value.uplink.data_values,
            24 => &mut value.uplink.payload_bytes,
            25 => &mut value.uplink.ownership_values,
            26 => &mut value.uplink.stored_bytes,
            27 => &mut value.uplink.staging_payload_views,
            28 => &mut value.uplink.staging_bytes,
            29 => &mut value.uplink.permit_bundles,
            30 => &mut value.uplink.flow_byte_permits,
            31 => &mut value.uplink.session_byte_permits,
            32 => &mut value.uplink.flow_segment_permits,
            33 => &mut value.uplink.session_segment_permits,
            34 => &mut value.uplink.factory_origins,
            35 => &mut value.uplink.progress_guards,
            36 => &mut value.owner.commands,
            37 => &mut value.owner.driver_inputs,
            38 => &mut value.owner.client_driver_inputs,
            39 => &mut value.owner.sealed_inbounds,
            40 => &mut value.owner.pending_status,
            41 => &mut value.owner.enqueued_status,
            42 => &mut value.owner.catch_up_arms,
            43 => &mut value.owner.status_receipts,
            44 => &mut value.owner.acceptance_reservations,
            45 => &mut value.owner.unexecuted_recoveries,
            46 => &mut value.owner.actor_events,
            47 => &mut value.owner.arrival_witnesses,
            48 => &mut value.owner.high_waters,
            49 => &mut value.owner.pending_publications,
            50 => &mut value.owner.pending_losses,
            51 => &mut value.owner.exact_terminals,
            _ => unreachable!(),
        }
    }
    for n in 0..52 {
        let mut one = ErrorColdInventory::empty();
        *axis(&mut one, n) = 1;
        assert!(!one.is_exact_zero());
        let mut two = one;
        *axis(&mut two, n) = 2;
        assert_eq!(one.checked_add(one), Ok(two));
        let mut full = one;
        *axis(&mut full, n) = usize::MAX;
        assert_eq!(full.checked_add(ErrorColdInventory::empty()), Ok(full));
        assert_eq!(full.checked_add(one), Err(ErrorAuditFailure::CountOverflow));
    }
    assert!(ErrorColdInventory::empty().is_exact_zero());
}

#[test]
fn each_diagnostic_terminal_tail_keeps_its_original_owning_payload() {
    for kind in 0..5 {
        let exporter = 0xe4 + kind;
        let (io, reporter) = LegIo::for_authenticated_transport_with_reporter(
            LegId::A,
            LegEndpointRole::Owner,
            binding(exporter),
            LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
        );
        let request = standby_attach_request(2, 0x22);
        let proof = credentials().prove(&request, &binding(exporter)).unwrap();
        let mut queue = LegOutboundQueue::for_leg(io.established_leg(), 8, 32_768).unwrap();
        let bootstrap = SessionSupervisor::prepare_owner(capacity_plan(), standby_authority(1))
            .accept(
                io.established_leg(),
                io.established_leg()
                    .bind_received_frame(request.to_attach_frame(proof)),
                &mut queue,
            )
            .unwrap()
            .enqueue_acceptance(&mut queue)
            .unwrap();
        let (mut executor, mut handoff) = OwnerTargetExecutor::from_initial_bootstrap(
            OwnerTargetConfig::new(1, 1_024, 64).unwrap(),
            bootstrap,
            MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            FlowPortConfig::new(64, 8, 8, 8).unwrap(),
        )
        .unwrap();
        // These tails deliberately combine real tokens from the same checked
        // fixture without asserting a valid terminalization phase. Never run
        // or discard them through the production recovery transaction.
        let resume = if kind == 3 {
            let mut outputs = executor
                .apply_event(SessionEvent::PeerFrame {
                    leg: committed(&handoff.attached),
                    frame: Frame::try_new(
                        handoff.generation(),
                        Record::Open {
                            flow_id: SessionFlowId::new(1).unwrap(),
                            target: target(443),
                        },
                    )
                    .unwrap(),
                })
                .unwrap();
            let Some(OwnerTargetOutput::NeedsResume { resume }) = outputs.pop() else {
                panic!("the actual E1 OPEN keeps a resume");
            };
            Some(resume)
        } else {
            None
        };
        let generation = handoff.generation();
        let event = handoff.attached.resume_grace_expired_event();
        let terminal = reporter
            .report(LegTransportTerminalReason::PeerClosed)
            .unwrap();
        let loss = handoff.attached.bind_terminal(terminal).unwrap();
        let frame = Frame::try_new(
            generation,
            Record::Data {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"tail"),
            },
        )
        .unwrap();
        let mut effects = Vec::with_capacity(3);
        effects.push(SessionEffect::SessionExpired);
        let effect_capacity = effects.capacity();
        let tail = match kind {
            0 => TerminalOwnerAttachTail::Pending {
                endpoint: std::sync::Weak::new(),
                acceptance: frame,
                effects,
                admission: OwnerAttachAcceptanceAdmission::UnreservedForTest,
            },
            1 => TerminalOwnerAttachTail::Enqueued(EnqueuedOwnerTargetRecovery {
                acceptance: handoff.acceptance.take().unwrap(),
                effects,
            }),
            2 => {
                let mut pending = std::collections::VecDeque::with_capacity(3);
                pending.push_back(OwnerTargetOutput::Transmit(frame));
                TerminalOwnerAttachTail::Drain {
                    acceptance: handoff.acceptance.take(),
                    pending,
                    ready: Vec::with_capacity(2),
                }
            }
            3 => TerminalOwnerAttachTail::Resume {
                acceptance: handoff.acceptance.take().unwrap(),
                resume: resume.unwrap(),
            },
            4 => TerminalOwnerAttachTail::HandoffReady,
            _ => unreachable!(),
        };
        let pending = Box::new(PendingOwnerAttachLoss {
            owner: handoff.owner,
            generation,
            reason: loss.reason(),
            resume_grace_expired: event,
            loss,
            tail,
            phase: OwnerRecoveryLossPhase::BarrierCleared,
        });
        let identity = std::ptr::from_ref(pending.as_ref());
        let error = OwnerTargetError::RejectedTerminalAttachRecoveryDiscard {
            block: OwnerTargetInputBlock::WrongOwnerTargetExecutor,
            pending,
        };
        let audit = error.cold_inventory().unwrap();
        assert_eq!(
            (audit.owner.pending_losses, audit.owner.exact_terminals),
            (1, 1)
        );
        assert_eq!(
            (audit.payload.events, audit.payload.capability_values),
            (1, 1)
        );
        assert_eq!(
            audit.recovery.cursors, 0,
            "a terminal tail is not another cursor"
        );
        assert_eq!(
            audit.recovery.acceptance_receipts,
            usize::from((1..=3).contains(&kind))
        );
        assert_eq!(audit.recovery.resumes, usize::from(kind == 3));
        assert_eq!(audit.payload.effect_vectors, usize::from(kind <= 1));
        assert_eq!(
            audit.payload.effect_capacity,
            if kind <= 1 { effect_capacity } else { 0 }
        );
        assert_eq!(
            audit.payload.logical_bytes,
            if kind == 0 || kind == 2 { 4 } else { 0 }
        );
        assert!(!audit.is_exact_zero());
        assert_eq!(error.cold_inventory(), Ok(audit));
        let OwnerTargetError::RejectedTerminalAttachRecoveryDiscard { pending, .. } = &error else {
            unreachable!();
        };
        assert_eq!(std::ptr::from_ref(pending.as_ref()), identity);
        if let TerminalOwnerAttachTail::Drain { pending, ready, .. } = &pending.tail {
            assert_eq!(audit.recovery.pending_capacity, pending.capacity());
            assert_eq!(audit.recovery.ready_capacity, ready.capacity());
            assert_eq!(audit.recovery.output_values, 1);
            assert!(audit.recovery.container_bytes > 0);
        }
    }
}

#[test]
fn real_reducer_replay_and_sink_payloads_count_original_views_not_unique_heap() {
    use crate::resumable::payload_inventory::SessionPayloadInventory;
    let (_executor, handoff, _io, _queue) = checked_owner(0xec, 1);
    let leg = committed(&handoff.attached);
    let mut model = SessionModel::new(SessionRole::Owner, capacity_plan().session_config(), leg);
    let flow_id = SessionFlowId::new(1).unwrap();
    let opened = model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id,
                    target: target(443),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let [SessionEffect::PeerOpenRequested { request, flow, .. }] = opened.as_slice() else {
        panic!("real OPEN provides the reducer's original capabilities");
    };
    let (request, flow) = (*request, *flow);
    model
        .reduce(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::Opened,
        })
        .unwrap();
    let original = Bytes::from_static(b"data");
    let replay = model
        .reduce(SessionEvent::LocalData {
            flow,
            payload: original,
        })
        .unwrap();
    // TCP normalizes caller backing before it publishes either owning view.
    // Capture the original returned receipt, not the pre-reducer allocation.
    let identity = replay
        .iter()
        .find_map(|effect| match effect {
            SessionEffect::ReplayStored { receipt } => Some(receipt.payload().as_ptr()),
            _ => None,
        })
        .unwrap();
    let before = model.snapshot();
    let audit = SessionPayloadInventory::effect_vec(&replay).unwrap();
    assert_eq!((audit.receipt_values, audit.session_frames), (1, 1));
    assert_eq!(
        (audit.byte_views, audit.logical_bytes),
        (2, 8),
        "receipt and transmit retain two views, not two unique allocations"
    );
    assert_eq!(audit.effect_capacity, replay.capacity());
    assert_eq!(SessionPayloadInventory::effect_vec(&replay), Ok(audit));
    for effect in &replay {
        match effect {
            SessionEffect::ReplayStored { receipt } => {
                assert_eq!(receipt.payload().as_ptr(), identity);
                assert_eq!(receipt.payload().as_ref(), b"data");
            }
            SessionEffect::Transmit(frame) => {
                let Record::Data { payload, .. } = frame.record() else {
                    unreachable!()
                };
                assert_eq!(payload.as_ptr(), identity);
                assert_eq!(payload.as_ref(), b"data");
            }
            _ => panic!("only original replay receipt and DATA"),
        }
    }
    assert_eq!(model.snapshot(), before);
    let offered = model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Data {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"sink"),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let [SessionEffect::OfferToSink { segments, .. }] = offered.as_slice() else {
        panic!("real peer DATA produces the original sink offer");
    };
    let backing = segments.as_ptr();
    let audit = SessionPayloadInventory::effect_vec(&offered).unwrap();
    assert_eq!(audit.segment_values, segments.len());
    assert_eq!(audit.segment_capacity, segments.capacity());
    assert_eq!(
        audit.segment_array_bytes,
        std::alloc::Layout::array::<crate::resumable::TcpDataSegment>(segments.capacity())
            .unwrap()
            .size()
    );
    assert_eq!(
        audit.segment_backing_charges,
        segments.iter().map(|s| s.backing_bytes()).sum::<usize>()
    );
    assert_eq!((audit.byte_views, audit.logical_bytes), (1, 4));
    assert_eq!(SessionPayloadInventory::effect_vec(&offered), Ok(audit));
    assert_eq!(segments.as_ptr(), backing);
}
