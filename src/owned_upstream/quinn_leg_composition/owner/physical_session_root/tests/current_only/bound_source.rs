//! Actual Bound source-to-FIFO and cold-Retiring selection tracers.
//!
//! Setup reuses the real OPEN fixture and original initial/seed construction.
//! Only production Root polling dispatches work. These private test operations
//! assert the resident FIFO, flush one frame, and join its same current actor;
//! they never expose resources, reduce owner input or claim whole-root zero.

use super::*;
use crate::owned_upstream::owner_ready::{
    OwnerReadySessionLimits, OwnerReadySource, OwnerReadyUnit,
};
use crate::owned_upstream::owner_target::OwnerTargetOutput;
use crate::owned_upstream::quinn_leg_composition::owner::server_root::{
    OwnerControllerPreparation, OwnerReadyStep, OwnerServerRoot, ReadyProcessPlan,
};
use crate::resumable::{ByteOffset, Direction, Frame, Record, SessionEvent};
use std::task::Poll;

#[tokio::test(flavor = "current_thread")]
async fn bound_active_owner_source_reaches_real_output_once() {
    let PreparedActiveSourceFixture {
        owner,
        mut client,
        mut listener,
        peer,
        attached,
        local_flow,
        payload,
    } = PreparedActiveSourceFixture::prepare().await;
    let session_id = local_flow.session_id();
    let generation = peer.snapshot().session.generation();
    let expected = Frame::try_new(
        generation,
        Record::Data {
            flow_id: local_flow.flow_id(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: payload.clone(),
        },
    )
    .expect("expected DATA uses the actual opened Flow");
    let encoded_bytes = expected.encoded_len_checked().expect("valid DATA length");
    let process_plan = ReadyProcessPlan::checked(1, [1, 1, 1, 1])
        .expect("one actual resident has bounded class slices");
    let mut preparation = match OwnerServerRoot::cold(process_plan) {
        Ok(preparation) => preparation,
        Err(_failure) => panic!("actual process backing must prepare before binding"),
    };
    let PreparedCurrentOnlyOwner {
        actor,
        executor,
        handoff,
        capacity,
    } = owner;
    let limits = OwnerReadySessionLimits::checked(capacity.session_config().max_flows(), 1, 1, 1)
        .expect("positive mailbox limits and one open fit the real session");
    let gate = QuinnLegCompositionGate::mint_for_test();
    let clock = actor.session_clock(&gate);
    let input = OwnerControllerPreparation::new(
        session_id,
        executor,
        clock,
        capacity,
        limits,
        std::time::Duration::from_secs(1),
    );
    let controller = match preparation.build_checked_controller(input) {
        Ok(controller) => controller,
        Err(_failure) => panic!("the real prepared Executor must bind to this process"),
    };
    let (current, join_block) = finish_checked_current_only(actor, controller, handoff);
    match preparation.adopt_current_only(current) {
        Ok(()) => {}
        Err(_failure) => panic!("the same complete CurrentOnly must enter its original domain"),
    }
    let mut root = match preparation.start() {
        Ok(root) => root,
        Err(_failure) => panic!("the single bound position has its actual resident"),
    };
    root.assert_only_bound_fifo_for_test(session_id, 0, 0);
    tokio::time::timeout(TEST_TIMEOUT, async {
        // A timeout alone cannot interrupt an always-Ready tight loop. This
        // finite diagnostic ceiling is not a claimed scheduler delay bound.
        for _ in 0..1024 {
            let step = std::future::poll_fn(|cx| root.poll_ready_unit(cx)).await;
            match step {
                OwnerReadyStep::Visited {
                    session,
                    work: OwnerReadyUnit::OutputAdmitted,
                    ..
                } => {
                    assert_eq!(session, session_id);
                    return;
                }
                OwnerReadyStep::Visited { session, .. } => assert_eq!(session, session_id),
                OwnerReadyStep::Rescan => {}
                OwnerReadyStep::Quarantined => panic!("the real source path must remain healthy"),
            }
        }
        panic!("the real source must reach output within finite charged visits");
    })
    .await
    .expect("real charged selection must admit the original DATA");
    root.assert_only_bound_fifo_for_test(session_id, 1, encoded_bytes);
    root.flush_only_bound_frame_for_test(session_id);

    let received = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match client
                .try_transact_composition_inbound(
                    &gate,
                    STANDBY_CONTROL_V1,
                    |inbound, _arrival, _queue| {
                        let BoundInbound::Session(received) = inbound else {
                            panic!("the peer must receive session DATA");
                        };
                        let SessionEvent::PeerFrame { frame, .. } = attached
                            .accept_frame(received)
                            .expect("DATA retains the exact original peer attachment")
                        else {
                            panic!("the attachment must produce one peer-frame observation");
                        };
                        Ok(frame)
                    },
                )
                .expect("the peer remains physically live")
            {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => panic!("DATA observation must not block"),
                QuinnLegActorInboundPoll::Classified(frame) => break frame,
            }
        }
    })
    .await
    .expect("the actual peer decodes the emitted DATA");
    assert_eq!(received.leg_generation(), generation);
    assert_eq!(received.record(), expected.record());
    assert_eq!(
        received.encoded_len_checked().expect("decoded DATA length"),
        encoded_bytes
    );
    assert_eq!(local_flow.session_id(), session_id);

    // Run only the production selector through its remaining obligations.
    // A parked root plus an empty real FIFO excludes a second admitted DATA.
    tokio::time::timeout(TEST_TIMEOUT, async {
        for _ in 0..1024 {
            let parked = std::future::poll_fn(|cx| match root.poll_ready_unit(cx) {
                Poll::Pending => Poll::Ready(true),
                Poll::Ready(OwnerReadyStep::Quarantined) => {
                    panic!("post-output work must not hide a failure")
                }
                Poll::Ready(OwnerReadyStep::Visited {
                    work: OwnerReadyUnit::OutputAdmitted,
                    ..
                }) => panic!("the original source DATA must be admitted exactly once"),
                Poll::Ready(_) => Poll::Ready(false),
            })
            .await;
            if parked {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the production selector must not spin after finite source work");
    })
    .await
    .expect("the real root parks after finite source work");
    root.assert_only_bound_fifo_for_test(session_id, 0, 0);
    assert_eq!(client.actor_snapshot().queued_inbound_messages(), 0);
    join_block.release();
    let ((), peer_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            root.join_only_bound_current_for_test(session_id),
            client.join()
        )
    })
    .await
    .expect("same-resident structural joins stay bounded");
    let peer_joined = peer_joined.expect("peer actor joins cleanly");
    assert_eq!(peer_joined.owned_messages(), 0);
    assert_eq!(peer_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown stays bounded")
        .expect("physical actor registrations are settled");
}

#[tokio::test(flavor = "current_thread")]
async fn bound_cold_retiring_flow_reaches_actual_selected_release_once() {
    let PreparedActiveSourceFixture {
        mut owner,
        mut client,
        mut listener,
        peer,
        attached,
        local_flow,
        payload,
    } = PreparedActiveSourceFixture::prepare().await;
    let session_id = local_flow.session_id();
    let flow_id = local_flow.flow_id();
    let generation = peer.snapshot().session.generation();

    // Finish the original fixture's DATA before binding. Every protocol frame
    // crosses the real actor FIFO and original attachment; no ready command,
    // terminal proof or Flow backing is injected by the test.
    let outputs = owner
        .executor
        .try_read_target(flow_id)
        .expect("real Target read");
    let frames =
        deliver_retirement_setup_outputs(&mut owner, &mut client, &attached, outputs).await;
    assert!(
        matches!(frames.as_slice(), [frame] if matches!(frame.record(),
        Record::Data { flow_id: actual, direction: Direction::TargetToClient, offset, payload: bytes }
        if *actual == flow_id && *offset == ByteOffset::new(0) && *bytes == payload))
    );
    let data_ack = Frame::try_new(
        generation,
        Record::Ack {
            flow_id,
            direction: Direction::TargetToClient,
            next_accepted: ByteOffset::new(payload.len() as u64),
            final_accepted: false,
        },
    )
    .expect("actual delivered DATA releases its original replay/read credit");
    assert!(
        send_retirement_setup_frame(&mut owner, &mut client, data_ack)
            .await
            .is_empty()
    );

    let held = bytes::Bytes::from_static(b"held");
    for record in [
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: held.clone(),
        },
        Record::Close {
            flow_id,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(held.len() as u64),
        },
    ] {
        let is_close = matches!(record, Record::Close { .. });
        let frame = Frame::try_new(generation, record).expect("actual opened peer Flow");
        let outputs = send_retirement_setup_frame(&mut owner, &mut client, frame).await;
        let frames =
            deliver_retirement_setup_outputs(&mut owner, &mut client, &attached, outputs).await;
        assert!(!frames.is_empty());
        let mut final_ack = false;
        for frame in frames {
            let Record::Ack {
                flow_id: actual,
                direction,
                next_accepted,
                final_accepted,
            } = frame.record()
            else {
                panic!("the original DATA/CLOSE may only acknowledge accepted bytes");
            };
            assert_eq!(*actual, flow_id);
            assert_eq!(*direction, Direction::ClientToTarget);
            assert_eq!(*next_accepted, ByteOffset::new(held.len() as u64));
            final_ack |= *final_accepted;
        }
        assert_eq!(final_ack, is_close);
    }
    owner
        .executor
        .target_mut()
        .finish_read(flow_id)
        .expect("real Target EOF");
    let outputs = owner
        .executor
        .try_read_target(flow_id)
        .expect("original Source EOF");
    let frames =
        deliver_retirement_setup_outputs(&mut owner, &mut client, &attached, outputs).await;
    assert!(
        matches!(frames.as_slice(), [frame] if matches!(frame.record(),
        Record::Close { flow_id: actual, direction: Direction::TargetToClient, final_offset }
        if *actual == flow_id && *final_offset == ByteOffset::new(payload.len() as u64)))
    );
    let final_ack = Frame::try_new(
        generation,
        Record::Ack {
            flow_id,
            direction: Direction::TargetToClient,
            next_accepted: ByteOffset::new(payload.len() as u64),
            final_accepted: true,
        },
    )
    .expect("final ACK covers the real observed CLOSE");
    let outputs = send_retirement_setup_frame(&mut owner, &mut client, final_ack).await;
    let [OwnerTargetOutput::TerminalGraceStarted { terminal }] = outputs.as_slice() else {
        panic!("the actual completed protocol must own exactly one terminal grace");
    };
    assert_eq!(terminal.flow_id(), flow_id);
    assert_eq!(terminal.local_flow().session_id(), session_id);
    assert_eq!(owner.executor.snapshot().target_flows, 1);
    assert_eq!(owner.executor.snapshot().pending_joins, 1);
    assert_eq!(
        owner.executor.target().snapshot().buffered_bytes,
        held.len()
    );
    assert_eq!(owner.executor.target().snapshot().joined_flows, 0);

    // This is an explicit PREBIND expiry of the original proof, not a claim
    // that CurrentOnly installs lifecycle actions or drives their deadlines.
    assert!(
        owner
            .executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: *terminal,
            })
            .expect("original grace expiry preserves the genuinely blocked join")
            .is_empty()
    );
    assert_eq!(owner.executor.snapshot().pending_joins, 1);
    assert_eq!(
        owner
            .executor
            .target_mut()
            .take_written(flow_id, held.len())
            .expect("actual Target consumer releases its original backlog"),
        held
    );
    // Do not retry Executor/join after the byte release: the same cold Flow
    // must be imported and only production Root selection may retire it.
    let snapshot = owner.executor.snapshot();
    assert_eq!(snapshot.target_flows, 1);
    assert_eq!(snapshot.pending_joins, 1);
    // This snapshot counts reducer feedback, not terminal custody. The actual
    // terminal/join stays retained, with no Completion retry to confuse work.
    assert_eq!(snapshot.pending_target_completions, 0);
    assert_eq!(snapshot.pending_effects, 0);
    assert_eq!(snapshot.pending_writes, 0);
    assert_eq!(snapshot.target_read_owned_bytes, 0);
    assert_eq!(snapshot.target_read_owned_segments, 0);
    assert!(!snapshot.needs_resume);
    assert_eq!(owner.executor.target().snapshot().buffered_bytes, 0);
    assert_eq!(owner.executor.target().snapshot().joined_flows, 0);

    let process_plan = ReadyProcessPlan::checked(1, [1, 1, 1, 1]).expect("one real resident");
    let mut preparation = match OwnerServerRoot::cold(process_plan) {
        Ok(preparation) => preparation,
        Err(_) => panic!("actual process backing must prepare"),
    };
    let PreparedCurrentOnlyOwner {
        actor,
        executor,
        handoff,
        capacity,
    } = owner;
    let limits = OwnerReadySessionLimits::checked(capacity.session_config().max_flows(), 1, 1, 1)
        .expect("original checked capacity is unchanged");
    let gate = QuinnLegCompositionGate::mint_for_test();
    let clock = actor.session_clock(&gate);
    let input = OwnerControllerPreparation::new(
        session_id,
        executor,
        clock,
        capacity,
        limits,
        std::time::Duration::from_secs(1),
    );
    let controller = match preparation.build_checked_controller(input) {
        Ok(controller) => controller,
        Err(_) => panic!("the original cold Retiring Flow must install"),
    };
    let (current, join_block) = finish_checked_current_only(actor, controller, handoff);
    match preparation.adopt_current_only(current) {
        Ok(()) => {}
        Err(_) => panic!("the same complete CurrentOnly must activate"),
    }
    let mut root = match preparation.start() {
        Ok(root) => root,
        Err(_) => panic!("actual resident must start"),
    };
    root.assert_only_bound_fifo_for_test(session_id, 0, 0);
    let mut releases = 0;
    tokio::time::timeout(TEST_TIMEOUT, async {
        for _ in 0..1024 {
            let parked = std::future::poll_fn(|cx| match root.poll_ready_unit(cx) {
                Poll::Pending => Poll::Ready(true),
                Poll::Ready(OwnerReadyStep::Quarantined) => {
                    panic!("retirement must not quarantine")
                }
                Poll::Ready(OwnerReadyStep::Visited {
                    session,
                    source,
                    work,
                }) => {
                    assert_eq!(session, session_id);
                    match work {
                        OwnerReadyUnit::SourceRead => {
                            assert_eq!(source, OwnerReadySource::OwnerContinuation);
                            releases += 1;
                        }
                        OwnerReadyUnit::Waiting | OwnerReadyUnit::Registered => {}
                        other => {
                            panic!("cold empty terminal has no other source/output work: {other:?}")
                        }
                    }
                    Poll::Ready(false)
                }
                Poll::Ready(OwnerReadyStep::Rescan) => Poll::Ready(false),
            })
            .await;
            if parked {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("finite terminal work must park, not spin");
    })
    .await
    .expect("real Root selection remains bounded");
    // Empty terminal lanes produce Hint, not SourceRead. With no live reads,
    // failure work, output or receipts, this sole observed unit is the actual
    // RetirementReleased result after original source/join/metadata settlement.
    assert_eq!(
        releases, 1,
        "the actual Root must select and release its cold Retiring Flow exactly once"
    );
    root.assert_only_bound_fifo_for_test(session_id, 0, 0);
    assert_eq!(client.actor_snapshot().queued_inbound_messages(), 0);
    join_block.release();
    let ((), peer_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            root.join_only_bound_current_for_test(session_id),
            client.join()
        )
    })
    .await
    .expect("same-resident structural joins stay bounded");
    let peer_joined = peer_joined.expect("actual peer actor joins cleanly");
    assert_eq!(peer_joined.owned_messages(), 0);
    assert_eq!(peer_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown stays bounded")
        .expect("physical registrations are settled");
}

// These two child-private adapters are prebind protocol setup only. They
// transfer real frames through existing actors/attachments and call the
// original Executor; they contain no scheduler, reducer or cleanup policy.
async fn send_retirement_setup_frame(
    owner: &mut PreparedCurrentOnlyOwner,
    client: &mut StartedQuinnLegActor,
    frame: Frame,
) -> Vec<OwnerTargetOutput> {
    let gate = QuinnLegCompositionGate::mint_for_test();
    client
        .with_controller_parts_mut(&gate, |_io, queue| queue.push(frame))
        .expect("original peer FIFO")
        .expect("bounded setup frame");
    assert_eq!(
        client.try_flush(SimTime::ZERO).expect("actual peer flush"),
        QuinnLegActorFlushDisposition::Submitted
    );
    let outputs = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner
                .actor
                .try_transact_composition_inbound(
                    &gate,
                    STANDBY_CONTROL_V1,
                    |inbound, _arrival, _queue| {
                        let BoundInbound::Session(received) = inbound else {
                            panic!("real session frame")
                        };
                        let event = owner
                            .handoff
                            .accept_frame(received)
                            .expect("original owner attachment");
                        Ok(owner
                            .executor
                            .apply_event(event)
                            .expect("original prebind reducer"))
                    },
                )
                .expect("actual owner receive")
            {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => panic!("prebind setup must not block"),
                QuinnLegActorInboundPoll::Classified(outputs) => break outputs,
            }
        }
    })
    .await
    .expect("real frame reaches owner");
    wait_ordered_delivery(client).await;
    outputs
}

async fn deliver_retirement_setup_outputs(
    owner: &mut PreparedCurrentOnlyOwner,
    client: &mut StartedQuinnLegActor,
    attached: &crate::owned_upstream::leg::AttachedLeg,
    outputs: Vec<OwnerTargetOutput>,
) -> Vec<Frame> {
    let gate = QuinnLegCompositionGate::mint_for_test();
    let mut frames = Vec::new();
    for output in outputs {
        let OwnerTargetOutput::Transmit(frame) = output else {
            panic!("setup must not discard lifecycle or continuation output");
        };
        let expected = frame.record().clone();
        owner
            .actor
            .with_controller_parts_mut(&gate, |_io, queue| queue.push(frame))
            .expect("original owner FIFO")
            .expect("actual response admission");
        assert_eq!(
            owner
                .actor
                .try_flush(SimTime::ZERO)
                .expect("real response flush"),
            QuinnLegActorFlushDisposition::Submitted
        );
        let received = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match client
                    .try_transact_composition_inbound(
                        &gate,
                        STANDBY_CONTROL_V1,
                        |inbound, _arrival, _queue| {
                            let BoundInbound::Session(received) = inbound else {
                                panic!("real response")
                            };
                            let SessionEvent::PeerFrame { frame, .. } = attached
                                .accept_frame(received)
                                .expect("original peer attachment")
                            else {
                                panic!("physical response must yield actual peer frame");
                            };
                            Ok(frame)
                        },
                    )
                    .expect("actual peer receive")
                {
                    QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                    QuinnLegActorInboundPoll::Blocked => panic!("response must not block"),
                    QuinnLegActorInboundPoll::Classified(frame) => break frame,
                }
            }
        })
        .await
        .expect("real response reaches peer");
        assert_eq!(received.record(), &expected);
        frames.push(received);
        wait_ordered_delivery(&mut owner.actor).await;
    }
    frames
}

impl OwnerPhysicalSessionRoot<MemoryTarget> {
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn assert_fifo_for_bound_test(
        &mut self,
        frames: usize,
        bytes: usize,
    ) {
        let Self::CurrentOnly(current) = self;
        let gate = QuinnLegCompositionGate::mint_for_test();
        current
            .actor
            .actor
            .with_controller_parts_mut(&gate, |_io, queue| {
                let inventory = queue.cold_inventory().expect("real FIFO must sum exactly");
                assert!(inventory.was_active());
                assert_eq!(inventory.queued_frames(), frames);
                assert_eq!(inventory.summed_frame_bytes(), bytes);
                assert_eq!(inventory.stored_owned_bytes(), bytes);
                assert!(!inventory.ordered_delivery_wait());
            })
            .expect("inspection borrows the same resident actor/FIFO");
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn flush_one_for_bound_test(
        &mut self,
    ) {
        let Self::CurrentOnly(current) = self;
        assert_eq!(
            current
                .actor
                .actor
                .try_flush(SimTime::ZERO)
                .expect("one real DATA flush"),
            QuinnLegActorFlushDisposition::Submitted,
        );
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) async fn join_for_bound_test(
        &mut self,
    ) {
        let joined = self
            .join_current_for_shutdown()
            .await
            .expect("same CurrentOnly retains its complete structural result");
        assert_eq!(joined.actor_owned_messages(), 0);
        assert_eq!(joined.actor_owned_bytes(), 0);
        assert_eq!(joined.retained_executors(), 1);
        assert_eq!(joined.retained_controllers(), 1);
        assert_eq!(joined.retained_handoffs(), 1);
        assert_eq!(joined.retained_output_roots(), 1);
        assert!(!joined.is_whole_root_exact_zero());
    }
}
