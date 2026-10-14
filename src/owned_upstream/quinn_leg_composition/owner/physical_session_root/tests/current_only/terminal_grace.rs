//! Real checked C/CurrentOnly terminal expiry and carrying Completion.
//!
//! The private input Adapter moves actual peer deliveries into the existing C
//! guard. After input setup only the original ReadyIndex selects physical
//! service; no test invokes expiry or wakes the retained timer/Completion.
//! This does not claim the later process ingress route or whole-root shutdown.

use super::*;
use crate::owned_upstream::owner_ready::{OwnerReadyUnit, SessionCommandVisit};
use crate::owned_upstream::owner_session_controller::OwnerSessionActorEvent;
use crate::owned_upstream::owner_target::OwnerTargetOutput;
use crate::resumable::{
    ByteOffset, Direction, Frame, Record, ResetReason, SessionEvent, SessionFlowId,
};
use std::task::{Context, Poll, Waker};

mod pressure;

#[test]
fn terminal_service_units_use_actual_process_and_session_capacity() {
    use crate::owned_upstream::quinn_leg_composition::owner::server_root::ReadyProcessPlan;
    let session = owner_capacity_plan();
    let process = ReadyProcessPlan::checked(1, [1; 4]).unwrap();
    // Session C=1: (ceil(1/1)+1)*sum([1;4]) = 8 quanta.
    assert_eq!(
        process.terminal_service_units(session),
        Some([1560, 6240, 6240, 21840])
    );
    let overflow = ReadyProcessPlan::checked(1, [usize::MAX / 128; 4]).unwrap();
    assert_eq!(overflow.terminal_service_units(session), None);
}

async fn prepare_one_z_fixture() -> PreparedCurrentOnlyFixture {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
            TEST_TIMEOUT,
        )
        .expect("one exact initial-A actor fits"),
    )
    .await
    .expect("bind CurrentOnly listener");
    let (mut client, mut owner) = connect_actor_pair(&mut listener).await;
    let gate = QuinnLegCompositionGate::mint_for_test();

    let (awaiting, binding) = client
        .with_controller_parts_mut(&gate, |io, queue| {
            let binding = io.established_leg().standby_transport_binding();
            let request = AttachRequest::new(
                SessionId::new([0x93; 16]).expect("nonzero session id"),
                LegGeneration::new(2).expect("nonzero initial generation"),
                AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
                VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
                    .expect("single supported version"),
                FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
                    .expect("standby-control offer"),
            );
            let proof = attach_credentials()
                .prove(&request, &binding)
                .expect("initial attach proof");
            let awaiting = io
                .established_leg()
                .begin_attach(request)
                .bind_initial_frame(request.to_attach_frame(proof))
                .expect("bind exact initial frame")
                .enqueue(queue)
                .expect("initial request enters the exact FIFO");
            (awaiting, binding)
        })
        .expect("client actor retains its exact LegIo/FIFO");
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("initial attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let capacity_plan =
        OwnerSessionCapacityPlan::derive(owner_capacity_plan().replay_storage(), 1, 64).unwrap();
    let mut pending_owner = Some(SessionSupervisor::prepare_owner(
        capacity_plan,
        attach_authority(binding),
    ));
    let owner_bootstrap = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner
                .try_transact_controller_inbound_with_parts(
                    &gate,
                    &mut pending_owner,
                    |_pending, _io, _queue| Ok(QuinnLegActorInboundPreflight::Ready(())),
                    |pending, (), envelope, io, queue| {
                        let (delivery, _arrived_at) = envelope.into_parts();
                        let inbound = io
                            .receive_classified_delivery(delivery, STANDBY_CONTROL_V1)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol("initial attach classification failed")
                            })?;
                        let BoundInbound::Session(received) = inbound else {
                            return Err(QuinnLegActorError::Protocol(
                                "initial attach was misclassified as leg control",
                            ));
                        };
                        pending
                            .take()
                            .ok_or(QuinnLegActorError::Protocol(
                                "initial owner bootstrap was already consumed",
                            ))?
                            .accept(io.established_leg(), received, queue)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol("initial owner authentication failed")
                            })?
                            .enqueue_acceptance(queue)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol(
                                    "initial owner acceptance enqueue failed",
                                )
                            })
                    },
                )
                .expect("owner actor initial transaction remains live")
            {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => {
                    panic!("initial owner transaction unexpectedly blocked")
                }
                QuinnLegActorInboundPoll::Classified(bootstrap) => break bootstrap,
            }
        }
    })
    .await
    .expect("owner initial attach transaction is bounded");
    wait_ordered_delivery(&mut client).await;
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("initial acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let mut awaiting = Some(awaiting);
    let accepted_client = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match client
                .try_transact_composition_inbound(
                    &gate,
                    STANDBY_CONTROL_V1,
                    |inbound, _arrived_at, queue| {
                        let BoundInbound::Session(response) = inbound else {
                            return Err(QuinnLegActorError::Protocol(
                                "initial acceptance was misclassified as leg control",
                            ));
                        };
                        awaiting
                            .take()
                            .ok_or(QuinnLegActorError::Protocol(
                                "initial client acceptance was already consumed",
                            ))?
                            .validate_response(queue, response)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol(
                                    "initial client acceptance validation failed",
                                )
                            })
                    },
                )
                .expect("client acceptance transaction remains live")
            {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => {
                    panic!("client acceptance transaction unexpectedly blocked")
                }
                QuinnLegActorInboundPoll::Classified(accepted) => break accepted,
            }
        }
    })
    .await
    .expect("client initial acceptance is bounded");
    wait_ordered_delivery(&mut owner).await;

    let required_pending_effects = capacity_plan
        .session_config()
        .owner_pending_work_bound()
        .expect("owner pending-work bound is representable");
    let (executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(64, required_pending_effects, 64)
            .expect("bounded owner Target limits"),
        owner_bootstrap,
        MemoryTarget::new(
            MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
                .expect("bounded memory Target")
                .with_max_terminal_tombstones(1)
                .expect("matching terminal-tombstone capacity"),
        ),
        FlowPortConfig::new(64, 8, 8, 8).expect("bounded owner source"),
    )
    .expect("whole initial bootstrap installs without decomposition");
    PreparedCurrentOnlyFixture {
        owner: PreparedCurrentOnlyOwner {
            actor: owner,
            executor,
            handoff,
            capacity: capacity_plan,
        },
        client,
        listener,
        accepted_client,
    }
}

async fn setup_frame(
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

async fn setup_outputs(
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

struct TimedFixture {
    root: OwnerPhysicalSessionRoot<MemoryTarget>,
    client: StartedQuinnLegActor,
    listener: QuinnLegActorListener,
    attached: crate::owned_upstream::leg::AttachedLeg,
    ready: ReadyIndex,
    join_block: QuinnLegActorJoinBlock,
    generation: LegGeneration,
}

impl TimedFixture {
    async fn prepare() -> Self {
        let PreparedCurrentOnlyFixture {
            mut owner,
            mut client,
            listener,
            accepted_client,
        } = prepare_one_z_fixture().await;
        let gate = QuinnLegCompositionGate::mint_for_test();
        let bootstrap = client
            .with_controller_parts_mut_input(&gate, accepted_client, |accepted, _io, queue| {
                SessionSupervisor::bootstrap_client_for_test(
                    owner.capacity.session_config(),
                    accepted,
                    queue,
                )
            })
            .unwrap()
            .unwrap();
        let (peer, attached) = bootstrap.into_parts();
        let generation = peer.snapshot().session.generation();
        for id in [1, 2] {
            let frame = Frame::try_new(
                generation,
                Record::Open {
                    flow_id: SessionFlowId::new(id).unwrap(),
                    target: crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap()),
                },
            )
            .unwrap();
            let outputs = setup_frame(&mut owner, &mut client, frame).await;
            let frames = setup_outputs(&mut owner, &mut client, &attached, outputs).await;
            assert!(
                matches!(frames.as_slice(), [frame] if matches!(frame.record(), Record::OpenResult { .. }))
            );
        }
        // Prepare actual EOF, but let the bound selected read consume it and
        // close its own readiness phase before acknowledging the emitted CLOSE.
        let b = SessionFlowId::new(2).unwrap();
        owner.executor.target_mut().finish_read(b).unwrap();

        let mut ready = ReadyIndex::cold(
            &ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap(),
        )
        .unwrap();
        let PreparedCurrentOnlyOwner {
            actor,
            executor,
            handoff,
            capacity,
        } = owner;
        let clock = actor.session_clock(&gate);
        let reservation = ready
            .reserve_session(
                SessionId::new([0x93; 16]).unwrap(),
                OwnerReadySessionLimits::checked(capacity.session_config().max_flows(), 1, 8, 8)
                    .unwrap(),
            )
            .unwrap();
        let controller = OwnerSessionController::new_checked(
            executor,
            clock,
            capacity,
            reservation,
            Duration::from_millis(100),
        )
        .unwrap();
        let (mut root, join_block) = finish_checked_current_only(actor, controller, handoff);
        assert!(root.activate_owner_ready(&mut ready, 0).is_ok());
        let mut fixture = Self {
            root,
            client,
            listener,
            attached,
            ready,
            join_block,
            generation,
        };
        let frames = fixture.drain().await;
        assert!(matches!(frames.as_slice(), [frame] if matches!(
            frame.record(), Record::Close { flow_id, .. } if *flow_id == b
        )));
        fixture
            .input(Record::Ack {
                flow_id: b,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: true,
            })
            .await;
        assert!(fixture.drain().await.is_empty());
        fixture
    }

    async fn input(&mut self, record: Record) {
        let gate = QuinnLegCompositionGate::mint_for_test();
        self.client
            .with_controller_parts_mut(&gate, |_io, queue| {
                queue.push(Frame::try_new(self.generation, record).unwrap())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            self.client.try_flush(SimTime::ZERO).unwrap(),
            QuinnLegActorFlushDisposition::Submitted
        );
        let input = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut self.root;
                let clock = current.actor.actor.session_clock(&gate);
                let received = current.actor.actor.try_transact_composition_inbound(
                    &gate, STANDBY_CONTROL_V1, |inbound, arrival, _queue| {
                        let BoundInbound::Session(received) = inbound else { panic!("actual peer frame") };
                        let event = current.handoff.accept_frame(received).unwrap();
                        Ok(OwnerSessionActorEvent::from_quinn_actor(
                            event,
                            crate::owned_upstream::quinn_leg_composition::owner::QuinnOwnerActorArrival::from_actor_capture(&clock, arrival, &gate),
                            &gate,
                        ))
                    },
                ).unwrap();
                match received {
                    QuinnLegActorInboundPoll::Classified(input) => break input,
                    QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                    QuinnLegActorInboundPoll::Blocked => panic!("input adapter must retain a legal peer input"),
                }
            }
        }).await.unwrap();
        wait_ordered_delivery(&mut self.client).await;
        // Only this explicit new peer input uses the test ingress Adapter. The
        // expiry/pressure-relief interval below never calls this method.
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut self.root;
        let OwnerPhysicalReadyState::Active { output, .. } = &current.ready else {
            panic!("active original root")
        };
        output.wake_by_ref();
        let slot = self.ready.select(3).unwrap().unwrap();
        let claim = self.ready.claim(slot).unwrap();
        let publication = current.output.begin_ready_publication().unwrap();
        let drive = current
            .output
            .begin_inbound_admission()
            .expect("the actual terminal action must install before the next legal input")
            .apply_actor_event_and_admit(input);
        assert!(drive.is_ok(), "real C ordinary admission");
        let completed = SessionCommandVisit {
            work: OwnerReadyUnit::SourceRead,
            runnable: 1 << 4,
            blocked: None,
        };
        assert!(publication.finish(&completed).is_ok());
        self.ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
    }

    fn selected_step(&mut self) -> bool {
        let Some(slot) = self.ready.select(3).unwrap() else {
            return false;
        };
        let claim = self.ready.claim(slot).unwrap();
        let completed = self
            .root
            .visit_ready_source(claim.source, &mut Context::from_waker(Waker::noop()))
            .unwrap_or_else(|()| {
                let OwnerPhysicalSessionRoot::CurrentOnly(current) = &self.root;
                let OwnerPhysicalReadyState::Active { failure, .. } = &current.ready else {
                    panic!("inactive")
                };
                match failure {
                    Some(service::OwnerPhysicalServiceFailure::Actor(error)) => {
                        panic!("actor: {error:?}; C={:?}", current.output.snapshot())
                    }
                    Some(service::OwnerPhysicalServiceFailure::Audit(error)) => {
                        panic!("audit: {error:?}")
                    }
                    Some(service::OwnerPhysicalServiceFailure::Queue(error)) => {
                        panic!("queue: {error:?}")
                    }
                    Some(service::OwnerPhysicalServiceFailure::Controller) => {
                        panic!("controller: {:?}", current.output.snapshot())
                    }
                    _ => panic!("selected physical service invariant"),
                }
            });
        self.root.finish_ready_source(&completed).unwrap();
        self.ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        true
    }

    async fn peer_frames(&mut self) -> Vec<Frame> {
        let gate = QuinnLegCompositionGate::mint_for_test();
        let mut frames = Vec::new();
        for _ in 0..32 {
            match self
                .client
                .try_transact_composition_inbound(
                    &gate,
                    STANDBY_CONTROL_V1,
                    |inbound, _arrival, _queue| {
                        let BoundInbound::Session(received) = inbound else {
                            panic!("actual session output")
                        };
                        let SessionEvent::PeerFrame { frame, .. } =
                            self.attached.accept_frame(received).unwrap()
                        else {
                            panic!("actual attached peer output")
                        };
                        Ok(frame)
                    },
                )
                .unwrap()
            {
                QuinnLegActorInboundPoll::Classified(frame) => frames.push(frame),
                QuinnLegActorInboundPoll::Idle => break,
                QuinnLegActorInboundPoll::Blocked => panic!("output observation cannot block"),
            }
        }
        frames
    }

    async fn drain(&mut self) -> Vec<Frame> {
        let mut frames = Vec::new();
        // Finite diagnostic ceiling, not the unimplemented process-wide bound.
        for _ in 0..512 {
            let worked = self.selected_step();
            frames.extend(self.peer_frames().await);
            tokio::task::yield_now().await;
            if !worked && self.ready.select(3).unwrap().is_none() {
                return frames;
            }
        }
        panic!("finite selected work must park");
    }

    async fn join_actor_only(mut self) {
        self.join_block.release();
        let (joined, peer) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(self.root.join_current_for_shutdown(), self.client.join())
        })
        .await
        .unwrap();
        let joined = joined.unwrap();
        assert_eq!(
            (joined.actor_owned_messages(), joined.actor_owned_bytes()),
            (0, 0)
        );
        assert!(!joined.is_whole_root_exact_zero());
        let peer = peer.unwrap();
        assert_eq!((peer.owned_messages(), peer.owned_bytes()), (0, 0));
        tokio::time::timeout(TEST_TIMEOUT, self.listener.shutdown())
            .await
            .unwrap()
            .unwrap();
    }

    fn observations(&self) -> usize {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &self.root;
        let snapshot = current.output.snapshot();
        assert!(!snapshot.failed && !snapshot.session_expired);
        snapshot.retained_observations
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bound_terminal_expiry_releases_z_and_resumes_original_completion() {
    let mut fixture = TimedFixture::prepare().await;
    let a = SessionFlowId::new(1).unwrap();
    let b = SessionFlowId::new(2).unwrap();
    fixture
        .input(Record::Reset {
            flow_id: a,
            reason: ResetReason::TargetFailure,
        })
        .await;
    assert!(
        fixture.drain().await.is_empty(),
        "peer RESET emits no synthetic ACK"
    );
    assert_eq!(
        fixture.observations(),
        1,
        "C owns the original A terminal fact"
    );
    fixture
        .input(Record::Close {
            flow_id: b,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        })
        .await;
    let before = fixture.drain().await;
    assert!(
        before
            .iter()
            .all(|frame| matches!(frame.record(), Record::Ack {
        flow_id, direction: Direction::ClientToTarget, next_accepted, final_accepted: false,
    } if *flow_id == b && *next_accepted == ByteOffset::new(0)))
    );
    assert_eq!(
        fixture.observations(),
        1,
        "full Z retains B's carrying completion, not a second terminal"
    );

    // All real I/O setup is finished. Drive the actual registered timer using
    // virtual time; no expiry call, queue mutation, ready bit or manual wake.
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::time::resume();
    let after = tokio::time::timeout(TEST_TIMEOUT, async {
        let mut frames = Vec::new();
        loop {
            frames.extend(fixture.drain().await);
            if frames.iter().any(|frame| {
                matches!(frame.record(), Record::Ack {
                flow_id, direction: Direction::ClientToTarget, next_accepted, final_accepted: true,
            } if *flow_id == b && *next_accepted == ByteOffset::new(0))
            }) {
                break frames;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut fixture.root;
        let plan = current.output.ready_service_plan(true, &current.terminal_timer).unwrap();
        panic!("actual expiry/final ACK: {error:?}; C={:?}; sources={} install={} timer={} frame={} requested={} eligible={}",
            current.output.snapshot(), plan.sources, plan.terminal_install, plan.terminal_timer,
            plan.output_frame, plan.requested, plan.eligible);
    });
    assert_eq!(
        after
            .iter()
            .filter(|frame| matches!(
                frame.record(),
                Record::Ack {
                    final_accepted: true,
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(
        fixture.observations(),
        1,
        "successor B replaces settled A; not false zero"
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::time::resume();
    // advance() does not await timer-driver delivery. Observe actual selected
    // work until the same owner settles; never supply a timer wake or expiry.
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            assert!(
                fixture.drain().await.is_empty(),
                "successor cannot repeat the ACK"
            );
            if fixture.observations() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original successor expiry must be selected and settle");
    assert_eq!(
        fixture.observations(),
        0,
        "both exact terminal facts settle once"
    );
    fixture.join_actor_only().await;
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_read_closure_clears_reset_wake_and_preserves_independent_flow_progress() {
    // prepare() registers A's real Read Waker and drives B's actual EOF.
    let mut fixture = TimedFixture::prepare().await;
    let a = SessionFlowId::new(1).unwrap();
    let b = SessionFlowId::new(2).unwrap();
    fixture
        .input(Record::Reset {
            flow_id: a,
            reason: ResetReason::TargetFailure,
        })
        .await;
    // Real Target RESET wakes A before FlowFinished closes derived reads.
    assert!(fixture.drain().await.is_empty());
    assert_eq!(fixture.observations(), 1);
    fixture
        .input(Record::Close {
            flow_id: b,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        })
        .await;
    let frames = fixture.drain().await;
    assert!(matches!(frames.as_slice(), [frame] if matches!(
        frame.record(), Record::Ack {
            flow_id, direction: Direction::ClientToTarget, next_accepted,
            final_accepted: false,
        } if *flow_id == b && *next_accepted == ByteOffset::new(0)
    )));
    assert_eq!(fixture.observations(), 1);
    // Actor cleanup is exact; the full consumer test still owes A/Z expiry.
    fixture.join_actor_only().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_observer_cancellation_retains_pending_terminal_and_timer() {
    let mut fixture = TimedFixture::prepare().await;
    fixture
        .input(Record::Reset {
            flow_id: SessionFlowId::new(1).unwrap(),
            reason: ResetReason::TargetFailure,
        })
        .await;
    assert!(fixture.drain().await.is_empty());
    assert_eq!(fixture.observations(), 1);
    // Start both physical cancellations without yielding to a peer-close race.
    // The original actor join block keeps the root's observation pending.
    fixture.client.request_cancel();
    {
        let observation = fixture.root.join_current_for_shutdown();
        tokio::pin!(observation);
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = observation.as_mut() => panic!("blocked root join cannot complete"),
                _ = fixture.join_block.wait_until_blocked() => {}
            }
        })
        .await
        .expect("real join must reach its retained block");
    }
    let OwnerPhysicalSessionRoot::CurrentOnly(current) = &fixture.root;
    assert!(matches!(
        current.actor.phase,
        OwnerPhysicalCurrentActorPhase::Joining { .. }
    ));
    assert_eq!(current.terminal_timer.inventory(), (1, 1));
    assert_eq!(fixture.observations(), 1);
    let admitted = fixture.root.current_only_inventory().unwrap();
    assert_eq!(
        (
            admitted.retained_timer_registrations,
            admitted.pending_terminal_timers
        ),
        (1, 1)
    );
    assert!(!admitted.is_whole_root_exact_zero());
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::time::resume();
    let OwnerPhysicalSessionRoot::CurrentOnly(current) = &fixture.root;
    assert_eq!(current.terminal_timer.inventory(), (1, 1));
    assert_eq!(fixture.observations(), 1);
    // No further ordinary service, manual expiry or timer wake during shutdown.
    // Re-enter the same actual join; actor-only zero cannot settle C's terminal.
    fixture.join_actor_only().await;
}
