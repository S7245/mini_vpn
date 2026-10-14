//! Real opened-Flow preparation for the Active owner-source vertical.
//!
//! Owns only protocol fixture setup and constructor characterization. It does
//! not implement bound scheduling, source reduction, or whole-session cleanup.
//! OPEN and OPEN_RESULT cross the actual actor FIFOs before Target data is fed;
//! the original CurrentOnly constructor must retain its unchanged preflight.

use super::*;
use crate::owned_upstream::owner_target::OwnerTargetOutput;
use crate::owned_upstream::target::TargetReadFeedCompletion;
use crate::resumable::{OpenResultCode, Record, SessionEffect};
use crate::shared::TargetAddr;
use bytes::Bytes;

impl PreparedActiveSourceFixture {
    pub(super) async fn prepare() -> Self {
        let PreparedCurrentOnlyFixture {
            mut owner,
            mut client,
            listener,
            accepted_client,
        } = prepare_current_only_fixture().await;
        let gate = QuinnLegCompositionGate::mint_for_test();
        let bootstrap = client
            .with_controller_parts_mut_input(&gate, accepted_client, |accepted, _io, queue| {
                SessionSupervisor::bootstrap_client_for_test(
                    owner.capacity.session_config(),
                    accepted,
                    queue,
                )
            })
            .expect("client bootstrap uses its original physical FIFO")
            .expect("delivered initial acceptance activates the actual client");
        let (mut peer, attached) = bootstrap.into_parts();
        let effects = peer
            .apply_event(
                attached.local_open_event(TargetAddr::IpPort(
                    "127.0.0.1:443"
                        .parse()
                        .expect("deterministic Target address"),
                )),
            )
            .expect("the real client reducer creates one OPEN");
        let mut local_flow = None;
        let mut open_frames = 0;
        for effect in effects {
            match effect {
                SessionEffect::LocalFlowOpened { flow } => {
                    assert!(local_flow.replace(flow).is_none());
                }
                SessionEffect::Transmit(frame) => {
                    assert!(matches!(frame.record(), Record::Open { .. }));
                    client
                        .with_controller_parts_mut(&gate, |_io, queue| queue.push(frame))
                        .expect("client retains the exact FIFO")
                        .expect("actual OPEN enters the FIFO");
                    open_frames += 1;
                }
                other => panic!("unexpected real OPEN effect: {other:?}"),
            }
        }
        assert_eq!(open_frames, 1);
        let local_flow = local_flow.expect("one real LocalFlow accompanies the OPEN");
        let flow_id = local_flow.flow_id();
        assert_eq!(
            client.try_flush(SimTime::ZERO).expect("client OPEN flush"),
            QuinnLegActorFlushDisposition::Submitted,
        );

        let mut outputs = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match owner
                    .actor
                    .try_transact_composition_inbound(
                        &gate,
                        STANDBY_CONTROL_V1,
                        |inbound, _arrival, _queue| {
                            let BoundInbound::Session(received) = inbound else {
                                panic!("OPEN must be a session record");
                            };
                            let event = owner
                                .handoff
                                .accept_frame(received)
                                .expect("original initial handoff validates the physical OPEN");
                            Ok(owner
                                .executor
                                .apply_event(event)
                                .expect("real owner reducer opens the actual Target"))
                        },
                    )
                    .expect("owner OPEN receive stays live")
                {
                    QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                    QuinnLegActorInboundPoll::Blocked => {
                        panic!("OPEN fixture unexpectedly blocked")
                    }
                    QuinnLegActorInboundPoll::Classified(outputs) => break outputs,
                }
            }
        })
        .await
        .expect("real OPEN reaches the owner within the local bound");
        assert_eq!(
            outputs.len(),
            1,
            "all OPEN outputs must be explicitly settled"
        );
        let OwnerTargetOutput::Transmit(response) = outputs.pop().expect("one OPEN output") else {
            panic!("fixture must not discard a continuation or lifecycle output");
        };
        assert!(
            matches!(response.record(), Record::OpenResult { flow_id: actual, result }
            if *actual == flow_id && *result == OpenResultCode::Opened)
        );
        owner
            .actor
            .with_controller_parts_mut(&gate, |_io, queue| queue.push(response))
            .expect("owner retains the same FIFO")
            .expect("real OPEN_RESULT enters the owner FIFO");
        assert_eq!(
            owner
                .actor
                .try_flush(SimTime::ZERO)
                .expect("OPEN_RESULT flush"),
            QuinnLegActorFlushDisposition::Submitted,
        );
        let resolved = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match client
                    .try_transact_composition_inbound(
                        &gate,
                        STANDBY_CONTROL_V1,
                        |inbound, _arrival, _queue| {
                            let BoundInbound::Session(received) = inbound else {
                                panic!("OPEN_RESULT must be a session record");
                            };
                            let event = attached
                                .accept_frame(received)
                                .expect("OPEN_RESULT uses the original client leg");
                            Ok(peer
                                .apply_event(event)
                                .expect("client applies actual OPEN_RESULT"))
                        },
                    )
                    .expect("client OPEN_RESULT receive stays live")
                {
                    QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                    QuinnLegActorInboundPoll::Blocked => panic!("OPEN_RESULT fixture blocked"),
                    QuinnLegActorInboundPoll::Classified(effects) => break effects,
                }
            }
        })
        .await
        .expect("client applies OPEN_RESULT within the local bound");
        assert!(
            matches!(resolved.as_slice(), [SessionEffect::LocalOpenResolved { flow_id: actual, result }]
            if *actual == flow_id && *result == OpenResultCode::Opened)
        );
        wait_ordered_delivery(&mut client).await;
        wait_ordered_delivery(&mut owner.actor).await;

        let payload = Bytes::from_static(b"hello");
        assert_eq!(
            owner
                .executor
                .target_mut()
                .feed_read(flow_id, payload.clone())
                .expect("real opened Target accepts bounded input"),
            TargetReadFeedCompletion::Queued {
                bytes: payload.len()
            },
        );
        let target = owner.executor.target().snapshot();
        assert_eq!(target.open_attempts, 1);
        assert_eq!(target.opened_flows, 1);
        assert_eq!(target.live_flows, 1);
        assert_eq!(target.buffered_bytes, payload.len());
        assert_eq!(target.read_calls, 0);
        let executor = owner.executor.snapshot();
        assert_eq!(executor.target_flows, 1);
        assert_eq!(executor.pending_effects, 0);
        assert_eq!(executor.pending_writes, 0);
        assert_eq!(executor.pending_joins, 0);
        assert_eq!(executor.pending_target_completions, 0);
        assert_eq!(executor.aborted_effect_guards, 0);
        assert_eq!(executor.target_read_owned_bytes, 0);
        assert_eq!(executor.target_read_owned_segments, 0);
        assert!(!executor.needs_resume);

        Self {
            owner,
            client,
            listener,
            peer,
            attached,
            local_flow,
            payload,
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn preopened_target_data_enters_current_only_without_being_read() {
    let mut ready_index =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let PreparedActiveSourceFixture {
        owner,
        mut client,
        mut listener,
        peer: _peer,
        attached: _attached,
        local_flow: _local_flow,
        payload: _payload,
    } = PreparedActiveSourceFixture::prepare().await;
    let (mut root, join_block) = finish_current_only(owner, &mut ready_index);
    let inventory = root
        .current_only_inventory()
        .expect("original preflight remains valid");
    assert_eq!(inventory.retained_executors(), 1);
    assert_eq!(inventory.retained_controllers(), 1);
    assert_eq!(inventory.actor_fifo_messages(), 0);
    assert_eq!(inventory.actor_fifo_bytes(), 0);
    assert!(inventory.controller_output_is_exact_zero());
    assert!(!inventory.is_whole_root_exact_zero());
    join_block.release();
    let (joined, peer_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join_current_for_shutdown(), client.join())
    })
    .await
    .expect("structural actor joins are bounded");
    let joined = joined.expect("owner actor joins without dropping CurrentOnly");
    assert_eq!(joined.actor_owned_messages(), 0);
    assert_eq!(joined.actor_owned_bytes(), 0);
    assert_eq!(joined.retained_executors(), 1);
    assert_eq!(joined.retained_controllers(), 1);
    assert_eq!(joined.retained_handoffs(), 1);
    assert_eq!(joined.retained_output_roots(), 1);
    assert!(!joined.is_whole_root_exact_zero());
    let peer_joined = peer_joined.expect("peer actor joins cleanly");
    assert_eq!(peer_joined.owned_messages(), 0);
    assert_eq!(peer_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("actor slot is settled");
}
