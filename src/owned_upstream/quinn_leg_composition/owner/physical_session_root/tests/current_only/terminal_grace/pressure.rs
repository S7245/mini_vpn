//! Actual actor-capacity pressure across an original terminal deadline.
//!
//! Owns one test and its bounded driver/readonly observations. The unchanged
//! TimedFixture supplies the real C/actor/Target route; the existing writer gate
//! holds original outbound permits. No timer, ready bit or queue is fabricated.
//! This is not process-budget, concrete TCP or whole-root shutdown acceptance.

use super::*;

fn outbound(fixture: &TimedFixture) -> (usize, usize) {
    let OwnerPhysicalSessionRoot::CurrentOnly(current) = &fixture.root;
    let snapshot = current.actor.actor.actor_snapshot();
    (
        snapshot.outbound_owned_messages(),
        snapshot.outbound_owned_bytes(),
    )
}

fn fifo(fixture: &mut TimedFixture) -> (usize, usize, usize, bool) {
    let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut fixture.root;
    current
        .actor
        .actor
        .with_controller_parts_mut(&QuinnLegCompositionGate::mint_for_test(), |_io, queue| {
            let inventory = queue
                .cold_inventory()
                .expect("original FIFO independently audits");
            (
                inventory.queued_frames(),
                inventory.summed_frame_bytes(),
                inventory.stored_owned_bytes(),
                inventory.ordered_delivery_wait(),
            )
        })
        .unwrap()
}

fn pressure_registered(fixture: &TimedFixture) -> bool {
    let OwnerPhysicalSessionRoot::CurrentOnly(current) = &fixture.root;
    matches!(
        current.ready,
        OwnerPhysicalReadyState::Active {
            flush: service::OwnerPhysicalFlush::Registered,
            ..
        }
    )
}

#[tokio::test(flavor = "current_thread")]
async fn actual_output_pressure_excludes_due_terminal_then_resumes_and_parks() {
    let mut fixture = TimedFixture::prepare().await;
    let a = SessionFlowId::new(1).unwrap();
    let b = SessionFlowId::new(2).unwrap();
    let writer = {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &fixture.root;
        current.actor.actor.arm_data_writer_block_for_test()
    };
    let ack = |offset, final_accepted| {
        Frame::try_new(
            fixture.generation,
            Record::Ack {
                flow_id: b,
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(offset),
                final_accepted,
            },
        )
        .unwrap()
    };
    let expected: Vec<_> = [1, 2, 3, 4, 4]
        .into_iter()
        .map(|offset| ack(offset, false))
        .collect();
    let final_ack = ack(4, true);
    let mut held_bytes = 0;
    for offset in 0..4 {
        fixture
            .input(Record::Data {
                flow_id: b,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(offset),
                payload: bytes::Bytes::from_static(b"x"),
            })
            .await;
        assert!(
            fixture.drain().await.is_empty(),
            "original writer gate prevents peer output"
        );
        if offset == 0 {
            tokio::time::timeout(TEST_TIMEOUT, writer.wait_until_blocked())
                .await
                .expect("the actual first ACK must reach the actor writer gate");
        }
        held_bytes += expected[offset as usize].encoded_len_checked().unwrap();
        assert_eq!(outbound(&fixture), (offset as usize + 1, held_bytes));
        assert_eq!(fifo(&mut fixture), (0, 0, 0, false));
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut fixture.root;
        assert!(
            current
                .output
                .ready_service_plan(true, &current.terminal_timer)
                .unwrap()
                .sources
        );
    }
    // Four distinct actual writes, not duplicate DATA and not peer-read delay,
    // occupy one writer-local record plus three original outbound records.
    fixture
        .input(Record::Reset {
            flow_id: a,
            reason: ResetReason::TargetFailure,
        })
        .await;
    assert!(fixture.drain().await.is_empty());
    assert_eq!(fixture.observations(), 1);
    fixture
        .input(Record::Close {
            flow_id: b,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(4),
        })
        .await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            fixture.selected_step();
            if pressure_registered(&fixture) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fifth real ACK must register actual actor-capacity pressure");
    let fifth_bytes = expected[4].encoded_len_checked().unwrap();
    assert_eq!(outbound(&fixture), (4, held_bytes));
    assert_eq!(fifo(&mut fixture), (1, fifth_bytes, fifth_bytes, false));
    let retained = {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut fixture.root;
        let plan = current
            .output
            .ready_service_plan(false, &current.terminal_timer)
            .unwrap();
        assert!(!plan.sources && !plan.terminal_timer);
        assert_eq!(current.terminal_timer.inventory(), (1, 1));
        current.output.snapshot()
    };
    assert_eq!(retained.retained_observations, 1);
    let clock = {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &fixture.root;
        current
            .actor
            .actor
            .session_clock(&QuinnLegCompositionGate::mint_for_test())
    };
    let before_advance = clock.now().unwrap();

    // Drive beyond the original same-clock grace while original output remains
    // excluded. This advances time, not an expiry call or a synthetic wake.
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(101)).await;
    tokio::time::resume();
    assert!(
        clock.now().unwrap()
            >= before_advance
                .checked_add(Duration::from_millis(100))
                .unwrap()
    );
    for _ in 0..32 {
        fixture.selected_step();
        tokio::task::yield_now().await;
        assert!(pressure_registered(&fixture));
        assert_eq!(outbound(&fixture), (4, held_bytes));
        assert_eq!(fifo(&mut fixture), (1, fifth_bytes, fifth_bytes, false));
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut fixture.root;
        assert_eq!(current.output.snapshot(), retained);
        assert_eq!(current.terminal_timer.inventory(), (1, 1));
        let plan = current
            .output
            .ready_service_plan(false, &current.terminal_timer)
            .unwrap();
        assert!(!plan.sources && !plan.terminal_timer);
    }
    // Release only the original physical cause. All subsequent work must come
    // from actual writer/actor notifications and the original selected service.
    writer.release();
    let received = tokio::time::timeout(TEST_TIMEOUT, async {
        let mut frames = Vec::new();
        loop {
            frames.extend(fixture.drain().await);
            if frames.iter().any(|frame| frame == &final_ack) {
                break frames;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original pressure release must resume expiry/Completion/final ACK");
    let mut expected = expected;
    expected.push(final_ack);
    assert_eq!(
        received, expected,
        "five exact nonfinal ACKs and one final ACK, in order"
    );
    assert_eq!(
        fixture.observations(),
        1,
        "B successor, not manufactured whole-root zero"
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(101)).await;
    tokio::time::resume();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            assert!(
                fixture.drain().await.is_empty(),
                "no duplicate completion output"
            );
            if fixture.observations() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original successor expiry must settle and park");
    assert!(fixture.ready.select(3).unwrap().is_none());
    assert_eq!(outbound(&fixture), (0, 0));
    assert_eq!(fifo(&mut fixture), (0, 0, 0, false));
    // Four bytes were actually written to MemoryTarget. Actor-only cleanup
    // explicitly retains the logical root; it cannot certify Target/root zero.
    fixture.join_actor_only().await;
}
