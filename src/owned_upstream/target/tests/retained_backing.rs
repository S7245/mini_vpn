//! Original MemoryTarget return custody and retained backing through real I/O.
//! Covers actual allocation/notification interruption and terminal settlement;
//! private physical observations are not whole-root allocation acceptance.

use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

struct PanicRelease(AtomicUsize);

impl Wake for PanicRelease {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("controlled Target release notification");
    }
}

struct CountRelease(AtomicUsize);

impl Wake for CountRelease {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn output_allocation_refusal_preserves_both_directions_for_exact_retry() {
    for read in [true, false] {
        let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap());
        let id = flow(1);
        io.open(id, &target(443)).unwrap();
        io.feed_read(id, Bytes::from_static(b"abcdef")).unwrap();
        assert_eq!(io.write(id, b"gh").unwrap().accepted_bytes(), 2);
        let before = io.cold_inventory().unwrap();
        let snapshot = io.snapshot();
        let backing = |io: &MemoryTarget| {
            let flow = &io.storage.get(&id).unwrap();
            (
                flow.readable.as_slices().0.as_ptr(),
                flow.readable.capacity(),
                flow.written.as_slices().0.as_ptr(),
                flow.written.capacity(),
            )
        };
        let original = backing(&io);
        let wakes = Arc::new(CountRelease(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        assert_eq!(
            io.poll_ready_notifications(&mut Context::from_waker(&waker)),
            Poll::Pending
        );
        memory_io::fail_next_output_allocation_for_test();
        if read {
            assert_eq!(io.read(id, 3), Err(TargetIoError::StorageReserve));
        } else {
            assert_eq!(io.take_written(id, 1), Err(TargetIoError::StorageReserve));
        }
        assert_eq!(backing(&io), original);
        assert!(
            io.storage
                .get(&id)
                .unwrap()
                .readable
                .iter()
                .copied()
                .eq(b"abcdef".iter().copied())
        );
        assert!(
            io.storage
                .get(&id)
                .unwrap()
                .written
                .iter()
                .copied()
                .eq(b"gh".iter().copied())
        );
        assert!(matches!(
            io.storage.get(&id).unwrap().output,
            MemoryTargetOutput::Idle
        ));
        assert_eq!(io.cold_inventory().unwrap(), before);
        let mut expected = snapshot;
        expected.read_calls += u64::from(read);
        assert_eq!(io.snapshot(), expected);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0);

        let (returned, count) = if read {
            let TargetReadCompletion::Data(bytes) = io.read(id, 3).unwrap() else {
                panic!("retry must return the original readable prefix");
            };
            assert_eq!(bytes.as_ref(), b"abc");
            (bytes, 3)
        } else {
            let bytes = io.take_written(id, 1).unwrap();
            assert_eq!(bytes.as_ref(), b"g");
            (bytes, 1)
        };
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            io.storage.get(&id).unwrap().output,
            MemoryTargetOutput::Idle
        ));
        let after = io.cold_inventory().unwrap();
        assert_eq!(after.summed_buffered_bytes(), 8 - count);
        assert_eq!(after.stored_buffered_bytes(), 8 - count);
        assert_eq!(io.snapshot().read_delivered_bytes, if read { 3 } else { 0 });
        assert_eq!(
            io.reset(id),
            Ok(TargetResetCompletion::Reset {
                released_read_bytes: if read { 3 } else { 6 },
                released_written_bytes: if read { 2 } else { 1 },
            })
        );
        assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
        io.retire_terminal_tombstone(id).unwrap();
        assert!(io.cold_inventory().unwrap().is_quiescent());
        assert_eq!(
            returned.as_ref(),
            if read { &b"abc"[..] } else { &b"g"[..] }
        );
    }
}

#[test]
fn write_growth_refusal_preserves_wrapped_bytes_and_directive_then_reuses_capacity() {
    for maximum in [9, 16] {
        let mut io =
            MemoryTarget::new(MemoryTargetConfig::new(2, maximum, maximum, 16, 16, 1).unwrap());
        let (a, b) = (flow(1), flow(2));
        for id in [a, b] {
            io.open(id, &target(443)).unwrap();
        }
        assert_eq!(io.write(a, b"abcdefgh").unwrap().accepted_bytes(), 8);
        assert_eq!(io.take_written(a, 3).unwrap().as_ref(), b"abc");
        assert_eq!(io.write(a, b"ijk").unwrap().accepted_bytes(), 3);
        io.push_write_directive(a, MemoryWriteDirective::accept_at_most(3).unwrap())
            .unwrap();
        let original = (
            io.storage.get(&a).unwrap().written.as_slices().0.as_ptr(),
            io.storage.get(&a).unwrap().written.as_slices().1.as_ptr(),
            io.storage.get(&a).unwrap().written.capacity(),
        );
        assert_eq!(original.2, 8);
        assert!(!io.storage.get(&a).unwrap().written.as_slices().1.is_empty());
        let held = io.cold_inventory().unwrap();
        let mut expected = io.snapshot();
        memory_io::fail_next_buffer_allocation_for_test();
        assert_eq!(io.write(a, b"XYZ"), Err(TargetIoError::StorageReserve));
        expected.write_calls += 1;
        assert_eq!(io.snapshot(), expected);
        assert_eq!(io.cold_inventory().unwrap(), held);
        assert_eq!(
            (
                io.storage.get(&a).unwrap().written.as_slices().0.as_ptr(),
                io.storage.get(&a).unwrap().written.as_slices().1.as_ptr(),
                io.storage.get(&a).unwrap().written.capacity()
            ),
            original
        );
        assert!(
            io.storage
                .get(&a)
                .unwrap()
                .written
                .iter()
                .copied()
                .eq(b"defghijk".iter().copied())
        );
        assert!(
            matches!(io.storage.get(&a).unwrap().write_directives.front(),
            Some(MemoryWriteDirective::AcceptAtMost(limit)) if limit.get() == 3)
        );
        let accepted = (maximum - 8).min(3);
        assert_eq!(io.write(a, b"XYZ").unwrap().accepted_bytes(), accepted);
        assert_eq!(io.storage.get(&a).unwrap().written.capacity(), maximum);
        assert!(io.storage.get(&a).unwrap().write_directives.is_empty());
        let returned = io.take_written(a, 16).unwrap();
        assert_eq!(returned.as_ref(), &b"defghijkXYZ"[..8 + accepted]);
        let backing = io.storage.get(&a).unwrap().written.as_slices().0.as_ptr();
        memory_io::fail_next_buffer_allocation_for_test();
        assert_eq!(io.write(a, b"a").unwrap().accepted_bytes(), 1);
        assert_eq!(
            io.storage.get(&a).unwrap().written.as_slices().0.as_ptr(),
            backing
        );
        // A's reuse did not allocate; B's actual new backing consumes the fault.
        assert_eq!(io.write(b, b"b"), Err(TargetIoError::StorageReserve));
        assert_eq!(io.write(b, b"b").unwrap().accepted_bytes(), 1);
        for id in [a, b] {
            assert_eq!(
                io.reset(id),
                Ok(TargetResetCompletion::Reset {
                    released_read_bytes: 0,
                    released_written_bytes: 1,
                })
            );
            assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
            io.retire_terminal_tombstone(id).unwrap();
        }
        assert!(io.cold_inventory().unwrap().is_quiescent());
    }
}

#[test]
fn drained_target_buffers_retain_backing_outside_live_byte_inventory() {
    let config = MemoryTargetConfig::new(2, 8, 8, 8, 8, 1).unwrap();
    let mut io = MemoryTarget::new(config);
    let (a, b) = (flow(1), flow(2));
    for id in [a, b] {
        assert_eq!(io.open(id, &target(443)), Ok(TargetOpenCompletion::Opened));
    }

    assert_eq!(io.write(a, b"aaaaaaaa").unwrap().accepted_bytes(), 8);
    let a_backing = io.storage.get(&a).unwrap().written.as_slices().0.as_ptr();
    let a_capacity = io.storage.get(&a).unwrap().written.capacity();
    let returned_a = io.take_written(a, 8).unwrap();
    assert_eq!(returned_a.as_ref(), b"aaaaaaaa");
    assert_ne!(returned_a.as_ptr(), a_backing);
    assert_eq!(io.storage.get(&a).unwrap().written.capacity(), a_capacity);
    assert!(a_capacity >= 8);

    assert_eq!(io.write(b, b"bbbbbbbb").unwrap().accepted_bytes(), 8);
    let b_backing = io.storage.get(&b).unwrap().written.as_slices().0.as_ptr();
    let b_capacity = io.storage.get(&b).unwrap().written.capacity();
    let returned_b = io.take_written(b, 8).unwrap();
    assert_eq!(returned_b.as_ref(), b"bbbbbbbb");
    assert_ne!(returned_b.as_ptr(), b_backing);
    assert_eq!(io.storage.get(&b).unwrap().written.capacity(), b_capacity);
    assert!(b_capacity >= 8);

    let cold = io.cold_inventory().unwrap();
    assert_eq!(cold.live_flows, 2);
    assert_eq!(cold.summed_buffered_bytes, 0);
    assert_eq!(cold.stored_buffered_bytes, 0);
    assert_eq!(io.snapshot().buffered_bytes, 0);
    let retained = io
        .storage
        .values()
        .map(|flow| flow.written.capacity() + flow.readable.capacity())
        .sum::<usize>();
    assert!(retained >= 16);
    assert!(retained > config.capacity_witness().max_buffered_bytes());

    // Both independently returned payloads are still caller-owned while the
    // original empty deques retain their own backing. Neither is a cold count.
    assert_eq!(returned_a.len() + returned_b.len(), 16);
    for id in [a, b] {
        io.reset(id).unwrap();
        assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert!(io.cold_inventory().unwrap().is_quiescent());
    assert_eq!(returned_a.as_ref(), b"aaaaaaaa");
    assert_eq!(returned_b.as_ref(), b"bbbbbbbb");
}

#[test]
fn feed_input_drop_unwind_keeps_copied_bytes_fully_accounted() {
    struct InputDrop {
        bytes: [u8; 4],
        drops: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for InputDrop {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for InputDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("controlled feed input drop");
        }
    }
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap());
    let id = flow(1);
    io.open(id, &target(443)).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let input = Bytes::from_owner(InputDrop {
        bytes: *b"feed",
        drops: Arc::clone(&drops),
    });
    let panic = catch_unwind(AssertUnwindSafe(|| io.feed_read(id, input))).unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"controlled feed input drop")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let cold = io.cold_inventory();
    assert!(
        matches!(&cold, Ok(value)
        if value.summed_buffered_bytes() == 4 && value.stored_buffered_bytes() == 4),
        "copied feed must be fully accounted: {cold:?}"
    );
    assert_eq!(io.snapshot().buffer_high_water, 4);
    assert_eq!(
        io.read(id, 4),
        Ok(TargetReadCompletion::Data(Bytes::from_static(b"feed")))
    );
    io.cancel(id).unwrap();
    assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
    io.retire_terminal_tombstone(id).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn output_preflight_keeps_error_priority_and_empty_returns_do_not_allocate() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap());
    let id = flow(1);
    io.open(id, &target(443)).unwrap();
    memory_io::fail_next_output_allocation_for_test();
    assert_eq!(io.read(id, 0), Err(TargetIoError::ZeroReadLimit));
    assert_eq!(io.take_written(id, 0), Err(TargetIoError::ZeroDrainLimit));
    assert_eq!(io.snapshot().read_calls, 0);
    assert_eq!(
        io.read(flow(2), 1),
        Err(TargetIoError::UnknownFlow { flow_id: flow(2) })
    );
    assert_eq!(io.snapshot().read_calls, 1);
    assert_eq!(io.read(id, 1), Ok(TargetReadCompletion::WouldBlock));
    assert_eq!(io.take_written(id, 1), Ok(Bytes::new()));
    assert!(matches!(
        io.storage.get(&id).unwrap().output,
        MemoryTargetOutput::Idle
    ));
    io.feed_read(id, Bytes::from_static(b"abc")).unwrap();
    let held = io.cold_inventory().unwrap();
    io.snapshot.read_calls = u64::MAX;
    assert_eq!(
        io.read(id, 3),
        Err(TargetIoError::CounterOverflow {
            counter: "read_calls"
        })
    );
    assert_eq!(io.snapshot().read_calls, u64::MAX);
    io.snapshot.read_calls = 2;
    io.snapshot.read_delivered_bytes = u64::MAX;
    assert_eq!(
        io.read(id, 3),
        Err(TargetIoError::CounterOverflow {
            counter: "read_delivered_bytes"
        })
    );
    assert_eq!(io.snapshot().read_calls, 3);
    assert_eq!(io.cold_inventory().unwrap(), held);
    io.snapshot.read_delivered_bytes = 0;
    assert_eq!(io.read(id, 3), Err(TargetIoError::StorageReserve));
    assert_eq!(io.snapshot().read_calls, 4);
    assert_eq!(io.cold_inventory().unwrap(), held);
    assert_eq!(
        io.read(id, 3),
        Ok(TargetReadCompletion::Data(Bytes::from_static(b"abc")))
    );
    assert_eq!(io.snapshot().read_calls, 5);
    assert_eq!(io.snapshot().read_delivered_bytes, 3);
    io.cancel(id).unwrap();
    assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
    io.retire_terminal_tombstone(id).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn directive_growth_refusal_preserves_wrapped_fifo_and_clamps_reusable_backing() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 8, 8, 8, 8, 9).unwrap());
    let id = flow(1);
    io.open(id, &target(443)).unwrap();
    for _ in 0..8 {
        io.push_write_directive(id, MemoryWriteDirective::WouldBlock)
            .unwrap();
    }
    for _ in 0..3 {
        assert_eq!(io.write(id, b"xy"), Ok(TargetWriteCompletion::WouldBlock));
    }
    for directive in [
        MemoryWriteDirective::Zero,
        MemoryWriteDirective::accept_at_most(1).unwrap(),
        MemoryWriteDirective::Zero,
    ] {
        io.push_write_directive(id, directive).unwrap();
    }
    assert!(
        !io.storage
            .get(&id)
            .unwrap()
            .write_directives
            .as_slices()
            .1
            .is_empty()
    );
    let original = (
        io.storage
            .get(&id)
            .unwrap()
            .write_directives
            .as_slices()
            .0
            .as_ptr(),
        io.storage
            .get(&id)
            .unwrap()
            .write_directives
            .as_slices()
            .1
            .as_ptr(),
    );
    let cold = io.cold_inventory().unwrap();
    let snapshot = io.snapshot();
    memory_io::fail_next_buffer_allocation_for_test();
    assert_eq!(
        io.push_write_directive(id, MemoryWriteDirective::accept_at_most(2).unwrap()),
        Err(TargetIoError::StorageReserve)
    );
    assert_eq!(io.snapshot(), snapshot);
    assert_eq!(io.cold_inventory().unwrap(), cold);
    assert_eq!(
        (
            io.storage
                .get(&id)
                .unwrap()
                .write_directives
                .as_slices()
                .0
                .as_ptr(),
            io.storage
                .get(&id)
                .unwrap()
                .write_directives
                .as_slices()
                .1
                .as_ptr()
        ),
        original
    );
    io.push_write_directive(id, MemoryWriteDirective::accept_at_most(2).unwrap())
        .unwrap();
    assert_eq!(io.storage.get(&id).unwrap().write_directives.capacity(), 9);
    for _ in 0..5 {
        assert_eq!(io.write(id, b"xy"), Ok(TargetWriteCompletion::WouldBlock));
    }
    assert_eq!(io.write(id, b"xy"), Ok(TargetWriteCompletion::Zero));
    assert_eq!(io.write(id, b"xy").unwrap().accepted_bytes(), 1);
    assert_eq!(io.write(id, b"xy"), Ok(TargetWriteCompletion::Zero));
    assert_eq!(io.write(id, b"xy").unwrap().accepted_bytes(), 2);
    assert_eq!(io.take_written(id, 8).unwrap().as_ref(), b"xxy");
    assert_eq!(io.snapshot().queued_write_directives, 0);
    memory_io::fail_next_buffer_allocation_for_test();
    io.push_write_directive(id, MemoryWriteDirective::Zero)
        .unwrap();
    assert_eq!(io.write(id, b"xy"), Ok(TargetWriteCompletion::Zero));
    assert_eq!(
        io.feed_read(id, Bytes::from_static(b"b")),
        Err(TargetIoError::StorageReserve)
    );
    io.feed_read(id, Bytes::from_static(b"b")).unwrap();
    assert_eq!(
        io.reset(id),
        Ok(TargetResetCompletion::Reset {
            released_read_bytes: 1,
            released_written_bytes: 0
        })
    );
    assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
    io.retire_terminal_tombstone(id).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn read_notification_unwind_retains_output_until_exact_flow_reset() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(2, 8, 16, 8, 8, 1).unwrap());
    let (a, b) = (flow(1), flow(2));
    for id in [a, b] {
        assert_eq!(io.open(id, &target(443)), Ok(TargetOpenCompletion::Opened));
    }
    io.feed_read(a, Bytes::from_static(b"retained")).unwrap();
    let wakes = Arc::new(PanicRelease(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    assert_eq!(
        io.poll_ready_notifications(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    let interrupted = catch_unwind(AssertUnwindSafe(|| io.read(a, 8)));
    assert!(interrupted.is_err());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let held = io.cold_inventory().unwrap();
    assert_eq!(held.summed_buffered_bytes(), 8);
    assert_eq!(held.stored_buffered_bytes(), 8);
    assert!(matches!(&io.storage.get(&a).unwrap().output,
        MemoryTargetOutput::Publishing { direction: MemoryTargetOutputDirection::Read, bytes }
        if bytes.as_ref() == b"retained"));
    assert_eq!(io.snapshot().read_delivered_bytes, 0);
    assert!(io.read(a, 8).is_err());
    assert_eq!(io.cold_inventory().unwrap(), held);

    // A's interrupted return must not close an unrelated flow.
    assert_eq!(io.write(b, b"b").unwrap().accepted_bytes(), 1);
    assert_eq!(io.take_written(b, 1).unwrap().as_ref(), b"b");
    assert_eq!(io.cold_inventory().unwrap(), held);
    assert_eq!(
        io.reset(a),
        Ok(TargetResetCompletion::Reset {
            released_read_bytes: 8,
            released_written_bytes: 0,
        })
    );
    io.cancel(b).unwrap();
    for id in [a, b] {
        assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn feed_growth_refusal_preserves_wrapped_source_and_retry_uses_caller_held_input() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 9, 9, 9, 9, 1).unwrap());
    let id = flow(1);
    io.open(id, &target(443)).unwrap();
    io.feed_read(id, Bytes::from_static(b"abcdefgh")).unwrap();
    assert_eq!(
        io.read(id, 3),
        Ok(TargetReadCompletion::Data(Bytes::from_static(b"abc")))
    );
    io.feed_read(id, Bytes::from_static(b"ijk")).unwrap();
    assert!(
        !io.storage
            .get(&id)
            .unwrap()
            .readable
            .as_slices()
            .1
            .is_empty()
    );
    let original = (
        io.storage.get(&id).unwrap().readable.as_slices().0.as_ptr(),
        io.storage.get(&id).unwrap().readable.as_slices().1.as_ptr(),
    );
    let cold = io.cold_inventory().unwrap();
    let snapshot = io.snapshot();
    // The test caller retains a clone deliberately: feed's Err does not return it.
    let input = Bytes::from_static(b"Z");
    memory_io::fail_next_buffer_allocation_for_test();
    assert_eq!(
        io.feed_read(id, input.clone()),
        Err(TargetIoError::StorageReserve)
    );
    assert_eq!(io.cold_inventory().unwrap(), cold);
    assert_eq!(io.snapshot(), snapshot);
    assert_eq!(
        (
            io.storage.get(&id).unwrap().readable.as_slices().0.as_ptr(),
            io.storage.get(&id).unwrap().readable.as_slices().1.as_ptr()
        ),
        original
    );
    assert!(
        io.storage
            .get(&id)
            .unwrap()
            .readable
            .iter()
            .copied()
            .eq(b"defghijk".iter().copied())
    );
    assert_eq!(
        io.feed_read(id, input),
        Ok(TargetReadFeedCompletion::Queued { bytes: 1 })
    );
    assert_eq!(io.storage.get(&id).unwrap().readable.capacity(), 9);
    assert_eq!(
        io.read(id, 9),
        Ok(TargetReadCompletion::Data(Bytes::from_static(b"defghijkZ")))
    );
    memory_io::fail_next_buffer_allocation_for_test();
    io.feed_read(id, Bytes::from_static(b"new")).unwrap();
    assert_eq!(io.storage.get(&id).unwrap().readable.capacity(), 9);
    assert_eq!(io.write(id, b"w"), Err(TargetIoError::StorageReserve));
    assert_eq!(io.write(id, b"w").unwrap().accepted_bytes(), 1);
    assert_eq!(
        io.reset(id),
        Ok(TargetResetCompletion::Reset {
            released_read_bytes: 3,
            released_written_bytes: 1
        })
    );
    assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
    io.retire_terminal_tombstone(id).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn partial_read_unwind_retains_exact_output_until_cancel_preflight_succeeds() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap());
    let id = flow(1);
    io.open(id, &target(443)).unwrap();
    io.feed_read(id, Bytes::from_static(b"abcdef")).unwrap();
    io.write(id, b"gh").unwrap();
    io.push_write_directive(id, MemoryWriteDirective::Zero)
        .unwrap();
    let wakes = Arc::new(PanicRelease(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    assert_eq!(
        io.poll_ready_notifications(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    let panic = catch_unwind(AssertUnwindSafe(|| io.read(id, 3))).unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"controlled Target release notification")
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let original = match &io.storage.get(&id).unwrap().output {
        MemoryTargetOutput::Publishing {
            direction: MemoryTargetOutputDirection::Read,
            bytes,
        } => {
            assert_eq!(bytes.as_ref(), b"abc");
            bytes.as_ptr()
        }
        _ => panic!("original flow must own the interrupted return"),
    };
    let held = io.cold_inventory().unwrap();
    assert_eq!(held.summed_buffered_bytes(), 8);
    assert_eq!(held.stored_buffered_bytes(), 8);
    assert!(
        io.storage
            .get(&id)
            .unwrap()
            .readable
            .iter()
            .copied()
            .eq(b"def".iter().copied())
    );
    for _ in 0..3 {
        assert_eq!(
            io.read(id, 3),
            Err(TargetIoError::OutputPublicationUnknown { flow_id: id })
        );
        assert!(matches!(&io.storage.get(&id).unwrap().output,
            MemoryTargetOutput::Publishing { bytes, .. }
            if bytes.as_ptr() == original && bytes.as_ref() == b"abc"));
        assert_eq!(io.cold_inventory().unwrap(), held);
    }
    io.snapshot.cancelled_flows = u64::MAX;
    assert_eq!(
        io.cancel(id),
        Err(TargetIoError::CounterOverflow {
            counter: "cancelled_flows"
        })
    );
    assert_eq!(io.cold_inventory().unwrap(), held);
    assert!(matches!(&io.storage.get(&id).unwrap().output,
        MemoryTargetOutput::Publishing { bytes, .. } if bytes.as_ptr() == original));
    io.snapshot.cancelled_flows = 0;
    assert_eq!(
        io.cancel(id),
        Ok(TargetCancelCompletion::Cancelled {
            released_read_bytes: 6,
            released_written_bytes: 2,
        })
    );
    assert_eq!(io.cancel(id), Ok(TargetCancelCompletion::AlreadyCancelled));
    assert_eq!(io.snapshot().buffered_bytes, 0);
    assert_eq!(io.snapshot().queued_write_directives, 0);
    assert_eq!(io.snapshot().read_delivered_bytes, 0);
    assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
    io.retire_terminal_tombstone(id).unwrap();
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

#[test]
fn graceful_written_unwind_preserves_partial_output_and_expiry_rearms_real_waiter() {
    for count in [3, 8] {
        let mut io = MemoryTarget::new(MemoryTargetConfig::new(2, 8, 8, 8, 8, 1).unwrap());
        let (a, b) = (flow(1), flow(2));
        for id in [a, b] {
            io.open(id, &target(443)).unwrap();
        }
        assert_eq!(io.write(a, b"abcdefgh").unwrap().accepted_bytes(), 8);
        io.finish_read(a).unwrap();
        io.half_close_write(a).unwrap();
        assert_eq!(io.read(a, 1), Ok(TargetReadCompletion::Eof));
        let panic_wakes = Arc::new(PanicRelease(AtomicUsize::new(0)));
        let panic_waker = Waker::from(Arc::clone(&panic_wakes));
        assert_eq!(
            io.poll_ready_notifications(&mut Context::from_waker(&panic_waker)),
            Poll::Pending
        );
        let interrupted = catch_unwind(AssertUnwindSafe(|| io.take_written(a, count)));
        assert!(interrupted.is_err());
        assert_eq!(panic_wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(&io.storage.get(&a).unwrap().output,
        MemoryTargetOutput::Publishing { direction: MemoryTargetOutputDirection::Written, bytes }
        if bytes.as_ref() == &b"abcdefgh"[..count]));
        assert!(
            io.storage
                .get(&a)
                .unwrap()
                .written
                .iter()
                .copied()
                .eq(b"abcdefgh"[count..].iter().copied())
        );
        let held = io.cold_inventory().unwrap();
        assert_eq!(held.summed_buffered_bytes(), 8);
        assert_eq!(held.stored_buffered_bytes(), 8);
        assert_eq!(
            io.join(a),
            Ok(TargetJoinCompletion::PendingOwnedState {
                bytes: 8,
                directives: 0
            })
        );
        let unknown = || TargetIoError::OutputPublicationUnknown { flow_id: a };
        assert_eq!(io.take_written(a, 1), Err(unknown()));
        assert_eq!(io.read(a, 1), Err(unknown()));
        assert_eq!(io.write(a, b"x"), Err(unknown()));
        assert_eq!(io.open(a, &target(443)), Err(unknown()));
        assert_eq!(io.feed_read(a, Bytes::from_static(b"x")), Err(unknown()));
        assert_eq!(io.finish_read(a), Err(unknown()));
        assert_eq!(io.half_close_write(a), Err(unknown()));
        assert_eq!(
            io.push_write_directive(a, MemoryWriteDirective::Zero),
            Err(unknown())
        );
        assert_eq!(io.readiness(a), Err(unknown()));
        for interest in [
            TargetReadyInterest::Read,
            TargetReadyInterest::Write,
            TargetReadyInterest::Join,
        ] {
            assert_eq!(
                io.poll_readiness(a, interest, &mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(unknown()))
            );
        }
        assert_eq!(io.cold_inventory().unwrap(), held);
        assert_eq!(
            io.cancel(a),
            Ok(TargetCancelCompletion::AlreadyTerminal(
                TargetTerminalKind::Graceful
            ))
        );
        assert_eq!(
            io.reset(a),
            Ok(TargetResetCompletion::AlreadyTerminal(
                TargetTerminalKind::Graceful
            ))
        );
        assert_eq!(io.cold_inventory().unwrap(), held);

        // Finish the old release pass before installing B's actual waiting writer.
        let common = Arc::new(CountRelease(AtomicUsize::new(0)));
        let common_waker = Waker::from(Arc::clone(&common));
        let mut common_cx = Context::from_waker(&common_waker);
        assert_eq!(
            io.poll_ready_notifications(&mut common_cx),
            Poll::Ready(Ok(()))
        );
        assert_eq!(io.poll_ready_notifications(&mut common_cx), Poll::Pending);
        assert_eq!(io.write(b, b"b"), Ok(TargetWriteCompletion::WouldBlock));
        let writer = Arc::new(CountRelease(AtomicUsize::new(0)));
        let writer_waker = Waker::from(Arc::clone(&writer));
        let mut writer_cx = Context::from_waker(&writer_waker);
        assert_eq!(
            io.poll_readiness(b, TargetReadyInterest::Write, &mut writer_cx),
            Poll::Pending
        );
        assert_eq!(
            io.expire_session_flow(a),
            Ok(TargetExpireCompletion {
                terminal: TargetTerminalKind::Graceful,
                released_read_bytes: 0,
                released_written_bytes: 8,
            })
        );
        assert_eq!(common.0.load(Ordering::SeqCst), 1);
        assert_eq!(writer.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            io.poll_ready_notifications(&mut common_cx),
            Poll::Ready(Ok(()))
        );
        assert_eq!(writer.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            io.poll_readiness(b, TargetReadyInterest::Write, &mut writer_cx),
            Poll::Ready(Ok(TargetReadyOutcome::Attempt))
        );
        assert_eq!(io.write(b, b"b").unwrap().accepted_bytes(), 1);
        assert_eq!(io.take_written(b, 1).unwrap().as_ref(), b"b");
        io.cancel(b).unwrap();
        for id in [a, b] {
            assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
            io.retire_terminal_tombstone(id).unwrap();
        }
        assert!(io.cold_inventory().unwrap().is_quiescent());
    }
}
