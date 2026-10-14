//! Remaining open lifecycle facts through the parent's real session/socket fixture.
//! Owns deadline, pressure and settlement tracers; never substitutes for F/S1 joins.

use super::*;
use tokio::time::Instant;

async fn poll_once<F: Future>(observer: F) -> Option<F::Output> {
    let mut observer = Box::pin(observer);
    std::future::poll_fn(|cx| {
        Poll::Ready(match observer.as_mut().poll(cx) {
            Poll::Ready(state) => Some(state),
            Poll::Pending => None,
        })
    })
    .await
}

fn deadlines(control: &SessionControl) -> Vec<(SocketHandle, u64, Instant)> {
    control
        .opens()
        .slots
        .iter()
        .flatten()
        .map(|slot| (slot.handle, slot.epoch, slot.deadline))
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn open_deadline_survives_observer_cancel_and_unconsumed_timeout_joins_on_stop() {
    let (mut session, mut apps, upstream, _downlink) = fixture();
    let start = Instant::now();
    let mut clock_paused = false;
    let captured = capture(async {
        let setup = ready(&mut session, &mut apps, &upstream).await;
        let established_at = Instant::now();
        let original = deadlines(&session.control);
        let first = original
            .iter()
            .map(|entry| entry.2)
            .min()
            .expect("pending open");
        let twenty_seconds = original.iter().all(|entry| {
            entry.2 >= start + Duration::from_secs(20)
                && entry.2 <= established_at + Duration::from_secs(20)
        });
        tokio::time::pause();
        clock_paused = true;
        tokio::time::advance(first - Instant::now() - Duration::from_millis(1)).await;
        let pending = poll_once(session.run()).await.is_none();
        let unchanged = original == deadlines(&session.control);
        let before = inventory(&session.control, &upstream);
        // Tokio rounds timer deadlines upward to milliseconds. Observe at first + 1ms.
        tokio::time::advance(Duration::from_millis(2)).await;
        // Let the original workers return, without polling the loop/channel consumer.
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        let finished = session
            .control
            .opens()
            .slots
            .iter()
            .flatten()
            .filter(|slot| matches!(&slot.phase, OpenPhase::Worker(worker) if worker.is_finished()))
            .count();
        let unconsumed = inventory(&session.control, &upstream);
        // Stop uses real elapsed time: automatic clock jumps cannot finish other opens.
        tokio::time::resume();
        clock_paused = false;
        let state = tokio::time::timeout(Duration::from_secs(5), session.observe_shutdown())
            .await
            .ok();
        let after = inventory(&session.control, &upstream);
        let ledger = session.control.ledger();
        let timed_out = ledger.first_failure.as_ref().is_some_and(|failure| {
            failure.facts().io_kind == Some(std::io::ErrorKind::TimedOut)
                && failure.facts().origin.phase == FailurePhase::TcpOpen
        });
        (
            setup,
            original.len(),
            twenty_seconds,
            pending,
            unchanged,
            before,
            finished,
            unconsumed,
            state,
            after,
            timed_out,
            ledger.additional_failures,
        )
    })
    .await;
    if clock_paused {
        tokio::time::resume();
    }
    let cleanup = cleanup(&mut session, &upstream).await;
    eprintln!("deadline observation={captured:?}; cleanup={cleanup:?}");
    let (
        setup,
        count,
        twenty_seconds,
        pending,
        unchanged,
        before,
        finished,
        unconsumed,
        state,
        after,
        timed_out,
        additional,
    ) = captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(setup && twenty_seconds && pending && unchanged && timed_out);
    assert_eq!(count, 128);
    assert_eq!(
        before,
        OpenInventory {
            slots: 128,
            workers: 128,
            live: 128,
            announced: 0,
            dropped: 0
        }
    );
    assert!(finished > 0 && finished < 128);
    assert_eq!(
        unconsumed,
        OpenInventory {
            slots: 128,
            workers: 128,
            live: 128,
            announced: 0,
            dropped: finished
        }
    );
    assert_eq!(state, Some(SessionDeviceState::DeviceFailedSettled));
    assert_eq!(
        after,
        OpenInventory {
            slots: 0,
            workers: 0,
            live: 0,
            announced: 0,
            dropped: 128
        }
    );
    assert_eq!(additional, (finished - 1) as u64);
    assert!(!cleanup.timed_out);
    assert_eq!(cleanup.entered, cleanup.dropped);
    assert_eq!(cleanup.state, state);
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn step<F: Future<Output = ()>>(
    driver: &mut Pin<Box<F>>,
    observer: &Arc<WakeCount>,
    completed: &mut bool,
) -> bool {
    if !*completed {
        let waker = std::task::Waker::from(Arc::clone(observer));
        *completed = driver
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_ready();
    }
    *completed
}

#[tokio::test(flavor = "current_thread")]
async fn open_settlement_retains_join_wakes_across_chunks_and_observer_reentry() {
    let control = SessionControl::default();
    let probe = Arc::new(PendingOpens::new());
    let mut sockets = SocketSet::new(vec![]);
    for index in [0, 64] {
        let handle = sockets.add(Socket::new(
            SocketBuffer::new(vec![0; 1]),
            SocketBuffer::new(vec![0; 1]),
        ));
        let probe = Arc::clone(&probe);
        control.opens().slots[index] = Some(OpenSlot {
            handle,
            epoch: 1,
            deadline: Instant::now() + Duration::from_secs(20),
            phase: OpenPhase::Worker(tokio::spawn(async move {
                let _guard = OpenGuard(Arc::clone(&probe.dropped));
                probe.entered.fetch_add(1, Ordering::SeqCst);
                probe.gates[index].notified().await;
                OpenOutcome::default()
            })),
        });
    }
    control.opens().live = 2;
    control.request_stop();
    let old = Arc::new(WakeCount::default());
    let new = Arc::new(WakeCount::default());
    let mut completed = false;
    let mut driver = Box::pin(control.settle_opens());
    let captured = capture(async {
        let mut pending = !step(&mut driver, &old, &mut completed); // chunk0
        probe.gates[0].notify_one();
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        let first_finished = matches!(
            &control.opens().slots[0].as_ref().expect("original slot0").phase,
            OpenPhase::Worker(worker) if worker.is_finished()
        );
        for _ in 0..2 {
            pending &= !step(&mut driver, &old, &mut completed);
        } // 32,64
        let before_wrap = old.0.load(Ordering::SeqCst);
        pending &= !step(&mut driver, &old, &mut completed); // 96: preserve earlier childwake
        let retry = old.0.load(Ordering::SeqCst) > before_wrap;
        pending &= !step(&mut driver, &old, &mut completed); // next0: actual join
        let first_joined = {
            let opens = control.opens();
            opens.slots[0].is_none() && opens.live == 1
        };
        for _ in 0..2 {
            pending &= !step(&mut driver, &old, &mut completed);
        } // 32,64
        let old_before_switch = old.0.load(Ordering::SeqCst);
        pending &= !step(&mut driver, &new, &mut completed); // 96: new observer; no repoll64
        let quiet = new.0.load(Ordering::SeqCst) == 0;
        probe.gates[64].notify_one();
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        let new_notified = new.0.load(Ordering::SeqCst) > 0;
        let old_unchanged = old.0.load(Ordering::SeqCst) == old_before_switch;
        for _ in 0..4 {
            step(&mut driver, &new, &mut completed);
        }
        (
            pending,
            first_finished,
            retry,
            first_joined,
            quiet,
            new_notified,
            old_unchanged,
            completed,
            inventory(&control, &probe),
        )
    })
    .await;
    // Safety cleanup never supplies the scheduling oracle: all observations are saved above.
    for gate in &probe.gates {
        gate.notify_one();
    }
    for _ in 0..16 {
        if completed {
            break;
        }
        tokio::task::yield_now().await;
        step(&mut driver, &new, &mut completed);
    }
    let cleanup = (completed, inventory(&control, &probe));
    eprintln!("join wake observation={captured:?}; cleanup={cleanup:?}");
    let (
        pending,
        first_finished,
        retry,
        first_joined,
        quiet,
        new_notified,
        old_unchanged,
        settled,
        after,
    ) = captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(pending && first_finished && first_joined && quiet && settled && cleanup.0);
    assert_eq!(
        after,
        OpenInventory {
            slots: 0,
            workers: 0,
            live: 0,
            announced: 0,
            dropped: 2
        }
    );
    assert_eq!(cleanup.1, after);
    assert!(
        retry,
        "completion wake must survive the remaining stop chunks"
    );
    assert!(
        new_notified && old_unchanged,
        "late join wake must reach the current observer"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn open_illegal_orphan_announcement_stays_quarantined_across_shutdown_reentry() {
    let (mut session, _apps, upstream, _downlink) = fixture();
    let captured = capture(async {
        let initial = poll_once(session.run()).await;
        let empty = inventory(&session.control, &upstream);
        // Fault injection only: normal workers cannot create an orphan consumer bit.
        session.control.announced[1].store(1u64 << 63, Ordering::Release);
        let before = inventory(&session.control, &upstream);
        let first = poll_once(session.observe_shutdown()).await;
        let after = inventory(&session.control, &upstream);
        let deadline = session.control.ledger().shutdown_deadline;
        let second = poll_once(session.observe_shutdown()).await;
        let reentered = inventory(&session.control, &upstream);
        let ledger = session.control.ledger();
        (
            initial,
            empty,
            before,
            (first, after),
            (second, reentered),
            deadline.is_some() && deadline == ledger.shutdown_deadline,
            ledger.quarantined,
        )
    })
    .await;
    let cleanup = cleanup(&mut session, &upstream).await;
    eprintln!("illegal orphan observation={captured:?}; cleanup={cleanup:?}");
    let (initial, empty, before, (first, after), (second, reentered), same_deadline, quarantined) =
        captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(initial.is_none());
    assert_eq!(
        empty,
        OpenInventory {
            slots: 0,
            workers: 0,
            live: 0,
            announced: 0,
            dropped: 0
        }
    );
    assert_eq!(
        before,
        OpenInventory {
            announced: 1,
            ..empty
        }
    );
    assert_eq!(after, before, "illegal bit must remain physical evidence");
    assert_eq!(reentered, before);
    assert_eq!(second, first);
    assert!(same_deadline);
    assert!(!cleanup.timed_out);
    assert_eq!(cleanup.entered, 0);
    assert_eq!(cleanup.dropped, 0);
    assert_eq!(cleanup.state, second);
    assert_eq!(
        first,
        Some(SessionDeviceState::Incomplete),
        "orphan announcement must prevent clean settlement"
    );
    assert!(quarantined);
}

#[tokio::test(flavor = "current_thread")]
async fn open_notice_before_task_return_keeps_custody_until_join_and_disposition() {
    let control = Arc::new(SessionControl {
        control_remaining: AtomicUsize::new(32),
        ..SessionControl::default()
    });
    let probe = Arc::new(PendingOpens::new());
    let (dispatch, mut receiver) = OpenDispatch::channel(128, Some(Arc::clone(&control)));
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 1]),
        SocketBuffer::new(vec![0; 1]),
    ));
    let worker_probe = Arc::clone(&probe);
    let sender = dispatch.sender.clone();
    control.opens().slots[0] = Some(OpenSlot {
        handle,
        epoch: 7,
        deadline: Instant::now() + Duration::from_secs(20),
        phase: OpenPhase::Worker(tokio::spawn(async move {
            let _guard = OpenGuard(Arc::clone(&worker_probe.dropped));
            worker_probe.entered.fetch_add(1, Ordering::SeqCst);
            sender.send(HandshakeNotice::Custodied(0)).await.unwrap();
            worker_probe.gates[0].notified().await;
            OpenOutcome {
                failure: Some(RetainedFailure::from_io(
                    open_origin(0, 7, FailurePhase::TcpOpen),
                    std::io::ErrorKind::ConnectionRefused.into(),
                )),
                ..OpenOutcome::default()
            }
        })),
    });
    control.opens().live = 1;
    let captured = capture(async {
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        let cancelled_pending = poll_once(dispatch.recv(&mut receiver)).await.is_none();
        let before = inventory(&control, &probe);
        let identity = control.opens().slots[0]
            .as_ref()
            .map(|slot| (slot.handle, slot.epoch));
        let observer = Arc::new(WakeCount::default());
        let waker = std::task::Waker::from(Arc::clone(&observer));
        let mut cx = std::task::Context::from_waker(&waker);
        let mut waiting = Box::pin(dispatch.recv(&mut receiver));
        let pending = waiting.as_mut().poll(&mut cx).is_pending();
        let wakes = observer.0.load(Ordering::SeqCst);
        probe.gates[0].notify_one();
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        let notified = observer.0.load(Ordering::SeqCst) > wakes;
        let finished = matches!(
            &control.opens().slots[0].as_ref().expect("original worker").phase,
            OpenPhase::Worker(worker) if worker.is_finished()
        );
        let unconsumed = inventory(&control, &probe);
        let completion = match waiting.as_mut().poll(&mut cx) {
            Poll::Ready(Some(OpenEvent::Completion(done, Some(0)))) => {
                done.handle == handle
                    && done.epoch == 7
                    && matches!(done.result, Err(ClientError::Io(error))
                        if error.kind() == std::io::ErrorKind::ConnectionRefused)
            }
            _ => false,
        };
        drop(waiting);
        let joined = inventory(&control, &probe);
        if completion {
            dispatch.consumed(Some(0));
        }
        let disposed = inventory(&control, &probe);
        let ledger = control.ledger();
        let bounded = ledger.first_failure.as_ref().is_some_and(|failure| {
            failure.facts().io_kind == Some(std::io::ErrorKind::ConnectionRefused)
                && failure.facts().origin.attempt_id == Some(7)
        }) && ledger.additional_failures == 0;
        (
            cancelled_pending,
            before,
            identity,
            pending,
            notified,
            finished,
            unconsumed,
            completion,
            joined,
            disposed,
            bounded,
        )
    })
    .await;
    probe.gates[0].notify_one();
    control.request_stop();
    let cleanup = tokio::time::timeout(Duration::from_secs(5), control.settle_opens())
        .await
        .is_ok();
    eprintln!("normal notice observation={captured:?}; cleanup={cleanup}");
    let (
        cancelled,
        before,
        identity,
        pending,
        notified,
        finished,
        unconsumed,
        completion,
        joined,
        disposed,
        bounded,
    ) = captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(cancelled && pending && notified && finished && completion && bounded && cleanup);
    assert_eq!(identity, Some((handle, 7)));
    assert_eq!(
        before,
        OpenInventory {
            slots: 1,
            workers: 1,
            live: 1,
            announced: 1,
            dropped: 0
        }
    );
    assert_eq!(
        unconsumed,
        OpenInventory {
            dropped: 1,
            ..before
        }
    );
    assert_eq!(
        joined,
        OpenInventory {
            workers: 0,
            announced: 0,
            ..unconsumed
        }
    );
    assert_eq!(
        disposed,
        OpenInventory {
            slots: 0,
            live: 0,
            ..joined
        }
    );
    assert_eq!(inventory(&control, &probe), disposed);
}

fn expected_inventory(
    slots: usize,
    workers: usize,
    live: usize,
    announced: u32,
    dropped: usize,
) -> OpenInventory {
    OpenInventory {
        slots,
        workers,
        live,
        announced,
        dropped,
    }
}

async fn settle_workers<F: Future<Output = ()>>(session: &mut ClientTunSession<F>) -> bool {
    session.request_stop();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = poll_once(session.run()).await;
            let workers = session
                .control
                .opens()
                .slots
                .iter()
                .flatten()
                .any(|slot| matches!(slot.phase, OpenPhase::Worker(_)));
            if !workers
                && session
                    .control
                    .device_settlement_observed
                    .load(Ordering::SeqCst)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

async fn fallback<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    upstream: &PendingOpens,
) -> (bool, FixtureCleanup) {
    session.request_stop();
    for gate in &upstream.gates {
        gate.notify_one();
    }
    let joined = settle_workers(session).await;
    (joined, cleanup(session, upstream).await)
}

#[tokio::test(flavor = "current_thread")]
async fn open_success_stays_retained_while_other_workers_and_device_actually_settle() {
    use tokio::io::AsyncReadExt;

    let (mut session, mut apps, upstream, _downlink) = fixture();
    *upstream.behavior.lock().unwrap() = OpenBehavior::Success;
    let captured = capture(async {
        assert!(ready(&mut session, &mut apps, &upstream).await);
        upstream.gates[0].notify_one();
        let retained = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                turn(&mut session, &mut apps, false).await;
                if session.control.opens().slots.iter().flatten().any(|slot| {
                    matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some())
                }) {
                    break;
                }
            }
        })
        .await
        .is_ok();
        assert!(retained);
        let held = inventory(&session.control, &upstream);
        assert_eq!(held, expected_inventory(128, 127, 128, 0, 1));
        let mut peer = upstream
            .peer
            .lock()
            .unwrap()
            .take()
            .expect("real duplex peer");
        assert!(poll_once(peer.read_u8()).await.is_none());
        let first = poll_once(session.observe_shutdown()).await;
        let first_inventory = inventory(&session.control, &upstream);
        assert!(settle_workers(&mut session).await);
        let after = inventory(&session.control, &upstream);
        let second = poll_once(session.observe_shutdown()).await;
        eprintln!(
            "retained success first={first:?}/{first_inventory:?}; final={second:?}/{after:?}"
        );
        assert_eq!(after, expected_inventory(1, 0, 1, 0, 128));
        assert_eq!(second, Some(SessionDeviceState::Incomplete));
        assert!(session.control.ledger().quarantined);
        assert!(session.control.ledger().first_failure.is_none());
        assert!(
            session
                .control
                .device_settlement_observed
                .load(Ordering::SeqCst)
        );
        assert!(session.control.opens().slots.iter().flatten().all(|slot| {
            matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some())
        }));
        assert!(poll_once(peer.read_u8()).await.is_none());
    })
    .await;
    let cleanup = fallback(&mut session, &upstream).await;
    eprintln!("success observation={captured:?}; fallback={cleanup:?}");
    captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(cleanup.0 && !cleanup.1.timed_out);
    assert_eq!(cleanup.1.entered, cleanup.1.dropped);
}

#[tokio::test(flavor = "current_thread")]
async fn open_poll_and_cancel_drop_panics_are_bounded_before_actual_join() {
    for (mode, phase, destructor_count) in [
        (OpenBehavior::PollPanic, FailurePhase::TcpOpen, 0),
        (OpenBehavior::CancelDropPanic, FailurePhase::TcpOpenDrop, 1),
    ] {
        let (mut session, mut apps, upstream, _downlink) = fixture();
        *upstream.behavior.lock().unwrap() = mode;
        let captured = capture(async {
            assert!(ready(&mut session, &mut apps, &upstream).await);
            if matches!(mode, OpenBehavior::PollPanic) {
                upstream.gates[0].notify_one();
                for _ in 0..256 {
                    tokio::task::yield_now().await;
                }
                let finished = session
                    .control
                    .opens()
                    .slots
                    .iter()
                    .flatten()
                    .filter(|slot| {
                        matches!(&slot.phase, OpenPhase::Worker(worker) if worker.is_finished())
                    })
                    .count();
                assert_eq!(finished, 1);
            }
            let before = inventory(&session.control, &upstream);
            assert_eq!(
                before,
                expected_inventory(
                    128,
                    128,
                    128,
                    0,
                    usize::from(matches!(mode, OpenBehavior::PollPanic))
                )
            );
            assert!(settle_workers(&mut session).await);
            let after = inventory(&session.control, &upstream);
            eprintln!("panic mode={mode:?}; before={before:?}; after={after:?}");
            assert_eq!(after, expected_inventory(0, 0, 0, 0, 128));
            let shutdown_deadline = session.control.ledger().shutdown_deadline;
            assert!(shutdown_deadline.is_some());
            let terminal = tokio::time::timeout(Duration::from_secs(5), session.observe_shutdown())
                .await
                .expect("outer driver must settle");
            assert_eq!(terminal, SessionDeviceState::DeviceFailedSettled);
            assert_eq!(
                session.control.ledger().shutdown_deadline,
                shutdown_deadline
            );
            assert_eq!(session.state(), SessionDeviceState::DeviceFailedSettled);
            assert_eq!(
                upstream.error_dropped.load(Ordering::SeqCst),
                destructor_count
            );
            let ledger = session.control.ledger();
            let failure = ledger.first_failure.as_ref().expect("bounded panic");
            let facts = failure.facts();
            assert_eq!(facts.class, crate::shared::errors::FailureClass::Panic);
            assert_eq!(facts.origin.phase, phase);
            assert_eq!(facts.origin.resource, FailureResource::TcpOpen);
            assert_eq!(facts.origin.attempt_id, Some(1));
            assert!(
                facts.opaque && facts.source_content_omitted && failure.text_bytes().is_empty()
            );
            assert_eq!(ledger.additional_failures, 0);
        })
        .await;
        let cleanup = fallback(&mut session, &upstream).await;
        eprintln!("panic observation={captured:?}; fallback={cleanup:?}");
        captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        assert!(cleanup.0 && !cleanup.1.timed_out);
        assert_eq!(cleanup.1.entered, cleanup.1.dropped);
    }
}

fn illegal_fixture_worker(control: &SessionControl, upstream: &PendingOpens) -> OpenPhase {
    upstream.entered.store(1, Ordering::SeqCst);
    let guard = OpenGuard(Arc::clone(&upstream.dropped));
    let stop = Arc::clone(&control.stop);
    OpenPhase::Worker(tokio::spawn(async move {
        let _guard = guard;
        stop.stopped().await;
        OpenOutcome::default()
    }))
}

#[tokio::test(flavor = "current_thread")]
async fn open_illegal_inventory_directions_keep_quarantine_and_nonworker_bits() {
    for case in 0..4 {
        let (mut session, apps, upstream, _downlink) = fixture();
        let captured = capture(async {
            assert!(poll_once(session.run()).await.is_none());
            let empty = inventory(&session.control, &upstream);
            assert_eq!(empty, expected_inventory(0, 0, 0, 0, 0));
            assert!(!session.control.ledger().quarantined);
            if case == 0 {
                session.control.opens().live = 1;
            } else {
                let phase = if case == 1 {
                    illegal_fixture_worker(&session.control, &upstream)
                } else if case == 2 {
                    OpenPhase::Retained(OpenOutcome::default())
                } else {
                    OpenPhase::AwaitingDisposition(OpenOutcome::default())
                };
                session.control.opens().slots[0] = Some(OpenSlot {
                    handle: apps.handles[0],
                    epoch: 1,
                    deadline: Instant::now() + Duration::from_secs(20),
                    phase,
                });
                if case >= 2 {
                    let phase = illegal_fixture_worker(&session.control, &upstream);
                    let mut opens = session.control.opens();
                    opens.slots[1] = Some(OpenSlot {
                        handle: apps.handles[1],
                        epoch: 1,
                        deadline: Instant::now() + Duration::from_secs(20),
                        phase,
                    });
                    opens.live = 2;
                    session.control.announced[0].store(1, Ordering::Release);
                }
            }
            let before = inventory(&session.control, &upstream);
            let expected = match case {
                0 => expected_inventory(0, 0, 1, 0, 0),
                1 => expected_inventory(1, 1, 0, 0, 0),
                _ => expected_inventory(2, 1, 2, 1, 0),
            };
            assert_eq!(before, expected);
            assert!(settle_workers(&mut session).await);
            let after = inventory(&session.control, &upstream);
            let first = poll_once(session.observe_shutdown()).await;
            let deadline = session.control.ledger().shutdown_deadline;
            let second = poll_once(session.observe_shutdown()).await;
            eprintln!("illegal case={case}; before={before:?}; after={after:?}; states={first:?}/{second:?}");
            assert_eq!(
                after,
                expected_inventory(
                    0, 0, usize::from(case == 0), u32::from(case >= 2), usize::from(case != 0)
                )
            );
            assert_eq!(first, Some(SessionDeviceState::Incomplete));
            assert_eq!(second, first);
            assert_eq!(inventory(&session.control, &upstream), after);
            let ledger = session.control.ledger();
            assert!(ledger.quarantined && ledger.first_failure.is_none());
            assert!(deadline.is_some() && deadline == ledger.shutdown_deadline);
        })
        .await;
        let cleanup = fallback(&mut session, &upstream).await;
        eprintln!("illegal observation={captured:?}; fallback={cleanup:?}");
        captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        assert!(cleanup.0 && !cleanup.1.timed_out);
        assert_eq!(cleanup.1.entered, cleanup.1.dropped);
    }
}
