use super::super::*;
use crate::tuic::tcp_fixture::UdpPair;

#[tokio::test(flavor = "current_thread")]
async fn receiver_close_releases_idle_public_udp_driver() {
    let pair = UdpPair::new(256, 65536).await;
    let result = catch_scenario(async {
        let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
        let weak = Arc::downgrade(&upstream);
        let mut receiver = upstream.start_udp();
        pair.peer
            .send_datagram(bytes::Bytes::from_static(b"ready"))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            b"ready"
        );
        drop(receiver);
        drop(upstream);
        let released = tokio::time::timeout(Duration::from_millis(200), async {
            while weak.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        assert!(
            released,
            "idle receiver closure must release the actual pump's strong upstream ownership"
        );
    })
    .await;
    let cleanup = catch_scenario(pair.close()).await;
    resume_first(result, cleanup);
}

type ScenarioResult = Result<(), Box<dyn std::any::Any + Send>>;

async fn catch_scenario(work: impl Future<Output = ()>) -> ScenarioResult {
    let mut work = Box::pin(work);
    let result = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await;
    let dropped = catch_unwind(AssertUnwindSafe(|| drop(work)));
    match result {
        Err(first) => {
            if dropped.is_err() {
                eprintln!("scenario Drop also failed during cleanup");
            }
            Err(first)
        }
        Ok(()) => dropped,
    }
}

fn resume_first(original: ScenarioResult, cleanup: ScenarioResult) {
    if let Err(first) = original {
        if cleanup.is_err() {
            eprintln!("cleanup also failed; retaining original scenario failure");
        }
        std::panic::resume_unwind(first);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
}

async fn finish_pair(pair: UdpPair, shutdown: &mut UdpShutdown, original: ScenarioResult) {
    let joined = catch_scenario(async {
        tokio::time::timeout(Duration::from_secs(15), shutdown.drive_shutdown())
            .await
            .expect("actual UDP subtree must join during fixture cleanup");
    })
    .await;
    let endpoints = catch_scenario(pair.close()).await;
    let cleanup = catch_scenario(async {
        resume_first(joined, endpoints);
    })
    .await;
    resume_first(original, cleanup);
}

struct ResumeClock;
impl Drop for ResumeClock {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}

async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual transport/runtime condition must become observable");
}

#[tokio::test(flavor = "current_thread")]
async fn queued_datagrams_do_not_starve_a_ready_uni_packet() {
    let pair = UdpPair::new(256, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut shutdown = upstream.take_udp_shutdown().unwrap();
    let result = catch_scenario(async {
        let before = pair.connection.stats().frame_rx.datagram;
        for _ in 0..512 {
            pair.peer
                .send_datagram(bytes::Bytes::from_static(b"d"))
                .unwrap();
        }
        let mut uni = pair.peer.open_uni().await.unwrap();
        uni.write_all(b"uni-marker").await.unwrap();
        uni.finish().unwrap();
        wait_for(|| {
            pair.connection.stats().frame_rx.datagram >= before + 512
                && pair.connection.stats().frame_rx.stream > 0
        })
        .await;
        let mut receiver = upstream.start_udp();
        let position = tokio::time::timeout(Duration::from_secs(2), async {
            for position in 0..513 {
                if receiver.recv().await.unwrap() == b"uni-marker" {
                    return position;
                }
            }
            panic!("uni packet missing");
        })
        .await
        .unwrap();
        assert_eq!(shutdown.drive_shutdown().await, UdpShutdownState::Settled);
        assert!(
            position < 256,
            "ready uni packet waited behind {position} queued datagrams"
        );
    })
    .await;
    finish_pair(pair, &mut shutdown, result).await;
}

fn assert_joined(upstream: &TuicUpstream, count: u64, quarantined: bool) {
    let state = upstream.udp_runtime.state();
    let outcome = state.outcome.as_ref().unwrap();
    assert!(state.task.is_none());
    assert_eq!(outcome.children_started, count);
    assert_eq!(outcome.children_joined, count);
    assert_eq!(outcome.quarantined, quarantined);
    assert_eq!(outcome.failure.is_some(), quarantined);
    assert_eq!(upstream.udp_runtime.child_count.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_start_preserves_original_and_stop_before_start_allocates_nothing() {
    for stop_first in [false, true] {
        let pair = UdpPair::new(256, 65536).await;
        let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
        let mut shutdown = upstream.take_udp_shutdown().unwrap();
        let result = catch_scenario(async {
            assert!(upstream.take_udp_shutdown().is_none());
            if stop_first {
                shutdown.request_stop();
            }
            let mut original = upstream.start_udp();
            let task = upstream
                .udp_runtime
                .state()
                .task
                .as_ref()
                .map(JoinHandle::id);
            let mut duplicate = upstream.start_udp();
            assert_eq!(
                duplicate.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            );
            assert_eq!(
                upstream
                    .udp_runtime
                    .state()
                    .task
                    .as_ref()
                    .map(JoinHandle::id),
                task
            );
            if stop_first {
                assert!(task.is_none());
                assert_eq!(
                    original.try_recv(),
                    Err(mpsc::error::TryRecvError::Disconnected)
                );
                assert!(!upstream.udp_runtime.monitor_started.load(Ordering::Acquire));
            } else {
                pair.peer
                    .send_datagram(bytes::Bytes::from_static(b"original"))
                    .unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), original.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                    b"original"
                );
            }
            assert_eq!(shutdown.drive_shutdown().await, UdpShutdownState::Settled);
            assert_eq!(shutdown.observe_shutdown().await, UdpShutdownState::Settled);
        })
        .await;
        finish_pair(pair, &mut shutdown, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_reader_bound_joins_actual_children_and_preserves_unrelated_tcp() {
    let pair = UdpPair::new(256, 65536).await;
    pair.connection
        .set_max_concurrent_uni_streams(512u32.into());
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut shutdown = upstream.take_udp_shutdown().unwrap();
    let result = catch_scenario(async {
        let _receiver = upstream.start_udp();
        let mut held = Vec::new();
        for _ in 0..256 {
            let mut uni = pair.peer.open_uni().await.unwrap();
            uni.write_all(b"pending").await.unwrap();
            held.push(uni);
        }
        wait_for(|| upstream.udp_runtime.child_count.load(Ordering::Relaxed) == 256).await;
        let before = upstream.metrics.snapshot(0, 0).udp_drops_down;
        let mut excess = pair.peer.open_uni().await.unwrap();
        excess.write_all(b"excess").await.unwrap();
        wait_for(|| upstream.metrics.snapshot(0, 0).udp_drops_down > before).await;
        pair.assert_bi_progress().await;
        assert_eq!(shutdown.drive_shutdown().await, UdpShutdownState::Settled);
        assert_joined(&upstream, 256, false);
        assert_eq!(
            upstream
                .udp_runtime
                .state()
                .outcome
                .as_ref()
                .unwrap()
                .children_peak,
            256
        );
        drop(held);
        drop(excess);
    })
    .await;
    finish_pair(pair, &mut shutdown, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_observation_retains_task_children_and_original_deadline() {
    let pair = UdpPair::new(256, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut shutdown = upstream.take_udp_shutdown().unwrap();
    let result = catch_scenario(async {
        let _receiver = upstream.start_udp();
        let mut uni = pair.peer.open_uni().await.unwrap();
        uni.write_all(b"pending").await.unwrap();
        wait_for(|| upstream.udp_runtime.child_count.load(Ordering::Relaxed) == 1).await;
        let task = upstream.udp_runtime.state().task.as_ref().unwrap().id();
        let before = tokio::time::Instant::now();
        let mut first_deadline = None;
        for _ in 0..2 {
            let mut observer = Box::pin(shutdown.observe_shutdown());
            assert!(
                observer
                    .as_mut()
                    .poll(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            drop(observer);
            let state = upstream.udp_runtime.state();
            let deadline = state.deadline.unwrap();
            assert!(deadline >= before + Duration::from_secs(15));
            assert!(deadline <= tokio::time::Instant::now() + Duration::from_secs(15));
            assert_eq!(*first_deadline.get_or_insert(deadline), deadline);
            assert_eq!(state.task.as_ref().unwrap().id(), task);
            assert_eq!(upstream.udp_runtime.child_count.load(Ordering::Relaxed), 1);
        }
        let deadline = first_deadline.unwrap();
        assert_eq!(shutdown.drive_shutdown().await, UdpShutdownState::Settled);
        assert_joined(&upstream, 1, false);
        assert_eq!(upstream.udp_runtime.state().deadline, Some(deadline));
        drop(uni);
    })
    .await;
    finish_pair(pair, &mut shutdown, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn stop_while_waiting_for_slot_lock_never_starts_a_new_handshake() {
    let pair = UdpPair::new(256, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut shutdown = upstream.take_udp_shutdown().unwrap();
    let result = catch_scenario(async {
        let guard = upstream.tcp_pool_slots[0].state.lock().await;
        guard
            .current
            .transport
            .close(0u32.into(), b"failed current");
        let before = upstream.endpoint.stats().outgoing_handshakes;
        let _receiver = upstream.start_udp();
        tokio::task::yield_now().await;
        shutdown.request_stop();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), shutdown.drive_shutdown())
                .await
                .unwrap(),
            UdpShutdownState::Settled
        );
        assert_eq!(upstream.endpoint.stats().outgoing_handshakes, before);
        assert_joined(&upstream, 0, false);
        drop(guard);
    })
    .await;
    finish_pair(pair, &mut shutdown, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn parent_and_child_panic_keep_quarantine_and_actual_child_joins() {
    for fault in [1, 2] {
        let pair = UdpPair::new(256, 65536).await;
        let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
        let mut shutdown = upstream.take_udp_shutdown().unwrap();
        let result = catch_scenario(async {
            upstream.udp_runtime.fault.store(fault, Ordering::Relaxed);
            let _receiver = upstream.start_udp();
            let mut uni = pair.peer.open_uni().await.unwrap();
            uni.write_all(b"pending").await.unwrap();
            wait_for(|| {
                upstream
                    .udp_runtime
                    .state()
                    .task
                    .as_ref()
                    .unwrap()
                    .is_finished()
            })
            .await;
            assert_eq!(
                shutdown.drive_shutdown().await,
                UdpShutdownState::Quarantined
            );
            assert_joined(&upstream, 1, true);
            assert_eq!(
                shutdown.observe_shutdown().await,
                UdpShutdownState::Quarantined
            );
            drop(uni);
        })
        .await;
        finish_pair(pair, &mut shutdown, result).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn child_failure_wakes_parent_blocked_on_full_datagram_channel() {
    let pair = UdpPair::new(256, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut shutdown = upstream.take_udp_shutdown().unwrap();
    let result = catch_scenario(async {
        let receiver = upstream.start_udp();
        let mut uni = pair.peer.open_uni().await.unwrap();
        uni.write_all(b"child").await.unwrap();
        wait_for(|| upstream.udp_runtime.child_count.load(Ordering::Relaxed) == 1).await;
        for batch in 1..=16 {
            for _ in 0..64 {
                pair.peer
                    .send_datagram(bytes::Bytes::from_static(b"d"))
                    .unwrap();
            }
            wait_for(|| receiver.len() == batch * 64).await;
        }
        pair.peer
            .send_datagram(bytes::Bytes::from_static(b"blocked"))
            .unwrap();
        wait_for(|| upstream.udp_runtime.blocked_send.load(Ordering::Relaxed)).await;
        upstream.udp_runtime.fault.store(4, Ordering::Relaxed);
        uni.finish().unwrap();
        // No observer stop: the actual child failure must wake the blocked parent.
        wait_for(|| {
            upstream
                .udp_runtime
                .state()
                .task
                .as_ref()
                .unwrap()
                .is_finished()
        })
        .await;
        assert!(*upstream.udp_runtime.stop.borrow());
        assert_eq!(receiver.len(), 1024);
        assert_eq!(
            shutdown.drive_shutdown().await,
            UdpShutdownState::Quarantined
        );
        assert_joined(&upstream, 1, true);
    })
    .await;
    finish_pair(pair, &mut shutdown, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn poisoned_or_missing_task_ownership_never_reports_settled() {
    for poison in [false, true] {
        let runtime = Arc::new(UdpRuntime::default());
        if poison {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let _state = runtime.state.lock().unwrap();
                panic!("interrupted publication");
            }));
        } else {
            runtime.state().started = true;
        }
        let mut shutdown = UdpShutdown(runtime.clone());
        assert_eq!(
            shutdown.drive_shutdown().await,
            UdpShutdownState::Quarantined
        );
        assert_eq!(
            shutdown.drive_shutdown().await,
            UdpShutdownState::Quarantined
        );
        assert!(runtime.state().outcome.as_ref().unwrap().failure.is_some());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_panic_payload_can_request_stop_without_reentering_a_held_lock() {
    struct StopOnDrop(Arc<UdpRuntime>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.request_stop();
        }
    }
    let runtime = Arc::new(UdpRuntime::default());
    let retained = Arc::clone(&runtime);
    let task: JoinHandle<UdpOutcome> =
        tokio::spawn(async move { std::panic::panic_any(StopOnDrop(retained)) });
    runtime.state().started = true;
    runtime.state().task = Some(task);
    wait_for(|| runtime.state().task.as_ref().unwrap().is_finished()).await;
    let mut shutdown = UdpShutdown(runtime.clone());
    assert_eq!(
        shutdown.drive_shutdown().await,
        UdpShutdownState::Quarantined
    );
    assert!(runtime.state().outcome.as_ref().unwrap().failure.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn stop_after_real_handshake_begins_joins_original_attempt_without_retry() {
    let pair = UdpPair::new(256, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut shutdown = upstream.take_udp_shutdown().unwrap();
    let mut successor_peer = None;
    let result = catch_scenario(async {
        pair.connection
            .close(0u32.into(), b"force same-node reconnect");
        let before = upstream.endpoint.stats().outgoing_handshakes;
        let _receiver = upstream.start_udp();
        wait_for(|| upstream.endpoint.stats().outgoing_handshakes == before + 1).await;
        shutdown.request_stop();
        let deadline = upstream.udp_runtime.state().deadline;
        // Stop wins over further attempts but does not discard this real begun
        // handshake. Complete that same attempt and validate the actual AUTH.
        successor_peer = Some(
            tokio::time::timeout(Duration::from_secs(2), async {
                pair.server.accept().await.unwrap().await.unwrap()
            })
            .await
            .unwrap(),
        );
        let peer = successor_peer.as_ref().unwrap();
        let auth = tokio::time::timeout(Duration::from_secs(2), async {
            peer.accept_uni()
                .await
                .unwrap()
                .read_to_end(50)
                .await
                .unwrap()
        })
        .await
        .unwrap();
        let mut token = [0; 32];
        peer.export_keying_material(&mut token, &[0; 16], b"")
            .unwrap();
        assert_eq!(auth, encode_authenticate(&[0; 16], &token));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), shutdown.drive_shutdown())
                .await
                .unwrap(),
            UdpShutdownState::Settled
        );
        assert_joined(&upstream, 0, false);
        assert_eq!(upstream.endpoint.stats().outgoing_handshakes, before + 1);
        assert_eq!(upstream.udp_runtime.state().deadline, deadline);
        // This certifies the UDP parent join, not physical Connecting/Endpoint drain.
    })
    .await;
    if let Some(peer) = successor_peer {
        peer.close(0u32.into(), b"test complete");
    }
    finish_pair(pair, &mut shutdown, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_join_preserves_first_error_class_and_counts_each_failure_once() {
    use crate::shared::errors::FailureClass;
    for panic in [false, true] {
        for prior in [false, true] {
            let runtime = Arc::new(UdpRuntime::default());
            let task: JoinHandle<UdpOutcome> = tokio::spawn(async move {
                if panic {
                    panic!("outer task boundary");
                }
                std::future::pending::<UdpOutcome>().await
            });
            // Abort is only a fixture for Tokio's real Cancelled error, never a
            // cleanup strategy or successful settlement assertion.
            if !panic {
                task.abort();
            }
            {
                let mut state = runtime.state();
                state.started = true;
                state.task = Some(task);
                if prior {
                    let mut outcome = UdpOutcome::default();
                    outcome.record(RetainedFailure::from_io(
                        udp_origin(),
                        std::io::Error::from_raw_os_error(5),
                    ));
                    state.outcome = Some(outcome);
                }
            }
            wait_for(|| runtime.state().task.as_ref().unwrap().is_finished()).await;
            let mut shutdown = UdpShutdown(runtime.clone());
            assert_eq!(
                shutdown.drive_shutdown().await,
                UdpShutdownState::Quarantined
            );
            assert_eq!(
                shutdown.observe_shutdown().await,
                UdpShutdownState::Quarantined
            );
            let state = runtime.state();
            let outcome = state.outcome.as_ref().unwrap();
            let first = outcome.failure.as_ref().unwrap().facts();
            assert_eq!(
                first.class,
                if prior {
                    FailureClass::Io
                } else if panic {
                    FailureClass::Panic
                } else {
                    FailureClass::Cancelled
                }
            );
            assert_eq!(first.raw_os_code, prior.then_some(5));
            assert_eq!(outcome.additional, u64::from(prior));
            assert!(state.task.is_none());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owner_control_deadline_retains_saved_join_until_real_task_completion() {
    // Isolated observer-control proof. No sockets or physical UDP/QUIC claims.
    let runtime = Arc::new(UdpRuntime::default());
    let release = Arc::new(tokio::sync::Notify::new());
    let task_release = release.clone();
    let task = tokio::spawn(async move {
        task_release.notified().await;
        UdpOutcome::default()
    });
    let task_id = task.id();
    {
        let mut state = runtime.state();
        state.started = true;
        state.task = Some(task);
    }
    let mut shutdown = UdpShutdown(runtime.clone());
    let result = catch_scenario(async {
        tokio::time::pause();
        let _clock = ResumeClock;
        for _ in 0..2 {
            let mut observer = Box::pin(shutdown.observe_shutdown());
            assert!(
                observer
                    .as_mut()
                    .poll(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
        }
        let deadline = runtime.state().deadline.unwrap();
        tokio::time::advance(Duration::from_secs(15)).await;
        assert_eq!(
            shutdown.observe_shutdown().await,
            UdpShutdownState::Incomplete
        );
        let state = runtime.state();
        assert_eq!(state.deadline, Some(deadline));
        assert_eq!(state.task.as_ref().unwrap().id(), task_id);
        assert!(!state.task.as_ref().unwrap().is_finished());
    })
    .await;
    release.notify_one();
    let cleanup = catch_scenario(async {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), shutdown.drive_shutdown())
                .await
                .unwrap(),
            UdpShutdownState::Settled
        );
        assert!(runtime.state().task.is_none());
    })
    .await;
    resume_first(result, cleanup);
}
