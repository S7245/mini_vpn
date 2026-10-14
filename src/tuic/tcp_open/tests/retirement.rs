use super::*;
use crate::tuic::tcp_fixture::UdpPair;
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn completed(upstream: Arc<TuicUpstream>, peer: &Connection) -> Attempt {
    let mut attempt = create(
        upstream,
        TcpOpenRequest {
            target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
            deadline: Instant::now() + Duration::from_secs(5),
        },
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while attempt.view().phase != TcpOpenPhase::Ready {
            attempt.step(
                &mut Context::from_waker(std::task::Waker::noop()),
                Instant::now(),
            );
            assert!(attempt.failure().is_none());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let OpenedTcpRelay::Generic(mut relay) = attempt.take_carrier(Instant::now()).unwrap() else {
        panic!("Generic");
    };
    relay.shutdown().await.unwrap();
    let (mut send, mut recv) = peer.accept_bi().await.unwrap();
    recv.read_to_end(1024).await.unwrap();
    send.finish().unwrap();
    relay.read_to_end(&mut Vec::new()).await.unwrap();
    drop(relay);
    drop(send);
    drop(recv);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let State::Transferred(custody) = &attempt.state else {
                panic!("must retain custody");
            };
            if custody.witness.as_ref().unwrap().is_retired() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("BOTH actual stream maps must retire before testing registry-only pressure");
    attempt
}

#[tokio::test]
async fn fully_retired_stream_cannot_settle_after_fixed_deadline() {
    let pair = UdpPair::new(8, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = completed(upstream.clone(), &pair.peer).await;
    let registry = upstream.tcp_pool_slots[0]
        .state
        .lock()
        .await
        .current
        .write_pressure
        .clone();
    let now = Instant::now();
    let deadline = now + Duration::from_secs(1);
    let guard = registry.writers.lock().unwrap();
    attempt.settle(
        &mut Context::from_waker(std::task::Waker::noop()),
        now,
        deadline,
    );
    let State::Retiring(custody) = &attempt.state else {
        panic!("registry-only pressure");
    };
    assert!(custody.witness.as_ref().unwrap().is_retired());
    assert!(!custody.pressure.as_ref().unwrap().removed);
    assert!(custody.lease.as_ref().unwrap().release.is_none());
    drop(guard);
    attempt.settle(
        &mut Context::from_waker(std::task::Waker::noop()),
        deadline,
        deadline + Duration::from_secs(20),
    );
    assert_eq!(attempt.view().phase, TcpOpenPhase::Quarantined);
    assert!(
        matches!(attempt.failure(), Some(ClientError::Io(e)) if e.kind() == io::ErrorKind::TimedOut)
    );
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    assert_eq!(registry.writers.lock().unwrap().len(), 1);
    drop(attempt);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn actual_zero_notification_panic_keeps_attempt_and_partial_debits_quarantined() {
    struct PanicWake;
    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("actual generation zero notification");
        }
        fn wake_by_ref(self: &Arc<Self>) {
            panic!("actual generation zero notification");
        }
    }
    let pair = UdpPair::new(8, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = completed(upstream.clone(), &pair.peer).await;
    let generation = upstream.tcp_pool_slots[0].current_activity().await;
    let mut zero = Box::pin(generation.zero.notified());
    let waker = std::task::Waker::from(Arc::new(PanicWake));
    assert!(
        zero.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let now = Instant::now();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        attempt.settle(
            &mut Context::from_waker(std::task::Waker::noop()),
            now,
            now + Duration::from_secs(2),
        );
    }));
    assert!(result.is_err());
    assert_eq!(attempt.view().phase, TcpOpenPhase::Quarantined);
    let State::Retiring(custody) = &attempt.state else {
        panic!("owner remains retained");
    };
    let report = custody.lease.as_ref().unwrap().release.unwrap();
    use crate::tuic::tcp_pool_admission::CounterRelease;
    assert_eq!(
        report.generation,
        CounterRelease::Decremented {
            before: 1,
            after: 0
        }
    );
    assert_eq!(report.slot, CounterRelease::Pending);
    assert_eq!(report.total, CounterRelease::Pending);
    assert!(custody.pressure.as_ref().unwrap().removed);
    assert_eq!(generation.active(), 0);
    for _ in 0..3 {
        attempt.step(&mut Context::from_waker(std::task::Waker::noop()), now);
        assert!(attempt.take_carrier(now).is_none());
        attempt.settle(
            &mut Context::from_waker(std::task::Waker::noop()),
            now,
            now + Duration::from_secs(2),
        );
    }
    assert_eq!(attempt.view().phase, TcpOpenPhase::Quarantined);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    assert_eq!(
        upstream.tcp_pool_admission.active_slots[0].load(Ordering::Acquire),
        1
    );
    drop(zero);
    drop(attempt);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    drop(upstream);
    pair.close().await;
}
