use super::*;
use crate::upstream::TcpOpenCancel;

#[tokio::test]
async fn connect_and_handshake_deadlines_are_capped_by_original_request() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    let request_deadline = Instant::now() + Duration::from_secs(2);
    open.request.deadline = request_deadline;
    turn(&mut open);
    assert_eq!(open.connect_deadline, Some(request_deadline));
    until(&mut open, TcpOpenPhase::Preparing).await;
    assert_eq!(open.handshake_deadline, Some(request_deadline));
    let (mut peer, _) = listener.accept().await.unwrap();
    turn(&mut open);
    let mut first = [0; 1];
    peer.read_exact(&mut first).await.unwrap();
    open.step(
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
        request_deadline,
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert!(
        matches!(open.failure(), Some(crate::shared::ClientError::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut)
    );
    assert_eq!(
        open.settle_deadline,
        Some(request_deadline + Duration::from_secs(5))
    );
    let mut rest = Vec::new();
    peer.read_to_end(&mut rest).await.unwrap();
}

#[tokio::test]
async fn ten_second_handshake_budget_does_not_wait_for_twenty_second_request() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    let before = Instant::now();
    until(&mut open, TcpOpenPhase::Preparing).await;
    let deadline = open.handshake_deadline.unwrap();
    assert!(deadline < open.request.deadline && deadline >= before + Duration::from_secs(10));
    let (mut peer, _) = listener.accept().await.unwrap();
    turn(&mut open);
    let mut first = [0; 1];
    peer.read_exact(&mut first).await.unwrap();
    open.step(
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
        deadline,
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert!(open.witness.is_none());
    let mut rest = Vec::new();
    peer.read_to_end(&mut rest).await.unwrap();
}

#[tokio::test]
async fn candidate_failure_closes_old_witness_before_next_real_connect() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let refused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refused_address = refused.local_addr().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let actual = listener.local_addr().unwrap();
    drop(refused);
    let mut open = local_attempt(actual.to_string());
    turn(&mut open);
    open.addresses =
        super::super::bounded_addresses([refused_address, actual].into_iter()).unwrap();
    turn(&mut open);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(open.state, super::super::State::NextAddress) {
            turn(&mut open);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(open.witness.is_none());
    assert!(open.shutdown.is_some() && open.connect_error.is_some());
    until(&mut open, TcpOpenPhase::Connecting).await;
    let (mut peer, _) = listener.accept().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    let mut first = [0; 1];
    assert_eq!(peer.read(&mut first).await.unwrap(), 0);
}

#[tokio::test]
async fn invalid_ready_receipt_cannot_publish_or_clear_physical_owner() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let (mut open, peer) = authenticated_peer().await;
    until(&mut open, TcpOpenPhase::Ready).await;
    let super::super::State::Ready(carrier) = &mut open.state else {
        panic!("real Ready");
    };
    let original = carrier.release.replace(Arc::new(super::super::Release {
        phase: std::sync::atomic::AtomicU8::new(super::super::LIVE),
        wake: std::sync::Mutex::new(None),
    }));
    assert!(open.take_carrier(Instant::now()).is_none());
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.witness.is_some());
    assert!(matches!(&open.state, super::super::State::Ready(carrier) if carrier.inner.is_some()));
    settle_at(
        &mut open,
        Instant::now(),
        Instant::now() + Duration::from_secs(5),
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    let super::super::State::Ready(carrier) = &mut open.state else {
        panic!("retained original carrier");
    };
    carrier.release = original;
    drop(open); // forced destruction of retained Q, no async-clean claim
    joined(peer).await;
}

#[tokio::test]
async fn cold_cancel_and_expired_request_create_no_socket() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    turn(&mut open);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert!(open.witness.is_none());
    let mut expired = local_attempt(listener.local_addr().unwrap().to_string());
    expired.request.deadline = Instant::now();
    turn(&mut expired);
    assert_eq!(expired.view().phase, TcpOpenPhase::Settled);
    assert!(expired.witness.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let bad = crate::shared::TargetAddr::DomainPort {
        host: String::new(),
        port: 443,
    };
    let upstream = Arc::new(RealityUpstream::new(RealityClientConfig {
        server: listener.local_addr().unwrap().to_string(),
        uuid: [0; 16],
        pbk: [0; 32],
        short_id: [0; 8],
        sni: "example.com".into(),
    }));
    assert!(matches!(
        upstream.staged_tcp_open(TcpOpenRequest {
            target: bad,
            deadline
        }),
        Err(crate::upstream::TcpOpenUnavailable::InvalidTarget)
    ));
}

#[tokio::test]
async fn connecting_cancel_closes_original_socket_and_duplicate_before_settled() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    until(&mut open, TcpOpenPhase::Connecting).await;
    let (mut peer, _) = listener.accept().await.unwrap();
    assert!(open.witness.is_some());
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Cancelled, deadline);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert!(open.witness.is_none() && open.shutdown.as_ref().is_some_and(Result::is_ok));
    let mut one = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peer.read(&mut one))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn partial_handshake_shutdown_closes_actual_tcp_and_preserves_first_error() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    until(&mut open, TcpOpenPhase::Preparing).await;
    let (mut peer, _) = listener.accept().await.unwrap();
    turn(&mut open); // poll the actual ClientHello write and pending server read
    let mut first = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peer.read(&mut first))
            .await
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(first[0], 0x16);
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    let first_error = open.failure().unwrap().to_string();
    open.cancel(TcpOpenCancel::Deadline, deadline + Duration::from_secs(50));
    settle_at(
        &mut open,
        Instant::now(),
        deadline + Duration::from_secs(100),
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert_eq!(open.settle_deadline, Some(deadline));
    assert_eq!(open.failure().unwrap().to_string(), first_error);
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
    assert!(open.witness.is_none());
}

#[tokio::test]
async fn ready_withdrawal_retires_real_carrier_before_publication() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let (mut open, peer) = authenticated_peer().await;
    until(&mut open, TcpOpenPhase::Ready).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Cancelled, deadline);
    assert!(open.take_carrier(Instant::now()).is_none());
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert_eq!(Arc::strong_count(&open.release), 1);
    assert_eq!(
        open.release
            .phase
            .load(std::sync::atomic::Ordering::Acquire),
        super::super::RELEASED
    );
    joined(peer).await;
}

#[tokio::test]
async fn held_then_returned_carrier_after_first_deadline_keeps_witness_quarantined() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let (mut open, peer) = authenticated_peer().await;
    until(&mut open, TcpOpenPhase::Ready).await;
    let carrier = open.take_carrier(Instant::now()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Retiring);
    drop(carrier);
    settle_at(&mut open, deadline, deadline + Duration::from_secs(100));
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.witness.is_some());
    assert_eq!(open.settle_deadline, Some(deadline));
    settle_at(
        &mut open,
        deadline + Duration::from_secs(1),
        deadline + Duration::from_secs(200),
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.view().lease_held && open.view().retirement_pending);
    drop(open); // forced Q destruction, not an async-clean receipt
    joined(peer).await;
}

#[tokio::test]
async fn actual_carrier_return_wakes_after_marker_arc_released() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let (mut open, peer) = authenticated_peer().await;
    until(&mut open, TcpOpenPhase::Ready).await;
    let carrier = open.take_carrier(Instant::now()).unwrap();
    struct Wake(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for Wake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
    let wake = Arc::new(Wake(std::sync::atomic::AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wake.clone());
    let deadline = Instant::now() + Duration::from_secs(5);
    open.settle(
        &mut std::task::Context::from_waker(&waker),
        Instant::now(),
        deadline,
    );
    drop(carrier);
    assert!(wake.0.load(std::sync::atomic::Ordering::Acquire) > 0);
    assert_eq!(Arc::strong_count(&open.release), 1);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    joined(peer).await;
}

#[tokio::test]
async fn real_peer_alert_failure_is_retained_through_cancel_and_close() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut hello = [0; 1];
        stream.read_exact(&mut hello).await.unwrap();
        stream
            .write_all(&[0x15, 0x03, 0x03, 0, 2, 2, 40])
            .await
            .unwrap();
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
    });
    until(&mut open, TcpOpenPhase::Preparing).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while open.failure().is_none() {
            turn(&mut open);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let first = open.failure().unwrap().to_string();
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert_eq!(open.failure().unwrap().to_string(), first);
    assert!(open.witness.is_none());
    joined(peer).await;
}

#[tokio::test]
async fn real_peer_transfer_requires_carrier_release_before_physical_close() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let uuid = [0x42; 16];
    let upstream = Arc::new(RealityUpstream::new(RealityClientConfig {
        server: listener.local_addr().unwrap().to_string(),
        uuid,
        pbk: pk,
        short_id: [0; 8],
        sni: "example.com".into(),
    }));
    let server = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        crate::reality_upstream::tests::reality_server_sim(&mut io, sk, uuid).await;
        let mut byte = [0; 1];
        assert_eq!(io.read(&mut byte).await.unwrap(), 0, "actual peer EOF");
    });
    let mut open = upstream
        .staged_tcp_open(TcpOpenRequest {
            target: crate::shared::TargetAddr::parse("1.2.3.4:443").unwrap(),
            deadline: Instant::now() + Duration::from_secs(20),
        })
        .expect("REALITY must support the production staged interface");
    assert_eq!(open.view().phase, TcpOpenPhase::Unstarted);
    until(open.as_mut(), TcpOpenPhase::Ready).await;
    let Some(OpenedTcpRelay::Generic(mut carrier)) = open.take_carrier(Instant::now()) else {
        panic!("Generic carrier");
    };
    carrier.write_all(b"ping").await.unwrap();
    carrier.flush().await.unwrap();
    let mut pong = [0; 4];
    carrier.read_exact(&mut pong).await.unwrap();
    assert_eq!(&pong, b"pong");
    let deadline = Instant::now() + Duration::from_secs(5);
    std::future::poll_fn(|cx| {
        open.settle(cx, Instant::now(), deadline);
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(open.view().phase, TcpOpenPhase::Retiring);
    assert!(open.view().lease_held && open.view().retirement_pending);
    assert!(!server.is_finished(), "carrier still owns physical TCP");
    drop(carrier);
    std::future::poll_fn(|cx| {
        open.settle(cx, Instant::now(), deadline);
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert!(!open.view().lease_held && !open.view().retirement_pending);
    assert!(open.failure().is_none());
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
