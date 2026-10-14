use super::super::*;
use crate::owned_upstream::{LegacyOwnedUpstream, OpenedOwnedTcp, OwnedUpstream};
use crate::tuic::tcp_fixture::UdpPair;
use crate::upstream::{
    RelayFamilyPolicySources, TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Open = Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>;

async fn completed_carrier(upstream: &Arc<TuicUpstream>, peer: &Connection) -> Open {
    let mut attempt = start(upstream, target());
    until(&mut attempt, TcpOpenPhase::Ready).await;
    let mut relay = carrier(&mut attempt);
    relay.shutdown().await.unwrap();
    let (mut send, mut recv) = peer.accept_bi().await.unwrap();
    assert_eq!(
        recv.read_to_end(1024).await.unwrap(),
        encode_connect(&target())
    );
    send.write_all(b"ok").await.unwrap();
    send.finish().unwrap();
    let mut result = Vec::new();
    relay.read_to_end(&mut result).await.unwrap();
    assert_eq!(result, b"ok");
    drop(relay);
    drop(send);
    drop(recv);
    attempt
}

#[tokio::test]
async fn real_authenticated_connect_reaches_owned_staged_factory_and_data() {
    let pair = UdpPair::new(8, 65536).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 65536, 100, true)));
    let mut config = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    config.tcp_pool = 1;
    let (upstream, peer) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            TuicUpstream::connect(&config, Arc::new(Metrics::default())),
            async { pair.server.accept().await.unwrap().await.unwrap() }
        )
    })
    .await
    .unwrap();
    let upstream = Arc::new(upstream.unwrap());
    let mut auth = tokio::time::timeout(Duration::from_secs(2), peer.accept_uni())
        .await
        .unwrap()
        .unwrap();
    let observed = auth.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
        .unwrap();
    assert_eq!(observed, encode_authenticate(&config.uuid, &token));
    let mut attempt = completed_carrier(&upstream, &peer).await;
    drained(&mut attempt).await;
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    assert!(attempt.failure().is_none());
    drop(attempt);
    let endpoint = upstream.endpoint.clone();
    endpoint.close(0u32.into(), b"test done");
    peer.close(0u32.into(), b"test done");
    drop(auth);
    drop(peer);
    drop(upstream);
    tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle())
        .await
        .unwrap();
    pair.close().await;
}

#[tokio::test]
async fn late_poll_and_settlement_failures_cannot_produce_clean_receipts() {
    for fault in 0..3 {
        let pair = UdpPair::new(8, 65536).await;
        let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
        let registry = pressure(&upstream).await;
        let mut attempt = completed_carrier(&upstream, &pair.peer).await;
        let now = Instant::now();
        let deadline = now + Duration::from_secs(2);
        let guard = registry.writers.lock().unwrap();
        settle(&mut attempt, now, deadline);
        assert_eq!(attempt.view().phase, TcpOpenPhase::Retiring);
        drop(guard);
        match fault {
            0 => settle(&mut attempt, deadline, deadline + Duration::from_secs(20)),
            1 => {
                registry.writers.lock().unwrap().clear();
            }
            _ => {
                upstream
                    .tcp_pool_admission
                    .pool_active_total
                    .store(0, Ordering::Release);
            }
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while attempt.view().phase != TcpOpenPhase::Quarantined {
                settle(&mut attempt, now, deadline);
                assert_ne!(attempt.view().phase, TcpOpenPhase::Settled);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let error = format!("{:?}", attempt.failure());
        assert!(attempt.failure().is_some());
        assert!(
            error.contains(match fault {
                0 => "deadline",
                1 => "Identity",
                _ => "Underflow",
            }),
            "{error}"
        );
        if fault == 2 {
            assert!(
                registry.writers.lock().unwrap().is_empty(),
                "successful removal retained before failed debit"
            );
            // Subsequent activity cannot be stolen by repeat settle or Drop.
            upstream
                .tcp_pool_admission
                .pool_active_total
                .store(1, Ordering::Release);
        }
        settle(&mut attempt, now, deadline);
        drop(attempt);
        assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
        pair.assert_bi_progress().await;
        drop(upstream);
        pair.close().await;
    }
}

#[tokio::test]
async fn auxiliary_open_failure_keeps_original_reconnect_policy() {
    let pair = UdpPair::new(8, 65536).await;
    let auxiliary = UdpPair::new(8, 65536).await;
    let mut upstream = pair.upstream(UdpRelayMode::Native).await;
    let slot = Arc::new(TcpPoolGenerationSlot::new(
        1,
        auxiliary.connection.clone(),
        1,
        1,
        upstream.clock,
    ));
    upstream.tcp_pool_slots.push(slot.clone());
    upstream.tcp_pool_admission = TcpPoolAdmission::with_current_generation_activity(vec![
        upstream.tcp_pool_slots[0].current_activity().await,
        slot.current_activity().await,
    ]);
    let upstream = Arc::new(upstream);
    let mut primary = start(&upstream, target());
    until(&mut primary, TcpOpenPhase::Opening).await;
    let mut attempt = start(&upstream, target());
    until(&mut attempt, TcpOpenPhase::Opening).await;
    let state = slot.state.lock().await.current.open_state.clone();
    assert!(!state.needs_reconnect(1));
    auxiliary
        .connection
        .close(0u32.into(), b"injected actual close");
    step(&mut attempt);
    assert!(
        matches!(attempt.failure(), Some(ClientError::Io(e)) if e.get_ref().is_some_and(|source| source.is::<quinn::ConnectionError>()))
    );
    assert!(state.needs_reconnect(1));
    let first = format!("{:?}", attempt.failure());
    attempt.cancel(
        TcpOpenCancel::Shutdown,
        Instant::now() + Duration::from_secs(2),
    );
    drained(&mut attempt).await;
    assert_eq!(format!("{:?}", attempt.failure()), first);
    primary.cancel(
        TcpOpenCancel::Cancelled,
        Instant::now() + Duration::from_secs(2),
    );
    drained(&mut primary).await;
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    drop(attempt);
    drop(primary);
    drop(upstream);
    auxiliary.close().await;
    pair.close().await;
}
fn target() -> TargetAddr {
    TargetAddr::IpPort("192.0.2.1:443".parse().unwrap())
}
fn start(upstream: &Arc<TuicUpstream>, target: TargetAddr) -> Open {
    Arc::new(LegacyOwnedUpstream::new(upstream.clone()))
        .staged_tcp_open(TcpOpenRequest {
            target,
            deadline: Instant::now() + Duration::from_secs(5),
        })
        .expect("actual Owned adapter must supply a staged TUIC attempt")
}
fn step(attempt: &mut Open) {
    attempt.step(
        &mut Context::from_waker(std::task::Waker::noop()),
        Instant::now(),
    );
}
fn settle(attempt: &mut Open, now: Instant, deadline: Instant) {
    attempt.settle(
        &mut Context::from_waker(std::task::Waker::noop()),
        now,
        deadline,
    );
}
async fn until(attempt: &mut Open, phase: TcpOpenPhase) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while attempt.view().phase != phase {
            step(attempt);
            assert_ne!(
                attempt.view().phase,
                TcpOpenPhase::Quarantined,
                "{:?}",
                attempt.failure()
            );
            assert!(attempt.failure().is_none(), "{:?}", attempt.failure());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {phase:?}, got {:?}", attempt.view()));
}
async fn drained(attempt: &mut Open) {
    let deadline = Instant::now() + Duration::from_secs(2);
    tokio::time::timeout(Duration::from_secs(2), async {
        while attempt.view().phase != TcpOpenPhase::Settled {
            settle(attempt, Instant::now(), deadline);
            assert_ne!(
                attempt.view().phase,
                TcpOpenPhase::Quarantined,
                "{:?}",
                attempt.failure()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("retirement did not finish: {:?}", attempt.view()));
}
async fn pressure(upstream: &TuicUpstream) -> Arc<TcpWritePressure> {
    upstream.tcp_pool_slots[0]
        .state
        .lock()
        .await
        .current
        .write_pressure
        .clone()
}
fn carrier(attempt: &mut Open) -> RelayStream {
    match attempt.take_carrier(Instant::now()).expect("ready carrier") {
        OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(stream)) => stream,
        _ => panic!("stored Generic policy changed carrier family"),
    }
}

#[tokio::test]
async fn actual_owned_factory_is_unstarted_and_prestart_cancel_settles() {
    let pair = UdpPair::new(8, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = start(&upstream, target());
    assert_eq!(attempt.view().phase, TcpOpenPhase::Unstarted);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    let now = Instant::now();
    attempt.cancel(TcpOpenCancel::Cancelled, now + Duration::from_secs(5));
    drained(&mut attempt).await;
    assert_eq!(attempt.view().connect_written, 0);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    assert!(attempt.take_carrier(now).is_none());
    drop(attempt);
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn actual_generic_modes_transfer_bytes_then_retire_registry_and_lease_once() {
    for mode in 0..3 {
        let pair = UdpPair::new(8, 65536).await;
        let mut upstream = pair.upstream(UdpRelayMode::Native).await;
        upstream.relay_policy =
            Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
                tuic_ordered_chunk: (mode == 1).then_some("1"),
                tuic_unordered_reassembly: (mode == 2).then_some("1"),
                ..Default::default()
            }));
        let upstream = Arc::new(upstream);
        let registry = pressure(&upstream).await;
        let mut attempt = start(&upstream, target());
        until(&mut attempt, TcpOpenPhase::Ready).await;
        assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
        // Registration contention cannot erase typed halves or return a carrier.
        let guard = registry.writers.lock().unwrap();
        assert!(attempt.take_carrier(Instant::now()).is_none());
        assert_eq!(attempt.view().phase, TcpOpenPhase::Ready);
        drop(guard);
        let mut relay = carrier(&mut attempt);
        assert!(attempt.take_carrier(Instant::now()).is_none());
        assert_eq!(registry.writers.lock().unwrap().len(), 1);
        // The open deadline does not kill an already-transferred flow.
        attempt.step(
            &mut Context::from_waker(std::task::Waker::noop()),
            Instant::now() + Duration::from_secs(10),
        );
        assert_eq!(attempt.view().phase, TcpOpenPhase::Transferred);
        let expected = [encode_connect(&target()), b"request".to_vec()].concat();
        tokio::time::timeout(Duration::from_secs(2), async {
            let (client, server) = tokio::join!(
                async {
                    relay.write_all(b"request").await.unwrap();
                    relay.shutdown().await.unwrap();
                    let mut response = Vec::new();
                    relay.read_to_end(&mut response).await.unwrap();
                    response
                },
                async {
                    let (mut send, mut recv) = pair.peer.accept_bi().await.unwrap();
                    let request = recv.read_to_end(1024).await.unwrap();
                    send.write_all(b"response").await.unwrap();
                    send.finish().unwrap();
                    request
                }
            );
            assert_eq!(client, b"response");
            assert_eq!(server, expected);
        })
        .await
        .expect("real CONNECT and bidirectional DATA must complete");
        // Carrier Drop has metrics authority only; the root owns registry retirement.
        let guard = registry.writers.lock().unwrap();
        drop(relay);
        assert_eq!(guard.len(), 1);
        let now = Instant::now();
        settle(&mut attempt, now, now + Duration::from_secs(2));
        assert_eq!(attempt.view().phase, TcpOpenPhase::Retiring);
        assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
        drop(guard);
        drained(&mut attempt).await;
        assert_eq!(
            attempt.view().connect_written,
            encode_connect(&target()).len()
        );
        assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
        assert!(registry.writers.lock().unwrap().is_empty());
        let next = start(&upstream, target());
        for _ in 0..3 {
            settle(&mut attempt, Instant::now(), now);
        }
        assert!(attempt.failure().is_none());
        drop(next);
        pair.assert_bi_progress().await;
        drop(attempt);
        drop(upstream);
        pair.close().await;
    }
}

#[tokio::test]
async fn credit_and_slot_pressure_keep_one_exact_reservation_until_cancel() {
    let pair = UdpPair::with_limits(8, 65536, 0).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = start(&upstream, target());
    until(&mut attempt, TcpOpenPhase::Reserved).await;
    let guard = upstream.tcp_pool_slots[0].state.lock().await;
    for _ in 0..32 {
        step(&mut attempt);
    }
    assert_eq!(attempt.view().phase, TcpOpenPhase::Reserved);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    drop(guard);
    until(&mut attempt, TcpOpenPhase::Opening).await;
    for _ in 0..32 {
        step(&mut attempt);
    }
    assert_eq!(attempt.view().phase, TcpOpenPhase::Opening);
    assert!(!attempt.view().retirement_pending);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    // Shared UDP progresses although no bi credit exists.
    pair.connection
        .send_datagram(bytes::Bytes::from_static(b"udp"))
        .unwrap();
    assert_eq!(pair.peer.read_datagram().await.unwrap().as_ref(), b"udp");
    attempt.cancel(
        TcpOpenCancel::Shutdown,
        Instant::now() + Duration::from_secs(2),
    );
    drained(&mut attempt).await;
    assert_eq!(attempt.view().connect_written, 0);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    drop(attempt);
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn partial_connect_cancel_preserves_cursor_and_waits_for_real_retirement() {
    let pair = UdpPair::new(8, 4).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = start(&upstream, target());
    until(&mut attempt, TcpOpenPhase::Connecting).await;
    step(&mut attempt);
    assert_eq!(attempt.view().connect_written, 4);
    for _ in 0..32 {
        step(&mut attempt);
    }
    assert_eq!(attempt.view().connect_written, 4);
    let (send, mut recv) = tokio::time::timeout(Duration::from_secs(2), pair.peer.accept_bi())
        .await
        .unwrap()
        .unwrap();
    let now = Instant::now();
    attempt.cancel(TcpOpenCancel::Cancelled, now + Duration::from_secs(2));
    settle(&mut attempt, now, now + Duration::from_secs(2));
    assert_eq!(attempt.view().phase, TcpOpenPhase::Retiring);
    assert_eq!(attempt.view().connect_written, 4);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    // There was no full CONNECT; reset is visible to the actual peer.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), recv.read_to_end(1024))
            .await
            .unwrap()
            .is_err()
    );
    drop(send);
    drop(recv);
    drained(&mut attempt).await;
    assert_eq!(attempt.view().connect_written, 4);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    assert!(
        matches!(attempt.failure(), Some(ClientError::Io(e)) if e.kind() == io::ErrorKind::Interrupted)
    );
    pair.assert_bi_progress().await;
    drop(attempt);
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn deadline_quarantine_keeps_lease_and_first_cancellation_sticky() {
    let pair = UdpPair::new(8, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = start(&upstream, target());
    until(&mut attempt, TcpOpenPhase::Ready).await;
    let relay = carrier(&mut attempt);
    let (send, recv) = pair.peer.accept_bi().await.unwrap();
    let now = Instant::now();
    attempt.cancel(TcpOpenCancel::Shutdown, now);
    // A later deadline cannot extend the first retirement deadline.
    settle(&mut attempt, now, now + Duration::from_secs(30));
    assert_eq!(attempt.view().phase, TcpOpenPhase::Quarantined);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 1);
    drop(relay);
    drop(send);
    drop(recv);
    step(&mut attempt);
    settle(
        &mut attempt,
        now + Duration::from_secs(60),
        now + Duration::from_secs(90),
    );
    assert_eq!(attempt.view().phase, TcpOpenPhase::Quarantined);
    assert!(attempt.take_carrier(now).is_none());
    assert!(
        matches!(attempt.failure(), Some(ClientError::Io(e)) if e.to_string().contains("Shutdown"))
    );
    drop(attempt);
    assert_eq!(
        upstream.tcp_pool_admission.active_total(),
        1,
        "accidental Drop cannot forge a receipt"
    );
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn generation_mismatch_rejects_without_open_and_retains_first_failure() {
    let pair = UdpPair::new(8, 65536).await;
    let upstream = Arc::new(pair.upstream(UdpRelayMode::Native).await);
    let mut attempt = start(&upstream, target());
    until(&mut attempt, TcpOpenPhase::Reserved).await;
    {
        let mut slot = upstream.tcp_pool_slots[0].state.lock().await;
        slot.current.activity = Arc::new(TcpPoolGenerationActivity::new());
    }
    step(&mut attempt);
    assert!(attempt.failure().is_some());
    let original = format!("{:?}", attempt.failure());
    attempt.cancel(
        TcpOpenCancel::Shutdown,
        Instant::now() + Duration::from_secs(2),
    );
    drained(&mut attempt).await;
    assert_eq!(format!("{:?}", attempt.failure()), original);
    assert_eq!(upstream.tcp_pool_admission.active_total(), 0);
    assert_eq!(attempt.view().connect_written, 0);
    drop(attempt);
    drop(upstream);
    pair.close().await;
}
