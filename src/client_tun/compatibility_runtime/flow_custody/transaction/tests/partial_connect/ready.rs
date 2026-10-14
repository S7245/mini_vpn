//! Ready custody under the real TUIC writer-registry mutex, at both entries.
//! The guard spans provider retirement and physical RST/flush/release. Tests
//! observe original state and deadlines; they never replace the open provider.
use super::*;
use crate::tuic::tcp_fixture::with_ready_registry_held;

#[derive(Debug, PartialEq, Eq)]
struct FrozenReady {
    id: FlowIdentity,
    provider: usize,
    deadline: tokio::time::Instant,
    open_deadline: std::time::Instant,
    view: TcpOpenView,
    escrow: Vec<u8>,
}

fn ready_custody(owner: &SessionControl) -> (FrozenReady, tokio::time::Instant) {
    let seen = owner.flow_observation().unwrap();
    assert_eq!(
        (seen.live, seen.w, seen.f, seen.factory_started),
        (1, 1, 1, 1)
    );
    assert_eq!(
        (seen.escrow_bytes, seen.reserved),
        (PAYLOAD.len(), MAX_UPLINK_BUFFER)
    );
    assert_eq!(
        (seen.published, seen.released, seen.joined, seen.running),
        (0, 0, 0, 0)
    );
    assert!(seen.audit && !owner.ledger().quarantined);
    let opens = owner.opens();
    let slot = opens.slots.iter().flatten().next().unwrap();
    let OpenPhase::Flow(flow) = &slot.phase else {
        panic!("flow mode")
    };
    assert_eq!(flow.stage, FlowStage::Ready);
    assert!(flow.physical && flow.w_held && flow.f_held);
    assert!(flow.roles.is_none() && flow.carrier.is_none());
    assert_eq!(flow.escrow.as_slice(), PAYLOAD);
    let attempt = flow.attempt.as_ref().unwrap();
    let view = attempt.view();
    assert_eq!(view.phase, TcpOpenPhase::Ready);
    assert_eq!(view.connect_written, encode_connect(&target()).len());
    assert_eq!(view.connect_bytes, view.connect_written);
    assert!(view.lease_held && view.retirement_pending);
    let (stage, probe) = flow.physical_check.unwrap();
    assert_eq!(stage, FlowStage::Ready);
    (
        FrozenReady {
            id: flow.id,
            provider: attempt.as_ref() as *const _ as *const () as usize,
            deadline: slot.deadline,
            open_deadline: flow.open_deadline,
            view,
            escrow: flow.escrow.clone(),
        },
        probe,
    )
}

fn ready_settled(owner: &SessionControl) -> (usize, String) {
    let seen = owner.flow_observation().unwrap();
    assert_eq!(
        (seen.live, seen.w, seen.f, seen.reserved, seen.escrow_bytes),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(
        (
            seen.factory_started,
            seen.released,
            seen.published,
            seen.joined,
            seen.running
        ),
        (1, 1, 0, 0, 0)
    );
    assert!(seen.audit && !owner.ledger().quarantined);
    let failure = owner.retained_provider_failure().unwrap();
    assert_eq!(failure.1.phase, TcpOpenPhase::Settled);
    assert_eq!(failure.1.connect_written, encode_connect(&target()).len());
    assert_eq!(failure.1.connect_bytes, failure.1.connect_written);
    assert!(!failure.1.lease_held && !failure.1.retirement_pending);
    assert_eq!(
        owner.retained_provider_io_kind(),
        Some(std::io::ErrorKind::Interrupted)
    );
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().io_kind,
        Some(std::io::ErrorKind::Other)
    );
    assert_eq!(ledger.additional_failures, 0);
    (failure.0, failure.2)
}

async fn full_connect(ready: &ReadyPeer) -> (quinn::SendStream, quinn::RecvStream) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let (send, mut recv) = ready.peer.accept_bi().await.unwrap();
        assert_eq!(recv.id().index(), 0);
        let expected = encode_connect(&target());
        let mut bytes = vec![0; expected.len()];
        recv.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected);
        (send, recv)
    })
    .await
    .unwrap()
}

async fn same_carrier_progress(ready: &ReadyPeer) {
    let connection = upstream_ready_connection(&ready.upstream).unwrap();
    assert_eq!(connection.stable_id(), ready.id);
    tokio::time::timeout(Duration::from_secs(2), async {
        let (client, peer) = tokio::join!(
            async {
                let (mut send, mut recv) = connection.open_bi().await.unwrap();
                assert_eq!(send.id().index(), 1);
                send.write_all(b"same").await.unwrap();
                send.finish().unwrap();
                recv.read_to_end(4).await.unwrap()
            },
            async {
                let (mut send, mut recv) = ready.peer.accept_bi().await.unwrap();
                let bytes = recv.read_to_end(4).await.unwrap();
                send.write_all(&bytes).await.unwrap();
                send.finish().unwrap();
                bytes
            }
        );
        assert_eq!(client, b"same");
        assert_eq!(peer, b"same");
    })
    .await
    .unwrap();
    ready.assert_current(2);
}

#[tokio::test]
async fn actual_ready_registry_contention_retires_without_unlocking_or_losing_custody() {
    let ready = ReadyPeer::with_window(65_536).await;
    let setup = capture(Physical::new(ready.upstream.clone())).await;
    let mut physical = match setup {
        Ok(physical) => physical,
        Err(panic) => {
            let _ = capture(ready.close()).await;
            std::panic::resume_unwind(panic)
        }
    };
    let mut halves = None;
    let scenario = capture(with_ready_registry_held(&ready.upstream, async {
        ready.assert_current(0);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !view(&physical.owner).is_some_and(|v| v.phase == TcpOpenPhase::Ready) {
                physical.control().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (frozen, first_probe) = ready_custody(&physical.owner);
        let mut last_probe = first_probe;
        let mut retries = 0;
        for _ in 0..16 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            // Each real turn scans at most 32 of the original 4224 slots.
            // Walk the cursor without bypassing it; each observer is dropped.
            for _ in 0..=(FLOW_LIMIT + OPEN_LIMIT) / 32 {
                physical.control().await;
            }
            let (now, probe) = ready_custody(&physical.owner);
            assert_eq!(now, frozen);
            retries += usize::from(probe > last_probe);
            last_probe = probe;
        }
        assert!(
            retries >= 2 && last_probe > first_probe,
            "cross the original 5ms physical retry"
        );
        halves = Some(full_connect(&ready).await);
        physical.owner.request_stop();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if view(&physical.owner).is_some_and(|v| v.phase == TcpOpenPhase::Retiring) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let source = active_error(&physical.owner);
        tokio::time::timeout(
            Duration::from_secs(2),
            peer_retirement(halves.take().unwrap()),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if view(&physical.owner).unwrap().phase == TcpOpenPhase::Settled
                    && physical
                        .server
                        .sockets
                        .get::<TcpSocket>(frozen.id.handle)
                        .state()
                        == smoltcp::socket::tcp::State::Closed
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(active_error(&physical.owner), source);
        let seen = physical.owner.flow_observation().unwrap();
        assert_eq!(
            (seen.live, seen.w, seen.f, seen.released, seen.joined),
            (1, 1, 1, 0, 0)
        );
        assert!(
            physical
                .server
                .sockets
                .get::<TcpSocket>(frozen.id.handle)
                .remote_endpoint()
                .is_some()
        );
        assert_eq!(
            physical.contexts[&frozen.id.handle].conn_epoch,
            frozen.id.epoch
        );
        assert!(!physical.dispatch.flow_shutdown_complete(
            &physical.server.sockets,
            &mut physical.contexts,
            &physical.pool
        ));
        physical.flush().await;
        assert_eq!(
            physical
                .client
                .sockets
                .get::<TcpSocket>(physical.client.handle)
                .state(),
            smoltcp::socket::tcp::State::Closed
        );
        assert!(physical.cleanup().await);
        assert_eq!(ready_settled(&physical.owner), source);
        assert_eq!(
            physical.contexts[&frozen.id.handle].conn_epoch,
            frozen.id.epoch + 1
        );
        assert!(physical.contexts[&frozen.id.handle].flow_mirror.is_none());
        assert_eq!(
            physical
                .server
                .sockets
                .get::<TcpSocket>(frozen.id.handle)
                .state(),
            smoltcp::socket::tcp::State::Listen
        );
        for _ in 0..2 {
            assert!(physical.dispatch.flow_shutdown_complete(
                &physical.server.sockets,
                &mut physical.contexts,
                &physical.pool
            ));
            assert_eq!(ready_settled(&physical.owner), source);
        }
        same_carrier_progress(&ready).await;
    }))
    .await;
    drop(halves.take());
    let cleaned = capture(physical.cleanup()).await;
    drop(physical);
    let closed = capture(ready.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(cleaned.unwrap_or_else(|panic| std::panic::resume_unwind(panic)));
    closed.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

#[tokio::test]
async fn saved_loop_ready_registry_contention_preserves_full_connect_until_stop_settles() {
    let ready = ReadyPeer::with_window(65_536).await;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut client = Endpoint::new(outbound.clone(), inbound.clone(), false);
    let (_downlink, downlink_rx) = mpsc::channel(1); // No start_udp/recovery sampler.
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        ready.upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let mut halves = None;
    let scenario = capture(with_ready_registry_held(&ready.upstream, async {
        ready.assert_current(0);
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut sent = false;
            loop {
                let socket = client.sockets.get_mut::<TcpSocket>(client.handle);
                if !sent && socket.can_send() {
                    assert_eq!(socket.send_slice(PAYLOAD).unwrap(), PAYLOAD.len());
                    sent = true;
                }
                saved_turn(&mut session, &mut client).await;
                if view(&session.control).is_some_and(|v| v.phase == TcpOpenPhase::Ready) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let (frozen, first_probe) = ready_custody(&session.control);
        let mut last_probe = first_probe;
        let mut retries = 0;
        for _ in 0..16 {
            saved_turn(&mut session, &mut client).await;
            let (now, probe) = ready_custody(&session.control);
            assert_eq!(now, frozen);
            retries += usize::from(probe > last_probe);
            last_probe = probe;
        }
        assert!(
            retries >= 2 && last_probe > first_probe,
            "actual saved loop retries beyond 5ms"
        );
        halves = Some(full_connect(&ready).await);
        session.request_stop();
        let mut retiring = std::pin::pin!(peer_retirement(halves.take().unwrap()));
        let mut peer_done = false;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                client.turn().await;
                let state = session.state();
                tokio::select! {
                    result = session.run(), if state != SessionDeviceState::DeviceFailedSettled => {
                        assert_eq!(result, SessionDeviceState::DeviceFailedSettled);
                    }
                    () = &mut retiring, if !peer_done => peer_done = true,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
                if session.state() == SessionDeviceState::DeviceFailedSettled && peer_done {
                    break;
                }
            }
        })
        .await
        .unwrap();
        client.turn().await;
        assert_eq!(
            client.sockets.get::<TcpSocket>(client.handle).state(),
            smoltcp::socket::tcp::State::Closed
        );
        let source = ready_settled(&session.control);
        for _ in 0..2 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::DeviceFailedSettled
            );
            assert_eq!(ready_settled(&session.control), source);
        }
        same_carrier_progress(&ready).await;
    }))
    .await;
    drop(halves.take());
    session.request_stop();
    let cleaned = capture(tokio::time::timeout(
        Duration::from_secs(15),
        session.observe_shutdown(),
    ))
    .await;
    drop(session);
    let closed = capture(ready.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(
        cleaned
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            .unwrap(),
        SessionDeviceState::DeviceFailedSettled
    );
    closed.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}
