//! Actual partial CONNECT cancellation, with one root and real TCP/QUIC endpoints.
//! The physical tracer freezes pre-flush custody; the saved-loop tracer covers
//! the real entry. Neither fabricates receipts, providers or joined relay roles.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{Endpoint, receiving_pair};
use super::super::*;
use crate::client_tun::compatibility_runtime::{
    ClientTunSession, SessionDeviceState, prepare_client_tun_session,
};
use crate::client_tun::{
    DownlinkBackpressureConfig, HandshakeCompletion, NoopSink, TunRuntimeConfig,
    flush_tx_and_release_downlink_permits,
};
use crate::fake_ip::FakeIpPool;
use crate::harness::{LoopbackTunDevice, PacketLink};
use crate::metrics::Metrics;
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint, upstream_ready_connection};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use crate::upstream::TcpOpenView;
use std::collections::HashMap;
use std::time::Duration;

const PAYLOAD: &[u8] = b"untouched-rx";

async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

fn target() -> TargetAddr {
    TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap())
}

struct ReadyPeer {
    pair: UdpPair,
    upstream: Arc<TuicUpstream>,
    peer: quinn::Connection,
    auth: quinn::RecvStream,
    id: usize,
}

impl ReadyPeer {
    async fn new() -> Self {
        Self::with_window(4).await
    }

    async fn with_window(window: u32) -> Self {
        Self::with_limits(window, 1).await
    }

    async fn with_limits(window: u32, bi_credit: u32) -> Self {
        let pair = UdpPair::new(8, window).await;
        pair.server
            .set_server_config(Some(UdpPair::server_config(8, window, bi_credit, true)));
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
        let connected = capture(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    TuicUpstream::connect(&config, Arc::new(Metrics::default())),
                    async {
                        let peer = pair.server.accept().await.unwrap().await.unwrap();
                        // AUTH exceeds the window: read concurrently with connect.
                        let mut auth = peer.accept_uni().await.unwrap();
                        let observed = auth.read_to_end(64).await.unwrap();
                        let mut token = [0; 32];
                        peer.export_keying_material(
                            &mut token,
                            &config.uuid,
                            config.password.as_bytes(),
                        )
                        .unwrap();
                        assert_eq!(observed, encode_authenticate(&config.uuid, &token));
                        (peer, auth)
                    }
                )
            })
            .await
            .unwrap()
        })
        .await;
        let (upstream, peer, auth) = match connected {
            Ok((Ok(upstream), (peer, auth))) => (Arc::new(upstream), peer, auth),
            failure => {
                pair.close().await;
                match failure {
                    Err(panic) => std::panic::resume_unwind(panic),
                    _ => panic!("authenticated partial CONNECT setup failed"),
                }
            }
        };
        let id = upstream_ready_connection(&upstream).unwrap().stable_id();
        Self {
            pair,
            upstream,
            peer,
            auth,
            id,
        }
    }

    fn assert_current(&self, next: u64) {
        let current = upstream_ready_connection(&self.upstream).unwrap();
        assert_eq!(current.stable_id(), self.id);
        let (opened, credit, handshaking, closed, failed) = current.bi_stream_open_snapshot();
        assert_eq!(
            (opened, handshaking, closed, failed),
            (next, false, false, false)
        );
        assert!(credit >= 1);
        assert!(self.peer.close_reason().is_none());
    }

    async fn prefix(&self) -> (quinn::SendStream, quinn::RecvStream) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let (send, mut recv) = self.peer.accept_bi().await.unwrap();
            assert_eq!(recv.id().index(), 0);
            let mut bytes = [0; 4];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, &encode_connect(&target())[..4]);
            (send, recv)
        })
        .await
        .unwrap()
    }

    async fn close(self) {
        let endpoint = upstream_endpoint(&self.upstream);
        endpoint.close(0u32.into(), b"partial CONNECT cleanup");
        self.peer.close(0u32.into(), b"partial CONNECT cleanup");
        drop(self.upstream);
        drop(self.peer);
        drop(self.auth);
        let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
        self.pair.close().await;
        idle.unwrap();
    }
}

async fn peer_retirement((send, mut recv): (quinn::SendStream, quinn::RecvStream)) {
    let mut unexpected = [0; 16];
    let (read, stopped) = tokio::join!(recv.read(&mut unexpected), send.stopped());
    assert!(
        matches!(read, Err(quinn::ReadError::Reset(_))),
        "remaining CONNECT/payload must not be sent: {read:?}"
    );
    assert!(stopped.unwrap().is_some(), "actual STOP_SENDING");
    drop((send, recv));
}

fn view(owner: &SessionControl) -> Option<TcpOpenView> {
    let opens = owner.opens();
    let slot = opens.slots.iter().flatten().next()?;
    let OpenPhase::Flow(flow) = &slot.phase else {
        panic!("flow mode")
    };
    flow.attempt.as_ref().map(|attempt| attempt.view())
}

fn held(owner: &SessionControl) -> FlowIdentity {
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
    assert_eq!(flow.escrow.as_slice(), PAYLOAD);
    assert!(flow.physical && flow.roles.is_none() && flow.carrier.is_none());
    let view = flow.attempt.as_ref().unwrap().view();
    assert_eq!(view.connect_written, 4);
    assert_eq!(view.connect_bytes, encode_connect(&target()).len());
    flow.id
}

fn active_error(owner: &SessionControl) -> (usize, String) {
    let opens = owner.opens();
    let OpenPhase::Flow(flow) = &opens.slots.iter().flatten().next().unwrap().phase else {
        panic!("flow mode")
    };
    let source = flow.attempt.as_ref().unwrap().failure().unwrap();
    assert!(
        matches!(source, ClientError::Io(error) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    (source as *const ClientError as usize, source.to_string())
}

fn settled(owner: &SessionControl) -> (usize, String) {
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
    assert_eq!(failure.1.connect_written, 4);
    assert_eq!(failure.1.connect_bytes, encode_connect(&target()).len());
    assert!(!failure.1.lease_held && !failure.1.retirement_pending);
    let opens = owner.opens();
    let source = opens
        .flow
        .as_ref()
        .unwrap()
        .first_provider_failure
        .as_ref()
        .unwrap()
        .failure()
        .unwrap();
    assert!(
        matches!(source, ClientError::Io(error) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    assert_eq!(
        (failure.0, &failure.2),
        (source as *const ClientError as usize, &source.to_string())
    );
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().io_kind,
        Some(std::io::ErrorKind::Other)
    );
    assert_eq!(ledger.additional_failures, 0);
    (failure.0, failure.2)
}

struct Physical {
    client: Endpoint,
    server: Endpoint,
    owner: Arc<SessionControl>,
    dispatch: OpenDispatch,
    receiver: mpsc::Receiver<HandshakeNotice>,
    contexts: HashMap<SocketHandle, SocketCtx>,
    pool: FakeIpPool,
}

impl Physical {
    async fn new(upstream: Arc<TuicUpstream>) -> Self {
        Self::new_with_fake_ip(upstream, false).await
    }

    async fn new_with_fake_ip(upstream: Arc<TuicUpstream>, with_fake_ip: bool) -> Self {
        let (client, mut server) = receiving_pair().await;
        let owner = Arc::new(SessionControl::default());
        *owner.opens() = OpenCustody::with_flows();
        let (dispatch, receiver) = OpenDispatch::channel(4, Some(owner.clone()));
        let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
        let mut pool = FakeIpPool::new();
        let fake_ip = with_fake_ip.then(|| pool.alloc("receipt.test", 0).unwrap());
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            Arc::new(crate::owned_upstream::LegacyOwnedUpstream::new(upstream)),
            FlowAdmission {
                handle: server.handle,
                socket: server.sockets.get_mut::<TcpSocket>(server.handle),
                ctx: contexts.get_mut(&server.handle).unwrap(),
                fake_pool: &mut pool,
                fake_ip,
                target: target(),
                now: 0,
            },
        );
        Self {
            client,
            server,
            owner,
            dispatch,
            receiver,
            contexts,
            pool,
        }
    }

    async fn control(&mut self) {
        self.dispatch.begin_turn().await;
        let event = std::future::poll_fn(|cx| {
            let mut next = std::pin::pin!(self.dispatch.recv(&mut self.receiver));
            Poll::Ready(next.as_mut().poll(cx))
        })
        .await;
        if let Poll::Ready(Some(event)) = event {
            let (global_tx, _global_rx) = mpsc::channel(4);
            let (owned_action_tx, _owned_rx) = mpsc::channel(4);
            HandshakeCompletion::from_loop(
                &self.dispatch,
                event,
                HandshakeCompletion {
                    sockets: &mut self.server.sockets,
                    socket_ctxs: &mut self.contexts,
                    global_tx: &global_tx,
                    owned_action_tx: &owned_action_tx,
                    fake_pool: &mut self.pool,
                    now_secs: 0,
                    metrics_handle: &Metrics::default(),
                    downlink_backpressure: DownlinkBackpressureConfig::default(),
                    relay_read_hard_pause: false,
                },
            );
        }
    }

    async fn flush(&mut self) {
        self.server.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.server.device,
            &mut self.server.sockets,
        );
        flush_tx_and_release_downlink_permits(
            &mut self.server.device,
            &self.server.sockets,
            &mut self.contexts,
            "partial CONNECT",
        )
        .await
        .0
        .unwrap();
        self.client.turn().await;
    }

    async fn cleanup(&mut self) -> bool {
        self.owner.request_stop();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                self.control().await;
                self.flush().await;
                if self.dispatch.flow_shutdown_complete(
                    &self.server.sockets,
                    &mut self.contexts,
                    &self.pool,
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    }
}

#[tokio::test]
async fn actual_partial_connect_keeps_root_custody_until_retirement_and_physical_flush() {
    let ready = ReadyPeer::new().await;
    let setup = capture(Physical::new(ready.upstream.clone())).await;
    let mut physical = match setup {
        Ok(physical) => physical,
        Err(panic) => {
            ready.close().await;
            std::panic::resume_unwind(panic)
        }
    };
    let mut halves = None;
    let scenario = capture(async {
        ready.assert_current(0);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if view(&physical.owner)
                    .is_some_and(|v| v.phase == TcpOpenPhase::Connecting && v.connect_written == 4)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let id = held(&physical.owner);
        for _ in 0..256 {
            physical.control().await;
        }
        assert_eq!(
            view(&physical.owner).unwrap().phase,
            TcpOpenPhase::Connecting
        );
        assert_eq!(held(&physical.owner), id);
        halves = Some(ready.prefix().await);
        physical.owner.request_stop(); // No provider poll after returning read credit.
        for _ in 0..512 {
            physical.control().await;
            if view(&physical.owner).is_some_and(|v| v.phase == TcpOpenPhase::Retiring) {
                break;
            }
        }
        let retiring = view(&physical.owner).unwrap();
        assert_eq!(retiring.phase, TcpOpenPhase::Retiring);
        assert!(retiring.lease_held && retiring.retirement_pending);
        assert_eq!(
            physical.owner.flow_observation().unwrap().stopped_producers,
            1
        );
        assert_eq!(held(&physical.owner), id);
        let source = active_error(&physical.owner);
        tokio::time::timeout(
            Duration::from_secs(2),
            peer_retirement(halves.take().unwrap()),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while view(&physical.owner).unwrap().phase != TcpOpenPhase::Settled {
                physical.control().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..256 {
            physical.control().await;
        }
        assert_eq!(held(&physical.owner), id);
        assert_eq!(active_error(&physical.owner), source);
        assert!(physical.owner.ledger().first_failure.is_none());
        let socket = physical.server.sockets.get::<TcpSocket>(id.handle);
        assert_eq!(socket.state(), smoltcp::socket::tcp::State::Closed);
        assert!(
            socket.remote_endpoint().is_some(),
            "abort retains tuple before actual RST dispatch"
        );
        assert_eq!(physical.contexts[&id.handle].conn_epoch, id.epoch);
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
        assert_eq!(settled(&physical.owner), source);
        assert_eq!(physical.contexts[&id.handle].conn_epoch, id.epoch + 1);
        assert!(physical.contexts[&id.handle].flow_mirror.is_none());
        assert_eq!(
            physical.server.sockets.get::<TcpSocket>(id.handle).state(),
            smoltcp::socket::tcp::State::Listen
        );
        for _ in 0..2 {
            assert!(physical.dispatch.flow_shutdown_complete(
                &physical.server.sockets,
                &mut physical.contexts,
                &physical.pool
            ));
            assert_eq!(settled(&physical.owner), source);
        }
        ready.assert_current(1);
    })
    .await;
    drop(halves.take());
    let cleaned = physical.cleanup().await;
    drop(physical);
    ready.close().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(cleaned);
}

async fn saved_turn<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    client: &mut Endpoint,
) {
    client.turn().await;
    tokio::select! {
        state = session.run() => panic!("saved entry ended early: {state:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
}

#[tokio::test]
async fn saved_loop_partial_connect_stop_preserves_original_failure_and_offset() {
    let ready = ReadyPeer::new().await;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut client = Endpoint::new(outbound.clone(), inbound.clone(), false);
    let (_downlink, downlink_rx) = mpsc::channel(1);
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
    let scenario = capture(async {
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
                if view(&session.control)
                    .is_some_and(|v| v.phase == TcpOpenPhase::Connecting && v.connect_written == 4)
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let id = held(&session.control);
        for _ in 0..8 {
            saved_turn(&mut session, &mut client).await;
        }
        assert_eq!(held(&session.control), id);
        assert_eq!(
            view(&session.control).unwrap().phase,
            TcpOpenPhase::Connecting
        );
        // saved_turn's observation has been dropped before the peer returns credit.
        halves = Some(ready.prefix().await);
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
        let source = settled(&session.control);
        for _ in 0..2 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::DeviceFailedSettled
            );
            assert_eq!(settled(&session.control), source);
        }
        ready.assert_current(1);
    })
    .await;
    drop(halves.take());
    session.request_stop();
    let cleaned = tokio::time::timeout(Duration::from_secs(15), session.observe_shutdown()).await;
    drop(session);
    ready.close().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(cleaned.unwrap(), SessionDeviceState::DeviceFailedSettled);
}

mod partial_start;
mod ready;
mod receipt;

mod census;
mod precommit;
