//! Saved-loop partial start failure with real TUIC carrier and three role tasks.
//! Per-flow test fault removes one real gate; original publication, stop and
//! joins retain nonzero quarantine. Endpoint teardown is forced cleanup, not
//! successful provider/physical settlement.
use super::*;
use crate::device::TunIo;
use crate::tuic::tcp_fixture::with_ready_registry_held;

type Identity = (
    FlowIdentity,
    usize,
    tokio::time::Instant,
    std::time::Instant,
);

fn identity(owner: &SessionControl) -> Identity {
    let opens = owner.opens();
    assert_eq!(opens.live, 1);
    let slot = opens.slots.iter().flatten().next().unwrap();
    let OpenPhase::Flow(flow) = &slot.phase else {
        panic!("actual flow")
    };
    (
        flow.id,
        flow.attempt.as_ref().unwrap().as_ref() as *const _ as *const () as usize,
        slot.deadline,
        flow.open_deadline,
    )
}

fn quarantined(owner: &SessionControl, missing: usize) {
    let seen = owner.flow_observation().unwrap();
    assert_eq!(
        (seen.live, seen.f, seen.w, seen.reserved, seen.escrow_bytes),
        (1, 1, 0, 0, 0)
    );
    assert_eq!(
        (
            seen.factory_started,
            seen.published,
            seen.released,
            seen.joined,
            seen.running
        ),
        (1, 1, 0, 3, 0)
    );
    assert_eq!(seen.stopped_producers, 1);
    assert!(seen.audit && owner.ledger().quarantined);
    let opens = owner.opens();
    let OpenPhase::Flow(flow) = &opens.slots.iter().flatten().next().unwrap().phase else {
        panic!("actual flow")
    };
    assert_eq!(flow.stage, FlowStage::Quarantined);
    assert!(flow.physical && flow.f_held && flow.published && flow.producer_stopped);
    assert!(flow.missing_start_gate.is_none() && flow.carrier.is_none());
    let roles = flow.roles.as_ref().unwrap();
    let mut expected = [true; 3];
    expected[missing] = false;
    assert_eq!(roles.joined_starts_for_test(), Some(expected));
    assert!(roles.quarantined() && !roles.settled());
    let view = flow.attempt.as_ref().unwrap().view();
    assert_eq!(view.phase, TcpOpenPhase::Transferred);
    assert_eq!(view.connect_written, encode_connect(&target()).len());
    assert!(view.lease_held && view.retirement_pending);
}

async fn observe<F: Future<Output = ()>>(session: &mut ClientTunSession<F>, client: &mut Endpoint) {
    client.turn().await;
    std::future::poll_fn(|cx| {
        let state = session.poll_driver(cx);
        assert!(!matches!(
            state,
            SessionDeviceState::DeviceSettled | SessionDeviceState::DeviceFailedSettled
        ));
        Poll::Ready(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1)).await;
}

async fn partial_start(missing: usize) {
    let ready = ReadyPeer::with_window(65_536).await;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut client = Endpoint::new(outbound.clone(), inbound.clone(), false);
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound.clone(), outbound),
        ready.upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let mut halves = None;
    let scenario = capture(async {
        let original = with_ready_registry_held(&ready.upstream, async {
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
            let original = identity(&session.control);
            {
                let mut opens = session.control.opens();
                let flow = opens.flow_mut(original.0).unwrap();
                assert_eq!(flow.stage, FlowStage::Ready);
                assert!(flow.roles.is_none() && flow.carrier.is_none());
                assert_eq!(flow.escrow.as_slice(), PAYLOAD);
                assert!(flow.missing_start_gate.replace(missing).is_none());
            }
            halves = Some(
                tokio::time::timeout(Duration::from_secs(2), async {
                    let (send, mut recv) = ready.peer.accept_bi().await.unwrap();
                    let expected = encode_connect(&target());
                    let mut bytes = vec![0; expected.len()];
                    recv.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(bytes, expected);
                    (send, recv)
                })
                .await
                .unwrap(),
            );
            original
        })
        .await; // Release only the fixture lock; actual root still owns Ready.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                observe(&mut session, &mut client).await;
                let seen = session.control.flow_observation().unwrap();
                if seen.joined == 3
                    && seen.stopped_producers == 1
                    && session.control.ledger().quarantined
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        quarantined(&session.control, missing);
        assert_eq!(identity(&session.control), original);
        assert!(
            !session.control.stop.stopping.load(Ordering::Acquire),
            "single-flow quarantine is observed before explicit session stop"
        );
        session.request_stop();
        let deadline = session.control.ledger().shutdown_deadline.unwrap();
        // An actual new app SYN after stop must never start another provider.
        let mut late = TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
        );
        late.connect(
            client.iface.context(),
            (smoltcp::wire::IpAddress::v4(10, 0, 0, 2), 10000),
            20001,
        )
        .unwrap();
        let late = client.sockets.add(late);
        {
            let mut probe = LateSyn(&mut client.device, 0);
            client.iface.poll(
                smoltcp::time::Instant::now(),
                &mut probe,
                &mut client.sockets,
            );
            assert_eq!(probe.1, 1, "actual app emits the late SYN");
        }
        client.device.flush_tx().await.unwrap();
        for _ in 0..16 {
            observe(&mut session, &mut client).await;
            quarantined(&session.control, missing);
            assert_eq!(identity(&session.control), original);
            assert_eq!(session.state(), SessionDeviceState::Incomplete);
            assert_eq!(session.control.ledger().shutdown_deadline, Some(deadline));
        }
        assert_eq!(
            client.sockets.get::<TcpSocket>(late).state(),
            smoltcp::socket::tcp::State::SynSent
        );
        assert_eq!(inbound.dropped_packets(), 0);
        for _ in 0..2 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::Incomplete
            );
            quarantined(&session.control, missing);
            assert_eq!(identity(&session.control), original);
            assert_eq!(session.control.ledger().shutdown_deadline, Some(deadline));
        }
        assert!(!session.control.driver_complete.load(Ordering::Acquire));
    })
    .await;
    session.request_stop();
    let deadline = session.control.ledger().shutdown_deadline.unwrap();
    let cleanup = capture(drain_roles(&mut session, deadline)).await;
    // A failed scenario must not abandon a running role merely because Q
    // makes observation return immediately. Close actual I/O only as fallback.
    let forced = if !matches!(cleanup, Ok(true)) {
        upstream_endpoint(&ready.upstream).close(0u32.into(), b"failed test cleanup");
        ready.peer.close(0u32.into(), b"failed test cleanup");
        Some(
            capture(drain_roles(
                &mut session,
                tokio::time::Instant::now() + Duration::from_secs(2),
            ))
            .await,
        )
    } else {
        None
    };
    // Actual joins were checked while this owner remained alive. Q cannot be
    // normalized; its final destruction here is forced test cleanup.
    drop(session);
    drop(halves.take());
    let closed = capture(ready.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic)));
    if let Some(forced) = forced {
        assert!(forced.unwrap_or_else(|panic| std::panic::resume_unwind(panic)));
    }
    closed.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

async fn drain_roles<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    deadline: tokio::time::Instant,
) -> bool {
    tokio::time::timeout_at(deadline, async {
        loop {
            std::future::poll_fn(|cx| {
                session.poll_driver(cx);
                Poll::Ready(())
            })
            .await;
            let seen = session.control.flow_observation().unwrap();
            if seen.running == 0
                && (seen.live == 0 || (seen.joined == 3 && session.control.ledger().quarantined))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .is_ok()
}

// Observe the application's actual TxToken; do not consume host input.
struct LateSyn<'a>(&'a mut LoopbackTunDevice, usize);
struct LateSynTx<'a, T>(T, &'a mut usize);
impl<T: smoltcp::phy::TxToken> smoltcp::phy::TxToken for LateSynTx<'_, T> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        self.0.consume(len, |packet| {
            let result = f(packet);
            if smoltcp::wire::Ipv4Packet::new_checked(&*packet).is_ok_and(|ip| {
                smoltcp::wire::TcpPacket::new_checked(ip.payload())
                    .is_ok_and(|tcp| tcp.syn() && !tcp.ack() && tcp.src_port() == 20001)
            }) {
                *self.1 += 1;
            }
            result
        })
    }
}
impl smoltcp::phy::Device for LateSyn<'_> {
    type RxToken<'a>
        = <LoopbackTunDevice as smoltcp::phy::Device>::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = LateSynTx<'a, <LoopbackTunDevice as smoltcp::phy::Device>::TxToken<'a>>
    where
        Self: 'a;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.0
            .receive(now)
            .map(|(rx, tx)| (rx, LateSynTx(tx, &mut self.1)))
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.0.transmit(now).map(|tx| LateSynTx(tx, &mut self.1))
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}

#[tokio::test]
async fn saved_loop_actual_partial_start_gates_join_three_and_keep_nonzero_quarantine() {
    for missing in 0..3 {
        partial_start(missing).await;
    }
}
