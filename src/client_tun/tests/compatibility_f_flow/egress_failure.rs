//! Real DATA/FIN egress faults through the saved session and actual TUIC roles.
//! The device drains a real frame then reports uncertain IO; later Ok cannot
//! certify that failed write. No successful provider or receipt is fabricated.
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::shared::errors::FailurePhase;
use std::sync::atomic::AtomicUsize;

#[derive(Clone, Copy, Debug)]
enum FaultAt {
    Data,
    Fin,
    Dns,
    Retained,
}

struct Probe {
    at: FaultAt,
    pending: AtomicBool,
    again: AtomicBool,
    fired: AtomicUsize,
    later_ok: AtomicUsize,
    resets: AtomicUsize,
    post_fault_syn: AtomicUsize,
    post_fault_reads: AtomicUsize,
    source_drops: AtomicUsize,
}

#[derive(Debug)]
struct WriteFault(Arc<Probe>);

impl std::fmt::Debug for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Probe").field("at", &self.at).finish()
    }
}
impl std::fmt::Display for WriteFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual-frame egress failure")
    }
}
impl std::error::Error for WriteFault {}
impl Drop for WriteFault {
    fn drop(&mut self) {
        self.0.source_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct FaultDevice {
    inner: LoopbackTunDevice,
    probe: Arc<Probe>,
}
struct ObservedTx<T>(T, Arc<Probe>);
impl<T: smoltcp::phy::TxToken> smoltcp::phy::TxToken for ObservedTx<T> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        self.0.consume(len, |bytes| {
            let result = f(bytes);
            if let Ok(ip) = Ipv4Packet::new_checked(&*bytes) {
                if let Ok(tcp) = TcpPacket::new_checked(ip.payload()) {
                    self.1
                        .resets
                        .fetch_add(usize::from(tcp.rst()), Ordering::SeqCst);
                    let selected = match self.1.at {
                        FaultAt::Data => !tcp.payload().is_empty(),
                        FaultAt::Fin => tcp.fin(),
                        FaultAt::Dns | FaultAt::Retained => false,
                    };
                    if selected && self.1.fired.load(Ordering::SeqCst) == 0 {
                        self.1.pending.store(true, Ordering::SeqCst);
                    }
                }
            }
            result
        })
    }
}
impl smoltcp::phy::Device for FaultDevice {
    type RxToken<'a> = <LoopbackTunDevice as smoltcp::phy::Device>::RxToken<'a>;
    type TxToken<'a> = ObservedTx<<LoopbackTunDevice as smoltcp::phy::Device>::TxToken<'a>>;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner
            .receive(now)
            .map(|(rx, tx)| (rx, ObservedTx(tx, self.probe.clone())))
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.inner
            .transmit(now)
            .map(|tx| ObservedTx(tx, self.probe.clone()))
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.inner.capabilities()
    }
}
impl TunIo for FaultDevice {
    fn request_stop(&mut self) {
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<crate::device::DeviceShutdown> {
        self.inner.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        if self.probe.fired.load(Ordering::SeqCst) > 0 {
            self.probe.post_fault_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        if self.probe.fired.load(Ordering::SeqCst) > 0 {
            self.probe.post_fault_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.inner.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.inner.rx_take()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        let selected = self.probe.fired.load(Ordering::SeqCst) > 0
            && self.inner.rx_peek().is_some_and(|bytes| {
                Ipv4Packet::new_checked(bytes).is_ok_and(|ip| {
                    TcpPacket::new_checked(ip.payload())
                        .is_ok_and(|tcp| tcp.syn() && tcp.src_port() == 40001)
                })
            });
        let staged = self.inner.stage_rx_for_iface();
        if staged && selected {
            self.probe.post_fault_syn.fetch_add(1, Ordering::SeqCst);
        }
        staged
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.inner.flush_tx().await?;
        if self.probe.pending.swap(false, Ordering::SeqCst) {
            assert_eq!(self.inner.queued_tx_bytes(), 0);
            assert_eq!(
                self.probe.resets.load(Ordering::SeqCst),
                0,
                "ordinary frame, no reset"
            );
            assert_eq!(self.probe.fired.fetch_add(1, Ordering::SeqCst), 0);
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                WriteFault(self.probe.clone()),
            ));
        }
        if self.probe.again.swap(false, Ordering::SeqCst) {
            self.probe.fired.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::Error::from_raw_os_error(5));
        }
        if self.probe.fired.load(Ordering::SeqCst) != 0 {
            self.probe.later_ok.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    fn queued_tx_bytes(&self) -> usize {
        self.inner.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<crate::device::TunFlushedTcpPacket> {
        self.inner.take_flushed_tcp_packets()
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        if matches!(self.probe.at, FaultAt::Dns | FaultAt::Retained)
            && self.probe.fired.load(Ordering::SeqCst) == 0
        {
            let udp = crate::client_tun::parse_inbound_udp(pkt).expect("real DNS response");
            assert_eq!(udp.src_port, 53);
            assert_eq!(&udp.payload[..2], &[0x12, 0x34]);
            assert_eq!(udp.payload[3] & 0x0f, 0);
            self.probe.pending.store(true, Ordering::SeqCst);
        }
        self.inner.inject_ip_packet(pkt);
    }
}

#[tokio::test]
async fn saved_ordinary_data_egress_fault_retains_source_and_physical_custody() {
    run(FaultAt::Data).await;
}
#[tokio::test]
async fn saved_graceful_fin_egress_fault_retains_source_and_physical_custody() {
    run(FaultAt::Fin).await;
}

#[tokio::test]
async fn saved_retained_ack_after_dns_egress_fault_cannot_release_custody() {
    run(FaultAt::Retained).await;
}

async fn run(at: FaultAt) {
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 100, true)));
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
                async { pair.server.accept().await.unwrap().await.unwrap() }
            )
        })
        .await
        .unwrap()
    })
    .await;
    let (upstream, peer) = match connected {
        Ok((Ok(upstream), peer)) => (Arc::new(upstream), peer),
        failure => {
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("TUIC setup"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let probe = Arc::new(Probe {
        at,
        pending: AtomicBool::new(false),
        again: AtomicBool::new(false),
        fired: AtomicUsize::new(0),
        later_ok: AtomicUsize::new(0),
        resets: AtomicUsize::new(0),
        post_fault_syn: AtomicUsize::new(0),
        post_fault_reads: AtomicUsize::new(0),
        source_drops: AtomicUsize::new(0),
    });
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        FaultDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound),
            probe: probe.clone(),
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let remote = peer.clone();
    let (finish, gate) = tokio::sync::oneshot::channel();
    let mut finish = Some(finish);
    let server = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut send, mut recv) = remote.accept_bi().await.unwrap();
            let mut expected =
                encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
            expected.extend_from_slice(b"f0-retained");
            let mut bytes = vec![0; expected.len()];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
            send.write_all(b"egress-reply").await.unwrap();
            let _ = gate.await;
            match at {
                FaultAt::Fin | FaultAt::Retained => {
                    send.finish().unwrap();
                }
                FaultAt::Dns => unreachable!("DNS uses the no-TCP fixture"),
                FaultAt::Data => {
                    assert!(send.stopped().await.unwrap().is_some());
                }
            }
            match recv.read_to_end(1).await {
                Ok(bytes) => assert!(bytes.is_empty()),
                Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_))) => {}
                result => panic!("actual peer retirement: {result:?}"),
            }
        })
        .await
    });
    let mut auth = None;
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            auth = Some(peer.accept_uni().await.unwrap());
            let observed = auth.as_mut().unwrap().read_to_end(64).await.unwrap();
            let mut token = [0; 32];
            peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
                .unwrap();
            assert_eq!(observed, encode_authenticate(&config.uuid, &token));
        })
        .await
        .unwrap();
        let mut received = Vec::new();
        let mut held_before = None;
        tokio::time::timeout(Duration::from_secs(4), async {
            while probe.fired.load(Ordering::SeqCst) == 0 {
                if held_before.is_none() {
                    apps.turn().await;
                }
                let _ = poll_once(session.run()).await;
                let socket = apps.sockets.get_mut::<Socket>(apps.handle);
                if socket.can_recv() {
                    socket
                        .recv(|bytes| {
                            received.extend_from_slice(bytes);
                            (bytes.len(), ())
                        })
                        .unwrap();
                }
                if matches!(at, FaultAt::Fin | FaultAt::Retained)
                    && received == b"egress-reply"
                    && finish.is_some()
                {
                    if matches!(at, FaultAt::Retained) {
                        session.control.hold_only_published_receipt();
                    }
                    finish.take().unwrap().send(()).unwrap();
                }
                if matches!(at, FaultAt::Retained) {
                    if socket.state() == smoltcp::socket::tcp::State::CloseWait {
                        socket.close();
                    }
                    if held_before.is_none() {
                        if let Some(held) = session
                            .control
                            .held_receipt()
                            .filter(|held| held.calls >= 3)
                        {
                            assert!(
                                held.rearming
                                    && held.row_held
                                    && held.original_matches
                                    && held.provider_settled
                            );
                            assert_eq!(held.role_joins, 3);
                            held_before = Some(held);
                            inject_dns(&inbound);
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual selected frame must reach the failed flush");
        if held_before.is_some() {
            // Consume the real DNS response separately; the TCP-only app helper
            // must not parse a UDP frame as TCP. Preserve any accompanying TCP.
            let mut replies = 0;
            while apps.device.try_recv_rx().unwrap() {
                let packet = apps.device.rx_peek().unwrap();
                if let Some(udp) = crate::client_tun::parse_inbound_udp(packet) {
                    assert_eq!(udp.src_port, 53);
                    assert_eq!(&udp.payload[..2], &[0x12, 0x34]);
                    apps.device.rx_take();
                    replies += 1;
                } else {
                    assert!(apps.device.stage_rx_for_iface());
                }
            }
            assert_eq!(replies, 1);
            session.control.release_held_receipt();
        }
        // Existing observations make this a runtime RED, not a missing API RED.
        assert_eq!(
            probe.source_drops.load(Ordering::SeqCst),
            0,
            "original write error must remain owned"
        );
        assert!(
            session.control.stop.stopping.load(Ordering::Acquire),
            "write fault closes session admission"
        );
        let source = assert_source(&session.control, &probe, 1);
        let deadline = session.control.ledger().shutdown_deadline;
        if let Some(finish) = finish.take() {
            let _ = finish.send(());
        }
        // Emit a real post-fault SYN; the stopped root must leave it unread.
        // Physical reset and QUIC retirement continue independently of new TUN input.
        let mut other = Socket::new(
            SocketBuffer::new(vec![0; 4096]),
            SocketBuffer::new(vec![0; 4096]),
        );
        other
            .connect(
                apps.iface.context(),
                (IpAddress::v4(192, 0, 2, 11), 10000),
                40001,
            )
            .unwrap();
        let other = apps.sockets.add(other);
        {
            let mut emitted = AppSyn(&mut apps.device, 0);
            apps.iface.poll(
                smoltcp::time::Instant::now(),
                &mut emitted,
                &mut apps.sockets,
            );
            assert_eq!(emitted.1, 1, "actual app SYN emitted");
        }
        apps.device.flush_tx().await.unwrap();
        assert_eq!(inbound.dropped_packets(), 0);
        assert!(inbound.high_water_packets() > 0);
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                apps.turn().await;
                let other = apps.sockets.get_mut::<Socket>(other);
                if other.can_send() {
                    other.send_slice(b"blocked").unwrap();
                }
                let socket = apps.sockets.get_mut::<Socket>(apps.handle);
                if socket.state() == smoltcp::socket::tcp::State::CloseWait {
                    socket.close();
                }
                let _ = poll_once(session.run()).await;
                let view = session.control.flow_observation().unwrap();
                if view.joined == 3
                    && view.settled_providers == 1
                    && probe.later_ok.load(Ordering::SeqCst) >= 3
                    && session.control.ledger().quarantined
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual roles and provider retire without releasing uncertain physical custody");
        assert_eq!(probe.post_fault_syn.load(Ordering::SeqCst), 0);
        assert_eq!(probe.post_fault_reads.load(Ordering::SeqCst), 0);
        assert_eq!(
            apps.sockets.get::<Socket>(other).state(),
            smoltcp::socket::tcp::State::SynSent
        );
        if let Some(before) = held_before {
            let after = session.control.held_receipt().unwrap();
            assert_eq!(after.id, before.id);
            assert_eq!(after.disposition, before.disposition);
            assert!(
                after.row_held
                    && after.quarantined
                    && after.original_matches
                    && after.provider_settled
            );
            assert_eq!(after.role_joins, 3);
        }
        assert_eq!(assert_source(&session.control, &probe, 1), source);
        assert_eq!(session.control.ledger().shutdown_deadline, deadline);
        assert_eq!(
            session.control.flow_observation().unwrap().factory_started,
            1
        );
        let view = session.control.flow_observation().unwrap();
        assert_eq!(
            (
                view.live,
                view.w,
                view.f,
                view.published,
                view.released,
                view.retry
            ),
            (1, 0, 1, 1, 0, 0)
        );
        assert_eq!(
            (view.running, view.joined, view.settled_providers),
            (0, 3, 1)
        );
        assert!(view.audit && session.control.ledger().quarantined);
        // Only the original ordinary error caused the nonzero quarantine.
        // A later different error is now allowed to test first-source retention.
        probe.again.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(1), async {
            while probe.fired.load(Ordering::SeqCst) != 2 {
                let _ = poll_once(session.run()).await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("later IO error must be observed");
        assert_eq!(assert_source(&session.control, &probe, 2), source);
        assert_eq!(session.control.ledger().shutdown_deadline, deadline);
        let later = session.control.flow_observation().unwrap();
        assert_eq!(
            (later.live, later.f, later.released, later.retry),
            (1, 1, 0, 0)
        );
        assert_eq!(probe.source_drops.load(Ordering::SeqCst), 0);
        assert!(
            !session
                .control
                .device_settlement_observed
                .load(Ordering::SeqCst)
        );
    })
    .await;
    drop(finish.take());
    session.request_stop();
    let state = session.observe_shutdown().await;
    let peer_result = server.await;
    endpoint.close(0u32.into(), b"egress fixture cleanup");
    peer.close(0u32.into(), b"egress fixture cleanup");
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    let cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::Incomplete);
    assert!(matches!(peer_result, Ok(Ok(()))), "{peer_result:?}");
    assert_eq!(probe.source_drops.load(Ordering::SeqCst), 1);
    idle.unwrap();
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}

fn assert_source(control: &SessionControl, probe: &Arc<Probe>, count: u64) -> usize {
    let ledger = control.ledger();
    let source = ledger
        .first_egress_io
        .as_ref()
        .expect("raw source retained by session");
    assert_eq!(source.kind(), std::io::ErrorKind::BrokenPipe);
    let original = source
        .get_ref()
        .unwrap()
        .downcast_ref::<WriteFault>()
        .unwrap();
    assert!(Arc::ptr_eq(&original.0, probe));
    assert_eq!(ledger.egress_failures, count);
    let facts = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(facts.origin.phase, FailurePhase::DeviceWrite);
    assert_eq!(facts.io_kind, Some(std::io::ErrorKind::BrokenPipe));
    assert!(facts.opaque);
    Arc::as_ptr(source) as usize
}

#[tokio::test]
async fn saved_direct_dns_egress_fault_without_flow_is_failed_settled() {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let probe = Arc::new(Probe {
        at: FaultAt::Dns,
        pending: AtomicBool::new(false),
        again: AtomicBool::new(false),
        fired: AtomicUsize::new(0),
        later_ok: AtomicUsize::new(0),
        resets: AtomicUsize::new(0),
        post_fault_syn: AtomicUsize::new(0),
        post_fault_reads: AtomicUsize::new(0),
        source_drops: AtomicUsize::new(0),
    });
    let upstream = Arc::new(FlowRoleUpstream::new());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        FaultDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound),
            probe: probe.clone(),
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    inject_dns(&inbound);
    let scenario = capture(async {
        let state = tokio::time::timeout(Duration::from_secs(3), session.run())
            .await
            .unwrap();
        assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(probe.fired.load(Ordering::SeqCst), 1);
        assert_source(&session.control, &probe, 1);
        assert_eq!(probe.source_drops.load(Ordering::SeqCst), 0);
        let view = session.control.flow_observation().unwrap();
        assert_eq!(
            (
                view.live,
                view.w,
                view.f,
                view.published,
                view.released,
                view.joined,
                view.factory_started
            ),
            (0, 0, 0, 0, 0, 0, 0)
        );
        assert!(view.audit && !session.control.ledger().quarantined);
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        assert!(
            session
                .control
                .device_settlement_observed
                .load(Ordering::SeqCst)
        );
    })
    .await;
    session.request_stop();
    let _ = session.observe_shutdown().await;
    drop(session);
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(probe.source_drops.load(Ordering::SeqCst), 1);
}

fn inject_dns(inbound: &PacketLink) {
    let query =
        b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x04fail\x04test\x00\x00\x01\x00\x01";
    let packet = crate::client_tun::build_udp_ip_packet(
        ("10.0.0.1".parse().unwrap(), 20053),
        ("8.8.8.8".parse().unwrap(), 53),
        query,
    )
    .unwrap();
    assert!(inbound.push(bytes::BytesMut::from(packet.as_slice())));
}

// Observe the actual application's emitted SYN without consuming the host input.
struct AppSyn<'a>(&'a mut LoopbackTunDevice, usize);
struct AppSynTx<'a, T>(T, &'a mut usize);
impl<T: smoltcp::phy::TxToken> smoltcp::phy::TxToken for AppSynTx<'_, T> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        self.0.consume(len, |packet| {
            let result = f(packet);
            if Ipv4Packet::new_checked(&*packet).is_ok_and(|ip| {
                TcpPacket::new_checked(ip.payload())
                    .is_ok_and(|tcp| tcp.syn() && tcp.src_port() == 40001)
            }) {
                *self.1 += 1;
            }
            result
        })
    }
}
impl smoltcp::phy::Device for AppSyn<'_> {
    type RxToken<'a>
        = <LoopbackTunDevice as smoltcp::phy::Device>::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = AppSynTx<'a, <LoopbackTunDevice as smoltcp::phy::Device>::TxToken<'a>>
    where
        Self: 'a;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.0
            .receive(now)
            .map(|(rx, tx)| (rx, AppSynTx(tx, &mut self.1)))
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.0.transmit(now).map(|tx| AppSynTx(tx, &mut self.1))
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
