//! Actual wire reset, egress pressure and rejection in the saved loop.
use super::*;
#[path = "compatibility_f_reset/ingress_profile.rs"]
mod ingress_profile;

#[tokio::test]
async fn saved_f_stale_fake_ip_is_refused_before_receive_or_factory() {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::with_target(
        outbound.clone(),
        inbound.clone(),
        IpAddress::v4(198, 18, 0, 99),
    );
    let upstream = Arc::new(FlowRoleUpstream::new());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        let mut established = false;
        let reset = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                apps.turn().await;
                established |= apps.established();
                let _ = poll_once(session.run()).await;
                if apps.sent
                    && apps.sockets.get::<Socket>(apps.handle).state()
                        == smoltcp::socket::tcp::State::Closed
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(reset.is_ok(), "refuse reset: established={established} sent={} state={:?} app_tx={} flow={:?} factory={}",
            apps.sent, apps.sockets.get::<Socket>(apps.handle).state(),
            apps.sockets.get::<Socket>(apps.handle).send_queue(),
            session.control.flow_observation(), upstream.entered.load(Ordering::SeqCst));
        assert!(established);
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        let snapshot = session.control.flow_observation().unwrap();
        assert_eq!(
            (snapshot.live, snapshot.w, snapshot.f, snapshot.reserved),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (
                snapshot.retained_capacity,
                snapshot.published,
                snapshot.released
            ),
            (0, 0, 0)
        );
        assert!(snapshot.audit);
        let ledger = session.control.ledger();
        assert!(!ledger.quarantined && ledger.first_failure.is_none());
        // The actual app observed RST. Exact epoch/recv ordering is additionally
        // covered at the original-loop admission/gateway seam, not inferred
        // from a fabricated global event or direct context mutation.
    })
    .await;
    session.request_stop();
    let state = session.observe_shutdown().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::DeviceSettled);
}

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime::tests) struct ResetFlushGate {
    pub(in crate::client_tun::compatibility_runtime::tests) reset_seen: AtomicBool,
    pub(in crate::client_tun::compatibility_runtime::tests) single_rx_proofs: AtomicUsize,
    pub(in crate::client_tun::compatibility_runtime::tests) waiting: AtomicBool,
    pub(in crate::client_tun::compatibility_runtime::tests) blocked: AtomicBool,
    pub(in crate::client_tun::compatibility_runtime::tests) fail_reset_once: AtomicBool,
    profile: Option<ingress_profile::IngressProfile>,
    wake: Notify,
}
impl ResetFlushGate {
    pub(in crate::client_tun::compatibility_runtime::tests) fn profiled() -> Self {
        Self {
            profile: Some(ingress_profile::IngressProfile::default()),
            ..Self::default()
        }
    }
    pub(in crate::client_tun::compatibility_runtime::tests) fn ingress_boundary(&self, name: &str) {
        if let Some(profile) = &self.profile {
            profile.boundary(name);
        }
    }
    pub(in crate::client_tun::compatibility_runtime::tests) fn release(&self) {
        self.blocked.store(false, Ordering::SeqCst);
        self.wake.notify_waiters();
    }
}
pub(in crate::client_tun::compatibility_runtime::tests) struct GatedResetDevice {
    pub(in crate::client_tun::compatibility_runtime::tests) inner: LoopbackTunDevice,
    pub(in crate::client_tun::compatibility_runtime::tests) gate: Arc<ResetFlushGate>,
}
pub(in crate::client_tun::compatibility_runtime::tests) struct ResetTx<T>(T, Arc<ResetFlushGate>);
impl<T: smoltcp::phy::TxToken> smoltcp::phy::TxToken for ResetTx<T> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        self.0.consume(len, |bytes| {
            let result = f(bytes);
            if let Some(profile) = &self.1.profile {
                profile.produced(bytes);
            }
            if let Ok(ip) = Ipv4Packet::new_checked(&*bytes) {
                if TcpPacket::new_checked(ip.payload()).is_ok_and(|tcp| tcp.rst()) {
                    self.1.reset_seen.store(true, Ordering::SeqCst);
                }
            }
            result
        })
    }
}
impl smoltcp::phy::Device for GatedResetDevice {
    type RxToken<'a> = <LoopbackTunDevice as smoltcp::phy::Device>::RxToken<'a>;
    type TxToken<'a> = ResetTx<<LoopbackTunDevice as smoltcp::phy::Device>::TxToken<'a>>;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner
            .receive(now)
            .map(|(rx, tx)| (rx, ResetTx(tx, self.gate.clone())))
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.inner
            .transmit(now)
            .map(|tx| ResetTx(tx, self.gate.clone()))
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.inner.capabilities()
    }
}
impl TunIo for GatedResetDevice {
    fn request_stop(&mut self) {
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<crate::device::DeviceShutdown> {
        self.inner.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        self.inner.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        self.inner.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.inner.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.inner.rx_take()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        let single = self.inner.can_stage_single_iface_rx();
        if single {
            self.gate.single_rx_proofs.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(profile) = &self.gate.profile {
            profile.begin_prepare();
        }
        single
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        let Some(profile) = &self.gate.profile else {
            return self.inner.stage_rx_for_iface();
        };
        profile.end_prepare();
        let packet = self.inner.rx_peek().and_then(ingress_profile::packet);
        let staged = self.inner.stage_rx_for_iface();
        if staged {
            profile.staged(packet);
        }
        staged
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        while self.gate.reset_seen.load(Ordering::SeqCst) {
            let wake = self.gate.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            if !self.gate.blocked.load(Ordering::SeqCst) {
                break;
            }
            self.gate.waiting.store(true, Ordering::SeqCst);
            wake.await;
        }
        self.inner.flush_tx().await?;
        if self.gate.reset_seen.load(Ordering::SeqCst)
            && self.gate.fail_reset_once.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::from_raw_os_error(5));
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
        self.inner.inject_ip_packet(pkt);
    }
}

#[tokio::test]
async fn saved_f_pending_reset_survives_observer_cancellation_and_shutdown() {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::with_target(
        outbound.clone(),
        inbound.clone(),
        IpAddress::v4(198, 18, 0, 99),
    );
    let gate = Arc::new(ResetFlushGate::default());
    gate.blocked.store(true, Ordering::SeqCst);
    let upstream = Arc::new(FlowRoleUpstream::new());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        GatedResetDevice {
            inner: LoopbackTunDevice::new(inbound, outbound),
            gate: gate.clone(),
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !gate.waiting.load(Ordering::SeqCst) {
                apps.turn().await;
                assert!(poll_once(session.run()).await.is_none());
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(apps.sent && apps.established());
        // Only the actual smoltcp RST token enables the gate, never a test slot.
        assert!(gate.reset_seen.load(Ordering::SeqCst));
        session.request_stop();
        for _ in 0..4 {
            assert!(poll_once(session.observe_shutdown()).await.is_none());
            assert_eq!(session.state(), SessionDeviceState::Stopping);
            assert!(
                !session
                    .control
                    .device_settlement_observed
                    .load(Ordering::SeqCst)
            );
        }
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        assert_eq!(session.control.flow_observation().unwrap().live, 0);
        gate.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                if apps.sockets.get::<Socket>(apps.handle).state()
                    == smoltcp::socket::tcp::State::Closed
                    && session.state() == SessionDeviceState::DeviceSettled
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    })
    .await;
    gate.release();
    session.request_stop();
    let state = session.observe_shutdown().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::DeviceSettled);
}

#[tokio::test]
async fn saved_f_provider_rejection_sends_reset_before_releasing_physical_flow() {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let upstream = Arc::new(FlowRoleUpstream::new());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        let reset = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                let snapshot = session.control.flow_observation().unwrap();
                if apps.sent
                    && apps.sockets.get::<Socket>(apps.handle).state()
                        == smoltcp::socket::tcp::State::Closed
                    && snapshot.released == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            reset.is_ok(),
            "provider rejection reset: sent={} app={:?} flow={:?} factory={}",
            apps.sent,
            apps.sockets.get::<Socket>(apps.handle).state(),
            session.control.flow_observation(),
            upstream.entered.load(Ordering::SeqCst)
        );
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 1);
        let snapshot = session.control.flow_observation().unwrap();
        assert_eq!(
            (snapshot.live, snapshot.w, snapshot.f, snapshot.reserved),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (snapshot.published, snapshot.released, snapshot.joined),
            (0, 1, 0)
        );
        assert!(snapshot.audit);
        let ledger = session.control.ledger();
        assert!(!ledger.quarantined);
        assert_eq!(
            ledger.first_failure.as_ref().unwrap().facts().io_kind,
            Some(std::io::ErrorKind::Unsupported)
        );
        assert_eq!(ledger.additional_failures, 0);
        // The real adapter refused the staged factory. No fake success/provider
        // resource or direct slot fill was used to reach this physical rejection.
    })
    .await;
    session.request_stop();
    let state = session.observe_shutdown().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
}

#[tokio::test]
async fn saved_f_rejected_flow_keeps_w_f_and_escrow_through_blocked_reset_and_stop() {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let gate = Arc::new(ResetFlushGate::default());
    gate.blocked.store(true, Ordering::SeqCst);
    let upstream = Arc::new(FlowRoleUpstream::new());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        GatedResetDevice {
            inner: LoopbackTunDevice::new(inbound, outbound),
            gate: gate.clone(),
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !gate.waiting.load(Ordering::SeqCst) {
                apps.turn().await;
                assert!(poll_once(session.run()).await.is_none());
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(apps.sent && apps.established());
        // Only the actual smoltcp RST token enables the gate, never a test slot.
        assert!(gate.reset_seen.load(Ordering::SeqCst));
        let before = session.control.flow_observation().unwrap();
        assert_eq!((before.live, before.w, before.f), (1, 1, 1));
        assert_eq!(before.reserved, crate::client_tun::MAX_UPLINK_BUFFER);
        assert_eq!(before.escrow_bytes, b"f0-retained".len());
        assert_eq!(
            (before.published, before.released, before.joined),
            (0, 0, 0)
        );
        assert!(before.audit);
        session.request_stop();
        for _ in 0..4 {
            assert!(poll_once(session.observe_shutdown()).await.is_none());
            assert_eq!(session.state(), SessionDeviceState::Stopping);
            let held = session.control.flow_observation().unwrap();
            assert_eq!((held.live, held.w, held.f, held.released), (1, 1, 1, 0));
            assert_eq!(held.escrow_bytes, b"f0-retained".len());
            assert!(held.audit);
            assert!(
                !session
                    .control
                    .device_settlement_observed
                    .load(Ordering::SeqCst)
            );
        }
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 1);
        assert_eq!(session.control.flow_observation().unwrap().live, 1);
        gate.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                if apps.sockets.get::<Socket>(apps.handle).state()
                    == smoltcp::socket::tcp::State::Closed
                    && session.state() == SessionDeviceState::DeviceFailedSettled
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    })
    .await;
    gate.release();
    session.request_stop();
    let state = session.observe_shutdown().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    let after = session.control.flow_observation().unwrap();
    assert_eq!(
        (after.live, after.w, after.f, after.escrow_bytes),
        (0, 0, 0, 0)
    );
    assert_eq!(after.released, 1);
    assert_eq!(
        session
            .control
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .io_kind,
        Some(std::io::ErrorKind::Unsupported)
    );
}

#[tokio::test]
async fn saved_f_real_tuic_provider_failure_retains_source_after_actual_reset_and_shutdown() {
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
                _ => panic!("actual TUIC connection setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
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
        // Fail the real authenticated carrier before its ready-pool attempt.
        // The actual TuicUpstream factory and root control path own the failure.
        endpoint.close(71u32.into(), b"provider-open fault");
        tokio::time::timeout(Duration::from_secs(2), peer.closed())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                let seen = session.control.flow_observation().unwrap();
                if apps.sent && apps.received_resets == 1 && seen.released == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            apps.sockets.get::<Socket>(apps.handle).state(),
            smoltcp::socket::tcp::State::Closed
        );
        let seen = session.control.flow_observation().unwrap();
        assert_eq!(
            (seen.live, seen.w, seen.f, seen.reserved, seen.escrow_bytes),
            (0, 0, 0, 0, 0)
        );
        assert_eq!((seen.published, seen.released, seen.joined), (0, 1, 0));
        assert!(seen.audit);
        let source = session.control.retained_provider_failure().unwrap();
        assert_eq!(
            source.2,
            "I/O error: TUIC ready pool generation/lease identity changed"
        );
        assert_eq!(source.1.connect_written, 0);

        assert_eq!(source.1.phase, crate::upstream::TcpOpenPhase::Settled);
        assert!(!source.1.lease_held && !source.1.retirement_pending);
        for _ in 0..3 {
            let _ = poll_once(session.run()).await;
            assert_eq!(session.control.retained_provider_failure().unwrap(), source);
            assert_eq!(session.control.flow_observation().unwrap().released, 1);
        }
        session.request_stop();
        assert_eq!(
            session.observe_shutdown().await,
            SessionDeviceState::DeviceFailedSettled
        );
        assert_eq!(session.control.retained_provider_failure().unwrap(), source);
        assert!(!session.control.ledger().quarantined);
    })
    .await;
    session.request_stop();
    let state = session.observe_shutdown().await;
    endpoint.close(0u32.into(), b"provider fixture cleanup");
    peer.close(0u32.into(), b"provider fixture cleanup");
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    pair.close().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    idle.unwrap();
}
