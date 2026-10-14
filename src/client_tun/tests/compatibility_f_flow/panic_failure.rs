//! Whole-loop panic with actual published TUIC roles and queued DATA.
//! The passive bridge distinguishes retained receiver ownership from queue loss.
//! No successful provider, physical receipt or DATA disposition is fabricated.
use super::*;
use crate::client_tun::MetricsSink;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::shared::errors::{FailureClass, FailurePhase};
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Probe {
    armed: AtomicBool,
    fired: AtomicUsize,
    metrics_dropped: AtomicUsize,
    payload_dropped: AtomicUsize,
    peer_stop: AtomicBool,
    panic_on_payload_drop: AtomicBool,
    secondary_dropped: AtomicUsize,
    site: AtomicUsize,
    bridge:
        std::sync::Mutex<Option<Arc<crate::client_tun::legacy_event_loop::RelayEventTestBridge>>>,
}

struct PanicAtPoll(Arc<Probe>);
struct PollPanic(Arc<Probe>);
struct SecondaryPanic(Arc<Probe>);

impl Drop for SecondaryPanic {
    fn drop(&mut self) {
        self.0.secondary_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Probe {
    fn selected(&self) -> Option<(u64, usize, usize, usize)> {
        self.bridge
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|bridge| bridge.selected_data())
    }
    fn fire(self: &Arc<Self>, site: usize) {
        if self.site.load(Ordering::SeqCst) == site && self.armed.swap(false, Ordering::SeqCst) {
            self.fired.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(PollPanic(self.clone()));
        }
    }
}
impl MetricsSink for PanicAtPoll {
    fn enter_poll(&mut self) {
        self.0.fire(0);
    }
    fn loop_park_end(&mut self) {
        if self.0.selected().is_some() {
            self.0.fire(1);
        }
    }
}
struct PanicDevice {
    inner: LoopbackTunDevice,
    probe: Arc<Probe>,
}
impl smoltcp::phy::Device for PanicDevice {
    type RxToken<'a> = <LoopbackTunDevice as smoltcp::phy::Device>::RxToken<'a>;
    type TxToken<'a> = <LoopbackTunDevice as smoltcp::phy::Device>::TxToken<'a>;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner.receive(now)
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.inner.transmit(now)
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.inner.capabilities()
    }
}
impl TunIo for PanicDevice {
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
        if self.probe.site.load(Ordering::SeqCst) == 3
            && self.probe.selected().is_some()
            && self.probe.armed.swap(false, Ordering::SeqCst)
        {
            self.probe.fired.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::Error::from_raw_os_error(5));
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
        self.inner.stage_rx_for_iface()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.inner.flush_tx().await?;
        if self.probe.selected().is_some_and(|v| v.3 > 0) {
            self.probe.fire(2);
        }
        Ok(())
    }
    fn queued_tx_bytes(&self) -> usize {
        self.inner.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<crate::device::TunFlushedTcpPacket> {
        self.inner.take_flushed_tcp_packets()
    }
    fn tun_ingress_pump_snapshot(&self) -> crate::device::TunIngressPumpSnapshot {
        self.inner.tun_ingress_pump_snapshot()
    }
    fn inject_ip_packet(&mut self, packet: &[u8]) {
        self.inner.inject_ip_packet(packet);
    }
    fn interface_name(&self) -> Option<&str> {
        self.inner.interface_name()
    }
}

impl Drop for PanicAtPoll {
    fn drop(&mut self) {
        self.0.metrics_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for PollPanic {
    fn drop(&mut self) {
        self.0.payload_dropped.fetch_add(1, Ordering::SeqCst);
        if self.0.panic_on_payload_drop.load(Ordering::SeqCst) {
            std::panic::panic_any(SecondaryPanic(self.0.clone()));
        }
    }
}

fn original_slot(
    control: &SessionControl,
) -> (
    usize,
    smoltcp::iface::SocketHandle,
    u64,
    tokio::time::Instant,
) {
    let opens = control.opens();
    assert_eq!(opens.live, 1);
    opens
        .slots
        .iter()
        .enumerate()
        .find_map(|(index, slot)| {
            slot.as_ref()
                .map(|slot| (index, slot.handle, slot.epoch, slot.deadline))
        })
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn saved_published_f_poll_panic_keeps_physical_owner_until_actual_joins() {
    published_loop_panic(false, 0).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_published_f_panic_payload_drop_preserves_first_error_and_actual_joins() {
    published_loop_panic(true, 0).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_selected_data_panic_before_admission_keeps_exact_custody() {
    published_loop_panic(false, 1).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_transferred_data_flush_panic_retains_original_vec_and_lane() {
    published_loop_panic(false, 2).await;
}

#[tokio::test(flavor = "current_thread")]
async fn saved_selected_data_ingress_error_keeps_exact_custody_and_first_io_failure() {
    published_loop_panic(false, 3).await;
}

async fn published_loop_panic(payload_drop_panics: bool, site: usize) {
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
    runtime.downlink_backpressure = crate::client_tun::DownlinkBackpressureConfig {
        high_bytes: 4096,
        low_bytes: 2048,
    };
    runtime.bounded_global_rx_receive_window = true;
    if site == 3 {
        runtime.tun_rx_drain_budget = 1;
    }
    let probe = Arc::new(Probe::default());
    probe.site.store(site, Ordering::SeqCst);
    probe
        .panic_on_payload_drop
        .store(payload_drop_panics, Ordering::SeqCst);
    let mut session = prepare_client_tun_session(
        PanicDevice {
            inner: LoopbackTunDevice::new(inbound, outbound),
            probe: probe.clone(),
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::new()),
        PanicAtPoll(probe.clone()),
    );
    let remote = peer.clone();
    let remote_probe = probe.clone();
    let (payload_start, payload_gate) = tokio::sync::oneshot::channel();
    let mut payload_start = Some(payload_start);
    let (finish, gate) = tokio::sync::oneshot::channel::<()>();
    let mut finish = Some(finish);
    let server = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut send, mut recv) = remote.accept_bi().await.unwrap();
            let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
            let mut expected = encode_connect(&target);
            expected.extend_from_slice(b"f0-retained");
            let mut bytes = vec![0; expected.len()];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
            if payload_gate.await.is_err() {
                return;
            }
            send.write_all(&vec![0x63; 2 * 1024 * 1024]).await.unwrap();
            tokio::select! {
                result = send.stopped() => {
                    assert!(result.unwrap().is_some(), "actual STOP_SENDING");
                    remote_probe.peer_stop.store(true, Ordering::SeqCst);
                }
                _ = gate => {}
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
        let bridge = tokio::time::timeout(Duration::from_secs(3), async {
            while session.control.flow_observation().unwrap().published == 0 {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                tokio::task::yield_now().await;
            }
            let bridge = session.control.relay_event_test_bridge().unwrap();
            *probe.bridge.lock().unwrap() = Some(bridge.clone());
            payload_start.take().unwrap().send(()).unwrap();
            while bridge.queued_len() == 0 {
                tokio::task::yield_now().await;
            }
            if site != 0 { return bridge; }
            loop {
                apps.turn().await;
                let _ = poll_once(session.run()).await;
                if bridge.snapshot().is_some_and(|s| {
                    s.context.is_some_and(|c| c.pending_downlink_bytes >= 16_384)
                }) && bridge.queued_len() > 0
                    && apps.sockets.get::<Socket>(apps.handle).recv_queue() == 4096
                {
                    break bridge;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real published F must reach queued DATA and physical pressure");
        let slot = original_slot(&session.control);
        let before = session.control.flow_observation().unwrap();
        assert_eq!((before.live, before.f, before.published, before.running), (1, 1, 1, 3));
        assert_eq!((before.joined, before.released, before.retry), (0, 0, 0));
        assert!(before.audit && bridge.receiver_is_open());
        assert!(session.control.ledger().first_failure.is_none());
        probe.armed.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(1), async {
            while probe.fired.load(Ordering::SeqCst) == 0 {
                let _ = poll_once(session.run()).await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panic must occur at original MetricsSink::enter_poll");
        let at_panic = session.control.flow_observation().unwrap();
        let selected_at_panic = probe.selected();
        let queued_at_panic = bridge.queued_len();
        if site != 0 {
            let (epoch, len, ptr, transferred) = selected_at_panic.expect("actual selected DATA");
            eprintln!("F panic selected: site={site}, tuple={selected_at_panic:?}, queue={queued_at_panic}");
            assert_eq!(epoch, slot.2);
            // QUIC may deliver any nonempty chunk; this oracle proves complete
            // Vec custody after transfer, not an unadmitted pending tail.
            assert!(len > 0 && ptr != 0);
            assert_eq!(transferred, if site == 2 { len } else { 0 });
        }
        eprintln!(
            "F panic immediate: slot={slot:?}; flow={at_panic:?}; receiver_open={}; queue={}; metrics_drop={}; payload_drop={}; device_settled={}; debug={:?}",
            bridge.receiver_is_open(), bridge.queued_len(),
            probe.metrics_dropped.load(Ordering::SeqCst),
            probe.payload_dropped.load(Ordering::SeqCst),
            session.control.device_settlement_observed.load(Ordering::SeqCst),
            session.control.flow_debug(),
        );
        assert!(bridge.receiver_is_open(), "panic must not destroy the original DATA receiver");
        assert_eq!(probe.metrics_dropped.load(Ordering::SeqCst), 0);
        assert!(!session.control.device_settlement_observed.load(Ordering::SeqCst));
        assert_eq!(original_slot(&session.control), slot);
        let deadline = session.control.ledger().shutdown_deadline.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let _ = poll_once(session.run()).await;
                let view = session.control.flow_observation().unwrap();
                if view.joined == 3
                    && view.settled_providers == 1
                    && probe.peer_stop.load(Ordering::SeqCst)
                    && session.control.ledger().quarantined
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same original F must join actual roles and retire provider after loop panic");
        let after = session.control.flow_observation().unwrap();
        assert_eq!((after.live, after.f, after.joined, after.running), (1, 1, 3, 0));
        assert_eq!((after.released, after.retry), (0, 0));
        assert!(probe.peer_stop.load(Ordering::SeqCst));
        assert!(bridge.receiver_is_open());
        assert_eq!(original_slot(&session.control), slot);
        assert_eq!(probe.fired.load(Ordering::SeqCst), 1);
        assert_eq!(probe.metrics_dropped.load(Ordering::SeqCst), 0);
        assert!(!session.control.device_settlement_observed.load(Ordering::SeqCst));
        let ledger = session.control.ledger();
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
        let facts = ledger.first_failure.as_ref().unwrap().facts();
        if site == 3 {
            assert_eq!(facts.class, FailureClass::Io);
            assert_eq!(facts.origin.phase, FailurePhase::DeviceRead);
            assert_eq!(facts.raw_os_code, Some(5));
        } else {
            assert_eq!(facts.class, FailureClass::Panic);
            assert_eq!(facts.origin.phase, FailurePhase::SessionLoop);
        }
        assert!(ledger.quarantined && after.audit);
        if site == 1 {
            let ([published, transferred, discarded], in_flight) = bridge.fatal_census().unwrap();
            assert_eq!(in_flight, 0);
            assert!(published.0 > 0 && published.1 > 0 && discarded.0 > 0);
            assert_eq!(published, (transferred.0 + discarded.0, transferred.1 + discarded.1));
            assert_eq!(probe.selected(), None, "known unstarted DATA was actually disposed");
        } else if site == 2 || site == 3 {
            assert_eq!(bridge.fatal_census().unwrap().1, 1);
            assert_eq!(probe.selected(), selected_at_panic, "same original Vec pointer/length/transfer");
            assert!(bridge.queued_len() >= queued_at_panic, "unknown in-flight event forbids more receives");
        }
        assert_eq!(probe.secondary_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(ledger.additional_failures, u64::from(payload_drop_panics));
    })
    .await;
    // Cleanup is independent of success: keep the original failure, release
    // the real peer, and remove endpoint resources before propagating it.
    drop(payload_start.take());
    drop(finish.take());
    let server_result = server.await;
    session.request_stop();
    let state = session.observe_shutdown().await;
    endpoint.close(0u32.into(), b"F panic fixture cleanup");
    peer.close(0u32.into(), b"F panic fixture cleanup");
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    let cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(matches!(server_result, Ok(Ok(()))), "{server_result:?}");
    assert_eq!(state, SessionDeviceState::Incomplete);
    assert_eq!(probe.metrics_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.payload_dropped.load(Ordering::SeqCst),
        usize::from(site != 3)
    );
    assert_eq!(
        probe.secondary_dropped.load(Ordering::SeqCst),
        usize::from(payload_drop_panics)
    );
    idle.unwrap();
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
