//! Last session-handle destruction under actual TUIC DATA pressure.
//! The original control and passive lane observations outlive the handle;
//! neither substitutes for the original driver or fabricates a join receipt.
use super::*;
use crate::client_tun::MetricsSink;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::client_tun::compatibility_runtime::SessionControl;
use crate::shared::errors::FailurePhase;

#[derive(Default)]
struct LocalProbe {
    metric_drops: Cell<usize>,
    device_drops: Cell<usize>,
    panic: Cell<bool>,
    control: RefCell<Option<Arc<SessionControl>>>,
}

struct LocalSink(Rc<LocalProbe>);
struct LocalDevice<'a> {
    inner: LoopbackTunDevice,
    // This actual TunIo is both non-Send and borrowed (not 'static).
    probe: &'a Rc<LocalProbe>,
}

impl MetricsSink for LocalSink {
    fn enter_poll(&mut self) {
        assert!(!self.0.panic.replace(false), "real host loop panic");
    }
}

impl Drop for LocalSink {
    fn drop(&mut self) {
        self.0.metric_drops.set(self.0.metric_drops.get() + 1);
    }
}

impl Drop for LocalDevice<'_> {
    fn drop(&mut self) {
        self.probe
            .device_drops
            .set(self.probe.device_drops.get() + 1);
    }
}

impl smoltcp::phy::Device for LocalDevice<'_> {
    type RxToken<'a>
        = <LoopbackTunDevice as smoltcp::phy::Device>::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = <LoopbackTunDevice as smoltcp::phy::Device>::TxToken<'a>
    where
        Self: 'a;
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
impl TunIo for LocalDevice<'_> {
    fn request_stop(&mut self) {
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<crate::device::DeviceShutdown> {
        let binding = self.probe.control.borrow();
        let view = binding.as_ref().unwrap().flow_observation().unwrap();
        assert_eq!(
            (view.live, view.f, view.w, view.joined, view.released),
            (0, 0, 0, 3, 1),
            "actual F physical ACK and three joins must precede device settlement"
        );
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
    fn stage_rx_for_iface(&mut self) -> bool {
        self.inner.stage_rx_for_iface()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.inner.flush_tx().await
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

#[derive(Default)]
struct ObserverWake(AtomicUsize);
impl std::task::Wake for ObserverWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn host_owned_non_send_session_survives_control_handle_drop_until_real_settlement() {
    exercise_host(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn host_quarantine_keeps_non_send_driver_after_handle_drop_and_reports_forced_exit() {
    exercise_host(true).await;
}

async fn exercise_host(quarantine: bool) {
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
    let probe = Rc::new(LocalProbe::default());
    let mut host = prepare_client_tun_session(
        LocalDevice {
            inner: LoopbackTunDevice::new(inbound, outbound),
            probe: &probe,
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::new()),
        LocalSink(probe.clone()),
    );
    let control = host.control.clone();
    *probe.control.borrow_mut() = Some(control.clone());
    let mut handle = Some(host.take_handle().expect("the one session control handle"));
    assert!(host.take_handle().is_none());
    let remote = peer.clone();
    let (payload_start, payload_gate) = tokio::sync::oneshot::channel::<()>();
    let mut payload_start = Some(payload_start);
    let (finish, gate) = tokio::sync::oneshot::channel::<()>();
    let mut finish = Some(finish);
    let peer_stop = Arc::new(AtomicBool::new(false));
    let actual_stop = peer_stop.clone();
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
                    assert!(result.unwrap().is_some(), "real STOP_SENDING");
                    actual_stop.store(true, Ordering::SeqCst);
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
            while control.flow_observation().unwrap().published == 0 {
                apps.turn().await;
                let _ = poll_once(host.drive_until_settled()).await;
                tokio::task::yield_now().await;
            }
            let bridge = control.relay_event_test_bridge().unwrap();
            payload_start.take().unwrap().send(()).unwrap();
            while bridge.queued_len() == 0 {
                tokio::task::yield_now().await;
            }
            loop {
                apps.turn().await;
                let _ = poll_once(host.drive_until_settled()).await;
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
        .expect("actual published F and zero-window DATA pressure");
        let before = control.flow_observation().unwrap();
        assert_eq!((before.live, before.f, before.running, before.joined), (1, 1, 3, 0));
        assert!(before.audit && bridge.receiver_is_open());
        assert_eq!(probe.metric_drops.get(), 0);
        let original = {
            let opens = control.opens();
            let (index, slot) = opens.slots.iter().enumerate()
                .find_map(|(i, slot)| slot.as_ref().map(|slot| (i, slot))).unwrap();
            (index, slot.handle, slot.epoch, slot.deadline)
        };
        let wakes = Arc::new(ObserverWake::default());
        let waker = std::task::Waker::from(wakes.clone());
        if quarantine {
            probe.panic.set(true);
            tokio::time::timeout(Duration::from_secs(1), async {
                while probe.panic.get() {
                    assert!(poll_once(host.drive_until_settled()).await.is_none());
                    tokio::task::yield_now().await;
                }
            }).await.expect("actual original-loop panic");
            let mut observer = Box::pin(handle.as_mut().unwrap().observe_shutdown());
            assert!(observer.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
            let deadline = control.ledger().shutdown_deadline.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    assert!(poll_once(host.drive_until_settled()).await.is_none());
                    let after = control.flow_observation().unwrap();
                    if after.joined == 3 && after.settled_providers == 1
                        && peer_stop.load(Ordering::SeqCst) && control.ledger().quarantined {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("actual fatal joins/provider retirement with retained Q");
            assert!(wakes.0.load(Ordering::SeqCst) > 0, "actual Q transition wakes observer");
            assert_eq!(observer.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Ready(SessionDeviceState::Incomplete));
            drop(observer);
            drop(handle.take().unwrap());
            for _ in 0..3 {
                assert!(poll_once(host.drive_until_settled()).await.is_none());
                assert!(host.take_handle().is_none());
            }
            let opens = control.opens();
            let row = opens.slots[original.0].as_ref().unwrap();
            assert_eq!((row.handle, row.epoch, row.deadline), (original.1, original.2, original.3));
            drop(opens);
            let after = control.flow_observation().unwrap();
            assert_eq!((after.live, after.f, after.joined, after.released), (1, 1, 3, 0));
            assert!(after.audit && bridge.receiver_is_open());
            assert_eq!((probe.metric_drops.get(), probe.device_drops.get()), (0, 0));
            assert!(!control.driver_complete.load(Ordering::Acquire));
            let ledger = control.ledger();
            assert_eq!(ledger.shutdown_deadline, Some(deadline));
            assert_eq!(ledger.first_failure.as_ref().unwrap().facts().origin.phase, FailurePhase::SessionLoop);
            assert_eq!(ledger.additional_failures, 0);
            return;
        }
        // Cancel an actual registered observer; the original host keeps ownership.
        let mut observer = Box::pin(handle.as_mut().unwrap().observe_shutdown());
        assert!(observer.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        drop(observer);
        let deadline = control.ledger().shutdown_deadline.unwrap();
        drop(handle.take().unwrap());
        let immediate = control.flow_observation().unwrap();
        eprintln!(
            "last-owner Drop: flow={immediate:?}; receiver_open={}; queue={}; metrics_drop={}; device_settled={}; debug={:?}",
            bridge.receiver_is_open(), bridge.queued_len(), probe.metric_drops.get(),
            control.device_settlement_observed.load(Ordering::SeqCst), control.flow_debug(),
        );
        assert!(control.stop.stopping.load(Ordering::Acquire));
        assert!(bridge.receiver_is_open(), "handle Drop must retain original DATA receiver");
        assert_eq!(probe.metric_drops.get(), 0, "original driver still owns its non-Send sink");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                apps.turn().await;
                let _ = poll_once(host.drive_until_settled()).await;
                let after = control.flow_observation().unwrap();
                if after.live == 0 && after.joined == 3
                    && control.device_settlement_observed.load(Ordering::SeqCst)
                    && peer_stop.load(Ordering::SeqCst)
                {
                    assert!(after.audit);
                    assert_eq!(control.ledger().shutdown_deadline, Some(deadline));
                    assert!(control.driver_complete.load(Ordering::Acquire));
                    assert_eq!(host.state(), SessionDeviceState::DeviceSettled);
                    assert!(host.take_handle().is_none());
                    assert_eq!((after.w, after.f, after.reserved, after.released), (0, 0, 0, 1));
                    let (messages, bytes, transferred, discarded, pending_discard) =
                        after.last_data_disposition.unwrap();
                    assert!(messages > 0 && transferred > 0 && discarded > 0 && pending_discard > 0);
                    assert_eq!(transferred + discarded, bytes);
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("real F/provider/device settlement after last handle Drop");
    }).await;
    drop(payload_start.take());
    drop(finish.take());
    drop(handle.take());
    host.request_stop();
    let _ = host.observe_shutdown().await;
    let server_result = server.await;
    endpoint.close(0u32.into(), b"owner Drop fixture cleanup");
    peer.close(0u32.into(), b"owner Drop fixture cleanup");
    drop(host);
    // Explicit forced host teardown is not a successful Q shutdown.
    if scenario.is_ok() && quarantine {
        assert_eq!(control.state(), SessionDeviceState::Incomplete);
        assert!(!control.driver_complete.load(Ordering::Acquire));
        let ledger = control.ledger();
        assert_eq!(
            ledger.first_failure.as_ref().unwrap().facts().origin.phase,
            FailurePhase::SessionLoop
        );
        assert_eq!(ledger.additional_failures, 1);
    }
    drop(upstream);
    drop(auth);
    drop(peer);
    drop(control);
    let idle = tokio::time::timeout(Duration::from_secs(3), endpoint.wait_idle()).await;
    let cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert!(matches!(server_result, Ok(Ok(()))), "{server_result:?}");
    assert_eq!((probe.metric_drops.get(), probe.device_drops.get()), (1, 1));
    idle.unwrap();
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
