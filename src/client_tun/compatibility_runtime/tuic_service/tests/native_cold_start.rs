//! Real saved factory → Native Device bind → original Node startup. The UDP
//! sink never completes QUIC; Device failure must independently stop startup.
use super::*;
use crate::shared::errors::{FailurePhase, FailureResource};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_source_failure_interrupts_pending_node_start_and_keeps_original_first() {
    const LIMIT: usize = 32 * 1024 * 1024;
    let sink = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = Configuration {
        tuic: fixture::config(sink.local_addr().unwrap()).with_relay_policy(fixture::native_policy()),
        composition: (),
    };
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(fixture::native_policy())
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(LIMIT), 1)
        .unwrap();
    runtime.tun_mtu = 1200;
    let inbound = PacketLink::bounded(1);
    // External peer data deliberately exceeds the original legal frame bound.
    // Actual PacketSource admission fails before copying/removing this front.
    let original = inbound.clone();
    let factory = move |meter: Option<bytes::BackingMeter>| async move {
        LoopbackTunDevice::native_with_factory(1, 1200, &meter.unwrap(), || {
            Ok((inbound, PacketLink::bounded(1)))
        })
        .map_err(ClientError::Io)
    };
    let mut service = prepare_factory(
        factory,
        config,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let budget = service.control.backing_budget.clone().unwrap();
    let mut initial_seen = false;
    let before_manual_stop = tokio::time::timeout(Duration::from_millis(500), async {
        let trigger = async {
            let mut datagram = [0u8; 2048];
            let (len, _) = sink.recv_from(&mut datagram).await.unwrap();
            assert!(len >= 1200, "actual QUIC Initial reached the local sink");
            initial_seen = true;
            assert!(original.push(bytes::BytesMut::zeroed(65540)));
        };
        let (_, report) = tokio::join!(trigger, service.run());
        report
    })
    .await;
    let completed_on_failure = before_manual_stop.is_ok();
    let report = match before_manual_stop {
        Ok(report) => report,
        Err(_) => {
            handle.request_stop();
            tokio::time::timeout(Duration::from_secs(3), service.run())
                .await
                .unwrap()
        }
    };
    assert!(
        initial_seen,
        "the original Node actually entered pending QUIC startup"
    );
    let owner = &service.completion.as_ref().unwrap().control;
    let ledger = owner.ledger();
    let first = ledger.first_failure.as_ref().unwrap().facts();
    eprintln!(
        "completed_on_source_failure={completed_on_failure} first={first:?} additional={} deadline={:?}",
        ledger.additional_failures, ledger.shutdown_deadline
    );
    assert_eq!(first.origin.resource, FailureResource::TunPump);
    assert_eq!(first.origin.phase, FailurePhase::DeviceRead);
    assert_eq!(first.io_kind, Some(std::io::ErrorKind::InvalidInput));
    assert_eq!(
        ledger.shutdown_deadline,
        *service.control.deadline.lock().unwrap()
    );
    assert_eq!(
        ledger.additional_failures, 0,
        "early transfer must not become another shutdown IO"
    );
    assert!(report.device_failed() && report.device_settled());
    assert_eq!(original.test_pop().unwrap().len(), 65540);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(
        completed_on_failure,
        "actual pump failed but Service kept waiting for QUIC startup"
    );
}

#[tokio::test(start_paused = true)]
async fn observer_fault_commits_original_service_stop_and_q_without_manual_stop() {
    use super::device::{Gate, GatedDevice};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    let gate = Arc::new(Gate::default());
    gate.open();
    let payload = Arc::new(AtomicUsize::new(53));
    *gate.observer_payload.lock().unwrap() = Some(payload.clone());
    let drop_payload = Arc::new(AtomicUsize::new(59));
    *gate.drop_payload.lock().unwrap() = Some(drop_payload.clone());
    let actual = gate.clone();
    type Probe = Box<dyn Fn() -> Option<bytes::AllocationIdentity> + Send + Sync>;
    let inventory: Arc<std::sync::Mutex<Option<(Probe, bytes::AllocationIdentity)>>> = Arc::default();
    let captured = inventory.clone();
    let factory = move |meter: Option<bytes::BackingMeter>| async move {
        let inner = LoopbackTunDevice::native_with_factory(1, 1500, &meter.unwrap(), || {
            Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
        })?;
        let probe = inner.test_rx_inventory_probe();
        let identity = probe().unwrap();
        *captured.lock().unwrap() = Some((Box::new(probe), identity));
        Ok::<_, ClientError>(GatedDevice { inner, gate: actual })
    };
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(fixture::native_policy())
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(65536 + crate::device::RxPacket::test_protection_cold_bytes()), 1)
        .unwrap();
    let mut service = prepare_factory(
        factory,
        Configuration {
            tuic: fixture::config("127.0.0.1:9".parse().unwrap()).with_relay_policy(fixture::native_policy()),
            composition: (),
        },
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let expected_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    {
        let mut run = std::pin::pin!(service.run());
        assert!(matches!(
            run.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    let first_phase = *saved.phase.lock().unwrap();
    let first_deadline = *saved.deadline.lock().unwrap();
    let owner = saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    assert!(owner.ledger().quarantined());
    assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(&payload), 2);
    tokio::time::advance(Duration::from_secs(3)).await;
    {
        let mut run = std::pin::pin!(service.run());
        assert!(matches!(
            run.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    let repeated = *saved.deadline.lock().unwrap();
    eprintln!(
        "observer unknown: first_phase={first_phase:?} first_deadline={first_deadline:?} repeated={repeated:?}"
    );
    drop(service);
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
    assert!(Arc::ptr_eq(
        saved
            .terminal
            .disposal
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &drop_payload
    ));
    assert_eq!(Arc::strong_count(&payload), 2);
    let inventory = inventory.lock().unwrap();
    let (probe, identity) = inventory.as_ref().unwrap();
    assert_eq!(probe(), Some(*identity), "unknown Device Drop retains actual cold RX allocation");
    assert_eq!(egress::deadline::inventory_for_test(&owner), Some((1, false, 0, false, false, 0)),
        "actual unused Session callback Core is still held under Q");
    assert_eq!(budget.used(), crate::device::RxPacket::test_protection_cold_bytes()
        + crate::device::egress::CallbackCustody::test_cold_bytes(2)
        + egress::deadline::layout_bytes());
    assert!(budget.quarantined());
    assert_eq!(first_phase, Phase::Quarantined);
    assert_eq!(first_deadline, Some(expected_deadline));
    assert_eq!(repeated, first_deadline);
    assert_eq!(owner.ledger().shutdown_deadline, first_deadline);
}
