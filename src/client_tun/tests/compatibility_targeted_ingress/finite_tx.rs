//! Deferred ingress under finite TX must still reach the original admission owner.
//! The real application stops driving after DATA is staged: only the saved host's
//! original timer/full poll can consume it. No replay, fake socket or TUIC claim.
use super::*;

#[tokio::test]
async fn saved_timer_consumes_deferred_data_and_preserves_its_admission_effect() {
    let up = PacketLink::bounded(64);
    let down = PacketLink::bounded(64);
    let mut apps = RealApps::unconnected(down.clone(), up.clone(), 1);
    apps.targets.fill(0);
    apps.connect_range(0..1);
    let probe = Arc::new(Probe::default());
    let upstream = Arc::new(PendingOpens::new());
    let (_udp, rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    config.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        WireDevice(
            LoopbackTunDevice::new(up.clone(), down.clone()),
            probe.clone(),
        ),
        upstream.clone(),
        rx,
        config,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while apps.established() != 1 || probe.received.load(Ordering::SeqCst) < 2 {
                super::super::f_pressure::credit_turn(&mut session, &mut apps, true).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        let staged = probe.staged.load(Ordering::SeqCst);
        let received = probe.received.load(Ordering::SeqCst);
        probe.tx_blocked.store(true, Ordering::SeqCst);
        apps.targets[0] = 1;
        apps.turn(true).await;
        assert_eq!(apps.sent[0], 1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while probe.staged.load(Ordering::SeqCst) == staged {
                assert!(
                    tokio::time::timeout(Duration::from_millis(1), session.run())
                        .await
                        .is_err()
                );
            }
        })
        .await
        .unwrap();
        assert_eq!(probe.staged.load(Ordering::SeqCst), staged + 1);
        assert_eq!(probe.received.load(Ordering::SeqCst), received);
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        assert_eq!(
            session.control.flow_observation().unwrap().factory_started,
            0
        );
        probe.tx_blocked.store(false, Ordering::SeqCst);
        let resumed = tokio::time::timeout(Duration::from_secs(1), async {
            while session.control.flow_observation().unwrap().factory_started == 0 {
                assert!(
                    tokio::time::timeout(Duration::from_millis(1), session.run())
                        .await
                        .is_err()
                );
            }
        })
        .await;
        eprintln!(
            "deferred DATA: staged_delta={} received_delta={} factory_started={} resumed={}",
            probe.staged.load(Ordering::SeqCst) - staged,
            probe.received.load(Ordering::SeqCst) - received,
            session.control.flow_observation().unwrap().factory_started,
            resumed.is_ok()
        );
        assert_eq!(probe.staged.load(Ordering::SeqCst), staged + 1);
        assert_eq!(probe.received.load(Ordering::SeqCst), received + 1);
        assert!(
            resumed.is_ok(),
            "original timer consumed DATA but its admission effect was lost"
        );
        assert_eq!(
            session.control.flow_observation().unwrap().factory_started,
            1
        );
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
    })
    .await;
    probe.tx_blocked.store(false, Ordering::SeqCst);
    session.request_stop();
    for gate in &upstream.gates {
        gate.notify_waiters();
    }
    let cleanup = capture(async {
        let factory = session.control.flow_observation().unwrap().factory_started;
        assert!(factory <= 1);
        let expected = if factory == 0 {
            SessionDeviceState::DeviceSettled
        } else {
            SessionDeviceState::DeviceFailedSettled
        };
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), session.run())
                .await
                .unwrap(),
            expected
        );
        let seen = session.control.flow_observation().unwrap();
        assert_eq!(
            (
                seen.live,
                seen.w,
                seen.f,
                seen.escrow_bytes,
                seen.published,
                seen.joined
            ),
            (0, 0, 0, 0, 0, 0)
        );
        assert_eq!(seen.released, factory);
        if factory == 1 {
            let ledger = session.control.ledger();
            let facts = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(facts.origin.phase, FailurePhase::TcpOpen);
            assert_eq!(facts.io_kind, Some(std::io::ErrorKind::Unsupported));
        }
        assert_eq!(
            upstream.entered.load(Ordering::SeqCst),
            upstream.dropped.load(Ordering::SeqCst)
        );
    })
    .await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
