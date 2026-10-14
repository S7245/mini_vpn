//! Actual no-payload TCP shutdown without a Flow row or upstream factory.
//! The saved loop owns all work; the backend only gates/fails an actual RST flush.
use super::super::flow_custody::f_flow::reset::{GatedResetDevice, ResetFlushGate};
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Normal,
    HeldFlush,
    FailedFlush,
}

async fn poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        Poll::Ready(match future.as_mut().poll(cx) {
            Poll::Ready(value) => Some(value),
            Poll::Pending => None,
        })
    })
    .await
}

async fn idle_shutdown(end: End) {
    let up = PacketLink::bounded(64);
    let down = PacketLink::bounded(64);
    let mut apps = RealApps::unconnected(down.clone(), up.clone(), 2);
    apps.targets.fill(0);
    apps.connect_range(0..1);
    let gate = Arc::new(ResetFlushGate::default());
    let upstream = Arc::new(PendingOpens::new());
    let (_udp, rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        GatedResetDevice {
            inner: LoopbackTunDevice::new(up, down),
            gate: gate.clone(),
        },
        upstream.clone(),
        rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while apps.established() != 1 {
                super::f_pressure::credit_turn(&mut session, &mut apps, true).await;
            }
        })
        .await
        .unwrap();
        for _ in 0..8 {
            super::f_pressure::credit_turn(&mut session, &mut apps, true).await;
        }
        assert_eq!(apps.sent, [0, 0]);
        assert_eq!((apps.queued(0), apps.queued(1)), (0, 0));
        assert!(!gate.reset_seen.load(Ordering::SeqCst));
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        gate.blocked.store(end == End::HeldFlush, Ordering::SeqCst);
        gate.fail_reset_once
            .store(end == End::FailedFlush, Ordering::SeqCst);
        session.request_stop();
        let deadline = session.control.ledger().shutdown_deadline.unwrap();
        if end == End::HeldFlush {
            tokio::time::timeout_at(deadline, async {
                while !gate.waiting.load(Ordering::SeqCst) {
                    apps.turn(true).await;
                    assert!(
                        poll_once(session.observe_shutdown()).await.is_none(),
                        "an idle real connection must not be omitted from physical shutdown"
                    );
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(gate.reset_seen.load(Ordering::SeqCst));
            for _ in 0..4 {
                assert!(poll_once(session.observe_shutdown()).await.is_none());
                assert_eq!(session.state(), SessionDeviceState::Stopping);
                assert_eq!(session.control.ledger().shutdown_deadline, Some(deadline));
                assert!(
                    !session
                        .control
                        .device_settlement_observed
                        .load(Ordering::SeqCst)
                );
            }
            gate.release();
        }
        let expected = if end == End::FailedFlush {
            SessionDeviceState::Incomplete
        } else {
            SessionDeviceState::DeviceSettled
        };
        assert_eq!(
            tokio::time::timeout_at(deadline, session.observe_shutdown())
                .await
                .unwrap(),
            expected
        );
        apps.turn(true).await;
        assert!(
            gate.reset_seen.load(Ordering::SeqCst),
            "actual RST must precede local settlement"
        );
        assert_eq!(
            apps.sockets.get::<Socket>(apps.handles[0]).state(),
            smoltcp::socket::tcp::State::Closed
        );
        assert_eq!(
            apps.sockets.get::<Socket>(apps.handles[1]).state(),
            smoltcp::socket::tcp::State::Closed
        );
        let seen = session.control.flow_observation().unwrap();
        assert_eq!(
            (
                seen.live,
                seen.w,
                seen.f,
                seen.attempts,
                seen.running,
                seen.reserved,
                seen.escrow_bytes
            ),
            (0, 0, 0, 0, 0, 0, 0)
        );
        assert_eq!(
            (
                seen.factory_started,
                seen.published,
                seen.released,
                seen.joined
            ),
            (0, 0, 0, 0)
        );
        assert!(seen.audit);
        assert_eq!(upstream.entered.load(Ordering::SeqCst), 0);
        if end == End::FailedFlush {
            let (source, facts) = {
                let ledger = session.control.ledger();
                assert!(ledger.quarantined);
                let source = ledger.first_egress_io.as_ref().unwrap().clone();
                assert_eq!(source.raw_os_error(), Some(5));
                (
                    source,
                    format!("{:?}", ledger.first_failure.as_ref().unwrap().facts()),
                )
            };
            assert!(!gate.fail_reset_once.load(Ordering::SeqCst));
            for _ in 0..3 {
                assert_eq!(
                    session.observe_shutdown().await,
                    SessionDeviceState::Incomplete
                );
                let ledger = session.control.ledger();
                assert!(ledger.quarantined);
                assert!(Arc::ptr_eq(
                    &source,
                    ledger.first_egress_io.as_ref().unwrap()
                ));
                assert_eq!(
                    format!("{:?}", ledger.first_failure.as_ref().unwrap().facts()),
                    facts
                );
                assert_eq!(ledger.shutdown_deadline, Some(deadline));
                assert!(
                    !session
                        .control
                        .device_settlement_observed
                        .load(Ordering::SeqCst)
                );
            }
        } else {
            assert!(!session.control.ledger().quarantined);
            assert!(session.control.ledger().first_failure.is_none());
        }
    })
    .await;
    gate.release();
    session.request_stop();
    let cleanup = tokio::time::timeout(Duration::from_secs(16), session.observe_shutdown()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(
        cleanup.unwrap(),
        if end == End::FailedFlush {
            SessionDeviceState::Incomplete
        } else {
            SessionDeviceState::DeviceSettled
        }
    );
}

#[tokio::test]
async fn idle_unadmitted_tcp_is_reset_before_device_settlement() {
    idle_shutdown(End::Normal).await;
}

#[tokio::test]
async fn idle_unadmitted_reset_flush_survives_observer_cancellation() {
    idle_shutdown(End::HeldFlush).await;
}

#[tokio::test]
async fn idle_unadmitted_reset_flush_error_preserves_physical_quarantine() {
    idle_shutdown(End::FailedFlush).await;
}
