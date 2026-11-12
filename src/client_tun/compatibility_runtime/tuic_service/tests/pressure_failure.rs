//! Real W128 pressure through the saved production service; only TunIo fault
//! injection and passive original-owner observations are private. The original
//! device/pump, actual QUIC halves and pending providers remain physically owned
//! in Q. Forced final teardown is never client/node asynchronous settlement.
use super::*;
use device::{Gate, GatedDevice};
use std::future::Future;
use std::sync::atomic::Ordering;
use std::task::Poll;
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;

async fn load<S: TunService>(
    service: &mut S,
    app: &mut wire::App,
    peer: &quinn::Connection,
    owner: &SessionControl,
) -> (quinn::SendStream, quinn::RecvStream) {
    let stream = wire::drive(service, async {
        let remote = async { tokio::join!(wire::healthy(peer), wire::udp_reply(peer)) };
        let application = async {
            while app.tcp_reply != b"first" || app.udp_reply != b"udp-reply" {
                app.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let ((stream, ()), ()) = tokio::join!(remote, application);
        stream
    })
    .await;
    wire::drive(service, async {
        for _ in 0..129 {
            app.add();
            while !app.last_acked() {
                app.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        loop {
            let state = owner.flow_observation().unwrap();
            assert!(state.audit && state.w <= 128);
            if state.w == 128 && state.opening_providers == 128 {
                assert_eq!(
                    (state.live, state.f, state.factory_started),
                    (129, 129, 129)
                );
                assert_eq!((state.running, state.published), (3, 1));
                println!("original W128 before failure {state:?}");
                break;
            }
            app.turn().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    stream
}

fn first_error(owner: &SessionControl) -> (usize, Option<i32>, String) {
    let ledger = owner.ledger();
    let error = ledger.first_failure.as_ref().unwrap();
    (
        error as *const _ as usize,
        error.facts().raw_os_code,
        format!("{:?}", error.facts()),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn w128_actual_read_fatal_keeps_original_error_and_unjoined_physical_q() {
    let server = wire::server();
    let cfg = wire::config(&server);
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    gate.open();
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare(
        GatedDevice {
            inner,
            gate: gate.clone(),
        },
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peer = None;
    let mut stream = None;
    let scenario = wire::capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        }).await;
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        stream = Some(load(&mut service, &mut app, peer.as_ref().unwrap(), &owner).await);
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 129);
        assert!(owner.ledger().first_failure.is_none());
        gate.read_error.store(true, Ordering::Release);
        app.ping(); // Real ingress wakes any already-pending original read.
        wire::drive(&mut service, async {
            while owner.ledger().first_failure.is_none() {
                app.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        assert!(!gate.read_error.load(Ordering::Acquire), "actual TunIo error returned");
        let original = first_error(&owner);
        assert_eq!(original.1, Some(5));
        assert_eq!(owner.ledger().first_failure.as_ref().unwrap().facts().origin.phase, FailurePhase::DeviceRead);
        let deadline = owner.ledger().shutdown_deadline.unwrap();
        let mut observer = Box::pin(handle.observe_shutdown());
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(1), service.run()).await.is_err());
        }
        let parent = saved.deadline.lock().unwrap().unwrap();
        let phase = tokio::select! {
            biased;
            report = service.run() => panic!("fatal work cannot complete {report:?}"),
            phase = observer.as_mut() => phase,
        };
        let observed_at = tokio::time::Instant::now();
        match phase {
            Phase::Incomplete => {
                assert!(observed_at >= parent, "original observer deadline, no fresh15");
                let state = owner.flow_observation().unwrap();
                assert!(state.live > 0 && state.f > 0 && state.audit);
                assert!(!owner.business_complete.load(Ordering::Acquire));
                assert!(!owner.driver_complete.load(Ordering::Acquire));
                assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0,0,0));
            }
            Phase::Quarantined => assert!(owner.ledger().quarantined()),
            other => panic!("fatal pressure observer falsely completed {other:?}"),
        }
        println!("fatal original ownerfirst15={deadline:?} parentfirst15={parent:?} observer={phase:?} actual_at={observed_at:?}; no join");
        // The original 15s observer has finished above. This independent 5s
        // diagnostic cap observes actual cold census/Q publication, never grants
        // a new business deadline or certifies cleanup after the original limit.
        wire::drive(&mut service, async {
            while handle.phase() != Phase::Quarantined {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        for _ in 0..4 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
            assert_eq!(first_error(&owner), original);
            assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
            assert_eq!(*saved.deadline.lock().unwrap(), Some(parent));
            assert!(owner.ledger().quarantined());
            assert!(!owner.business_complete.load(Ordering::Acquire));
            assert!(!owner.driver_complete.load(Ordering::Acquire));
            let state = owner.flow_observation().unwrap();
            assert!(state.audit && state.live > 0 && state.f > 0);
            assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0));
            assert_eq!(gate.polls.load(Ordering::Acquire), 0);
            assert!(service.completion.is_none());
            assert!(peer.as_ref().unwrap().close_reason().is_none());
            assert!(!handle.request_rebuild());
        }
        let after = owner.flow_contracts();
        assert_eq!(after.len(), contracts.len());
        for (before, after) in contracts.iter().zip(after.iter()) {
            assert_eq!(before.id, after.id);
            assert_eq!(before.open_deadline, after.open_deadline);
            assert_eq!(before.slot_deadline, after.slot_deadline);
        }
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("actual read OS5 retained facts/value identity; W pressure stays charged Q, no business/device join or successor");
    }).await;
    // Drop is an explicitly forced test boundary. No actor abort is called or
    // unconsumed client/device/Node join converted into a clean result.
    drop(stream);
    drop(service);
    let drained = wire::capture(wire::drain(server, peer)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn w128_noncooperating_peer_keeps_final_offset_debt_through_original_first15() {
    let server = wire::server();
    let cfg = wire::config(&server);
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    gate.open();
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare(
        GatedDevice {
            inner,
            gate: gate.clone(),
        },
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peer = None;
    let mut stream = None;
    let scenario = wire::capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        })
        .await;
        let owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        stream = Some(load(&mut service, &mut app, peer.as_ref().unwrap(), &owner).await);
        let contracts = owner.flow_contracts();
        assert!(
            handle.request_rebuild(),
            "one original public rebuild intent"
        );
        // Poll the same original observer once: its stop fixes the public15,
        // then the saved driver sees that stop. No actual successor is allowed
        // while the original physical stream has an unknown final offset.
        let mut observer = Box::pin(handle.observe_shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), observer.as_mut())
                .await
                .is_err()
        );
        let parent = saved.deadline.lock().unwrap().unwrap();
        wire::drive(&mut service, async {
            let (tx, rx) = stream.as_mut().unwrap();
            let code = tx.stopped().await.unwrap().expect("actual STOP_SENDING");
            assert!(
                rx.read_to_end(0).await.unwrap().is_empty(),
                "actual original writer EOF"
            );
            // Intentionally neither RESET nor FIN this actual SendStream. Keep
            // it physically stored here until forced test teardown, including
            // all original first5/first15 observations.
            println!("actual peer STOP={code:?}/writerEOF; final offset still physically withheld");
            while owner.retained_provider_failure().is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let deadline = owner.ledger().shutdown_deadline.unwrap();
        let original = owner.retained_provider_failure().unwrap();
        assert_eq!(
            owner.retained_provider_io_kind(),
            Some(std::io::ErrorKind::Interrupted)
        );
        assert_eq!(original.1.phase, crate::upstream::TcpOpenPhase::Settled);
        assert!(!original.1.lease_held && !original.1.retirement_pending);
        let failure = first_error(&owner);
        for _ in 0..4 {
            assert!(
                tokio::time::timeout(Duration::from_millis(1), service.run())
                    .await
                    .is_err()
            );
            assert_eq!(owner.retained_provider_failure(), Some(original.clone()));
            assert_eq!(first_error(&owner), failure);
            assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
            assert_eq!(*saved.deadline.lock().unwrap(), Some(parent));
            assert_eq!(
                (
                    gate.polls.load(Ordering::Acquire),
                    gate.joins.load(Ordering::Acquire),
                    gate.drops.load(Ordering::Acquire)
                ),
                (0, 0, 0)
            );
            assert!(!handle.request_rebuild());
        }
        let observed = tokio::select! {
            biased;
            report = service.run() => panic!("unknown final offset cannot complete {report:?}"),
            phase = observer.as_mut() => phase,
        };
        match observed {
            Phase::Incomplete => assert!(tokio::time::Instant::now() >= parent),
            Phase::Quarantined => assert!(owner.ledger().quarantined()),
            other => panic!("unknown final offset falsely settled {other:?}"),
        }
        // Q can be published before public15. Continue driving the same saved
        // work and withhold the SAME physical final offset through that original
        // deadline, never restart15 from the earlier observer return.
        tokio::select! {
            biased;
            report = service.run() => panic!("charged Q cannot complete {report:?}"),
            _ = tokio::time::sleep_until(parent.max(deadline)) => {},
        }
        assert!(tokio::time::Instant::now() >= parent && tokio::time::Instant::now() >= deadline);
        wire::drive(&mut service, async {
            while handle.phase() != Phase::Quarantined {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
        assert!(owner.ledger().quarantined());
        assert!(!owner.business_complete.load(Ordering::Acquire));
        assert!(!owner.driver_complete.load(Ordering::Acquire));
        assert!(service.completion.is_none());
        assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
        assert_eq!(*saved.deadline.lock().unwrap(), Some(parent));
        assert_eq!(owner.retained_provider_failure(), Some(original));
        assert_eq!(first_error(&owner), failure);
        let state = owner.flow_observation().unwrap();
        println!(
            "noncooperating after parentfirst15={parent:?}/ownerfirst15={deadline:?}: {state:?}"
        );
        assert!(state.audit && state.live > 0 && state.f > 0 && state.released < 129);
        assert_eq!(
            (
                gate.polls.load(Ordering::Acquire),
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (0, 0, 0)
        );
        for after in owner.flow_contracts() {
            let before = contracts
                .iter()
                .find(|before| before.id == after.id)
                .unwrap();
            assert_eq!(before.slot_deadline, after.slot_deadline);
            assert_eq!(before.open_deadline, after.open_deadline);
        }
        assert!(peer.as_ref().unwrap().close_reason().is_none());
        assert!(!handle.request_rebuild());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    })
    .await;
    // The only final-offset release is explicit forced stream destruction here;
    // client service/device/Node have NOT settled asynchronously. No abort/join
    // substitution, diagnostic debt normalization or final clean report exists.
    drop(stream);
    drop(service);
    let drained = wire::capture(wire::drain(server, peer)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}
