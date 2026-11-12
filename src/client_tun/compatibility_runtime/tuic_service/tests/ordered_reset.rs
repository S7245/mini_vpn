//! Real Ordered ctx-cap terminal transaction, through the original saved factory.
//! Pause App turns, never manufacture a cap/flow/receipt; the original ACK gate
//! retains one actually minted receipt only after the real reset/flush and clear.
use super::*;
use crate::shared::errors::FailureClass;
use crate::tcp_downlink_pump::DownstreamPermitReleaseMode;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use device::{Gate, GatedDevice};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
mod io;
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;
use io::{ObservedDevice, RstIo};

#[derive(Clone, Copy)]
enum Fault {
    None,
    RstIo,
    Notify,
    ScenarioPanic,
}
struct PanicWake(AtomicUsize);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert_ne!(
            self.0.fetch_add(1, Ordering::SeqCst),
            0,
            "actual terminal ctx capacity Notify panic"
        );
    }
}
fn first_panic(owner: &SessionControl, index: usize, epoch: u64) -> (usize, String, Vec<u8>) {
    let value = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|first| {
            let facts = first.facts();
            (
                (
                    first as *const _ as usize,
                    format!("{facts:?}"),
                    first.text_bytes().to_vec(),
                ),
                (
                    facts.class == FailureClass::Panic,
                    facts.origin.phase == FailurePhase::TcpOpen,
                    facts.origin.resource == crate::shared::errors::FailureResource::TcpOpen,
                    facts.origin.resource_id,
                    facts.origin.attempt_id,
                    ledger.first_egress_io.is_none(),
                ),
            )
        })
    };
    let (retained, typed) = value.expect("original first flow Panic");
    assert_eq!(typed, (true, true, true, index as u64, Some(epoch), true));
    retained
}
#[tokio::test(flavor = "current_thread")]
async fn actual_terminal_ctx_notify_panic_keeps_pending_cap_payload_and_inflight_charged() {
    exercise(Fault::Notify).await;
}

fn parent_limit(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn owner_limit(owner: &SessionControl) -> Option<tokio::time::Instant> {
    owner.ledger().shutdown_deadline
}
fn first_none(owner: &SessionControl) -> bool {
    owner.ledger().first_failure.is_none()
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ordered_ctx_caps_reset_before_queue_zero_and_receipt() {
    exercise(Fault::None).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_ordered_rst_io_retains_first_source_and_charged_ctx_without_receipt() {
    exercise(Fault::RstIo).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_failed_ordered_scenario_keeps_first_panic_after_original_stop_and_drain() {
    let outcome = wire::capture(exercise(Fault::ScenarioPanic)).await;
    let first = outcome.expect_err("controlled scenario failure must survive cleanup");
    assert_eq!(
        first.downcast_ref::<&'static str>(),
        Some(&"original ordered scenario sentinel")
    );
}
fn first_io(owner: &SessionControl) -> (usize, usize, String, Vec<u8>) {
    let value = {
        let ledger = owner.ledger();
        ledger
            .first_egress_io
            .as_ref()
            .zip(ledger.first_failure.as_ref())
            .map(|(source, first)| {
                (
                    Arc::as_ptr(source) as usize,
                    source.raw_os_error(),
                    first as *const _ as usize,
                    first.facts().raw_os_code,
                    first.facts().origin.phase == FailurePhase::DeviceWrite,
                    format!("{:?}", first.facts()),
                    first.text_bytes().to_vec(),
                )
            })
    };
    let (source, raw, retained, code, phase, facts, text) =
        value.expect("original retained egress source");
    assert_eq!((raw, code, phase), (Some(5), Some(5), true));
    (source, retained, facts, text)
}
async fn exercise(fault: Fault) {
    let fail_rst = matches!(fault, Fault::RstIo);
    let fail_notify = matches!(fault, Fault::Notify);
    let server = wire::server();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        ordered_egress_permit: Some("1"),
        ..Default::default()
    }));
    let cfg = wire::config(&server).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let io = Arc::new(RstIo::default());
    let mut service = prepare(
        ObservedDevice {
            inner: GatedDevice {
                inner,
                gate: gate.clone(),
            },
            io: io.clone(),
        },
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peer = None;
    let mut stream = None;
    let mut owner = None;
    let mut retirement_started = false;
    let mut observer = Box::pin(handle.observe_shutdown());
    let panic_wake = Arc::new(PanicWake(AtomicUsize::new(0)));
    let mut waiter = None;
    let scenario = wire::capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        }).await;
        owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade();
        let owner = owner.as_ref().unwrap();
        stream = Some(wire::drive(&mut service, async {
            let remote = async { tokio::join!(wire::healthy(peer.as_ref().unwrap()), wire::udp_reply(peer.as_ref().unwrap())) };
            let application = async {
                while app.tcp_reply != b"first" || app.udp_reply != b"udp-reply" {
                    app.turn().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let ((stream, ()), ()) = tokio::join!(remote, application);
            stream
        }).await);
        let id = owner.flow_contracts()[0].id;
        let queue = owner.test_permit_queue(id).unwrap();
        owner.hold_published_receipt(id);
        owner.watch_ordered_physical(id);
        println!("actual original TCP5/UDP complete; watching exact published {id:?}");
        // The actual App is now parked. Real socket send_slice admits bytes,
        // while its original advertised window prevents further DATA dispatch.
        let payload = vec![73; 128 * 1024];
        let before = wire::drive(&mut service, async {
            stream.as_mut().unwrap().0.write_all(&payload).await.unwrap();
            loop {
                if let Some(snapshot) = owner.ordered_physical() {
                    if snapshot.0 == id && snapshot.2 > 0 && snapshot.4 > 0 {
                        break snapshot;
                    }
                }
                tokio::task::yield_now().await;
            }
        }).await;
        assert_eq!(before.1, id.epoch);
        assert_eq!(before.2, before.3);
        assert!(before.4 > 0 && before.5 > 0 && before.6 && !before.7);
        assert!(!queue.physical_zero());
        println!("actual original ctx before stop id/epoch/pending/caps/inflight/socket/noD16/reset={before:?}");
        io.fail.store(fail_rst, Ordering::Release);
        handle.request_stop();
        let parent = parent_limit(&saved);
        wire::drive(&mut service, async {
            while owner.flow_observation().unwrap().joined != 4 { tokio::task::yield_now().await; }
        }).await;
        let deadline = owner_limit(owner);
        assert!(deadline.is_some());
        assert!(!queue.physical_zero());
        assert!(queue.snapshot_now().leased_bytes > 0);
        assert!(owner.held_exact_receipt().is_none());
        assert_eq!(gate.polls.load(Ordering::Acquire), 0);
        assert!(first_none(owner));
        println!("actual four joins still charged ctx cap, provider final offset held; no receipt/D");
        if matches!(fault, Fault::ScenarioPanic) {
            panic!("original ordered scenario sentinel");
        }
        let before_notify = if fail_notify {
            let bridge = owner.relay_event_test_bridge().unwrap();
            let snapshot = wire::drive(&mut service, async {
                loop {
                    let physical = owner.ordered_physical().unwrap();
                    let custody = owner.ordered_custody().unwrap();
                    if bridge.queued_len() == 0 && owner.test_queued_data(id.handle) == Some(0)
                        && queue.snapshot_now().leased_bytes == physical.3 + physical.4
                        && custody.pending.0 > 0 && custody.inflight_bytes > 0 {
                        break custody;
                    }
                    tokio::task::yield_now().await;
                }
            }).await;
            assert_eq!(snapshot.epoch, id.epoch);
            assert!(snapshot.pending_caps.0 > 0);
            assert!(snapshot.pending_caps.1.unwrap().0 > 0);
            assert_eq!(snapshot.pending_caps.1.unwrap().1, DownstreamPermitReleaseMode::OnEgressDrain);
            assert!(!snapshot.busy && !snapshot.quarantined && !snapshot.mirror_quarantined);
            assert_eq!(panic_wake.0.load(Ordering::Acquire), 0);
            // Original close/joins and Main drain are complete. Only now register
            // the original not_full waiter before the real provider final offset.
            waiter = Some(Box::pin(queue.clone().test_original_capacity_notify()));
            {
                let waker = Waker::from(panic_wake.clone());
                let mut cx = Context::from_waker(&waker);
                assert!(waiter.as_mut().unwrap().as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(panic_wake.0.load(Ordering::Acquire), 0);
            let leased = queue.snapshot_now().leased_bytes;
            println!("terminal Notify armed original ctx={snapshot:?} leased={leased}, actualMainempty");
            Some((snapshot, leased))
        } else { None };
        retirement_started = true;
        wire::drive(&mut service, wire::retire(&mut stream, &AtomicBool::new(true))).await;
        if fail_notify {
            let (before, leased) = before_notify.unwrap();
            let after = wire::drive(&mut service, async {
                loop {
                    if let Some(custody) = owner.ordered_custody() {
                        if !first_none(owner) && (custody.busy || custody.mirror_quarantined) {
                            break custody;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }).await;
            assert_eq!(panic_wake.0.load(Ordering::Acquire), 1);
            assert_eq!((io.emitted.load(Ordering::Acquire), io.flushed.load(Ordering::Acquire),
                io.failed.load(Ordering::Acquire), io.first.load(Ordering::Acquire)), (1,1,0,1));
            assert_eq!(after.epoch, before.epoch);
            assert_eq!(after.pending, before.pending, "original Vec len/ptr/cap stays custodied");
            assert_eq!(after.pending_caps.0, before.pending_caps.0, "original front is not popped");
            assert_eq!(after.pending_caps.1, Some((0, DownstreamPermitReleaseMode::OnEgressDrain)));
            assert_eq!(after.inflight_caps, before.inflight_caps);
            assert_eq!(after.inflight_bytes, before.inflight_bytes);
            assert!(after.busy && after.quarantined && after.mirror_quarantined);
            assert!(owner.ordered_physical().unwrap().7, "same original Reset marker exists");
            let charged = leased - before.pending_caps.1.unwrap().0;
            assert!(charged > 0);
            assert_eq!(queue.snapshot_now().leased_bytes, charged);
            assert!(!queue.physical_zero());
            let original = first_panic(owner, id.index, id.epoch);
            assert!(owner.held_exact_receipt().is_none());
            let mut observed = None;
            for _ in 0..4 {
                if observed.is_none() {
                    if let Ok(phase) = tokio::time::timeout(Duration::from_millis(2), observer.as_mut()).await {
                        observed = Some(phase);
                    }
                }
                assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
                assert_eq!(first_panic(owner, id.index, id.epoch), original);
                assert_eq!(parent_limit(&saved), parent);
                assert_eq!(owner_limit(owner), deadline);
                assert_eq!(owner.ordered_custody(), Some(after));
            }
            let phase = match observed {
                Some(phase) => phase,
                None => tokio::select! {
                    report = service.run() => panic!("interrupted terminal ctx cannot settle {report:?}"),
                    phase = observer.as_mut() => phase,
                },
            };
            if phase == Phase::Incomplete {
                assert!(tokio::time::Instant::now() >= parent.unwrap());
                wire::drive(&mut service, async {
                    while handle.phase() != Phase::Quarantined { tokio::task::yield_now().await; }
                }).await;
            } else { assert_eq!(phase, Phase::Quarantined); }
            for _ in 0..4 {
                assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
                assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
                assert_eq!(owner.ordered_custody(), Some(after));
                assert_eq!(first_panic(owner, id.index, id.epoch), original);
                assert_eq!(parent_limit(&saved), parent);
                assert_eq!(owner_limit(owner), deadline);
                assert_eq!(queue.snapshot_now().leased_bytes, charged);
                assert!(!queue.physical_zero());
                assert!(owner.held_exact_receipt().is_none());
                let state = owner.flow_observation().unwrap();
                assert!(state.audit);
                assert_eq!((state.live,state.f,state.joined,state.released), (1,1,4,0));
                assert!(!owner.business_complete.load(Ordering::Acquire));
                assert_eq!((gate.polls.load(Ordering::Acquire),gate.joins.load(Ordering::Acquire),
                    gate.drops.load(Ordering::Acquire)), (0,0,0));
                assert!(service.completion.is_none());
                assert!(peer.as_ref().unwrap().close_reason().is_none());
                assert!(!handle.request_rebuild());
            }
            assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
            println!("actualterminalclear Notify pendingfrontSome0 unpopped payloadsame/inflightheld {after:?}; queueleased={charged} first={original:?} sameobserver={phase:?} originalparent={parent:?} owner={deadline:?}; no receipt/Frelease/DNode/successor");
            return;
        }
        if fail_rst {
            wire::drive(&mut service, async {
                while io.failed.load(Ordering::Acquire) == 0 { tokio::task::yield_now().await; }
            }).await;
            let original = first_io(owner);
            assert_eq!(io.emitted.load(Ordering::Acquire), 1);
            assert_eq!(io.failed.load(Ordering::Acquire), 1);
            assert_eq!(io.first.load(Ordering::Acquire), 2);
            println!("first actual RST flush failed; later actual successful flush count={}",io.flushed.load(Ordering::Acquire));
            assert!(!queue.physical_zero());
            assert!(queue.snapshot_now().leased_bytes > 0);
            assert!(owner.held_exact_receipt().is_none());
            let mut observed = None;
            for _ in 0..4 {
                if observed.is_none() {
                    if let Ok(phase) = tokio::time::timeout(Duration::from_millis(2), observer.as_mut()).await {
                        observed = Some(phase);
                    }
                }
                assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
                assert_eq!(first_io(owner), original);
                assert_eq!(parent_limit(&saved), parent);
                assert_eq!(owner_limit(owner), deadline);
                assert!(!queue.physical_zero());
                assert!(owner.held_exact_receipt().is_none());
            }
            let state = owner.flow_observation().unwrap();
            assert_eq!((state.live,state.f,state.joined,state.released),(1,1,4,0));
            assert!(!owner.business_complete.load(Ordering::Acquire));
            assert_eq!(gate.polls.load(Ordering::Acquire), 0);
            assert!(!handle.request_rebuild());
            println!("actual target RST flush controlled IO5 first={original:?}; charged caps/liveF, no receipt/D/Node cleanup {state:?}");
            let phase = match observed {
                Some(phase) => phase,
                None => tokio::select! { biased;
                    report = service.run() => panic!("charged original F cannot complete {report:?}"),
                    phase = observer.as_mut() => phase,
                },
            };
            match phase {
                Phase::Incomplete => assert!(tokio::time::Instant::now() >= parent.unwrap()),
                Phase::Quarantined => { let q = owner.ledger().quarantined(); assert!(q); }
                other => panic!("charged original F falsely settled {other:?}"),
            }
            println!("original first15 parent={parent:?} owner={deadline:?}; same observer {phase:?}; no run Report/D/Node cleanup");
            assert_eq!(parent_limit(&saved),parent);
            assert_eq!(owner_limit(owner),deadline);
            assert_eq!(first_io(owner), original);
            assert!(owner.held_exact_receipt().is_none());
            assert!(!queue.physical_zero());
            assert!(!handle.request_rebuild());
            return;
        }
        let receipt = wire::drive(&mut service, async {
            loop {
                if let Some(receipt) = owner.held_exact_receipt() { break receipt; }
                tokio::task::yield_now().await;
            }
        }).await;
        assert_eq!(receipt.id, id);
        assert!(receipt.original_matches && receipt.provider_settled && receipt.rearming);
        assert!(receipt.row_held && !receipt.quarantined && receipt.producer_stopped);
        assert_eq!(receipt.role_joins, 4);
        assert!(receipt.discarded_inflight > 0);
        assert_eq!(io.emitted.load(Ordering::Acquire), 1);
        assert_eq!(io.flushed.load(Ordering::Acquire), 1);
        assert_eq!(io.failed.load(Ordering::Acquire), 0);
        assert_eq!(io.first.load(Ordering::Acquire), 1);
        assert!(receipt.disposition.4 > 0, "actual unadmitted pending alone is abandoned");
        assert!(queue.physical_zero());
        assert_eq!(gate.polls.load(Ordering::Acquire), 0);
        assert_eq!(parent_limit(&saved), parent);
        assert_eq!(owner_limit(owner), deadline);
        assert!(first_none(owner));
        println!("actual RESET/flush then original owner clear -> original queue physicalzero before ACK {receipt:?}");
        owner.release_held_receipt();
        wire::drive(&mut service, async {
            while gate.polls.load(Ordering::Acquire) == 0 { tokio::task::yield_now().await; }
        }).await;
        let state = owner.flow_observation().unwrap();
        assert!(state.audit);
        assert_eq!((state.live, state.w, state.f, state.running, state.attempts), (0,0,0,0,0));
        assert_eq!((state.published, state.released, state.joined), (1,1,4));
        assert!(owner.business_complete.load(Ordering::Acquire));
        assert!(!handle.request_rebuild());
    }).await;
    if let Some(owner) = &owner {
        if scenario.is_err() {
            let first = {
                let ledger = owner.ledger();
                ledger
                    .first_failure
                    .as_ref()
                    .map(|e| format!("{:?}", e.facts()))
            };
            eprintln!(
                "original scenario failure snapshot={:?} flow={:?} first={first:?}",
                owner.ordered_physical(),
                owner.flow_observation()
            );
            eprintln!(
                "original reset stage phase={:?} emitted={} flushed={} failed={} first={} parent={:?} owner={:?}",
                handle.phase(),
                io.emitted.load(Ordering::Acquire),
                io.flushed.load(Ordering::Acquire),
                io.failed.load(Ordering::Acquire),
                io.first.load(Ordering::Acquire),
                parent_limit(&saved),
                owner_limit(owner),
            );
        }
        owner.release_held_receipt();
    }
    if scenario.is_err() && retirement_started && stream.is_some() {
        eprintln!(
            "partial retirement Unknown: dispose original remaining halves, no second retire"
        );
        drop(stream.take());
    }
    drop(waiter);
    if fail_rst || fail_notify {
        // Forced disposal after original Q/Incomplete observation is not clean.
        let stream_disposal = wire::capture(async { drop(stream) }).await;
        let service_disposal = wire::capture(async { drop(service) }).await;
        let drained = wire::capture(wire::drain(server, peer)).await;
        println!(
            "forced original Q disposal; actual server drain={}",
            drained.is_ok()
        );
        if let Err(e) = scenario {
            std::panic::resume_unwind(e);
        }
        if let Err(e) = stream_disposal {
            std::panic::resume_unwind(e);
        }
        if let Err(e) = service_disposal {
            std::panic::resume_unwind(e);
        }
        if let Err(e) = drained {
            std::panic::resume_unwind(e);
        }
        assert_eq!(
            (
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (0, 1)
        );
        return;
    }
    gate.open();
    handle.request_stop();
    let parent = parent_limit(&saved);
    let mut actual_report = None;
    let cleanup = wire::capture(async {
        let run = async {
            tokio::join!(service.run(), async {
                if !retirement_started && stream.is_some() {
                    wire::retire(&mut stream, &AtomicBool::new(true)).await;
                }
            })
        };
        if scenario.is_err() {
            actual_report = fixture::observe_failed_parent_stop(run, parent)
                .await
                .map(|(report, ())| report);
            eprintln!(
                "failed Ordered parent stop: completed={} phase={:?} physical={:?}",
                actual_report.is_some(),
                handle.phase(),
                owner.as_ref().and_then(|owner| owner.ordered_physical()),
            );
            return;
        }
        let (report, ()) = tokio::time::timeout_at(parent.unwrap(), run).await.unwrap();
        actual_report = Some(report);
        println!(
            "actual Ordered ctx cleanup={report:?}, phase={:?}",
            handle.phase()
        );
        if scenario.is_ok() {
            assert!(report.device_settled() && !report.device_failed());
            assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
            assert_eq!(report.rebuilds, 0);
            assert_eq!(handle.observe_shutdown().await, Phase::Complete);
        }
    })
    .await;
    println!("actual cleanup report={actual_report:?}");
    let stream_disposal = wire::capture(async { drop(stream) }).await;
    let service_disposal = wire::capture(async { drop(service) }).await;
    let drained = wire::capture(wire::drain(server, peer)).await;
    println!(
        "actual Ordered ctx cleanup={} serverdrain={}",
        cleanup.is_ok(),
        drained.is_ok()
    );
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = stream_disposal {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = service_disposal {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = drained {
        std::panic::resume_unwind(e);
    }
    assert_eq!(
        (
            gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire)
        ),
        (1, 1)
    );
}
