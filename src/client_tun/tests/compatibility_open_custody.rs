//! Open admission through the saved session, original loop, and two real TCP stacks.
//! The controlled upstream never claims TUIC or flow settlement acceptance.

use super::*;
use crate::harness::{LoopbackTunDevice, PacketLink};
use crate::shared::{ClientError, TargetAddr};
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{Socket, SocketBuffer};
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, Ipv4Address, Ipv4Packet, TcpPacket};
use std::time::Duration;

#[path = "compatibility_open_fixture.rs"]
mod fixture;
use fixture::{
    FixtureCleanup, OpenBehavior, OpenGuard, OpenInventory, PendingOpens, RealApps, capture,
    cleanup, fixture, fixture_with_config, fixture_with_rx, inventory, ready, turn,
};
#[path = "compatibility_open_pressure_events.rs"]
mod events;
#[path = "compatibility_f_ack_rounds.rs"]
mod f_ack_rounds;
#[path = "compatibility_f_full.rs"]
mod f_full;
#[path = "compatibility_f_partial_full.rs"]
mod f_partial_full;
#[path = "compatibility_f_pressure.rs"]
mod f_pressure;
#[path = "compatibility_open_pressure.rs"]
mod pressure;
#[path = "compatibility_open_settlement.rs"]
mod settlement;
#[path = "compatibility_targeted_ingress.rs"]
mod targeted_ingress;
#[path = "compatibility_unadmitted_shutdown.rs"]
mod unadmitted_shutdown;
pub(in crate::client_tun::compatibility_runtime) use pressure::PressureProbe;

#[derive(Debug)]
struct AdmissionObservation {
    setup_ready: bool,
    established: usize,
    initially_pending: usize,
    pressure_ready: bool,
    entered: usize,
    window: Option<u16>,
    queued: usize,
    released_progress: Option<bool>,
    dropped_after_release: usize,
    blocked: Option<(SocketHandle, u64, usize, u64)>,
    first_blocked: Option<(SocketHandle, u64, usize, u64)>,
    admitted_identity: Option<(SocketHandle, u64)>,
}

#[tokio::test(flavor = "current_thread")]
async fn open_admission_retains_socket_bytes_until_custody_available() {
    let (mut session, mut apps, upstream, _downlink) = fixture();
    let captured = capture(async {
        let setup_ready = ready(&mut session, &mut apps, &upstream).await;
        let established = apps.established();
        let initially_pending = upstream.entered.load(Ordering::SeqCst);
        let mut first_blocked = None;
        let pressure_ready = setup_ready
            && tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while apps.sent[128] != 8192 || apps.queued(128) > 4096 {
                    turn(&mut session, &mut apps, true).await;
                }
                first_blocked = session.control.opens().blocked;
                // Repeated service cycles, not an ACK delay: SUT disables delayed ACKs.
                for _ in 0..40 {
                    turn(&mut session, &mut apps, true).await;
                }
            })
            .await
            .is_ok();
        let mut observed = AdmissionObservation {
            setup_ready,
            established,
            initially_pending,
            pressure_ready,
            entered: upstream.entered.load(Ordering::SeqCst),
            window: apps.windows[128],
            queued: apps.queued(128),
            released_progress: None,
            dropped_after_release: upstream.dropped.load(Ordering::SeqCst),
            blocked: session.control.opens().blocked,
            first_blocked,
            admitted_identity: None,
        };
        if pressure_ready && observed.entered == 128 && observed.queued == 4096 {
            upstream.gates[0].notify_one();
            observed.released_progress = Some(
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while upstream.entered.load(Ordering::SeqCst) != 129 || apps.queued(128) != 0 {
                        turn(&mut session, &mut apps, true).await;
                    }
                })
                .await
                .is_ok(),
            );
            observed.dropped_after_release = upstream.dropped.load(Ordering::SeqCst);
            if let Some((handle, epoch, _, _)) = observed.blocked {
                observed.admitted_identity = session
                    .control
                    .opens()
                    .slots
                    .iter()
                    .flatten()
                    .find(|slot| slot.handle == handle && slot.epoch == epoch.wrapping_add(1))
                    .map(|slot| (slot.handle, slot.epoch));
            }
        }
        observed
    })
    .await;
    let cleanup = cleanup(&mut session, &upstream).await;
    eprintln!("fixture observation={captured:?}; cleanup={cleanup:?}");
    let observed = captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(
        observed.setup_ready,
        "129 real handshakes and first128 opens must become ready"
    );
    assert_eq!(observed.established, 129);
    assert_eq!(observed.initially_pending, 128);
    assert!(
        observed.pressure_ready,
        "last TCP flow must deliver its first window"
    );
    assert_eq!(
        observed.entered, 128,
        "W128 must bound workers before extracting socket DATA"
    );
    assert_eq!(observed.window, Some(0));
    assert_eq!(
        observed.first_blocked, observed.blocked,
        "original socket identity and bytes survive repeated service"
    );
    let (handle, epoch, retained_bytes, fingerprint) =
        observed.blocked.expect("actual blocked socket");
    assert_eq!(
        observed.admitted_identity,
        Some((handle, epoch.wrapping_add(1)))
    );
    assert_eq!(retained_bytes, 4096);
    assert_eq!(
        fingerprint,
        (0..4096).fold(0u64, |hash, _| hash.wrapping_mul(257).wrapping_add(128))
    );
    assert_eq!(
        observed.queued, 4096,
        "second window must stay unacknowledged under W pressure"
    );
    assert_eq!(observed.released_progress, Some(true));
    assert_eq!(observed.dropped_after_release, 1);
    assert!(!cleanup.timed_out, "fixture cleanup must not time out");
    assert_eq!(cleanup.entered, cleanup.dropped);
    assert_eq!(cleanup.state, Some(SessionDeviceState::DeviceFailedSettled));
}

#[tokio::test(flavor = "current_thread")]
async fn open_stop_joins_all_workers_after_observer_cancellation_without_gate_release() {
    let (mut session, mut apps, upstream, _downlink) = fixture();
    let captured = capture(async {
        let setup = ready(&mut session, &mut apps, &upstream).await;
        let before = inventory(&session.control, &upstream);
        let pending = {
            let mut observer = Box::pin(session.observe_shutdown());
            std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await
        };
        let cancelled = inventory(&session.control, &upstream);
        let sticky = session.control.stop.stopping.load(Ordering::Acquire);
        let deadline = session.control.ledger().shutdown_deadline;
        // No upstream gate has been released: only the original stop can wake workers.
        let state = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            session.observe_shutdown(),
        )
        .await
        .ok();
        let after = inventory(&session.control, &upstream);
        let same_deadline =
            deadline.is_some() && deadline == session.control.ledger().shutdown_deadline;
        (
            setup,
            before,
            pending,
            cancelled,
            sticky,
            state,
            after,
            same_deadline,
        )
    })
    .await;
    let cleanup = cleanup(&mut session, &upstream).await;
    eprintln!("stop before gate release={captured:?}; cleanup={cleanup:?}");
    let (setup, before, pending, cancelled, sticky, state, after, same_deadline) =
        captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(setup && pending && sticky && same_deadline);
    assert_eq!(
        before,
        OpenInventory {
            slots: 128,
            workers: 128,
            live: 128,
            announced: 0,
            dropped: 0
        }
    );
    assert_eq!(
        cancelled, before,
        "cancelled observer retains original worker custody"
    );
    assert_eq!(state, Some(SessionDeviceState::DeviceSettled));
    assert_eq!(
        after,
        OpenInventory {
            slots: 0,
            workers: 0,
            live: 0,
            announced: 0,
            dropped: 128
        }
    );
    assert!(!cleanup.timed_out);
    assert_eq!(cleanup.entered, cleanup.dropped);
    assert_eq!(cleanup.state, state);
}

#[tokio::test(flavor = "current_thread")]
async fn open_error_drop_panic_retains_completed_worker_until_notice_join_and_disposition() {
    let (mut session, mut apps, upstream, _downlink) = fixture();
    upstream.fail_with_drop_panic.store(true, Ordering::SeqCst);
    let captured = capture(async {
        let setup = ready(&mut session, &mut apps, &upstream).await;
        upstream.gates[0].notify_one();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let finished = session.control.opens().slots.iter().flatten()
                    .filter(|slot| matches!(&slot.phase, OpenPhase::Worker(worker) if worker.is_finished()))
                    .count();
                if finished == 1 { break; }
                tokio::task::yield_now().await;
            }
        }).await.is_ok();
        // The original loop has not consumed the notice, so task completion cannot free W.
        let unconsumed = inventory(&session.control, &upstream);
        let disposed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while session.control.opens().live == 128 {
                turn(&mut session, &mut apps, false).await;
            }
        })
        .await
        .is_ok();
        let after = inventory(&session.control, &upstream);
        let ledger = session.control.ledger();
        let bounded = ledger.first_failure.as_ref().is_some_and(|failure| {
            let facts = failure.facts();
            facts.class == crate::shared::errors::FailureClass::Panic
                && facts.origin.phase == FailurePhase::TcpOpen
                && facts.origin.resource == FailureResource::TcpOpen
                && facts.origin.attempt_id == Some(1)
                && facts.opaque
                && facts.source_content_omitted
                && failure.text_bytes().is_empty()
        }) && ledger.additional_failures == 0;
        (
            setup,
            completed,
            unconsumed,
            disposed,
            after,
            bounded,
            upstream.error_dropped.load(Ordering::SeqCst),
        )
    })
    .await;
    let cleanup = cleanup(&mut session, &upstream).await;
    eprintln!("error drop observation={captured:?}; cleanup={cleanup:?}");
    let (setup, completed, unconsumed, disposed, after, bounded, error_dropped) =
        captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(setup && completed && disposed && bounded);
    assert_eq!(
        unconsumed,
        OpenInventory {
            slots: 128,
            workers: 128,
            live: 128,
            announced: 0,
            dropped: 1
        }
    );
    assert_eq!(
        after,
        OpenInventory {
            slots: 127,
            workers: 127,
            live: 127,
            announced: 0,
            dropped: 1
        }
    );
    assert_eq!(error_dropped, 1);
    assert!(!cleanup.timed_out);
    assert_eq!(cleanup.entered, cleanup.dropped);
    assert_eq!(cleanup.state, Some(SessionDeviceState::DeviceFailedSettled));
}
