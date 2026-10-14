//! Original socket pressure, pending-byte inventories, close tails and wire progress.
//! Read-only observations never admit, consume, rearm or settle a resource.
//! Uses the parent's one session/two-stack fixture; entry is the original dispatch.

use super::*;
use crate::client_tun::{MAX_UPLINK_BUFFER, SocketCtx, SocketState};
use std::collections::HashMap;
use tokio::time::Instant;

#[derive(Clone, Debug)]
struct PendingSnapshot {
    handle: SocketHandle,
    epoch: u64,
    bytes: usize,
    queued: usize,
    app_port: Option<u16>,
}

#[derive(Clone, Debug, PartialEq)]
struct RecvObservation {
    buffer_before: usize,
    queued_before: usize,
    available: usize,
    limit: usize,
    consumed: usize,
    queued_after: usize,
}

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) struct PressureProbe {
    tcp_requested: bool,
    tcp_states: Option<[usize; 4]>,
    requested: bool,
    generation: usize,
    pending: Vec<PendingSnapshot>,
    selected: Option<(SocketHandle, u64)>,
    read_calls: usize,
    read: Option<RecvObservation>,
    full_calls: usize,
    full: Option<(usize, usize, Option<u8>)>,
}

impl PressureProbe {
    pub(super) fn request_tcp_states(&mut self) {
        self.tcp_requested = true;
        self.tcp_states = None;
    }
    pub(super) fn tcp_states(&self) -> Option<[usize; 4]> {
        self.tcp_states
    }
}

impl OpenDispatch {
    pub(in crate::client_tun) fn observe_pressure(
        &self,
        sockets: &SocketSet<'_>,
        contexts: &HashMap<SocketHandle, SocketCtx>,
    ) {
        let Some(owner) = &self.owner else { return };
        let mut opens = owner.opens();
        let probe = &mut opens.pressure;
        if std::mem::take(&mut probe.tcp_requested) {
            let mut states = [0; 4];
            for (_, socket) in sockets.iter() {
                let smoltcp::socket::Socket::Tcp(socket) = socket else {
                    continue;
                };
                let index = match socket.state() {
                    smoltcp::socket::tcp::State::Listen => 0,
                    smoltcp::socket::tcp::State::SynReceived => 1,
                    smoltcp::socket::tcp::State::Established => 2,
                    _ => 3,
                };
                states[index] += 1;
            }
            probe.tcp_states = Some(states);
            eprintln!(
                "F host after-app-established next-original-pass: listen/synreceived/established/other={states:?}"
            );
        }
        if !std::mem::take(&mut probe.requested) {
            return;
        }
        probe.pending = contexts
            .iter()
            .filter_map(|(handle, ctx)| {
                (ctx.state == SocketState::HandshakePending).then(|| PendingSnapshot {
                    handle: *handle,
                    epoch: ctx.conn_epoch,
                    bytes: ctx.uplink_buffer.len(),
                    queued: sockets.get::<Socket>(*handle).recv_queue(),
                    app_port: sockets
                        .get::<Socket>(*handle)
                        .remote_endpoint()
                        .map(|endpoint| endpoint.port),
                })
            })
            .collect();
        probe.generation += 1;
    }

    pub(in crate::client_tun::compatibility_runtime) fn observe_recv(
        &self,
        handle: SocketHandle,
        ctx: &SocketCtx,
        (queued_before, available, limit, consumed): (usize, usize, usize, usize),
    ) {
        let Some(owner) = &self.owner else { return };
        let mut opens = owner.opens();
        let probe = &mut opens.pressure;
        if probe.selected != Some((handle, ctx.conn_epoch)) {
            return;
        }
        probe.read_calls += 1;
        probe.read = Some(RecvObservation {
            buffer_before: ctx.uplink_buffer.len(),
            queued_before,
            available,
            limit,
            consumed,
            queued_after: usize::MAX,
        });
    }

    pub(in crate::client_tun::compatibility_runtime) fn observe_recv_after(
        &self,
        handle: SocketHandle,
        ctx: &SocketCtx,
        queued: usize,
    ) {
        let Some(owner) = &self.owner else { return };
        let mut opens = owner.opens();
        let probe = &mut opens.pressure;
        if probe.selected == Some((handle, ctx.conn_epoch)) {
            if let Some(read) = &mut probe.read {
                read.queued_after = queued;
            }
        }
    }

    pub(in crate::client_tun::compatibility_runtime) fn observe_full(
        &self,
        handle: SocketHandle,
        ctx: &SocketCtx,
        socket: &mut Socket<'_>,
    ) {
        let Some(owner) = &self.owner else { return };
        let mut opens = owner.opens();
        let probe = &mut opens.pressure;
        if probe.selected == Some((handle, ctx.conn_epoch)) {
            probe.full_calls += 1;
            probe.full = Some((
                ctx.uplink_buffer.len(),
                socket.recv_queue(),
                socket.peek(1).ok().and_then(|bytes| bytes.first().copied()),
            ));
        }
    }
}

async fn pressure_step<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
) -> (bool, bool) {
    let generation = {
        let mut opens = session.control.opens();
        opens.pressure.requested = true;
        opens.pressure.generation
    };
    apps.turn(false).await;
    let pending = {
        let mut observer = Box::pin(session.run());
        std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending())).await
    };
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(1)).await;
    (
        pending,
        session.control.opens().pressure.generation > generation,
    )
}

async fn pressure_until<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    steps: usize,
    mut check: impl FnMut(&SessionControl, &RealApps) -> bool,
) -> bool {
    for _ in 0..steps {
        let (pending, fresh) = pressure_step(session, apps).await;
        if !pending {
            return false;
        }
        if fresh && check(&session.control, apps) {
            return true;
        }
    }
    false
}

fn open_deadlines(control: &SessionControl) -> Vec<(SocketHandle, u64, Instant)> {
    control
        .opens()
        .slots
        .iter()
        .flatten()
        .map(|slot| (slot.handle, slot.epoch, slot.deadline))
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn cheap_open_keeps_w_custody_until_original_failure_consumption() {
    let (mut session, mut apps, upstream, _downlink) = super::fixture::fixture_with_cheap();
    let captured = capture(async {
        let setup = upstream.open_is_cheap() && ready(&mut session, &mut apps, &upstream).await;
        let before = inventory(&session.control, &upstream);
        let observer_cancelled = {
            let mut observer = Box::pin(session.run());
            let pending =
                std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx).is_pending()))
                    .await;
            drop(observer);
            pending
        };
        let after_observer_cancel = inventory(&session.control, &upstream);
        let mut first_blocked = None;
        let pressure_ready = setup
            && tokio::time::timeout(Duration::from_secs(5), async {
                while apps.sent[128] != 8192 || apps.queued(128) > 4096 {
                    turn(&mut session, &mut apps, true).await;
                }
                first_blocked = session.control.opens().blocked;
                for _ in 0..40 {
                    turn(&mut session, &mut apps, true).await;
                }
            })
            .await
            .is_ok();
        let blocked_before_release = session.control.opens().blocked;
        let queued_before_release = apps.queued(128);
        let window_before_release = apps.windows[128];
        upstream.gates[0].notify_one();
        let worker_finished = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if session.control.opens().slots.iter().flatten().any(
                    |slot| matches!(&slot.phase, OpenPhase::Worker(worker) if worker.is_finished()),
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let after_finished = inventory(&session.control, &upstream);
        let blocked_after_finished = session.control.opens().blocked;
        let queued_after_finished = apps.queued(128);
        let reentered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let first_failure = session.control.ledger().first_failure.is_some();
                let installed = first_blocked.is_some_and(|(handle, epoch, _, _)| {
                    session.control.opens().slots.iter().flatten().any(|slot| {
                        slot.handle == handle
                            && slot.epoch == epoch.wrapping_add(1)
                            && matches!(&slot.phase, OpenPhase::Worker(_))
                    })
                });
                if first_failure
                    && installed
                    && upstream.entered.load(Ordering::SeqCst) == 129
                    && apps.queued(128) == 0
                {
                    break;
                }
                turn(&mut session, &mut apps, true).await;
            }
        })
        .await
        .is_ok();
        session.request_stop();
        let stop_state = tokio::time::timeout(Duration::from_secs(5), session.observe_shutdown())
            .await
            .ok();
        let after_stop = inventory(&session.control, &upstream);
        (
            setup,
            before,
            observer_cancelled,
            after_observer_cancel,
            pressure_ready,
            first_blocked,
            blocked_before_release,
            queued_before_release,
            window_before_release,
            worker_finished,
            after_finished,
            blocked_after_finished,
            queued_after_finished,
            reentered,
            stop_state,
            after_stop,
        )
    })
    .await;
    let cleanup = cleanup(&mut session, &upstream).await;
    let (
        setup,
        before,
        observer_cancelled,
        after_observer_cancel,
        pressure_ready,
        first_blocked,
        blocked_before_release,
        queued_before_release,
        window_before_release,
        worker_finished,
        after_finished,
        blocked_after_finished,
        queued_after_finished,
        reentered,
        stop_state,
        after_stop,
    ) = captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    let expected_blocked = Some((
        apps.handles[128],
        0,
        4096,
        (0..4096).fold(0u64, |hash, _| hash.wrapping_mul(257).wrapping_add(128)),
    ));
    let full = OpenInventory {
        slots: 128,
        workers: 128,
        live: 128,
        announced: 0,
        dropped: 0,
    };
    assert!(
        setup,
        "cheap fixture must still use the saved session owner"
    );
    assert_eq!(before, full);
    assert!(observer_cancelled);
    assert_eq!(after_observer_cancel, full);
    assert!(pressure_ready, "the real last peer must reach W pressure");
    assert_eq!(first_blocked, expected_blocked);
    assert_eq!(blocked_before_release, expected_blocked);
    assert_eq!(queued_before_release, 4096);
    assert_eq!(window_before_release, Some(0));
    assert!(worker_finished);
    assert_eq!(
        after_finished,
        OpenInventory { dropped: 1, ..full },
        "a finished worker remains in W before the original loop consumes it"
    );
    assert_eq!(blocked_after_finished, expected_blocked);
    assert_eq!(queued_after_finished, 4096);
    assert!(reentered);
    assert_eq!(stop_state, Some(SessionDeviceState::DeviceFailedSettled));
    assert_eq!(
        after_stop,
        OpenInventory {
            slots: 0,
            workers: 0,
            live: 0,
            announced: 0,
            dropped: 129,
        }
    );
    assert!(!cleanup.timed_out);
    assert_eq!(cleanup.entered, cleanup.dropped);
    assert_eq!(cleanup.entered, 129);
    assert_eq!(cleanup.state, stop_state);
}

#[tokio::test(flavor = "current_thread")]
async fn open_pending_byte_caps_retain_original_rx_and_append_keeps_absolute_deadline() {
    let (mut session, mut apps, upstream, _downlink) = fixture_with_rx(4097);
    apps.send_batch = Some(4096);
    let mut paused = false;
    let captured = capture(async {
        assert!(
            ready(&mut session, &mut apps, &upstream).await,
            "real W128 setup"
        );
        let original = open_deadlines(&session.control);
        let &(handle, epoch, deadline) = original.iter().min_by_key(|entry| entry.2).unwrap();
        tokio::time::pause();
        paused = true;
        assert!(
            pressure_until(&mut session, &mut apps, 20, |control, _| {
                control.opens().pressure.pending.len() == 128
            })
            .await,
            "fresh original pending snapshot"
        );
        let index = session
            .control
            .opens()
            .pressure
            .pending
            .iter()
            .find(|entry| (entry.handle, entry.epoch) == (handle, epoch))
            .and_then(|entry| entry.app_port)
            .map(|port| usize::from(port - 40_000))
            .unwrap();
        apps.targets[..128].fill(MAX_UPLINK_BUFFER);
        apps.targets[index] -= 1;
        let fill_started = Instant::now();
        let filled = pressure_until(&mut session, &mut apps, 2000, |control, apps| {
            let opens = control.opens();
            let snapshot = &opens.pressure.pending;
            snapshot.len() == 128
                && snapshot.iter().all(|entry| {
                    entry.bytes == MAX_UPLINK_BUFFER - usize::from(entry.handle == handle)
                        && entry.queued == 0
                })
                && (0..128)
                    .all(|index| apps.sent[index] == apps.targets[index] && apps.queued(index) == 0)
        })
        .await;
        let sum_before: usize = session
            .control
            .opens()
            .pressure
            .pending
            .iter()
            .map(|entry| entry.bytes)
            .sum();
        eprintln!(
            "byte fill ready={filled} sum={sum_before} elapsed={:?}",
            Instant::now() - fill_started
        );
        assert!(filled, "32MiB minus one must reach original pending queues");
        assert!(Instant::now() - fill_started <= Duration::from_secs(2));
        assert!(Instant::now() < deadline - Duration::from_secs(1));
        tokio::time::advance(deadline - Duration::from_secs(1) - Instant::now()).await;
        session.control.opens().pressure.selected = Some((handle, epoch));
        let sent_before = apps.sent[index];
        let sent = apps
            .sockets
            .get_mut::<Socket>(apps.handles[index])
            .send_slice(&[0xA5, 0x5A])
            .unwrap();
        apps.sent[index] += sent;
        let clipped = pressure_until(&mut session, &mut apps, 200, |control, _| {
            let opens = control.opens();
            opens.pressure.read.is_some()
                && opens.pressure.full.is_some()
                && opens
                    .pressure
                    .pending
                    .iter()
                    .map(|entry| entry.bytes)
                    .sum::<usize>()
                    == 128 * MAX_UPLINK_BUFFER
        })
        .await;
        let (read, full_before, full_calls, generation) = {
            let opens = session.control.opens();
            (
                opens.pressure.read.clone(),
                opens.pressure.full,
                opens.pressure.full_calls,
                opens.pressure.generation,
            )
        };
        let mut continued = true;
        for _ in 0..40 {
            continued &= pressure_step(&mut session, &mut apps).await.0;
        }
        let (snapshot, full_after, repeated, read_calls, fresh) = {
            let opens = session.control.opens();
            (
                opens.pressure.pending.clone(),
                opens.pressure.full,
                opens.pressure.full_calls > full_calls,
                opens.pressure.read_calls,
                opens.pressure.generation > generation,
            )
        };
        let identities = snapshot.len() == 128
            && snapshot.iter().all(|entry| {
                original
                    .iter()
                    .any(|slot| (slot.0, slot.1) == (entry.handle, entry.epoch))
            });
        let deadlines_unchanged = original == open_deadlines(&session.control);
        tokio::time::advance(deadline - Duration::from_millis(1) - Instant::now()).await;
        let worker_finished = |control: &SessionControl| {
            control
                .opens()
                .slots
                .iter()
                .flatten()
                .find(|slot| (slot.handle, slot.epoch) == (handle, epoch))
                .is_some_and(
                    |slot| matches!(&slot.phase, OpenPhase::Worker(worker) if worker.is_finished()),
                )
        };
        let before_deadline = !worker_finished(&session.control);
        tokio::time::advance(Duration::from_millis(2)).await;
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        let expired = worker_finished(&session.control);
        let held = inventory(&session.control, &upstream);
        tokio::time::resume();
        paused = false;
        session.request_stop();
        let state = tokio::time::timeout(Duration::from_secs(5), session.observe_shutdown())
            .await
            .ok();
        let after = inventory(&session.control, &upstream);
        let timed_out = session
            .control
            .ledger()
            .first_failure
            .as_ref()
            .is_some_and(|failure| failure.facts().io_kind == Some(std::io::ErrorKind::TimedOut));
        (
            (sent_before, sent, sum_before, clipped),
            read,
            (full_before, full_after, repeated, read_calls, fresh),
            snapshot,
            (
                continued,
                identities,
                deadlines_unchanged,
                before_deadline,
                expired,
                timed_out,
            ),
            held,
            state,
            after,
        )
    })
    .await;
    if paused {
        tokio::time::resume();
    }
    let cleanup = cleanup(&mut session, &upstream).await;
    eprintln!("byte pressure observation={captured:?}; cleanup={cleanup:?}");
    let (
        (sent_before, sent, sum_before, clipped),
        read,
        (full_before, full_after, repeated, read_calls, fresh),
        snapshot,
        (continued, identities, unchanged, before_deadline, expired, timed_out),
        held,
        state,
        after,
    ) = captured.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(
        clipped && continued && identities && unchanged && before_deadline && expired && timed_out
    );
    assert_eq!(
        (sent_before, sent, sum_before),
        (MAX_UPLINK_BUFFER - 1, 2, 128 * MAX_UPLINK_BUFFER - 1)
    );
    assert_eq!(
        read,
        Some(RecvObservation {
            buffer_before: MAX_UPLINK_BUFFER - 1,
            queued_before: 2,
            available: 2,
            limit: 1,
            consumed: 1,
            queued_after: 1
        })
    );
    assert_eq!(full_before, Some((MAX_UPLINK_BUFFER, 1, Some(0x5A))));
    assert_eq!(full_after, full_before);
    assert!(repeated && fresh);
    assert_eq!(read_calls, 1);
    assert_eq!(
        snapshot.iter().map(|entry| entry.bytes).sum::<usize>(),
        32 * 1024 * 1024
    );
    assert!(
        snapshot
            .iter()
            .all(|entry| entry.bytes == MAX_UPLINK_BUFFER)
    );
    assert_eq!(
        (held.slots, held.workers, held.live, held.announced),
        (128, 128, 128, 0)
    );
    assert!(held.dropped > 0);
    assert_eq!(state, Some(SessionDeviceState::DeviceFailedSettled));
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
