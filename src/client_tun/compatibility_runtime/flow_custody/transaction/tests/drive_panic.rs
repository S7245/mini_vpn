//! Real AUTH/window4 → original CONNECT step → one original Box unwind.
//! Owns only the fault decorator and passive bounded diagnostics. The original
//! attempt/lease/escrow remain in the root; forced Drop is never a clean receipt.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::receiving_pair;
use super::super::*;
use super::partial_connect::ReadyPeer;
use crate::client_tun::{DownlinkBackpressureConfig, HandshakeCompletion};
use crate::fake_ip::FakeIpPool;
use crate::owned_upstream::{LegacyOwnedUpstream, UdpUplink};
use crate::shared::errors::FailureClass;
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::TuicUpstream;
use crate::upstream::{StagedTcpOpen, TcpOpenView};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::task::Context;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

struct OriginalPanic {
    tag: u64,
    drops: Arc<AtomicUsize>,
}
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultSite {
    Step,
    View,
    Cancel,
    Settle,
}
struct Facts {
    site: FaultSite,
    step_returns: AtomicUsize,
    cancelled: Mutex<Option<(TcpOpenCancel, Instant)>>,
    settle_call: Mutex<Option<(Instant, Instant)>>,
    payload: Mutex<Option<Box<OriginalPanic>>>,
    fired: AtomicBool,
    // factory, step, view, cancel, carrier, settle, failure; no event history.
    calls: [AtomicUsize; 7],
    reentries: [AtomicUsize; 7],
    inner: AtomicUsize,
    inner_dropped: AtomicUsize,
    last_view: Mutex<Option<TcpOpenView>>,
}
impl Facts {
    fn enter(&self, index: usize) {
        self.calls[index].fetch_add(1, Ordering::AcqRel);
        if self.fired.load(Ordering::Acquire) {
            self.reentries[index].fetch_add(1, Ordering::AcqRel);
            panic!("poisoned original provider callback reentered: {index}");
        }
    }
    fn unwind_at(&self, site: FaultSite, view: TcpOpenView) {
        let phase = if site == FaultSite::Settle {
            TcpOpenPhase::Retiring
        } else {
            TcpOpenPhase::Connecting
        };
        if self.site == site
            && view.phase == phase
            && view.connect_written == 4
            && view.lease_held
            && view.retirement_pending
        {
            let payload = self.payload.lock().unwrap().take();
            let payload = payload.expect("one original non-ZST fault Box");
            assert!(!self.fired.swap(true, Ordering::AcqRel));
            std::panic::panic_any(payload);
        }
    }
    fn counts(&self) -> ([usize; 7], [usize; 7]) {
        (
            std::array::from_fn(|i| self.calls[i].load(Ordering::Acquire)),
            std::array::from_fn(|i| self.reentries[i].load(Ordering::Acquire)),
        )
    }
}
struct FaultUpstream {
    inner: Arc<LegacyOwnedUpstream<TuicUpstream>>,
    facts: Arc<Facts>,
}
#[async_trait::async_trait]
impl OwnedUpstream for FaultUpstream {
    fn staged_tcp_open(self: Arc<Self>, request: TcpOpenRequest) -> StagedTcpOpen<OpenedOwnedTcp> {
        self.facts.enter(0);
        let inner = self.inner.clone().staged_tcp_open(request)?;
        self.facts.inner.store(
            &*inner as *const dyn TcpOpenAttempt<OpenedOwnedTcp> as *const () as usize,
            Ordering::Release,
        );
        Ok(Box::new(FaultAttempt {
            inner: Some(inner),
            facts: self.facts.clone(),
        }))
    }
    async fn open_tcp_flow(&self, _: &TargetAddr) -> Result<OpenedOwnedTcp, ClientError> {
        panic!("staged drive tracer cannot enter async-open fallback");
    }
    async fn send_udp(&self, _: UdpUplink<'_>) {
        panic!("staged drive tracer has no UDP path");
    }
}
struct FaultAttempt {
    inner: Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>,
    facts: Arc<Facts>,
}
impl Drop for FaultAttempt {
    fn drop(&mut self) {
        // Count only after the actual original Box destructor has returned.
        drop(self.inner.take());
        self.facts.inner_dropped.fetch_add(1, Ordering::AcqRel);
    }
}
impl TcpOpenAttempt<OpenedOwnedTcp> for FaultAttempt {
    fn step(&mut self, cx: &mut Context<'_>, now: Instant) {
        self.facts.enter(1);
        let inner = self.inner.as_mut().unwrap();
        inner.step(cx, now);
        let view = inner.view();
        *self.facts.last_view.lock().unwrap() = Some(view);
        self.facts.unwind_at(FaultSite::Step, view);
        self.facts.step_returns.fetch_add(1, Ordering::AcqRel);
    }
    fn view(&self) -> TcpOpenView {
        self.facts.enter(2);
        let view = self.inner.as_ref().unwrap().view();
        self.facts.unwind_at(FaultSite::View, view);
        view
    }
    fn cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        self.facts.enter(3);
        let inner = self.inner.as_mut().unwrap();
        inner.cancel(reason, deadline);
        let view = inner.view();
        *self.facts.last_view.lock().unwrap() = Some(view);
        *self.facts.cancelled.lock().unwrap() = Some((reason, deadline));
        self.facts.unwind_at(FaultSite::Cancel, view);
    }
    fn take_carrier(&mut self, now: Instant) -> Option<OpenedOwnedTcp> {
        self.facts.enter(4);
        self.inner.as_mut().unwrap().take_carrier(now)
    }
    fn settle(&mut self, cx: &mut Context<'_>, now: Instant, deadline: Instant) {
        self.facts.enter(5);
        let inner = self.inner.as_mut().unwrap();
        inner.settle(cx, now, deadline);
        if self.facts.site == FaultSite::Settle {
            let view = inner.view();
            *self.facts.last_view.lock().unwrap() = Some(view);
            *self.facts.settle_call.lock().unwrap() = Some((now, deadline));
            self.facts.unwind_at(FaultSite::Settle, view);
        }
    }
    fn failure(&self) -> Option<&ClientError> {
        self.facts.enter(6);
        self.inner.as_ref().unwrap().failure()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Snapshot {
    id: FlowIdentity,
    slot_deadline: tokio::time::Instant,
    open_deadline: Instant,
    stop_deadline: Option<Instant>,
    stage: FlowStage,
    storage: (usize, usize, usize),
    escrow: Vec<u8>,
    held: (bool, bool, usize),
    attempt: Option<usize>,
    installed: (bool, bool, bool),
    producer: (bool, bool),
    payload: Option<(usize, u64)>,
}
fn snapshot(owner: &SessionControl, id: FlowIdentity) -> Option<Snapshot> {
    let opens = owner.opens();
    let slot = opens.slots.get(id.index)?.as_ref()?;
    let OpenPhase::Flow(flow) = &slot.phase else {
        return None;
    };
    Some(Snapshot {
        id: flow.id,
        slot_deadline: slot.deadline,
        open_deadline: flow.open_deadline,
        stop_deadline: *flow.stop_deadline,
        stage: flow.stage,
        storage: (
            flow.escrow.len(),
            flow.escrow.as_ptr() as usize,
            flow.escrow.capacity(),
        ),
        escrow: flow.escrow.to_vec(),
        held: (flow.w_held, flow.f_held, flow.reserved),
        attempt: flow.attempt.as_ref().map(|value| {
            &**value as *const dyn TcpOpenAttempt<OpenedOwnedTcp> as *const () as usize
        }),
        installed: (flow.carrier.is_some(), flow.roles.is_some(), flow.published),
        producer: (flow.producer_stopped, flow.provider_cancelled),
        payload: flow
            .panic
            .as_ref()
            .and_then(|value| value.downcast_ref::<Box<OriginalPanic>>())
            .map(|value| (&**value as *const OriginalPanic as usize, value.tag)),
    })
}
#[derive(Clone, Copy, Debug, PartialEq)]
struct Census {
    live: usize,
    w: usize,
    f: usize,
    reserved: usize,
    escrow_bytes: usize,
    factory_started: u64,
    published: u64,
    released: u64,
    joined: u64,
    audit: bool,
}
fn census(owner: &SessionControl) -> Option<Census> {
    let opens = owner.opens();
    let accounting = opens.flow.as_ref()?;
    Some(Census {
        live: opens.live,
        w: accounting.w,
        f: accounting.f,
        reserved: accounting.bytes,
        escrow_bytes: accounting.escrow_bytes,
        factory_started: accounting.factory_started,
        published: accounting.published,
        released: accounting.released,
        joined: accounting.joined,
        audit: opens.audit_flows(),
    })
}
fn first(owner: &SessionControl) -> Option<(usize, String, bool, u64, Option<u64>, Vec<u8>)> {
    let ledger = owner.ledger();
    ledger.first_failure.as_ref().map(|value| {
        let facts = value.facts();
        (
            value as *const _ as usize,
            format!("{facts:?}"),
            facts.class == FailureClass::Panic
                && facts.origin.phase == FailurePhase::TcpOpen
                && facts.origin.resource == FailureResource::TcpOpen,
            facts.origin.resource_id,
            facts.origin.attempt_id,
            value.text_bytes().to_vec(),
        )
    })
}
async fn poll_once(
    dispatch: &OpenDispatch,
    receiver: &mut mpsc::Receiver<HandshakeNotice>,
) -> Poll<Option<OpenEvent>> {
    dispatch.begin_turn().await;
    std::future::poll_fn(|cx| {
        let mut next = std::pin::pin!(dispatch.recv(receiver));
        Poll::Ready(next.as_mut().poll(cx))
    })
    .await
}
async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

#[tokio::test]
async fn actual_connect_four_byte_lease_drive_panic_retains_original_box_and_charged_root() {
    run(false, FaultSite::Step).await;
}

#[tokio::test]
async fn actual_prior_io_survives_connect_four_byte_lease_drive_panic_with_one_additional() {
    run(true, FaultSite::Step).await;
}

#[tokio::test]
async fn actual_connect_four_byte_lease_view_panic_after_step_return_retains_charged_root() {
    run(false, FaultSite::View).await;
}

#[tokio::test]
async fn actual_stopped_producer_cancel_panic_keeps_partial_connect_and_original_limits_charged() {
    run(false, FaultSite::Cancel).await;
}

#[tokio::test]
async fn actual_cancel_return_then_settle_panic_retains_real_retirement_and_charged_limits() {
    run(false, FaultSite::Settle).await;
}

async fn run(prior_io: bool, site: FaultSite) {
    let peer = ReadyPeer::new().await;
    let result = capture(exercise(&peer, prior_io, site)).await;
    let cleanup = capture(peer.close()).await;
    if let Err(panic) = result {
        if cleanup.is_err() {
            eprintln!("B54 cleanup also failed; preserve original scenario failure first");
        }
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
async fn exercise(peer: &ReadyPeer, prior_io: bool, site: FaultSite) {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let dispatch = dispatch.with_relay_policy(Arc::new(RelayFamilyPolicy::from_sources(
        crate::upstream::RelayFamilyPolicySources {
            ordered_egress_permit: Some("1"),
            ..Default::default()
        },
    )));
    let drops = Arc::new(AtomicUsize::new(0));
    let payload = Box::new(OriginalPanic {
        tag: 54,
        drops: drops.clone(),
    });
    let identity = &*payload as *const OriginalPanic as usize;
    let facts = Arc::new(Facts {
        site,
        step_returns: AtomicUsize::new(0),
        cancelled: Mutex::new(None),
        settle_call: Mutex::new(None),
        payload: Mutex::new(Some(payload)),
        fired: AtomicBool::new(false),
        calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        reentries: std::array::from_fn(|_| AtomicUsize::new(0)),
        inner: AtomicUsize::new(0),
        inner_dropped: AtomicUsize::new(0),
        last_view: Mutex::new(None),
    });
    let upstream = Arc::new(FaultUpstream {
        inner: Arc::new(LegacyOwnedUpstream::new(peer.upstream.clone())),
        facts: facts.clone(),
    });
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    let mut pool = FakeIpPool::new();
    dispatch.begin_turn().await;
    dispatch.admit_flow(
        upstream.clone(),
        FlowAdmission {
            handle,
            socket: server.sockets.get_mut::<TcpSocket>(handle),
            ctx: contexts.get_mut(&handle).unwrap(),
            fake_pool: &mut pool,
            fake_ip: None,
            target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
            now: 0,
        },
    );
    let id = {
        let opens = owner.opens();
        opens
            .slots
            .iter()
            .flatten()
            .find_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) => Some(flow.id),
                _ => None,
            })
    }
    .expect("actual admitted identity");
    let before = snapshot(&owner, id).expect("original admitted row");
    assert_eq!(before.stage, FlowStage::Waiting);
    assert_eq!(before.held, (true, false, MAX_UPLINK_BUFFER));
    assert_eq!(before.escrow, b"untouched-rx");
    assert!(before.attempt.is_none() && before.payload.is_none());
    let prior_first = if prior_io {
        let (mut writer, peer) = tokio::io::duplex(1);
        drop(peer);
        let error = writer.write_all(b"x").await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        owner.record(RetainedFailure::from_io(
            open_origin(id.index, id.epoch, FailurePhase::TcpOpen),
            error,
        ));
        let value = first(&owner).expect("actual prior IO retained by original owner");
        let (additional, kind, root_limit) = {
            let ledger = owner.ledger();
            (
                ledger.additional_failures,
                ledger
                    .first_failure
                    .as_ref()
                    .and_then(|value| value.facts().io_kind),
                ledger.shutdown_deadline,
            )
        };
        assert_eq!(
            (additional, kind, root_limit),
            (0, Some(std::io::ErrorKind::BrokenPipe), None)
        );
        assert!(!value.2);
        println!("B55 actualpriorIO beforestep first={value:?} additional={additional}");
        Some(value)
    } else {
        None
    };
    let mut early_stop = None;
    let mut prefetched_halves = None;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = poll_once(&dispatch, &mut receiver).await;
            if facts.fired.load(Ordering::Acquire) {
                if matches!(site, FaultSite::Cancel | FaultSite::Settle) {
                    // The actual Closing event already disconnected this producer.
                    assert!(!matches!(event, Poll::Ready(Some(OpenEvent::Flow(_)))));
                } else {
                    assert!(
                        matches!(event, Poll::Ready(Some(OpenEvent::Flow(actual))) if actual == id)
                    );
                }
                break;
            }
            if matches!(site, FaultSite::Cancel | FaultSite::Settle) {
                let passive = *facts.last_view.lock().unwrap();
                if early_stop.is_none()
                    && passive.is_some_and(|v| v.connect_written == 4 && v.lease_held)
                {
                    if site == FaultSite::Settle {
                        // Receive before actual reset; never poll the original step again.
                        prefetched_halves = Some(peer.prefix().await);
                    }
                    let requested = tokio::time::Instant::now();
                    early_stop = Some((requested, owner.request_stop()));
                }
                if let Poll::Ready(Some(OpenEvent::Flow(actual))) = event {
                    assert_eq!(actual, id);
                    let closing = snapshot(&owner, id).unwrap();
                    assert_eq!(closing.stage, FlowStage::Closing);
                    assert_eq!(closing.producer, (false, false));
                    let (global_tx, _global_rx) = crate::client_tun::relay_channel::channel(4);
                    let (owned_tx, _owned_rx) = mpsc::channel(4);
                    HandshakeCompletion::from_loop(
                        &dispatch,
                        OpenEvent::Flow(actual),
                        HandshakeCompletion {
                            sockets: &mut server.sockets,
                            socket_ctxs: &mut contexts,
                            global_tx: &global_tx,
                            owned_action_tx: &owned_tx,
                            fake_pool: &mut pool,
                            now_secs: 0,
                            metrics_handle: &crate::metrics::Metrics::default(),
                            downlink_backpressure: DownlinkBackpressureConfig::default(),
                            relay_read_hard_pause: false,
                        },
                    );
                    let disconnected = snapshot(&owner, id).unwrap();
                    assert_eq!(disconnected.producer, (true, false));
                    assert_eq!(disconnected.stop_deadline, closing.stop_deadline);
                    assert!(contexts[&handle].uplink_tx.is_none());
                    assert!(contexts[&handle].relay_read_credit_tx.is_none());
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original step must reach actual partial CONNECT");
    let after = snapshot(&owner, id).expect("original poisoned attempt retained");
    let last_view = *facts.last_view.lock().unwrap();
    let counts = facts.counts();
    let step_returns = facts.step_returns.load(Ordering::Acquire);
    if site == FaultSite::View {
        assert_eq!(
            step_returns, counts.0[1],
            "actual step returned before view unwind"
        );
    }
    let inner = facts.inner.load(Ordering::Acquire);
    let original_first = first(&owner);
    let observed = census(&owner).unwrap();
    let quarantined = owner.ledger().quarantined();
    println!(
        "B56 realdrive site={site:?} stepReturns={step_returns} priorIO={prior_io} before={before:?} after={after:?} originalBox={identity} inner={inner} view={last_view:?} counts={counts:?} drops={} innerDropped={} ownerQ={quarantined} census={observed:?} first={original_first:?}",
        drops.load(Ordering::Acquire),
        facts.inner_dropped.load(Ordering::Acquire)
    );
    assert_eq!(after.stage, FlowStage::Quarantined);
    assert_eq!(
        (after.id, after.slot_deadline, after.open_deadline),
        (before.id, before.slot_deadline, before.open_deadline)
    );
    if matches!(site, FaultSite::Cancel | FaultSite::Settle) {
        assert!(after.stop_deadline.is_some());
        assert_eq!(after.producer, (true, site == FaultSite::Settle));
        let cancelled = *facts.cancelled.lock().unwrap();
        assert_eq!(
            cancelled,
            // D152 (A1): the provider bound is the tail plus one loss bound.
            Some((
                TcpOpenCancel::Cancelled,
                after.stop_deadline.unwrap() + PROVIDER_LOSS_BOUND
            ))
        );
        assert_eq!(
            (counts.0[3], counts.0[4], counts.0[5]),
            (1, 0, usize::from(site == FaultSite::Settle))
        );
        assert_eq!(step_returns, counts.0[1]);
        if site == FaultSite::Settle {
            let call = *facts.settle_call.lock().unwrap();
            let (now, limit) = call.expect("actual inner settle returned before Box unwind");
            // D152 (A1): the provider bound is the tail plus one loss bound.
            assert_eq!(
                Some(limit),
                after.stop_deadline.map(|d| d + PROVIDER_LOSS_BOUND)
            );
            assert!(now < limit);
            println!("B58 actual settle returned now={now:?} originalFlowLimit={limit:?}");
        }
    } else {
        assert_eq!(after.stop_deadline, None);
    }
    assert_eq!(after.storage, before.storage);
    assert_eq!(after.escrow, before.escrow);
    assert_eq!(after.held, (true, true, MAX_UPLINK_BUFFER));
    assert!(after.attempt.is_some() && inner != 0);
    assert_eq!(after.installed, (false, false, false));
    assert_eq!(after.payload, Some((identity, 54)));
    let view = last_view.expect("passive copy of actual inner view after actual step");
    assert_eq!(
        view.phase,
        if site == FaultSite::Settle {
            TcpOpenPhase::Retiring
        } else {
            TcpOpenPhase::Connecting
        }
    );
    assert_eq!(view.connect_written, 4);
    assert!(view.connect_bytes > 4 && view.lease_held && view.retirement_pending);
    assert_eq!(counts.0[0], 1);
    assert!(counts.0[1] > 0);
    assert_eq!(counts.1, [0; 7]);
    assert_eq!(
        (
            drops.load(Ordering::Acquire),
            facts.inner_dropped.load(Ordering::Acquire)
        ),
        (0, 0)
    );
    assert!(quarantined && observed.audit);
    assert_eq!(
        (
            observed.live,
            observed.w,
            observed.f,
            observed.factory_started,
            observed.published,
            observed.released,
            observed.joined
        ),
        (1, 1, 1, 1, 0, 0, 0)
    );
    let original_first = original_first.expect("drive catch must record original typed Flow Panic");
    if let Some(prior) = prior_first {
        assert_eq!(
            original_first, prior,
            "original IO VALUE/facts/text must survive drive panic"
        );
    } else {
        assert_eq!(
            (original_first.2, original_first.3, original_first.4),
            (true, id.index as u64, Some(id.epoch))
        );
        assert!(original_first.5.is_empty());
    }
    let initial_ledger = {
        let ledger = owner.ledger();
        (ledger.additional_failures, ledger.shutdown_deadline)
    };
    // D216: a drive/provider panic is Session scope: bounded deadline (parent min).
    assert_eq!(initial_ledger.0, u64::from(prior_io));
    assert!(initial_ledger.1.is_some_and(|deadline| early_stop.is_none_or(|(_, limit)| deadline <= limit)));
    assert!(owner.ledger().session_quarantined());
    let halves = match prefetched_halves {
        Some(halves) => halves,
        None => peer.prefix().await,
    };
    peer.assert_current(1);
    println!(
        "B54 actual peer received original CONNECT prefix=4; AUTH/window4/current connection retained; passive leaseheld=true retirement=true"
    );
    if !matches!(site, FaultSite::Cancel | FaultSite::Settle) {
        let (global_tx, _global_rx) = crate::client_tun::relay_channel::channel(4);
        let (owned_tx, _owned_rx) = mpsc::channel(4);
        HandshakeCompletion::from_loop(
            &dispatch,
            OpenEvent::Flow(id),
            HandshakeCompletion {
                sockets: &mut server.sockets,
                socket_ctxs: &mut contexts,
                global_tx: &global_tx,
                owned_action_tx: &owned_tx,
                fake_pool: &mut pool,
                now_secs: 0,
                metrics_handle: &crate::metrics::Metrics::default(),
                downlink_backpressure: DownlinkBackpressureConfig::default(),
                relay_read_hard_pause: false,
            },
        );
    }
    // D219: the panic's Session deadline is the earliest stop deadline; a
    // later test stop keeps it (never extends it).
    let session_deadline = initial_ledger.1.expect("Session-scope panic fixed a deadline");
    let deadline = match early_stop {
        Some((requested_at, deadline)) => {
            assert!(deadline >= requested_at + Duration::from_secs(15));
            assert_eq!(session_deadline, deadline);
            deadline
        }
        None => {
            let requested_at = tokio::time::Instant::now();
            let deadline = owner.request_stop();
            assert_eq!(deadline, session_deadline);
            assert!(deadline <= requested_at + Duration::from_secs(15));
            deadline
        }
    };
    assert!(deadline <= tokio::time::Instant::now() + Duration::from_secs(15));
    let mut stopped = None;
    // The original cursor scans 32 rows per turn; never set it or close the row
    // from the test. Freeze only the actual first stopping visit to this row.
    for _ in 0..FLOW_SLOT_LIMIT.div_ceil(32) + 2 {
        let _ = poll_once(&dispatch, &mut receiver).await;
        owner.settle_opens().await;
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert_eq!(owner.request_stop(), deadline);
        assert_eq!(first(&owner), Some(original_first.clone()));
        assert_eq!(facts.counts(), counts);
        let row = snapshot(&owner, id).expect("original Q row during stopping scan");
        assert_eq!(row.payload, after.payload);
        assert_eq!(row.attempt, after.attempt);
        assert_eq!(row.storage, after.storage);
        assert_eq!(row.held, after.held);
        if row.stop_deadline.is_some() {
            stopped = Some(row);
            break;
        }
    }
    let stopped = stopped.expect("bounded original stopping scan must visit the actual row");
    assert!(stopped.stop_deadline.unwrap() >= deadline.into_std());
    for _ in 0..4 {
        let _ = poll_once(&dispatch, &mut receiver).await;
        owner.settle_opens().await;
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert_eq!(owner.request_stop(), deadline);
        assert_eq!(first(&owner), Some(original_first.clone()));
        assert_eq!(facts.counts(), counts);
        assert_eq!(snapshot(&owner, id), Some(stopped.clone()));
    }
    // Real runtime/QUIC remain alive: wait at the original deadline, never reset it.
    tokio::time::sleep_until(deadline).await;
    let _ = poll_once(&dispatch, &mut receiver).await;
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    assert_eq!(owner.request_stop(), deadline);
    assert_eq!(
        owner.state(),
        crate::client_tun::compatibility_runtime::SessionDeviceState::Incomplete
    );
    let final_row = snapshot(&owner, id).expect("original charged row after original15");
    let final_census = census(&owner).unwrap();
    assert_eq!(final_row, stopped);
    assert_eq!(first(&owner), Some(original_first));
    let final_additional = owner.ledger().additional_failures;
    assert_eq!(final_additional, u64::from(prior_io));
    assert_eq!(facts.counts(), counts);
    assert_eq!(facts.inner.load(Ordering::Acquire), inner);
    assert_eq!(facts.step_returns.load(Ordering::Acquire), step_returns);
    let final_view = *facts.last_view.lock().unwrap();
    assert_eq!(final_view, Some(view));
    assert_eq!(
        (
            drops.load(Ordering::Acquire),
            facts.inner_dropped.load(Ordering::Acquire)
        ),
        (0, 0)
    );
    assert_eq!(
        (
            final_census.live,
            final_census.w,
            final_census.f,
            final_census.reserved,
            final_census.escrow_bytes,
            final_census.published,
            final_census.released,
            final_census.joined
        ),
        (1, 1, 1, MAX_UPLINK_BUFFER, 12, 0, 0, 0)
    );
    println!(
        "B56 original15 expired site={site:?} stepReturns={step_returns} priorIO={prior_io} additional={final_additional} deadline={deadline:?}; row={final_row:?} census={final_census:?} counts={counts:?}; originalBox/inner undropped; no receipt/release/join/clean"
    );
    drop(dispatch);
    drop(owner);
    assert_eq!(
        (
            drops.load(Ordering::Acquire),
            facts.inner_dropped.load(Ordering::Acquire)
        ),
        (1, 1)
    );
    drop(halves);
    drop(upstream);
    println!(
        "B54 whole-root forcedDrop originalBox=1 originalInnerDropReturned=1; not provider clean; endpoint cleanup follows"
    );
}
