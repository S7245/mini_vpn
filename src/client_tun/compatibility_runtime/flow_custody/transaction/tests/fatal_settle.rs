//! Actual saved-loop IO → fatal four-role settlement → original provider unwind.
//! Owns only one fault decorator and bounded passive observations. The original
//! driver creates every slot/role/receipt; forced root Drop is never settlement.
use super::super::super::rearm::physical_flow::tests::admission::Endpoint;
use super::super::*;
use super::partial_connect::ReadyPeer;
use crate::client_tun::compatibility_runtime::{
    ClientTunSession, SessionDeviceState, SessionIngress, prepare_owned_client_tun_session,
};
use crate::client_tun::{NoopSink, TunRuntimeConfig};
use crate::device::TunIo;
use crate::harness::{LoopbackTunDevice, PacketLink};
use crate::metrics::Metrics;
use crate::owned_upstream::{LegacyOwnedUpstream, UdpDownlinkSource, UdpUplink};
use crate::shared::errors::FailureClass;
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::{TuicUpstream, encode_connect};
use crate::upstream::{RelayFamilyPolicySources, StagedTcpOpen, TcpOpenView};
use std::sync::{Weak, atomic::AtomicUsize};
use std::task::Context;
use std::time::{Duration, Instant};

#[path = "../../../tuic_service/tests/device.rs"]
mod device;

struct OriginalPanic {
    tag: u64,
    drops: Arc<AtomicUsize>,
}
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Clone, Debug, PartialEq)]
struct First {
    value: usize,
    facts: String,
    text: Vec<u8>,
    read_io5: bool,
    additional: u64,
    deadline: tokio::time::Instant,
}
fn first(owner: &SessionControl) -> Option<First> {
    let ledger = owner.ledger();
    let value = ledger.first_failure.as_ref()?;
    let f = value.facts();
    Some(First {
        value: value as *const _ as usize,
        facts: format!("{f:?}"),
        text: value.text_bytes().to_vec(),
        read_io5: f.class == FailureClass::Io
            && f.origin.phase == FailurePhase::DeviceRead
            && f.origin.resource == FailureResource::Session
            && f.raw_os_code == Some(5),
        additional: ledger.additional_failures,
        deadline: ledger.shutdown_deadline?,
    })
}
struct Facts {
    owner: Mutex<Weak<SessionControl>>,
    payload: Mutex<Option<Box<OriginalPanic>>>,
    // Original view, real settle arguments and first IO, after delegate returns.
    settled: Mutex<Option<(TcpOpenView, Instant, Instant, First)>>,
    fired: AtomicBool,
    calls: [AtomicUsize; 7],
    reentries: [AtomicUsize; 7],
    inner: AtomicUsize,
    inner_dropped: AtomicUsize,
}
impl Facts {
    fn enter(&self, index: usize) {
        self.calls[index].fetch_add(1, Ordering::AcqRel);
        if self.fired.load(Ordering::Acquire) {
            self.reentries[index].fetch_add(1, Ordering::AcqRel);
            panic!("original poisoned fatal provider reentered: {index}");
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
        panic!("fatal staged tracer cannot use async open");
    }
    async fn send_udp(&self, uplink: UdpUplink<'_>) {
        self.inner.send_udp(uplink).await;
    }
    fn open_is_cheap(&self) -> bool {
        self.inner.open_is_cheap()
    }
}
struct FaultAttempt {
    inner: Option<Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>>,
    facts: Arc<Facts>,
}
impl Drop for FaultAttempt {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.facts.inner_dropped.fetch_add(1, Ordering::AcqRel);
    }
}
impl TcpOpenAttempt<OpenedOwnedTcp> for FaultAttempt {
    fn step(&mut self, cx: &mut Context<'_>, now: Instant) {
        self.facts.enter(1);
        self.inner.as_mut().unwrap().step(cx, now);
    }
    fn view(&self) -> TcpOpenView {
        self.facts.enter(2);
        self.inner.as_ref().unwrap().view()
    }
    fn cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        self.facts.enter(3);
        self.inner.as_mut().unwrap().cancel(reason, deadline);
    }
    fn take_carrier(&mut self, now: Instant) -> Option<OpenedOwnedTcp> {
        self.facts.enter(4);
        self.inner.as_mut().unwrap().take_carrier(now)
    }
    fn settle(&mut self, cx: &mut Context<'_>, now: Instant, deadline: Instant) {
        self.facts.enter(5);
        let inner = self.inner.as_mut().unwrap();
        inner.settle(cx, now, deadline);
        let view = inner.view();
        let owner = self.facts.owner.lock().unwrap().upgrade().unwrap();
        let original = first(&owner).expect("original driver records IO before fatal callback");
        *self.facts.settled.lock().unwrap() = Some((view, now, deadline, original));
        let payload = self.facts.payload.lock().unwrap().take().unwrap();
        assert!(!self.facts.fired.swap(true, Ordering::AcqRel));
        std::panic::panic_any(payload);
    }
    fn failure(&self) -> Option<&ClientError> {
        self.facts.enter(6);
        self.inner.as_ref().unwrap().failure()
    }
}
#[derive(Clone, Debug, PartialEq)]
struct Row {
    id: FlowIdentity,
    stage: FlowStage,
    slot_limit: tokio::time::Instant,
    open_limit: Instant,
    flow_limit: Option<Instant>,
    held: (bool, bool, usize, usize),
    published: bool,
    producer: (bool, bool),
    attempt: Option<usize>,
    joined: usize,
    starts: Option<[bool; 3]>,
    roles_settled: bool,
    writer_receipt: bool,
    panic: Option<(usize, u64)>,
    accounting: (usize, usize, usize, usize, u64, u64, u64),
    audit: bool,
}
fn row(owner: &SessionControl) -> Option<Row> {
    let opens = owner.opens();
    let slot = opens.slots.iter().flatten().next()?;
    let OpenPhase::Flow(flow) = &slot.phase else {
        return None;
    };
    let roles = flow.roles.as_ref()?;
    let a = opens.flow.as_ref()?;
    Some(Row {
        id: flow.id,
        stage: flow.stage,
        slot_limit: slot.deadline,
        open_limit: flow.open_deadline,
        flow_limit: flow.stop_deadline,
        held: (flow.w_held, flow.f_held, flow.reserved, flow.escrow_bytes),
        published: flow.published,
        producer: (flow.producer_stopped, flow.provider_cancelled),
        attempt: flow
            .attempt
            .as_ref()
            .map(|a| &**a as *const dyn TcpOpenAttempt<OpenedOwnedTcp> as *const () as usize),
        joined: roles.join_count(),
        starts: roles.joined_starts_for_test(),
        roles_settled: roles.settled(),
        writer_receipt: roles.writer_receipt.is_some(),
        panic: flow
            .panic
            .as_ref()
            .and_then(|p| p.downcast_ref::<Box<OriginalPanic>>())
            .map(|p| (&**p as *const OriginalPanic as usize, p.tag)),
        accounting: (
            a.w,
            a.f,
            a.bytes,
            a.escrow_bytes,
            a.published,
            a.released,
            a.joined,
        ),
        audit: opens.audit_flows(),
    })
}
async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}
async fn driven<F: Future<Output = ()>, W: Future>(
    session: &mut ClientTunSession<F>,
    limit: tokio::time::Instant,
    work: W,
) -> W::Output {
    tokio::time::timeout_at(limit, async {
        tokio::select! {
            value = work => value,
            state = session.drive_until_settled() => panic!("fatal saved driver falsely completed: {state:?}"),
        }
    }).await.expect("fixed observation bound")
}

#[tokio::test(flavor = "current_thread")]
async fn actual_loop_io_fatal_owned_settle_unwind_preserves_first_and_charged_root() {
    let peer = ReadyPeer::new().await;
    let result = capture(exercise(&peer)).await;
    let cleanup = capture(peer.close()).await;
    if let Err(panic) = result {
        if cleanup.is_err() {
            eprintln!("B60 endpoint cleanup secondary; original scenario retained");
        }
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
async fn exercise(peer: &ReadyPeer) {
    let drops = Arc::new(AtomicUsize::new(0));
    let payload = Box::new(OriginalPanic {
        tag: 60,
        drops: drops.clone(),
    });
    let identity = &*payload as *const OriginalPanic as usize;
    let facts = Arc::new(Facts {
        owner: Mutex::new(Weak::new()),
        payload: Mutex::new(Some(payload)),
        settled: Mutex::new(None),
        fired: AtomicBool::new(false),
        calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        reentries: std::array::from_fn(|_| AtomicUsize::new(0)),
        inner: AtomicUsize::new(0),
        inner_dropped: AtomicUsize::new(0),
    });
    let upstream = Arc::new(FaultUpstream {
        inner: Arc::new(LegacyOwnedUpstream::new(peer.upstream.clone())),
        facts: facts.clone(),
    });
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Endpoint::new(outbound.clone(), inbound.clone(), false);
    let gate = Arc::new(device::Gate::default());
    gate.open();
    let (source, downlink) = mpsc::channel(1);
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        ordered_egress_permit: Some("1"),
        ..Default::default()
    }));
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_owned_client_tun_session(
        device::GatedDevice {
            inner: LoopbackTunDevice::new(inbound, outbound).with_ingress_pump(4),
            gate: gate.clone(),
        },
        Some(SessionIngress {
            upstream,
            downlink: UdpDownlinkSource::legacy_raw(downlink),
            health_join: None,
        }),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let owner = session.control.clone();
    *facts.owner.lock().unwrap() = Arc::downgrade(&owner);
    let setup = tokio::time::Instant::now() + Duration::from_secs(5);
    let scenario = capture(async {
        let application = async {
            let mut sent = false;
            loop {
                let socket = app.sockets.get_mut::<TcpSocket>(app.handle);
                if !sent && socket.can_send() {
                    assert_eq!(socket.send_slice(b"untouched-rx").unwrap(), 12);
                    sent = true;
                }
                app.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let remote = async {
            let (send, mut recv) = peer.prefix().await;
            let mut rest = [0; 5];
            recv.read_exact(&mut rest).await.unwrap();
            assert_eq!(rest, encode_connect(&TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()))[4..]);
            let mut byte = [0];
            recv.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [b'u']);
            (send, recv)
        };
        let halves = driven(&mut session, setup, async {
            tokio::select! { halves = remote => halves, _ = application => unreachable!() }
        }).await;
        peer.assert_current(1);
        let published = row(&owner).expect("original saved loop actually published four roles");
        assert_eq!(published.stage, FlowStage::Published);
        assert_eq!(published.held, (false, true, 0, 0));
        assert_eq!(published.accounting, (0, 1, 0, 0, 1, 0, 0));
        assert!(published.published && published.audit && published.attempt.is_some());
        assert_eq!(published.joined, 0);
        assert!(!facts.fired.load(Ordering::Acquire));
        assert!(first(&owner).is_none());
        let bridge = owner.relay_event_test_bridge().unwrap();
        let trigger = tokio::time::Instant::now();
        gate.read_error.store(true, Ordering::Release);
        driven(&mut session, trigger + Duration::from_secs(5), async {
            while !facts.fired.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        assert!(!gate.read_error.load(Ordering::Acquire));
        let (view, called_at, flow_limit, before) = facts.settled.lock().unwrap().clone().unwrap();
        let after = row(&owner).unwrap();
        let actual = first(&owner).unwrap();
        println!("B60 fatal delegate returned={view:?} settle=({called_at:?},{flow_limit:?}); firstBeforeBox={before:?} firstAfter={actual:?}; row={after:?}; counts={:?} inner={} originalBox={identity}", facts.counts(), facts.inner.load(Ordering::Acquire));
        assert!(before.read_io5 && actual.read_io5);
        assert_eq!(before.additional, 0);
        assert_eq!(actual.additional, 1);
        assert_eq!((actual.value, &actual.facts, &actual.text, actual.deadline), (before.value, &before.facts, &before.text, before.deadline));
        assert_eq!(after.id, published.id);
        assert_eq!(after.stage, FlowStage::Quarantined);
        assert_eq!(after.joined, 4);
        assert_eq!(after.starts, Some([true; 3]));
        assert!(after.roles_settled && after.writer_receipt && after.audit);
        assert_eq!(after.producer, (true, true));
        assert_eq!(after.panic, Some((identity, 60)));
        assert_eq!(after.held, published.held);
        assert_eq!(after.accounting, published.accounting);
        assert_eq!(after.attempt, published.attempt);
        assert_eq!(after.open_limit, published.open_limit);
        assert_eq!(after.slot_limit, published.slot_limit);
        assert_eq!(after.flow_limit, Some(flow_limit));
        assert!(flow_limit >= before.deadline.into_std());
        assert!(matches!(view.phase, TcpOpenPhase::Retiring | TcpOpenPhase::Settled));
        assert_eq!(view.connect_written, 9);
        let counts = facts.counts();
        assert_eq!((counts.0[0], counts.0[4], counts.0[5]), (1, 1, 1));
        assert_eq!(counts.0[3], 0, "Transferred provider needs no cancel callback");
        assert_eq!(counts.1, [0; 7]);
        assert_eq!(facts.inner_dropped.load(Ordering::Acquire), 0);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert!(before.deadline >= trigger + Duration::from_secs(15));
        // Reborrow the SAME saved driver through the original first root limit.
        // Fatal Retained samples its once-only physical census only at that limit.
        driven(&mut session, before.deadline + Duration::from_secs(5), async {
            while bridge.fatal_census().is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        assert!(tokio::time::Instant::now() >= before.deadline);
        assert_eq!(bridge.fatal_census(), Some(([(0, 0); 3], 0)));
        assert_eq!(bridge.selected_data(), None);
        assert_eq!(bridge.selected_capability(), None);
        assert_eq!(bridge.queued_len(), 0);
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1), session.drive_until_settled()).await.is_err());
            assert_eq!(row(&owner), Some(after.clone()));
            assert_eq!(first(&owner), Some(actual.clone()));
            assert_eq!(facts.counts(), counts);
            assert_eq!(owner.request_stop(), before.deadline);
            assert_eq!(session.state(), SessionDeviceState::Incomplete);
            assert!(!owner.execution_complete());
        }
        println!("B60 terminal root15 {:?} expired; fatal census={:?}/selected={:?}/cap={:?}/queued={}; original firstIO/Box/F1/provider retained with actualjoins4 not ledger0; flags pending+uncertain follow original fatal Err branch; no F receipt/clean/successor", before.deadline, bridge.fatal_census(), bridge.selected_data(), bridge.selected_capability(), bridge.queued_len());
        assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0, 0));
        drop(halves);
    }).await;
    drop(session);
    drop(owner);
    drop(source);
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(facts.inner_dropped.load(Ordering::Acquire), 1);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
    println!(
        "B60 forced whole-root Drop consumed original provider/Box once; devicejoin0, not clean"
    );
}
