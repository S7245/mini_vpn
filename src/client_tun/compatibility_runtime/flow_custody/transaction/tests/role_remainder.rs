//! Real AUTH/CONNECT publication → original four roles → actual joins → Rx close unwind.
//! Owns one fault waker and fixed passive snapshots. It never replaces a role,
//! consumes its receiver, creates a receipt, or treats forced root Drop as clean.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{Endpoint, receiving_pair};
use super::super::super::relay_writer::WriterDiscardReceipt;
use super::super::*;
use super::partial_connect::ReadyPeer;
use crate::client_tun::{
    DownlinkBackpressureConfig, HandshakeCompletion, RELAY_CHANNEL_CAPACITY, RelayCommand,
};
use crate::fake_ip::FakeIpPool;
use crate::metrics::Metrics;
use crate::owned_upstream::LegacyOwnedUpstream;
use crate::shared::TargetAddr;
use crate::shared::errors::FailureClass;
use crate::tuic::encode_connect;
use crate::upstream::RelayFamilyPolicySources;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Wake, Waker};
use std::time::{Duration, Instant};

struct OriginalPanic {
    tag: u64,
    drops: Arc<AtomicUsize>,
}
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
struct FaultWake {
    payload: Mutex<Option<Box<OriginalPanic>>>,
    calls: AtomicUsize,
}
impl Wake for FaultWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let payload = self.payload.lock().unwrap().take();
        self.calls.fetch_add(1, Ordering::AcqRel);
        if let Some(payload) = payload {
            std::panic::panic_any(payload);
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Data {
    current: Option<(usize, usize, usize, usize, bool)>,
    bytes: Option<[u8; 12]>,
    deferred: Option<(usize, usize, usize)>,
    receiver: Option<(usize, usize, bool)>,
    flags: [bool; 4],
    unaccounted: bool,
    receipt: WriterDiscardReceipt,
    source_error: bool,
    shutdown_error: bool,
    stopped: bool,
}
#[derive(Clone, Debug, PartialEq)]
struct Row {
    id: FlowIdentity,
    stage: FlowStage,
    slot_limit: tokio::time::Instant,
    flow_limit: Option<Instant>,
    held: (bool, bool, usize, usize),
    published: bool,
    producer: (bool, bool),
    attempt: Option<usize>,
    tasks_finished: [bool; 4],
    joined: usize,
    starts: Option<[bool; 3]>,
    roles_clean: bool,
    writer_receipt: bool,
    data: Option<Data>,
    panic: Option<(usize, u64)>,
    accounting: (usize, usize, usize, usize, u64, u64, u64),
    audit: bool,
}
fn row(owner: &SessionControl, id: FlowIdentity) -> Option<Row> {
    let opens = owner.opens();
    let slot = opens.slots.get(id.index)?.as_ref()?;
    let OpenPhase::Flow(flow) = &slot.phase else {
        return None;
    };
    let roles = flow.roles.as_ref()?;
    let (tasks_finished, writer) = roles.join_state_for_test();
    let accounting = opens.flow.as_ref()?;
    Some(Row {
        id: flow.id,
        stage: flow.stage,
        slot_limit: slot.deadline,
        flow_limit: *flow.stop_deadline,
        held: (flow.w_held, flow.f_held, flow.reserved, flow.escrow_bytes),
        published: flow.published,
        producer: (flow.producer_stopped, flow.provider_cancelled),
        attempt: flow
            .attempt
            .as_ref()
            .map(|a| &**a as *const dyn TcpOpenAttempt<OpenedOwnedTcp> as *const () as usize),
        tasks_finished,
        joined: roles.join_count(),
        starts: roles.joined_starts_for_test(),
        roles_clean: roles.settled(),
        writer_receipt: roles.writer_receipt.is_some(),
        data: writer.map(|writer| {
            let v = writer.remainder.custody_for_test();
            Data {
                current: v.current,
                bytes: v.bytes,
                deferred: v.deferred,
                receiver: v.receiver,
                flags: v.flags,
                unaccounted: v.unaccounted,
                receipt: v.receipt,
                source_error: writer.result.failure.is_some(),
                shutdown_error: writer.result.shutdown_failure.is_some(),
                stopped: matches!(
                    writer.result.terminal,
                    super::super::super::relay_writer::WriterTerminal::Stopped
                ),
            }
        }),
        panic: flow
            .panic
            .as_ref()
            .and_then(|p| p.downcast_ref::<Box<OriginalPanic>>())
            .map(|p| (&**p as *const OriginalPanic as usize, p.tag)),
        accounting: (
            accounting.w,
            accounting.f,
            accounting.bytes,
            accounting.escrow_bytes,
            accounting.published,
            accounting.released,
            accounting.joined,
        ),
        audit: opens.audit_flows(),
    })
}
fn first(owner: &SessionControl) -> Option<(usize, String, bool, u64, Option<u64>, Vec<u8>)> {
    let ledger = owner.ledger();
    ledger.first_failure.as_ref().map(|value| {
        let f = value.facts();
        (
            value as *const _ as usize,
            format!("{f:?}"),
            f.class == FailureClass::Panic
                && f.origin.phase == FailurePhase::TcpOpen
                && f.origin.resource == FailureResource::TcpOpen,
            f.origin.resource_id,
            f.origin.attempt_id,
            value.text_bytes().to_vec(),
        )
    })
}
struct Fixture {
    owner: Arc<SessionControl>,
    dispatch: OpenDispatch,
    receiver: mpsc::Receiver<HandshakeNotice>,
    _client: Endpoint,
    server: Endpoint,
    contexts: HashMap<SocketHandle, SocketCtx>,
    pool: FakeIpPool,
    global: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    _global_rx: crate::client_tun::relay_channel::Receiver<(SocketHandle, RelayEvent)>,
    owned: mpsc::Sender<crate::client_tun::OwnedActionEvent>,
    _owned_rx: mpsc::Receiver<crate::client_tun::OwnedActionEvent>,
    metrics: Metrics,
    id: FlowIdentity,
    setup_limit: tokio::time::Instant,
}
impl Fixture {
    async fn new(peer: &ReadyPeer) -> Self {
        let (client, mut server) = receiving_pair().await;
        let owner = Arc::new(SessionControl::default());
        *owner.opens() = OpenCustody::with_flows();
        let (dispatch, receiver) = OpenDispatch::channel(4, Some(owner.clone()));
        let dispatch = dispatch.with_relay_policy(Arc::new(RelayFamilyPolicy::from_sources(
            RelayFamilyPolicySources {
                ordered_egress_permit: Some("1"),
                ..Default::default()
            },
        )));
        let handle = server.handle;
        let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
        let mut pool = FakeIpPool::new();
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            Arc::new(LegacyOwnedUpstream::new(peer.upstream.clone())),
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
            opens.slots.iter().flatten().find_map(|s| match &s.phase {
                OpenPhase::Flow(f) => Some(f.id),
                _ => None,
            })
        }
        .expect("actual admitted flow");
        let (global, global_rx) = crate::client_tun::relay_channel::channel(32);
        let (owned, owned_rx) = mpsc::channel(4);
        Self {
            owner,
            dispatch,
            receiver,
            _client: client,
            server,
            contexts,
            pool,
            global,
            _global_rx: global_rx,
            owned,
            _owned_rx: owned_rx,
            metrics: Metrics::default(),
            id,
            setup_limit: tokio::time::Instant::now() + Duration::from_secs(5),
        }
    }
    async fn poll(&mut self) -> Poll<Option<OpenEvent>> {
        self.dispatch.begin_turn().await;
        std::future::poll_fn(|cx| {
            let mut work = std::pin::pin!(self.dispatch.recv(&mut self.receiver));
            Poll::Ready(work.as_mut().poll(cx))
        })
        .await
    }
    async fn turn(&mut self) {
        if let Poll::Ready(Some(event @ OpenEvent::Flow(_))) = self.poll().await {
            HandshakeCompletion::from_loop(
                &self.dispatch,
                event,
                HandshakeCompletion {
                    sockets: &mut self.server.sockets,
                    socket_ctxs: &mut self.contexts,
                    global_tx: &self.global,
                    owned_action_tx: &self.owned,
                    fake_pool: &mut self.pool,
                    now_secs: 0,
                    metrics_handle: &self.metrics,
                    downlink_backpressure: DownlinkBackpressureConfig::default(),
                    relay_read_hard_pause: false,
                },
            );
        }
    }
    async fn remote<F: Future>(&mut self, work: F) -> F::Output {
        tokio::time::timeout_at(self.setup_limit, async {
            let mut work = std::pin::pin!(work);
            loop {
                let value = std::future::poll_fn(|cx| Poll::Ready(work.as_mut().poll(cx))).await;
                self.turn().await;
                if let Poll::Ready(value) = value {
                    return value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original five-second setup bound")
    }
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
#[tokio::test(flavor = "current_thread")]
async fn actual_joined_roles_receiver_close_unwind_keeps_original_data_first_error_and_charged_root()
 {
    let peer = ReadyPeer::new().await;
    let result = capture(exercise(&peer)).await;
    let cleanup = capture(peer.close()).await;
    if let Err(panic) = result {
        if cleanup.is_err() {
            eprintln!("B59 cleanup secondary; original scenario error preserved");
        }
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}
async fn exercise(peer: &ReadyPeer) {
    let mut f = Fixture::new(peer).await;
    let halves = f.remote(peer.prefix()).await;
    let (send, mut recv) = halves;
    let mut rest = [0; 5];
    f.remote(recv.read_exact(&mut rest)).await.unwrap();
    let header = encode_connect(&TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()));
    assert_eq!(rest, header[4..]);
    let mut original_byte = [0];
    f.remote(recv.read_exact(&mut original_byte)).await.unwrap();
    assert_eq!(original_byte, [b'u']);
    peer.assert_current(1);
    let published = row(&f.owner, f.id).expect("real Ready publication created original roles");
    assert_eq!(published.stage, FlowStage::Published);
    assert_eq!(published.joined, 0);
    assert_eq!(published.held, (false, true, 0, 0));
    assert_eq!(published.accounting, (0, 1, 0, 0, 1, 0, 0));
    assert!(published.published && published.audit && published.attempt.is_some());
    let tx = f.contexts[&f.id.handle].uplink_tx.as_ref().unwrap().clone();
    let queued = vec![211, 212, 213];
    let queued_source = (queued.len(), queued.as_ptr() as usize, queued.capacity());
    tx.try_send(RelayCommand::Data(queued)).unwrap();
    assert_eq!(tx.capacity(), RELAY_CHANNEL_CAPACITY - 1);
    let requested = tokio::time::Instant::now();
    let deadline = f.owner.request_stop();
    let observe_limit = requested + Duration::from_secs(5);
    assert!(deadline >= requested + Duration::from_secs(15));
    tokio::time::timeout_at(observe_limit, async {
        loop {
            f.turn().await;
            let stopped = row(&f.owner, f.id).unwrap();
            if stopped.producer.0 {
                assert_eq!(stopped.stage, FlowStage::Closing);
                assert_eq!(stopped.producer, (true, false));
                assert_eq!(stopped.joined, 0);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(f.contexts[&f.id.handle].uplink_tx.is_none());
    assert!(f.contexts[&f.id.handle].relay_read_credit_tx.is_none());
    tokio::time::timeout_at(observe_limit, async {
        loop {
            let stopped = row(&f.owner, f.id).unwrap();
            assert_eq!(stopped.joined, 0);
            if stopped.tasks_finished == [true; 4] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(first(&f.owner).is_none());
    assert_eq!(tx.capacity(), RELAY_CHANNEL_CAPACITY - 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let payload = Box::new(OriginalPanic {
        tag: 59,
        drops: drops.clone(),
    });
    let identity = &*payload as *const OriginalPanic as usize;
    let fault = Arc::new(FaultWake {
        payload: Mutex::new(Some(payload)),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(fault.clone());
    let mut waiting = Box::pin(tx.legacy_sender_for_test().expect("original Legacy Tokio waiter").reserve_many(RELAY_CHANNEL_CAPACITY));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(fault.calls.load(Ordering::Acquire), 0);
    for _ in 0..FLOW_SLOT_LIMIT.div_ceil(32) + 2 {
        let _ = f.poll().await;
        if fault.calls.load(Ordering::Acquire) != 0 {
            break;
        }
    }
    assert_eq!(fault.calls.load(Ordering::Acquire), 1);
    let after = row(&f.owner, f.id).unwrap();
    let original = first(&f.owner).expect("original drive catch published first Flow Panic");
    let data = after
        .data
        .expect("actual consumed original writer JoinHandle outcome");
    println!(
        "B59 original DATA={original_byte:?} queuedSource={queued_source:?} originalBox={identity}; actualjoined={after:?} first={original:?}"
    );
    assert_eq!(after.stage, FlowStage::Quarantined);
    assert_eq!(after.joined, 4);
    assert_eq!(after.starts, Some([true; 3]));
    assert_eq!(after.tasks_finished, [true; 4]);
    assert!(!after.roles_clean && !after.writer_receipt);
    assert_eq!(after.held, published.held);
    assert_eq!(after.accounting, published.accounting);
    assert_eq!(after.attempt, published.attempt);
    assert_eq!(after.slot_limit, published.slot_limit);
    assert_eq!(after.producer, (true, false));
    assert_eq!(after.panic, Some((identity, 59)));
    assert!(after.audit);
    assert_eq!(data.bytes, Some(*b"untouched-rx"));
    let current = data.current.unwrap();
    assert_eq!((current.0, current.2), (12, 12));
    assert!(current.1 != 0 && current.3 > 0 && current.3 < 12 && !current.4);
    assert_eq!(data.deferred, None);
    assert_eq!(data.receiver, Some((RELAY_CHANNEL_CAPACITY, 1, true)));
    assert_eq!(data.flags, [false, false, true, false]);
    assert!(!data.unaccounted && !data.source_error && !data.shutdown_error && data.stopped);
    assert_eq!(data.receipt, WriterDiscardReceipt::default());
    assert_eq!(
        (original.2, original.3, original.4),
        (true, f.id.index as u64, Some(f.id.epoch))
    );
    assert!(original.5.is_empty());
    assert_eq!(f.owner.ledger().additional_failures, 0);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    let flow_limit = after.flow_limit.expect("original first Flow stop limit");
    assert!(flow_limit >= deadline.into_std());
    for _ in 0..4 {
        let _ = f.poll().await;
        assert!(
            !f.dispatch
                .flow_shutdown_complete(&f.server.sockets, &mut f.contexts, &f.pool)
        );
        assert_eq!(row(&f.owner, f.id), Some(after.clone()));
        assert_eq!(first(&f.owner), Some(original.clone()));
        assert_eq!(f.owner.request_stop(), deadline);
        assert_eq!(fault.calls.load(Ordering::Acquire), 1);
    }
    tokio::time::sleep_until(deadline).await;
    let _ = f.poll().await;
    assert!(
        !f.dispatch
            .flow_shutdown_complete(&f.server.sockets, &mut f.contexts, &f.pool)
    );
    assert_eq!(row(&f.owner, f.id), Some(after.clone()));
    assert_eq!(first(&f.owner), Some(original.clone()));
    assert_eq!(f.owner.request_stop(), deadline);
    assert_eq!(f.owner.ledger().additional_failures, 0);
    assert_eq!(fault.calls.load(Ordering::Acquire), 1);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(
        f.owner.state(),
        crate::client_tun::compatibility_runtime::SessionDeviceState::Incomplete
    );
    println!(
        "B59 original root15 expired deadline={deadline:?} independentFlowLimit={flow_limit:?}; actualjoins4 but DATA/current/Rx/originalBox/first stillcharged; row={after:?} first={original:?}; no writerreceipt/Frelease/Nodeclean/successor"
    );
    drop(waiting);
    drop(tx);
    drop(waker);
    drop(f);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop((send, recv));
    println!(
        "B59 whole-root forcedDrop originalBox1 only; not provider/client clean; endpoint cleanup follows"
    );
}
