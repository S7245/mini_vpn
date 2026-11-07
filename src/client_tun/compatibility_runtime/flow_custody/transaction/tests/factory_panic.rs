//! Actual received TCP → original admission/escrow → original factory unwind.
//! Owns one fault callback/payload and real local sockets, no successful carrier,
//! Node, fabricated flow/receipt, or asynchronous-clean proof.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::receiving_pair;
use super::super::*;
use crate::client_tun::{DownlinkBackpressureConfig, HandshakeCompletion};
use crate::fake_ip::FakeIpPool;
use crate::owned_upstream::UdpUplink;
use crate::shared::errors::FailureClass;
use crate::shared::{ClientError, TargetAddr};
use crate::upstream::StagedTcpOpen;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
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
struct FactoryFault {
    payload: Mutex<Option<Box<OriginalPanic>>>,
    calls: AtomicUsize,
}
#[async_trait::async_trait]
impl OwnedUpstream for FactoryFault {
    fn staged_tcp_open(self: Arc<Self>, request: TcpOpenRequest) -> StagedTcpOpen<OpenedOwnedTcp> {
        assert_eq!(
            request.target,
            TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap())
        );
        self.calls.fetch_add(1, Ordering::AcqRel);
        let payload = self.payload.lock().unwrap().take();
        let payload = payload.expect("original fault callback cannot be entered twice");
        std::panic::panic_any(payload);
    }
    async fn open_tcp_flow(&self, _: &TargetAddr) -> Result<OpenedOwnedTcp, ClientError> {
        panic!("factory unwind cannot enter the async-open fallback");
    }
    async fn send_udp(&self, _: UdpUplink<'_>) {
        panic!("factory unwind tracer has no UDP path");
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    id: FlowIdentity,
    slot_deadline: tokio::time::Instant,
    open_deadline: std::time::Instant,
    stop_deadline: Option<std::time::Instant>,
    stage: FlowStage,
    escrow: Vec<u8>,
    storage: (usize, usize, usize),
    held: (bool, bool, usize),
    installed: (bool, bool, bool, bool),
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
        stop_deadline: flow.stop_deadline,
        stage: flow.stage,
        escrow: flow.escrow.to_vec(),
        storage: (
            flow.escrow.len(),
            flow.escrow.as_ptr() as usize,
            flow.escrow.capacity(),
        ),
        held: (flow.w_held, flow.f_held, flow.reserved),
        installed: (
            flow.attempt.is_some(),
            flow.carrier.is_some(),
            flow.roles.is_some(),
            flow.published,
        ),
        payload: flow
            .panic
            .as_ref()
            .and_then(|payload| payload.downcast_ref::<Box<OriginalPanic>>())
            .map(|payload| (&**payload as *const OriginalPanic as usize, payload.tag)),
    })
}
fn first(
    owner: &SessionControl,
) -> Option<(usize, String, bool, bool, bool, u64, Option<u64>, Vec<u8>)> {
    let ledger = owner.ledger();
    ledger.first_failure.as_ref().map(|value| {
        let facts = value.facts();
        (
            value as *const _ as usize,
            format!("{facts:?}"),
            facts.class == FailureClass::Panic,
            facts.origin.phase == FailurePhase::TcpOpen,
            facts.origin.resource == FailureResource::TcpOpen,
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

#[tokio::test(start_paused = true)]
async fn actual_factory_unwind_records_flow_first_before_retaining_original_charged_payload() {
    exercise(false).await;
}

#[tokio::test(start_paused = true)]
async fn actual_prior_io_first_survives_factory_unwind_with_exact_additional_count() {
    exercise(true).await;
}

async fn exercise(prior_io: bool) {
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
        tag: 53,
        drops: drops.clone(),
    });
    let identity = &*payload as *const OriginalPanic as usize;
    let upstream = Arc::new(FactoryFault {
        payload: Mutex::new(Some(payload)),
        calls: AtomicUsize::new(0),
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
    };
    let id = id.expect("actual admitted identity");
    let before = snapshot(&owner, id).expect("original admitted row");
    assert_eq!(before.stage, FlowStage::Waiting);
    assert_eq!(before.held, (true, false, MAX_UPLINK_BUFFER));
    assert_eq!(before.escrow, b"untouched-rx");
    assert_eq!(before.installed, (false, false, false, false));
    assert!(before.payload.is_none() && before.stop_deadline.is_none());
    let before_ledger = {
        let ledger = owner.ledger();
        (
            ledger.first_failure.is_none(),
            ledger.shutdown_deadline.is_none(),
        )
    };
    assert_eq!(before_ledger, (true, true));
    let prior_first = if prior_io {
        let (mut writer, peer) = tokio::io::duplex(1);
        drop(peer);
        let error = writer.write_all(b"x").await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        owner.record(RetainedFailure::from_io(
            open_origin(id.index, id.epoch, FailurePhase::TcpOpen),
            error,
        ));
        let value = first(&owner).expect("actual prior IO reported through original authority");
        let (additional, kind) = {
            let ledger = owner.ledger();
            (
                ledger.additional_failures,
                ledger
                    .first_failure
                    .as_ref()
                    .and_then(|value| value.facts().io_kind),
            )
        };
        assert_eq!(
            (additional, kind),
            (0, Some(std::io::ErrorKind::BrokenPipe))
        );
        assert!(!value.2);
        println!("B53 actualpriorIO beforefactory first={value:?} additional={additional}");
        Some(value)
    } else {
        None
    };
    let event = poll_once(&dispatch, &mut receiver).await;
    let after = snapshot(&owner, id).expect("original factory row");
    let observed = owner.flow_observation().unwrap();
    let original_first = first(&owner);
    let quarantined = owner.ledger().quarantined;
    println!(
        "B53 actualfactory before={before:?} after={after:?} originalPayload={identity} drops={} calls={} ownerQ={quarantined} actualCensus={observed:?} first={original_first:?}",
        drops.load(Ordering::Acquire),
        upstream.calls.load(Ordering::Acquire)
    );
    assert!(matches!(event, Poll::Ready(Some(OpenEvent::Flow(actual))) if actual == id));
    assert_eq!(after.stage, FlowStage::Quarantined);
    assert_eq!(after.id, before.id);
    assert_eq!(
        (after.slot_deadline, after.open_deadline),
        (before.slot_deadline, before.open_deadline)
    );
    assert_eq!(after.stop_deadline, None);
    assert_eq!(after.held, (true, true, MAX_UPLINK_BUFFER));
    assert_eq!(after.storage, before.storage);
    assert_eq!(after.escrow, before.escrow);
    assert_eq!(after.installed, (false, false, false, false));
    assert_eq!(after.payload, Some((identity, 53)));
    assert_eq!(
        (
            upstream.calls.load(Ordering::Acquire),
            drops.load(Ordering::Acquire)
        ),
        (1, 0)
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
    let no_shutdown_limit = owner.ledger().shutdown_deadline.is_none();
    assert!(no_shutdown_limit);
    let original_first = original_first
        .expect("factory unwind must publish original Flow Panic before payload/unknown work");
    if let Some(prior) = prior_first {
        assert_eq!(
            original_first, prior,
            "actual prior IO VALUE/facts/text cannot be replaced"
        );
    } else {
        assert_eq!(
            (
                original_first.2,
                original_first.3,
                original_first.4,
                original_first.5,
                original_first.6
            ),
            (true, true, true, id.index as u64, Some(id.epoch))
        );
    }
    let additional = owner.ledger().additional_failures;
    assert_eq!(additional, u64::from(prior_io));
    println!(
        "B53 actualfactory first retained={original_first:?} additional={additional} priorIO={prior_io}"
    );
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
    let deadline = owner.request_stop();
    assert_eq!(
        deadline,
        tokio::time::Instant::now() + Duration::from_secs(15)
    );
    for _ in 0..4 {
        let _ = poll_once(&dispatch, &mut receiver).await;
        owner.settle_opens().await;
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert_eq!(owner.request_stop(), deadline);
        assert_eq!(first(&owner), Some(original_first.clone()));
        assert_eq!(
            snapshot(&owner, id).expect("retained original row").payload,
            Some((identity, 53))
        );
        assert_eq!(
            (
                upstream.calls.load(Ordering::Acquire),
                drops.load(Ordering::Acquire)
            ),
            (1, 0)
        );
    }
    tokio::time::advance(Duration::from_secs(15)).await;
    assert_eq!(owner.request_stop(), deadline);
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    assert_eq!(
        owner.state(),
        crate::client_tun::compatibility_runtime::SessionDeviceState::Incomplete
    );
    assert_eq!(
        snapshot(&owner, id).expect("retained original row").payload,
        Some((identity, 53))
    );
    assert_eq!(first(&owner), Some(original_first));
    let final_row = snapshot(&owner, id).expect("original charged row after original15");
    let final_census = owner.flow_observation().unwrap();
    let final_additional = owner.ledger().additional_failures;
    assert_eq!(
        (
            final_row.id,
            final_row.slot_deadline,
            final_row.open_deadline
        ),
        (before.id, before.slot_deadline, before.open_deadline)
    );
    assert_eq!(final_row.storage, before.storage);
    assert_eq!(final_row.escrow, before.escrow);
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
    assert_eq!(final_additional, u64::from(prior_io));
    println!(
        "B53 actualfactory original15 expired; original row={final_row:?} actualCensus={final_census:?} additional={final_additional} priorIO={prior_io}; no receipt/release/join/clean"
    );
    // Controlled test payload may finally drop with the complete root. This is
    // forced destruction, not provider settlement or asynchronous cleanup.
    drop(dispatch);
    drop(owner);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}
