//! D147 (A') close-tail settlement on the real admitted flow row: the 15s
//! deadline counts from the last App progress, expiry resets only a flow whose
//! latest physical pass of the same stage waited on the App alone, and every
//! other expiry (uncertain custody, stale stage, second expiry) is sticky Q.
//! Service-level tracers: tuic_service/tests/native_n4_isolation.rs (M5(iii)).
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{NeverOpen, receiving_pair};
use super::super::*;
use crate::fake_ip::FakeIpPool;
use crate::shared::TargetAddr;
use std::collections::HashMap;
use std::time::Duration;

const TAIL: Duration = Duration::from_secs(15);

/// One admitted flow row, closed by a remote EOF (`stop_deadline` = now+15s).
async fn closed_flow() -> (Arc<SessionControl>, FlowIdentity) {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    let mut pool = FakeIpPool::new();
    dispatch.begin_turn().await;
    dispatch.admit_flow(
        Arc::new(NeverOpen::default()),
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
        let OpenPhase::Flow(flow) = &opens.slots[0].as_ref().unwrap().phase else {
            panic!("actual admission");
        };
        flow.id
    };
    let mut opens = owner.opens();
    let flow = opens.flow_mut(id).unwrap();
    flow.close(("remote_to_local", "remote_eof"));
    assert_eq!(flow.stage, FlowStage::Closing);
    assert_eq!(
        flow.stop_deadline,
        Some(tokio::time::Instant::now().into_std() + TAIL)
    );
    drop(opens);
    (owner, id)
}

fn drive(
    owner: &SessionControl,
    id: FlowIdentity,
) -> (FlowStage, bool, Option<std::time::Instant>) {
    let mut opens = owner.opens();
    let flow = opens.flow_mut(id).unwrap();
    flow.drive(&mut std::task::Context::from_waker(std::task::Waker::noop()));
    (flow.stage, flow.tail.aborted(), *flow.stop_deadline)
}

fn with_flow(owner: &SessionControl, id: FlowIdentity, f: impl FnOnce(&mut FlowTransaction)) {
    f(owner.opens().flow_mut(id).unwrap());
}

#[tokio::test(start_paused = true)]
async fn uncertain_custody_expiry_stays_sticky_q() {
    let (owner, id) = closed_flow().await;
    // The latest physical pass proved no App-only wait (cleared at pass start).
    with_flow(&owner, id, |flow| {
        flow.app_tail_wait(64);
        flow.clear_app_tail_wait();
    });
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Quarantined);
}

#[tokio::test(start_paused = true)]
async fn stale_app_wait_of_another_stage_stays_sticky_q() {
    let (owner, id) = closed_flow().await;
    with_flow(&owner, id, |flow| {
        flow.app_tail_wait(64);
        flow.stage = FlowStage::Rearming;
    });
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Quarantined);
}

#[tokio::test(start_paused = true)]
async fn idle_app_only_expiry_aborts_tail_once_then_q() {
    let (owner, id) = closed_flow().await;
    with_flow(&owner, id, |flow| flow.app_tail_wait(64));
    tokio::time::advance(TAIL).await;
    let now = tokio::time::Instant::now().into_std();
    let (stage, aborted, deadline) = drive(&owner, id);
    assert_eq!(
        (stage, aborted, deadline),
        (FlowStage::Closing, true, Some(now + TAIL))
    );
    // The remaining settlement gets one fresh tail; a second expiry is Q even
    // if the pass still reports an App-only wait.
    with_flow(&owner, id, |flow| flow.app_tail_wait(64));
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Quarantined);
}

#[tokio::test(start_paused = true)]
async fn app_progress_rearms_the_tail_deadline() {
    let (owner, id) = closed_flow().await;
    let closed = tokio::time::Instant::now().into_std();
    let deadline = |owner: &SessionControl| owner.opens().flow_mut(id).unwrap().stop_deadline;
    with_flow(&owner, id, |flow| flow.app_tail_wait(100)); // baseline, not progress
    assert_eq!(deadline(&owner), Some(closed + TAIL));
    tokio::time::advance(Duration::from_secs(10)).await;
    with_flow(&owner, id, |flow| flow.app_tail_wait(100)); // no drop: no progress
    assert_eq!(deadline(&owner), Some(closed + TAIL));
    with_flow(&owner, id, |flow| flow.app_tail_wait(60)); // App acknowledged 40 bytes
    let progressed = tokio::time::Instant::now().into_std();
    assert_eq!(deadline(&owner), Some(progressed + TAIL));
    tokio::time::advance(Duration::from_secs(5)).await;
    with_flow(&owner, id, |flow| flow.app_tail_wait(80)); // growth is not progress
    assert_eq!(deadline(&owner), Some(progressed + TAIL));
    tokio::time::advance(Duration::from_secs(9)).await; // closed+24s > closed+15s
    assert_eq!(
        drive(&owner, id).1,
        false,
        "progressing reader cut at the FIN deadline"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(drive(&owner, id).1, true);
}

/// D152 A1: a provider that keeps the original TcpOpenAttempt deadline
/// contract (fixed on first use, may only become earlier) and holds its lease
/// in `Retiring` until `retire` is set.
struct RetiringProvider {
    retire: Arc<std::sync::atomic::AtomicBool>,
    deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}
impl RetiringProvider {
    fn tighten(&self, deadline: std::time::Instant) {
        let mut held = self.deadline.lock().unwrap();
        *held = Some(held.map_or(deadline, |old| old.min(deadline)));
    }
}
impl TcpOpenAttempt<OpenedOwnedTcp> for RetiringProvider {
    fn step(&mut self, _: &mut std::task::Context<'_>, _: std::time::Instant) {}
    fn view(&self) -> crate::upstream::TcpOpenView {
        let now = tokio::time::Instant::now().into_std();
        let retired = self.retire.load(std::sync::atomic::Ordering::Acquire);
        let late = self.deadline.lock().unwrap().is_some_and(|d| now >= d);
        crate::upstream::TcpOpenView {
            phase: match (retired, late) {
                (true, _) => TcpOpenPhase::Settled,
                (false, true) => TcpOpenPhase::Quarantined,
                (false, false) => TcpOpenPhase::Retiring,
            },
            lease_held: !retired,
            retirement_pending: !retired,
            connect_bytes: 0,
            connect_written: 0,
        }
    }
    fn cancel(&mut self, _: TcpOpenCancel, deadline: std::time::Instant) {
        self.tighten(deadline);
    }
    fn take_carrier(&mut self, _: std::time::Instant) -> Option<OpenedOwnedTcp> {
        None
    }
    fn settle(
        &mut self,
        _: &mut std::task::Context<'_>,
        _: std::time::Instant,
        deadline: std::time::Instant,
    ) {
        self.tighten(deadline);
    }
    fn failure(&self) -> Option<&crate::shared::ClientError> {
        None
    }
}

type ProviderFacts = (
    Arc<std::sync::atomic::AtomicBool>,
    Arc<std::sync::Mutex<Option<std::time::Instant>>>,
);

/// A closed flow whose producer stopped and whose only remaining work is the
/// provider's own retirement (`Retiring`, lease held), as after a relay
/// half-close guard on a connection that later died.
async fn provider_retiring_flow() -> (Arc<SessionControl>, FlowIdentity, ProviderFacts) {
    let (owner, id) = closed_flow().await;
    let facts: ProviderFacts = Default::default();
    with_flow(&owner, id, |flow| {
        flow.producer_stopped = true;
        flow.attempt = Some(Box::new(RetiringProvider {
            retire: facts.0.clone(),
            deadline: facts.1.clone(),
        }));
    });
    assert_eq!(drive(&owner, id).0, FlowStage::Closing); // cancel + provider proof
    (owner, id, facts)
}

/// D181 (amends D152's value): peer idle + own idle (one-way loss re-arms
/// ours, RFC 9000 §10.1) + the Node's two bounded reconnect handshakes before
/// it releases the dead connection (D170) + 1 s margin; derived, never literal.
#[test]
fn provider_loss_bound_is_derived_from_its_parts() {
    let idle = Duration::from_secs(crate::quic::QUIC_MAX_IDLE_SECS);
    let release = crate::tuic::TUIC_RECONNECT_TIMEOUT * 2;
    assert_eq!(PROVIDER_LOSS_BOUND, idle * 2 + release + Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn provider_only_expiry_extends_once_then_q() {
    let closed = tokio::time::Instant::now().into_std();
    let (owner, id, facts) = provider_retiring_flow().await;
    tokio::time::advance(TAIL).await;
    let (stage, aborted, deadline) = drive(&owner, id);
    let extended = closed + TAIL + PROVIDER_LOSS_BOUND;
    assert_eq!(
        (stage, aborted, deadline),
        (FlowStage::Closing, false, Some(extended))
    );
    // The provider was given the same bound on first use, not the bare tail.
    assert_eq!(*facts.1.lock().unwrap(), Some(extended));
    tokio::time::advance(PROVIDER_LOSS_BOUND - Duration::from_millis(1)).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Closing);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        drive(&owner, id).0,
        FlowStage::Quarantined,
        "second expiry is Q"
    );
}

#[tokio::test(start_paused = true)]
async fn provider_retiring_within_the_extension_releases_normally() {
    let (owner, id, facts) = provider_retiring_flow().await;
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Closing);
    tokio::time::advance(Duration::from_secs(10)).await;
    facts.0.store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(drive(&owner, id).0, FlowStage::Rearming);
}

#[tokio::test(start_paused = true)]
async fn provider_wait_with_uncertain_custody_stays_q() {
    let (owner, id, _facts) = provider_retiring_flow().await;
    with_flow(&owner, id, |flow| flow.preparing = true);
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Quarantined);
}

#[tokio::test(start_paused = true)]
async fn app_abort_then_provider_wait_is_one_expiry_only() {
    let (owner, id) = closed_flow().await;
    with_flow(&owner, id, |flow| flow.app_tail_wait(64));
    tokio::time::advance(TAIL).await;
    assert!(drive(&owner, id).1, "App-only expiry aborts the tail");
    let facts: ProviderFacts = Default::default();
    with_flow(&owner, id, |flow| {
        flow.producer_stopped = true;
        flow.attempt = Some(Box::new(RetiringProvider {
            retire: facts.0.clone(),
            deadline: facts.1.clone(),
        }));
    });
    assert_eq!(drive(&owner, id).0, FlowStage::Closing);
    tokio::time::advance(TAIL).await;
    assert_eq!(
        drive(&owner, id).0,
        FlowStage::Quarantined,
        "second expiry is Q"
    );
}

/// D166 T5/T4: a limit the grant already answered (the expired parent limit,
/// re-offered by a later Native disposal or uplink/cold settlement) cannot pull
/// the extended deadline back into the past.
#[tokio::test(start_paused = true)]
async fn consumed_limit_after_extension_keeps_the_grant() {
    let closed = tokio::time::Instant::now().into_std();
    let (owner, id, _facts) = provider_retiring_flow().await;
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Closing);
    with_flow(&owner, id, |flow| flow.stop_deadline.limit(closed + TAIL));
    let (stage, _, deadline) = drive(&owner, id);
    assert_eq!(
        (stage, deadline),
        (FlowStage::Closing, Some(closed + TAIL + PROVIDER_LOSS_BOUND)),
        "a consumed limit undid the one A1 extension"
    );
}

/// D166 T5: a limit later than the answered expiry is new and still shortens
/// the grant (fail-closed), after either kind of grant.
#[tokio::test(start_paused = true)]
async fn new_limit_after_grant_still_shortens() {
    let (owner, id, _facts) = provider_retiring_flow().await;
    tokio::time::advance(TAIL).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Closing);
    let stop = tokio::time::Instant::now().into_std() + Duration::from_secs(1);
    with_flow(&owner, id, |flow| flow.stop_deadline.limit(stop));
    assert_eq!(drive(&owner, id).2, Some(stop));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(drive(&owner, id).0, FlowStage::Quarantined);

    let (owner, id) = closed_flow().await;
    with_flow(&owner, id, |flow| flow.app_tail_wait(64));
    tokio::time::advance(TAIL).await;
    assert!(drive(&owner, id).1, "App-only expiry aborts the tail");
    let stop = tokio::time::Instant::now().into_std() + Duration::from_secs(2);
    with_flow(&owner, id, |flow| flow.stop_deadline.limit(stop));
    assert_eq!(drive(&owner, id).2, Some(stop));
}

/// F4: App progress re-arms the tail but never past an applied external limit.
#[test]
fn rearm_after_limit_never_exceeds_the_limit() {
    let t = std::time::Instant::now();
    let mut deadline = super::super::stop_deadline::StopDeadline::default();
    deadline.open(t + TAIL);
    deadline.limit(t + Duration::from_secs(2));
    deadline.rearm(t + Duration::from_secs(30));
    assert_eq!(deadline, Some(t + Duration::from_secs(2)));
}

/// F4: a limit the grant already answered does not clamp later progress.
#[test]
fn consumed_limit_does_not_clamp_a_granted_rearm() {
    let t = std::time::Instant::now();
    let mut deadline = super::super::stop_deadline::StopDeadline::default();
    deadline.open(t + TAIL);
    deadline.limit(t + Duration::from_secs(2));
    deadline.grant(t + Duration::from_secs(20));
    deadline.rearm(t + Duration::from_secs(25));
    assert_eq!(deadline, Some(t + Duration::from_secs(25)));
}

impl SessionControl {
    /// D152 (A1) Service-tracer view of one Flow's close tail: the provider
    /// deadline fixed at its first cancel (tail + PROVIDER_LOSS_BOUND) and
    /// whether the one provider-only extension was used.
    pub(in crate::client_tun::compatibility_runtime) fn flow_provider_tail_for_test(
        &self,
        id: FlowIdentity,
    ) -> Option<(Option<tokio::time::Instant>, bool)> {
        self.opens().flow_mut(id).map(|flow| {
            let deadline = flow.tail.provider_deadline;
            (deadline.map(tokio::time::Instant::from_std), flow.tail.extended)
        })
    }

    /// F9: Service tracers read the production bound instead of a literal.
    pub(in crate::client_tun::compatibility_runtime) fn provider_loss_bound_for_test(&self) -> Duration {
        PROVIDER_LOSS_BOUND
    }
}
