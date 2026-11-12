//! D207/D210 scoped session Q tracers. Option-neutral invariant (D207): a
//! refusing (Session-scope) Q never coexists with a Running session whose root
//! still admits new flows. D210: a stale close identity is a root (identity)
//! fault ⇒ Session scope ⇒ stopped, visible to `state()`, admission closed. A
//! Flow-scope Q keeps the session Running and admitting, refuses no release
//! that began before it, and still ends the session Incomplete.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{NeverOpen, receiving_pair};
use super::super::*;
use crate::fake_ip::FakeIpPool;
use crate::shared::TargetAddr;
use crate::client_tun::compatibility_runtime::SessionDeviceState;

#[tokio::test]
async fn soft_session_q_does_not_keep_admitting_flows_it_cannot_release() {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    assert!(owner.app_meter.is_none(), "non-hard profile: soft release Q");
    let target = TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap());
    let (handle, mut ctx, mut pool) = (server.handle, SocketCtx::new(10000), FakeIpPool::new());
    dispatch.begin_turn().await;
    dispatch.admit_flow(upstream.clone(), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle), ctx: &mut ctx,
        fake_pool: &mut pool, fake_ip: None, target: target.clone(), now: 0,
    });
    let a = owner.opens().slots.iter().flatten().find_map(|slot| match &slot.phase {
        OpenPhase::Flow(flow) => Some(flow.id),
        _ => None,
    }).expect("actual admitted flow A");
    // Real soft Q source while Running: a stale close identity for A.
    assert!(dispatch.request_flow_close(handle, a.epoch.wrapping_add(1), Some(a), None, None));
    let refusing = owner.ledger().session_quarantined();
    let stopping = owner.stop.stopping.load(Ordering::Acquire);
    let state = owner.state();
    // A fresh listener socket asks the same root for an unrelated flow B.
    let b = server.sockets.add(crate::client_tun::build_listener_socket(
        &crate::client_tun::ListenerSpec { local_port: 10001 },
    ));
    dispatch.begin_turn().await;
    let admitted = dispatch.reserve_flow(upstream, target, b, 0).is_some();
    let live = owner.opens().live;
    println!("D207 soft_q refusing={refusing} stopping={stopping} state={state:?} admitted={admitted} live={live}");
    assert!(
        !(refusing && !stopping && admitted),
        "session Q refuses every later release, yet the session is {state:?} (stopping={stopping}) \
         and admitted unrelated flow B (live={live}); B can never release"
    );
    // D210 scope of this source: identity is root authority ⇒ Session.
    assert!(refusing && stopping && !admitted, "stale identity is Session scope");
    assert_eq!(state, SessionDeviceState::Incomplete, "Session Q is visible to state()");
    assert!(owner.ledger().shutdown_deadline.is_some(), "Session Q bounds the end");
}

#[test]
fn flow_scope_q_keeps_session_running_and_refuses_only_later_turns() {
    let owner = SessionControl::default();
    *owner.opens() = OpenCustody::with_flows();
    let before = owner.ledger().flow_q_epoch();
    owner.quarantine_flow(FlowQRow::unheld_for_test());
    let ledger = owner.ledger();
    assert!(ledger.quarantined(), "sticky: the session will end Incomplete");
    assert!(!ledger.session_quarantined() && ledger.shutdown_deadline.is_none());
    // B560: a Turn that began before the raise refuses; a later Turn does not.
    assert!(!ledger.release_q_clear(before));
    assert!(ledger.release_q_clear(ledger.flow_q_epoch()));
    drop(ledger);
    assert!(!owner.stop.stopping.load(Ordering::Acquire));
    assert_eq!(owner.state(), SessionDeviceState::Running);
    // Session scope always carries stop + bounded deadline and refuses all.
    owner.quarantine_quiet();
    let ledger = owner.ledger();
    assert!(ledger.session_quarantined() && ledger.shutdown_deadline.is_some());
    assert!(!ledger.release_q_clear(ledger.flow_q_epoch()));
    drop(ledger);
    assert_eq!(owner.state(), SessionDeviceState::Incomplete);
}

/// One actual admitted non-hard row in a fresh Flow root; returns its index.
pub(super) async fn admit_one(owner: &Arc<SessionControl>) -> usize {
    let (_client, mut server) = receiving_pair().await;
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let (handle, mut ctx, mut pool) = (server.handle, SocketCtx::new(10000), FakeIpPool::new());
    dispatch.begin_turn().await;
    dispatch.admit_flow(Arc::new(NeverOpen::default()), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle), ctx: &mut ctx,
        fake_pool: &mut pool, fake_ip: None,
        target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
    });
    owner.opens().slots.iter().position(Option::is_some).expect("actual admitted flow")
}

#[tokio::test]
async fn flow_q_row_marker_and_root_counter_audit_both_ways() {
    let owner = Arc::new(SessionControl::default());
    let index = admit_one(&owner).await;
    let mut opens = owner.opens();
    let stage = |opens: &mut OpenCustody, stage| match &mut opens.slots.get_mut(index).unwrap().phase {
        OpenPhase::Flow(flow) => flow.stage = stage,
        _ => panic!("actual Flow"),
    };
    stage(&mut opens, FlowStage::Quarantined);
    assert!(opens.audit_flows(), "an unregistered Q stage is legal for one pass");
    assert!(opens.register_flow_q(index, &owner) && opens.flow_q_rows() == 1);
    assert!(opens.register_flow_q(index, &owner) && opens.flow_q_rows() == 1, "registered once");
    assert!(opens.audit_flows());
    // Marker ⇒ stage: a registered row may never regress out of Q.
    stage(&mut opens, FlowStage::Closing);
    assert!(!opens.audit_flows(), "regressed Flow-Q row fails the root audit");
    stage(&mut opens, FlowStage::Quarantined);
    // Counter ⇒ markers: a removed marked row leaves the counter unmatched.
    let row = opens.slots.take(index);
    opens.live -= 1;
    assert!(!opens.audit_flows(), "root counter without its row fails the audit");
    drop(opens);
    std::mem::forget(row);
    assert!(owner.ledger().quarantined() && !owner.ledger().session_quarantined());
}


/// F8/D216: a root audit failure seen by `reserve_flow` is Session scope.
#[tokio::test]
async fn d216_reserve_flow_root_audit_failure_is_session_scope() {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let target = TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap());
    let (handle, mut ctx, mut pool) = (server.handle, SocketCtx::new(10000), FakeIpPool::new());
    dispatch.begin_turn().await;
    dispatch.admit_flow(upstream.clone(), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle), ctx: &mut ctx,
        fake_pool: &mut pool, fake_ip: None, target: target.clone(), now: 0,
    });
    {
        // Root fault: a registered Flow-Q row regressed out of Q.
        let mut opens = owner.opens();
        let index = opens.slots.iter().position(Option::is_some).expect("actual admitted flow");
        let stage = |opens: &mut OpenCustody, stage| match &mut opens.slots.get_mut(index).unwrap().phase {
            OpenPhase::Flow(flow) => flow.stage = stage,
            _ => panic!("actual Flow"),
        };
        stage(&mut opens, FlowStage::Quarantined);
        assert!(opens.register_flow_q(index, &owner));
        stage(&mut opens, FlowStage::Closing);
        assert!(!opens.audit_flows());
    }
    assert!(!owner.ledger().session_quarantined(), "only a Flow Q so far");
    let b = server.sockets.add(crate::client_tun::build_listener_socket(
        &crate::client_tun::ListenerSpec { local_port: 10001 },
    ));
    dispatch.begin_turn().await;
    assert!(dispatch.reserve_flow(upstream, target, b, 0).is_none());
    assert!(owner.ledger().session_quarantined() && owner.stop.stopping.load(Ordering::Acquire),
        "root audit failure at admission ⇒ Session scope");
    assert!(owner.ledger().shutdown_deadline.is_some());
}

/// D216: a preparing row (carrier outside the row) past its stop deadline is
/// Session scope; the row is marked Q and the session stops with a deadline.
#[tokio::test]
async fn d216_preparing_row_past_stop_deadline_is_session_scope() {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let (handle, mut ctx, mut pool) = (server.handle, SocketCtx::new(10000), FakeIpPool::new());
    dispatch.begin_turn().await;
    dispatch.admit_flow(Arc::new(NeverOpen::default()), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle), ctx: &mut ctx,
        fake_pool: &mut pool, fake_ip: None,
        target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
    });
    let index = owner.opens().slots.iter().position(Option::is_some).expect("actual admitted flow");
    {
        let mut opens = owner.opens();
        let OpenPhase::Flow(flow) = &mut opens.slots.get_mut(index).unwrap().phase else {
            panic!("actual Flow")
        };
        flow.preparing = true;
        flow.stop_deadline.open(std::time::Instant::now());
    }
    dispatch.begin_turn().await;
    let _ = std::future::poll_fn(|cx| {
        let mut recv = Box::pin(dispatch.recv(&mut receiver));
        std::task::Poll::Ready(std::future::Future::poll(recv.as_mut(), cx))
    }).await;
    let opens = owner.opens();
    let OpenPhase::Flow(flow) = &opens.slots[index].as_ref().unwrap().phase else {
        panic!("actual Flow")
    };
    assert_eq!(flow.stage, FlowStage::Quarantined);
    drop(opens);
    assert!(owner.ledger().session_quarantined() && owner.stop.stopping.load(Ordering::Acquire),
        "preparing past deadline ⇒ Session scope");
}
