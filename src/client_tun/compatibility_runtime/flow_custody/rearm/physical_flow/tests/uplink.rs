//! Actual smoltcp bytes and original Receiving/W rollback. The refusing upstream
//! is never invoked: this boundary precedes provider admission and owns no task.
use super::super::*;
use super::admission::{NeverOpen, receiving_pair};
use crate::client_tun::compatibility_runtime::flow_custody::transaction::{OpenCustody, OpenEvent, OpenPhase};
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "current_thread")]
async fn hard_initial_full_rolls_back_only_unreceived_w_then_preserves_native_escrow_identity() {
    let (_client, mut server) = receiving_pair().await;
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut control = SessionControl::default();
    control.app_meter = Some(meter.clone());
    let owner = Arc::new(control);
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    let epoch = contexts[&handle].conn_epoch;
    let mut pool = FakeIpPool::new();
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - header, &meter).unwrap();
    for _ in 1..=3 {
        dispatch.begin_turn().await;
        dispatch.admit_flow(upstream.clone(), FlowAdmission {
            handle, socket: server.sockets.get_mut::<TcpSocket>(handle),
            ctx: contexts.get_mut(&handle).unwrap(), fake_pool: &mut pool, fake_ip: None,
            target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
        });
        let opens = owner.opens();
        assert!(opens.audit_flows());
        assert_eq!(opens.live, 0);
        let accounting = opens.flow.as_ref().unwrap();
        assert_eq!((accounting.w, accounting.f, accounting.bytes, accounting.escrow_bytes), (0, 0, 0, 0));
        assert!(opens.slots.iter().all(Option::is_none));
        drop(opens);
        assert_eq!(contexts[&handle].conn_epoch, epoch);
        assert!(contexts[&handle].flow_mirror.is_none());
        assert_eq!(server.sockets.get_mut::<TcpSocket>(handle).peek(64).unwrap(), b"untouched-rx");
        assert_eq!(budget.used(), budget.limit());
        assert!(!budget.quarantined() && owner.ledger().first_failure.is_none());
        assert!(owner.ledger().shutdown_deadline.is_none());
    }
    drop(pressure);
    dispatch.begin_turn().await;
    dispatch.admit_flow(upstream.clone(), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle),
        ctx: contexts.get_mut(&handle).unwrap(), fake_pool: &mut pool, fake_ip: None,
        target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
    });
    let id = contexts[&handle].flow_mirror.as_ref().unwrap().id;
    assert_eq!(id.generation, 4, "Full rollback must not rewind issued generations");
    let original = {
        let mut opens = owner.opens();
        assert!(opens.audit_flows());
        let flow = opens.flow_mut(id).unwrap();
        assert_eq!((flow.escrow_bytes, flow.physical, flow.w_held, flow.f_held), (12, true, true, false));
        let crate::client_tun::uplink::Payload::Native(bytes) = &flow.escrow else { panic!("original hard escrow must carry actual backing") };
        assert!(bytes.backing_meter().unwrap().same_authority(&meter));
        bytes.allocation_identity().unwrap()
    };
    let charged = budget.used();
    let command = {
        let mut opens = owner.opens();
        opens.flow_mut(id).unwrap().escrow.clone().into_command()
    };
    let RelayCommand::NativeData(bytes) = command else { panic!("original publication keeps typed backing") };
    assert_eq!(bytes.allocation_identity(), Some(original));
    assert_eq!(budget.used(), charged);
    drop(bytes);
    assert_eq!(budget.used(), charged, "original escrow still owns the actual allocation");
    assert_eq!(server.sockets.get::<TcpSocket>(handle).recv_queue(), 0);
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);

    owner.request_stop();
    settle(&dispatch, &owner, &mut server, &mut contexts, &mut pool).await;
    assert!(owner.opens().slots.iter().all(|slot| !matches!(slot.as_ref().map(|s| &s.phase), Some(OpenPhase::Flow(_)))));
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined() && owner.ledger().first_failure.is_none());
}

async fn settle(
    dispatch: &OpenDispatch, owner: &Arc<SessionControl>,
    server: &mut super::admission::Endpoint,
    contexts: &mut HashMap<SocketHandle, SocketCtx>, pool: &mut FakeIpPool,
) {
    let (global_tx, _events) = crate::client_tun::relay_channel::channel(4);
    let (action_tx, _actions) = mpsc::channel(4);
    let metrics = crate::metrics::Metrics::default();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while owner.flow_observation().unwrap().live != 0 {
            dispatch.begin_turn().await;
            let event = std::future::poll_fn(|cx| std::task::Poll::Ready(
                match dispatch.poll_flows(cx) { std::task::Poll::Ready(event) => event, std::task::Poll::Pending => None }
            )).await;
            if let Some(OpenEvent::Flow(id)) = event {
                dispatch.service_flow(id, &mut crate::client_tun::HandshakeCompletion {
                    sockets: &mut server.sockets, socket_ctxs: contexts,
                    global_tx: &global_tx, owned_action_tx: &action_tx,
                    fake_pool: pool, now_secs: 0, metrics_handle: &metrics,
                    downlink_backpressure: DownlinkBackpressureConfig::default(), relay_read_hard_pause: false,
                });
            }
            let (flushed, _) = crate::client_tun::poll_egress_cycle(
                &mut server.device, &mut server.iface, &mut server.sockets,
                contexts, None, "initial_uplink_shutdown", None,
            ).await;
            flushed.unwrap();
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn hard_initial_too_large_keeps_parent_deadline_and_one_flow_failure() {
    let (_client, mut server) = receiving_pair().await;
    let cost = bytes::BytesMut::metered_copy_layout_for_len(12).unwrap();
    let budget = bytes::BackingBudget::new(cost - 1).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut control = SessionControl::default();
    control.app_meter = Some(meter);
    let parent = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    control.parent_deadline.set(Arc::new(std::sync::Mutex::new(Some(parent)))).unwrap();
    let owner = Arc::new(control);
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    let mut pool = FakeIpPool::new();
    dispatch.begin_turn().await;
    dispatch.admit_flow(upstream.clone(), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle),
        ctx: contexts.get_mut(&handle).unwrap(), fake_pool: &mut pool, fake_ip: Some(Ipv4Addr::new(198, 18, 0, 2)),
        target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
    });
    let id = contexts[&handle].flow_mirror.as_ref().unwrap().id;
    let deadline = {
        let mut opens = owner.opens();
        let flow = opens.flow_mut(id).unwrap();
        assert_eq!(flow.stage, FlowStage::Closing);
        assert_eq!(flow.rejection, Some(crate::client_tun::compatibility_runtime::flow_custody::transaction::FlowRejection::ReportedNativeUplink(bytes::MeteredCopyError::TooLarge)));
        assert!(flow.escrow.is_empty() && flow.attempt.is_none() && flow.roles.is_none());
        flow.stop_deadline
    };
    assert_eq!(server.sockets.get_mut::<TcpSocket>(handle).peek(64).unwrap(), b"untouched-rx");
    assert!(!owner.stop.stopping.load(Ordering::Acquire), "one rejected Flow cannot stop the Session");
    let facts = owner.ledger().first_failure.as_ref().unwrap().facts().clone();
    assert_eq!(facts.origin, crate::client_tun::compatibility_runtime::flow_custody::transaction::open_origin(
        id.index, id.epoch, crate::shared::errors::FailurePhase::TcpOpen));
    assert_eq!(facts.io_kind, Some(std::io::ErrorKind::WouldBlock));
    settle(&dispatch, &owner, &mut server, &mut contexts, &mut pool).await;
    assert_eq!(owner.ledger().first_failure.as_ref().unwrap().facts(), &facts);
    assert_eq!(owner.ledger().additional_failures, 0);
    assert_eq!(owner.opens().flow.as_ref().unwrap().first_rejection,
        Some(crate::client_tun::compatibility_runtime::flow_custody::transaction::FlowRejection::ReportedNativeUplink(bytes::MeteredCopyError::TooLarge)));
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined() && !owner.stop.stopping.load(Ordering::Acquire));
    assert_eq!(deadline, Some(parent.into_std()), "a preexisting parent deadline cannot restart at now+15");
}

#[tokio::test(flavor = "current_thread")]
async fn hard_initial_quarantine_keeps_parent_deadline_without_source_io() {
    let (_client, mut server) = receiving_pair().await;
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut control = SessionControl::default();
    control.app_meter = Some(meter.clone());
    let parent = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    control.parent_deadline.set(Arc::new(std::sync::Mutex::new(Some(parent)))).unwrap();
    let owner = Arc::new(control);
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let handle = server.handle;
    let mut ctx = SocketCtx::new(10000);
    let mut pool = FakeIpPool::new();
    meter.quarantine();
    dispatch.begin_turn().await;
    dispatch.admit_flow(upstream.clone(), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle), ctx: &mut ctx,
        fake_pool: &mut pool, fake_ip: None,
        target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
    });
    let id = ctx.flow_mirror.as_ref().unwrap().id;
    assert_eq!(server.sockets.get_mut::<TcpSocket>(handle).peek(64).unwrap(), b"untouched-rx");
    assert_eq!(owner.ledger().shutdown_deadline, Some(parent));
    assert!(owner.ledger().first_failure.is_none());
    assert!(owner.stop.stopping.load(Ordering::Acquire));
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    let mut opens = owner.opens();
    let flow = opens.flow_mut(id).unwrap();
    assert_eq!(flow.stage, FlowStage::Quarantined);
    assert!(flow.escrow.is_empty() && flow.roles.is_none() && flow.attempt.is_none());
    assert_eq!(flow.stop_deadline, Some(parent.into_std()));
    // Deliberately unresolved Q: no fake receipt, factory or detached task.
}

/// F5 (D221/D222): a hard-profile Q row still holding native escrow on the
/// shared `BackingBudget` is Session scope at both Flow-Q entries
/// (`quarantine_flow` and root registration), so a sticky row cannot starve
/// siblings while the Session reports Running (D221 starvation evidence).
async fn hard_q_row_holding_escrow(via_registration: bool) {
    let (_client, mut server) = receiving_pair().await;
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut control = SessionControl::default();
    control.app_meter = Some(meter.clone());
    let owner = Arc::new(control);
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let handle = server.handle;
    let mut ctx = SocketCtx::new(10000);
    let mut pool = FakeIpPool::new();
    dispatch.begin_turn().await;
    dispatch.admit_flow(upstream.clone(), FlowAdmission {
        handle, socket: server.sockets.get_mut::<TcpSocket>(handle), ctx: &mut ctx,
        fake_pool: &mut pool, fake_ip: None,
        target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()), now: 0,
    });
    let a = ctx.flow_mirror.as_ref().unwrap().id;
    let cost = bytes::BytesMut::metered_copy_layout_for_len(12).unwrap();
    assert_eq!(budget.used(), cost, "A's native escrow is charged on the shared budget");
    assert!(!owner.ledger().quarantined());
    {
        let mut opens = owner.opens();
        let flow = opens.flow_mut(a).unwrap();
        if via_registration { flow.stage = FlowStage::Quarantined; } else { flow.quarantine_flow(&owner); }
    }
    dispatch.begin_turn().await;
    let _ = std::future::poll_fn(|cx| std::task::Poll::Ready(dispatch.poll_flows(cx))).await;
    {
        let opens = owner.opens();
        assert!(opens.audit_flows());
        assert_eq!(opens.flow_q_rows(), 1, "the row stays root-registered and sticky");
        assert_eq!(opens.flow.as_ref().unwrap().escrow_bytes, 12);
    }
    let ledger = owner.ledger();
    assert!(ledger.session_quarantined(), "hard row holding budget bytes ⇒ Session scope (D222)");
    assert!(ledger.shutdown_deadline.is_some());
    drop(ledger);
    assert!(owner.stop.stopping.load(Ordering::Acquire));
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    // Deliberately unresolved Q: the Session drains under its bounded deadline.
}

#[tokio::test(flavor = "current_thread")]
async fn d222_hard_quarantine_flow_holding_escrow_is_session_scope() {
    hard_q_row_holding_escrow(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn d222_hard_registered_q_row_holding_escrow_is_session_scope() {
    hard_q_row_holding_escrow(true).await;
}
