//! Actual two-Root DATA pressure on the original Node and shared hard budget.
//! No synthetic reservation, refund, ACK replay or alternate endpoint. Original
//! Service/Node/Session/Flow own every lifetime; this peer only pauses/resumes App.
use super::*;

fn endpoint(control: &Control) -> quinn::EndpointStats {
    let weak = control.native_node_sample.lock().unwrap().as_ref().cloned().unwrap();
    weak.sample().expect("actual original Node must remain observable").endpoint
}

async fn sample(control: &Control, phase: &str, outbound: &PacketLink) -> (quinn::EndpointStats, usize) {
    let budget = control.backing_budget.as_ref().unwrap();
    let owner = control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
    for contract in owner.flow_contracts() {
        if let Some(queue) = owner.flow_native_chunk_queue_for_test(contract.id) {
            let physical = queue.custody_snapshot().await;
            eprintln!("[N1-A1-Flow] phase={phase} contract={contract:?} physical={physical:?}");
        } else {
            let queue = owner.flow_native_permit_queue_for_test(contract.id).unwrap();
            let physical = queue.snapshot_now();
            queue.assert_stock_credit_for_test(physical);
            eprintln!("[N1-A1-Flow] phase={phase} contract={contract:?} physical={physical:?} permit={:?}", queue.permit_release_audit_for_test());
        }
        eprintln!("[N1-A1-Flow-terminal] phase={phase} terminal={:?} original_stop={:?}",
            owner.flow_native_retained_terminal_for_test(contract.id), owner.flow_original_stop_for_test(contract.id));
    }
    let weak = control.native_node_sample.lock().unwrap().as_ref().cloned().unwrap();
    let node = weak.sample().expect("original Node presence is not clean zero");
    eprintln!("[N1-A1-stock] phase={phase} actual={:?}", owner.flow_stock_inventory_for_test());
    eprintln!("A1 realDATA phase={phase} used={} limit={} Q={} node={:?} app={:?} endpoint={:?} source={:?} flow={:?} first={:?} deadline={:?} external_highwater={}",
        budget.used(), budget.limit(), budget.quarantined(), node.backing,
        control.app_meter.as_ref().unwrap().snapshot(), node.endpoint,
        owner.flow_native_pump_source_read_bytes(), owner.flow_observation(),
        fixture::first_failure_identity(&owner), *control.deadline.lock().unwrap(),
        outbound.high_water_packets());
    (node.endpoint, budget.used())
}

pub(super) async fn exchange(
    streams: &mut [(quinn::SendStream, quinn::RecvStream)],
    apps: &mut Apps,
    control: &Control,
    outbound: &PacketLink,
) {
    const DATA: usize = 8 << 20;
    const MARKER: &[u8] = b"actual-second-flow-after-pressure";
    assert_eq!(streams.len(), 2);
    let budget = control.backing_budget.as_ref().unwrap();
    assert_eq!(budget.limit(), crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(PRESSURE_BUDGET));
    assert_eq!(apps.tcp_reply, b"reply-one");
    assert_eq!(apps.second_reply, b"reply-two");
    let (baseline, baseline_used) = sample(control, "before_App_pause", outbound).await;
    let (first, rest) = streams.split_first_mut().unwrap();
    let payload: Vec<u8> = (0..DATA).map(|i| (i % 251) as u8).collect();
    let write = first.0.write_all(&payload);
    tokio::pin!(write);
    let mut sent = false;
    let mut observed_peak = budget.used();
    // A2 Route 2: the Node charges the protocol tier, which App pressure cannot
    // consume, so honest DATA against a paused App must be held by QUIC flow
    // control and never reach a counted receive Full. Observe a bounded
    // window; remote flow control may legitimately block write_all.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                result = &mut write, if !sent => { result.unwrap(); sent = true; },
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
            observed_peak = observed_peak.max(budget.used());
            assert_eq!(endpoint(control).receive_backing_rejected_datagrams,
                baseline.receive_backing_rejected_datagrams,
                "honest DATA reached Quinn receive Full despite the protocol tier");
            assert!(!budget.quarantined(), "real DATA entered Q under honest pressure");
        }
    }).await;
    let (at_full, full_used) = sample(control, "App_still_paused", outbound).await;
    eprintln!("A1 realDATA original_write_completed={sent} observed_peak={observed_peak} reserve={}", budget.protocol_reserve());
    assert!(!sent, "8MiB DATA must stay flow-controlled while the App is paused");
    assert!(observed_peak <= budget.limit());
    assert_eq!(at_full.receive_backing_rejected_datagrams, baseline.receive_backing_rejected_datagrams);
    assert_eq!(at_full.budget_rejected_datagrams,
        at_full.receive_backing_rejected_datagrams.saturating_add(at_full.owned_event_rejected_datagrams));
    assert!(!budget.quarantined());
    let owner = control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
    assert!(owner.ledger().first_failure.is_none(), "real DATA source already failed before App ACK recovery");
    drop(owner);
    assert_eq!(apps.tcp_reply, b"reply-one", "App has not consumed pressure DATA or generated its ACKs");
    assert_eq!(apps.second_reply, b"reply-two");
    rest[0].0.write_all(MARKER).await.unwrap();
    // The only stimulus change is polling the original App. Its real TCP ACKs
    // return through the original PacketLink/pump/Device/smoltcp, with no replay.
    let mut resumed = 0usize;
    // D40 conservation oracle: the pressure payload is created after the
    // baseline sample, so every delivered byte was charged inside this window;
    // delivering more than the budget ever grew proves actual release.
    let mut max_used = observed_peak.max(full_used);
    let recovery = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                result = &mut write, if !sent => { result.unwrap(); sent = true; },
                _ = async { apps.turn().await; tokio::time::sleep(Duration::from_millis(1)).await; } => {},
            }
            resumed += 1;
            max_used = max_used.max(budget.used());
            if resumed == 1 { sample(control, "after_first_original_App_turn", outbound).await; }
            assert!(!budget.quarantined(), "real ACK/control path entered Q under DATA pressure");
            assert!(apps.tcp_reply.len() <= 9 + DATA && apps.second_reply.len() <= 9 + MARKER.len());
            if sent && apps.tcp_reply.len() == 9 + DATA && apps.second_reply.len() == 9 + MARKER.len() {
                break;
            }
        }
    }).await;
    if recovery.is_err() {
        eprintln!("[N1-A1-recovery-timeout] received={} second_received={} turns={resumed} max_used={max_used}", apps.tcp_reply.len(), apps.second_reply.len());
        sample(control, "original_recovery_timeout", outbound).await;
    }
    recovery.expect("actual App ACK/control path failed to release DATA pressure");
    assert_eq!(&apps.tcp_reply[..9], b"reply-one");
    assert_eq!(&apps.tcp_reply[9..], &payload);
    assert_eq!(&apps.second_reply[..9], b"reply-two");
    assert_eq!(&apps.second_reply[9..], MARKER);
    let delivered = (apps.tcp_reply.len() - 9) + (apps.second_reply.len() - 9);
    let grown = max_used.saturating_sub(baseline_used);
    eprintln!("A1 conservation delivered={delivered} baseline_used={baseline_used} max_used={max_used} grown={grown} released_at_least={}",
        delivered.saturating_sub(grown));
    assert!(delivered > grown,
        "must observe actual shared-budget release without test refund: delivered {delivered} <= growth {grown}");
    sample(control, "two_original_Flows_recovered", outbound).await;
}
