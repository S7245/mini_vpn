//! Real original Service/F4096 capacity measurement. Reuses the original full
//! App/Peer; no fake receipt, slot, role or transport. Counts charged backing
//! separately from configured smoltcp/actor instances and observed peaks.
use super::*;
use super::super::device::{Gate, GatedDevice};
use std::sync::atomic::Ordering;
#[path = "../../../../../../tests/tuic_service/full_fixture.rs"]
mod wire;

// Previously executed actual cold stock census, not a peak-derived allowance.
// Preserve the original 32MiB Service/Node working budget in this NEW full
// concurrency measurement. The existing 8,489,424B pressure oracle is unchanged.
const DEVICE_SLOTS: usize = 16_384; // Same real links as the original full fixture.
const HISTORICAL_FLOW_COLD: usize = 185_544;
const LIMIT: usize = (32 << 20) + wire::F * HISTORICAL_FLOW_COLD;

#[tokio::test(flavor = "current_thread")]
async fn original_native_service_f4096_w128_capacity_and_physical_stop() {
    full_capacity(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_permit_service_f4096_w128_capacity_and_physical_stop() {
    full_capacity(true).await;
}
async fn full_capacity(permit: bool) {
    // A2 step 3: process allocator peak (meaningful only when run alone).
    use crate::quic::honest_bound::tests::counting_alloc as alloc;
    let alloc_base = alloc::reset_peak();
    let node_base = alloc::node_reset_peak();
    let identity = alloc::foreign_sync(PublicCa::new);
    let mut server_config = alloc::foreign_sync(|| identity.server_config());
    let mut transport = quinn::TransportConfig::default();
    // The original full peer permits all F+W stream IDs; no artificial 100-ID
    // ceiling from the small-flow TLS identity helper may mask the target.
    transport.max_concurrent_bidi_streams((wire::TOTAL as u32).into());
    server_config.transport_config(Arc::new(transport));
    // §5.3: the peer's drivers run on their own Foreign thread (attribution.rs).
    let (server, peer_thread) = attribution::foreign_server(server_config);
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"), native_egress_permit: permit.then_some("1"), ..Default::default()
    }));
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap()
        .with_relay_policy(policy).with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(LIMIT), DEVICE_SLOTS).unwrap();
    runtime.select_f4096_for_test();
    let instances_runtime = runtime.clone(); // §5.3 verdict recomputes the production preflight.
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let inbound = PacketLink::bounded(DEVICE_SLOTS);
    let outbound = PacketLink::bounded(DEVICE_SLOTS);
    let link_observers = (inbound.clone(), outbound.clone());
    let mut app = alloc::foreign_sync(|| wire::App::new(LoopbackTunDevice::new(outbound.clone(), inbound.clone())));
    let gate = Arc::new(Gate::default()); // Original Device shutdown held until peer/App retire.
    let actual_gate = gate.clone();
    let metrics = Arc::new(Metrics::default());
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            Ok::<_, ClientError>(GatedDevice {
                inner: LoopbackTunDevice::native_with_factory(DEVICE_SLOTS, 1500, &meter.unwrap(), || Ok((inbound, outbound)))?,
                gate: actual_gate,
            })
        },
        Configuration { tuic: cfg.clone(), composition: () }, runtime,
        metrics.clone(), NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let app_meter = saved.app_meter.clone().unwrap();
    let mut peers = Vec::new();
    let mut current = None;
    let peak = std::cell::Cell::new(budget.used());
    let sample = |phase: &str, flows: usize| {
        let observer = saved.native_node_sample.lock().unwrap().as_ref().unwrap().clone();
        let node = observer.sample().unwrap();
        let used = budget.used();
        peak.set(peak.get().max(used));
        let actual_connections = node.connections.iter().flatten().filter(|c| c.actual_id.is_some()).count();
        eprintln!("[N1-full-capacity] permit={permit} phase={phase} F={flows} charged={used} limit={LIMIT} sampled_peak={} app={:?} node={:?} node_slots={} actual_connections={actual_connections} historical_per_flow_budget_basis={HISTORICAL_FLOW_COLD} configured_socket_payload_each={} process_RSS_not_claimed=true node_domain_alloc_live={} node_domain_alloc_peak={}",
            peak.get(), app_meter.snapshot(), node.backing, node.slots, 2 * 65535,
            alloc::node_live().saturating_sub(node_base), alloc::node_peak().saturating_sub(node_base));
        assert!(used <= LIMIT && !budget.quarantined());
        assert_eq!(actual_connections, 1);
        assert_eq!(node.slots, 4);
        let instances = node.instances;
        assert_eq!((instances.connection_len, instances.stats_limit), (4, 6));
        assert!(instances.connection_capacity >= instances.connection_len);
        assert_eq!(instances.connection_records, actual_connections);
        assert!(instances.stats_len <= instances.stats_limit && instances.stats_capacity >= instances.stats_limit);
        assert!(instances.stats_joined <= instances.stats_started);
        eprintln!("[N1-node-actual-instances] phase={phase} actual={instances:?} record_bytes={} stats_bytes={} monitor={:?} endpoint={:?} udp_owner={} UDP_children_callbacks_Quinn_drivers_independent=true",
            instances.connection_bytes, instances.stats_bytes, instances.monitor, instances.endpoint, instances.udp_owner);
    };
    let scenario = capture(async {
        wire::drive(&mut service, Duration::from_secs(5), alloc::foreign(async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            peers.push(peer.clone());
            current = Some(wire::Peer::new(peer));
        })).await;
        sample("startup-before-App", 0);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        geometry::sample(&mut service, &owner, "startup").await;
        sample("original-main-ready", 0);
        let census = attribution::census_base();
        // First one actual Flow, then the same original 128-Flow waves.
        let mut start = 0;
        for end in std::iter::once(1).chain((128..=wire::F).step_by(128)) {
            let began = std::time::Instant::now();
            alloc::foreign_sync(|| app.connect(start..end));
            wire::drive(&mut service, Duration::from_secs(20), alloc::foreign(async {
                let peer = current.as_mut().unwrap().accept_to(end);
                let app = async { while !app.acked(start..end) { app.turn().await; } };
                tokio::join!(peer, app);
            })).await;
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.live, flow.f, flow.published, flow.running), (end, end, end as u64, end * 5));
            assert!(flow.audit && owner.ledger().first_failure.is_none());
            eprintln!("[N1-full-capacity-wave-time] start={start} end={end} elapsed={:?}", began.elapsed());
            sample("actual-published-wave", end);
            if end == 1 || end == wire::F { geometry::sample(&mut service, &owner, "published").await; }
            start = end;
        }
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, wire::F as u64);
        alloc::foreign_sync(|| app.connect(wire::F..wire::TOTAL));
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.acked(wire::F..wire::TOTAL) || owner.flow_observation().unwrap().waiting != wire::W {
                alloc::foreign(app.turn()).await;
            }
        }).await;
        let waiting: Vec<_> = owner.flow_contracts().into_iter().filter(|r| r.waiting).collect();
        assert_eq!(waiting.len(), wire::W);
        assert!(waiting.iter().all(|r| r.w && !r.f && !r.attempt && !r.published));
        alloc::foreign_sync(|| app.start_c());
        wire::drive(&mut service, Duration::from_secs(5), alloc::foreign(async {
            while !app.c_full() { app.turn().await; }
        })).await;
        attribution::health(&mut service, &mut app, current.as_mut().unwrap()).await;
        let full = owner.flow_observation().unwrap();
        assert_eq!((full.live, full.f, full.w, full.waiting, full.attempts, full.running),
            (wire::TOTAL, wire::F, wire::W, wire::W, wire::F, wire::F * 5));
        assert_eq!((full.factory_started, full.published, full.released, full.joined, full.retry),
            (wire::F as u64, wire::F as u64, 0, 0, 0));
        assert_eq!((full.reserved, full.escrow_bytes), (wire::W * 256 * 1024, wire::W));
        assert!(full.audit && app.c_full());
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        sample("F4096-W128-C-full-healthy-TCP-UDP", wire::F);
        attribution::census_delta(&census, "F4096-W128-C-full");
        geometry::sample(&mut service, &owner, "F4096-W128-C-full").await;
        assert_eq!(owner.flow_contracts().into_iter().filter(|r| r.waiting).collect::<Vec<_>>(), waiting);
        let ids: Vec<_> = owner.flow_contracts().into_iter().filter(|r| r.f).map(|r| r.id).collect();
        let sampled_ids = [ids[0], ids[wire::F / 2], ids[wire::F - 1]];
        let peer_observer = current.as_ref().unwrap().connection.clone();
        handle.request_stop();
        let stop_began = std::time::Instant::now();
        let deadline = saved.deadline.lock().unwrap().unwrap();
        let session_before = owner.ledger().shutdown_deadline;
        let expected_session_deadline = session_before.map_or(deadline, |old| old.min(deadline));
        let mut peer_retired = false;
        let report = tokio::time::timeout_at(deadline, async {
            let retire = async {
                let peer = current.as_mut().unwrap().retire();
                let application = async { while !app.c_closed() { app.turn().await; } };
                alloc::foreign(async { tokio::join!(peer, application) }).await;
                peer_retired = true;
                gate.open();
                std::future::pending::<()>().await;
            };
            let observe = async {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    eprintln!("[N1-full-stop-observe] elapsed={:?} charged={} Q={} first={:?} peer_close={:?} links={:?} flow={:?} deadline={:?}",
                        stop_began.elapsed(), budget.used(), budget.quarantined(), fixture::first_failure_identity(&owner),
                        peer_observer.close_reason(), (link_observers.0.packet_count_for_test(), link_observers.1.packet_count_for_test()),
                        owner.flow_observation(), owner.ledger().shutdown_deadline);
                    let cause = owner.ledger().first_egress_io.as_ref().and_then(|io| io.get_ref())
                        .and_then(|e| e.downcast_ref::<crate::client_tun::downlink_custody::CaptureRefusal>()).copied();
                    eprintln!("[N1-full-stop-typed-egress] original_capture_cause={cause:?}");
                    for id in sampled_ids {
                        eprintln!("[N1-full-stop-fields] id={id:?} fields={:?}", owner.flow_native_retirement_for_test(id));
                    }
                }
            };
            tokio::select! {
                report = service.run() => report,
                () = retire => unreachable!("retirement observer remains Pending"),
                () = observe => unreachable!("passive observer remains Pending"),
            }
        }).await.unwrap();
        assert!(peer_retired);
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(*saved.deadline.lock().unwrap(), Some(deadline));
        assert_eq!(owner.ledger().shutdown_deadline, Some(expected_session_deadline));
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.f, flow.w, flow.running, flow.reserved, flow.escrow_bytes), (0, 0, 0, 0, 0, 0));
        assert_eq!((flow.factory_started, flow.published, flow.released, flow.joined),
            (wire::F as u64, wire::F as u64, wire::TOTAL as u64, (wire::F * 5) as u64));
        assert!(flow.audit && owner.flow_contracts().is_empty());
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        service.completion.as_ref().unwrap()._node.as_ref().unwrap().assert_service_clean_for_test();
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        assert_eq!(app_meter.snapshot().physical_bytes, 0);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        let hard = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(LIMIT);
        eprintln!("[A2-F4096-alloc] permit={permit} process_alloc_peak_delta={} budget_sampled_peak={} app_tier={LIMIT} hard={hard} formula_app_plus_node_gens={} includes_in_process_peer_and_generator=true",
            alloc::peak().saturating_sub(alloc_base), peak.get(),
            LIMIT + crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_reserve() / 2);
        attribution::verdict(permit, (node_base, alloc_base), peak.get(), LIMIT, (&cfg, &instances_runtime));
        eprintln!("[N1-full-capacity-final] exact_peer=4096 wait=128 C=full JOIN=20480 charged=0 nonQ sampled_peak={} limit={LIMIT} deadline={deadline:?} instance_and_overlap_partition_still_separate=true", peak.get());
    }).await;
    if scenario.is_err() {
        let owner = saved.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
        eprintln!("[N1-full-capacity-before-failure-cleanup] peer={} charged={} Q={} flow={:?} first={:?} deadline={:?}",
            current.as_ref().map_or(0, wire::Peer::count), budget.used(), budget.quarantined(),
            owner.as_ref().and_then(|o| o.flow_observation()),
            owner.as_ref().and_then(|owner| fixture::first_failure_identity(owner)), *saved.deadline.lock().unwrap());
        drop(current.take());
        gate.open();
        handle.request_stop();
        let deadline = saved.deadline.lock().unwrap().unwrap();
        let _ = capture(async {
            fixture::observe_failed_native_stop(service.run(), &handle, deadline, None).await;
            eprintln!("[N1-full-capacity-failure] charged={} Q={} deadline={deadline:?} actual_deadline={:?} app={:?}",
                budget.used(), budget.quarantined(), *saved.deadline.lock().unwrap(), app_meter.snapshot());
        }).await;
    }
    drop((current, service, handle));
    let drain = capture(alloc::foreign(wire::drain(server, peers))).await;
    drop(peer_thread);
    scenario.unwrap_or_else(|p| std::panic::resume_unwind(p));
    drain.unwrap_or_else(|p| std::panic::resume_unwind(p));
}

mod overlap;
mod geometry;
mod attribution;
