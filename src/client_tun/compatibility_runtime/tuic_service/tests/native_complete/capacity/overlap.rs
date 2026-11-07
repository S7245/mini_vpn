//! Allocation preflight on original Node owners sharing the original Service
//! budget. No production candidate publication/commit or Root receipt is added.
use super::*;
use crate::tuic::{NodeHost, NodeShutdown, NodeStartup};
use std::future::Future;
use std::task::Poll;

#[tokio::test(flavor = "current_thread")]
async fn same_service_budget_old_actual_drain_and_candidate_auth_inventory() {
    let identity = PublicCa::new();
    let server = quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap()
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(LIMIT), DEVICE_SLOTS).unwrap();
    runtime.select_f4096_for_test();
    let budget = bytes::BackingBudget::new(LIMIT).unwrap();
    let saved = control_with_budget(true, Some(budget.clone()));
    let old_parent = business::control(&runtime, saved.app_meter.clone());
    let new_parent = business::control(&runtime, saved.app_meter.clone());
    old_parent.parent_deadline.set(saved.deadline.clone()).unwrap();
    new_parent.parent_deadline.set(saved.deadline.clone()).unwrap();
    let old_sink: Arc<dyn crate::shared::errors::QuietFailureSink> = old_parent.clone();
    let new_sink: Arc<dyn crate::shared::errors::QuietFailureSink> = new_parent.clone();
    let mut old = NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let mut candidate = NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    old.bind_udp_parent(Arc::downgrade(&old_sink)).unwrap();
    candidate.bind_udp_parent(Arc::downgrade(&new_sink)).unwrap();
    let old_sample = old.native_weak_observer_for_test();
    let candidate_sample = candidate.native_weak_observer_for_test();
    let pending = old.retirement_probe_for_test();
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(old.start(), async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peer
            })
        }).await.unwrap();
        peers.push(peer);
        assert_eq!(started, NodeStartup::Ready);
        let auth_cost = budget.used();
        let data = old.data().unwrap();
        let full_cold = budget.used();
        let original = old_sample.sample().unwrap();
        let old_id = original.connections.iter().flatten().find_map(|c| c.actual_id).unwrap();
        assert!(original.connections.iter().flatten().all(|c| c.published));
        drop(data); // No observer holds the actual old transport across drain.
        let deadline = old_parent.request_stop();
        assert!(!saved.stop.stopping.load(Ordering::Acquire));
        assert_eq!(*saved.deadline.lock().unwrap(), None);
        old.request_stop();
        assert_eq!(old.shutdown_deadline_for_test(), Some(deadline));
        let mut drain = Box::pin(old.drive_shutdown());
        tokio::time::timeout_at(deadline, std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = drain.as_mut().poll(cx) {
                panic!("old Node completed before actual Pending witness: {result:?}");
            }
            if pending.lock().unwrap().iter().any(|id| *id == Some(old_id)) {
                Poll::Ready(())
            } else { Poll::Pending }
        })).await.unwrap();
        let old_tail = budget.used();
        let original_deadline = deadline;
        let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut drain => panic!("old actual drain completed before candidate AUTH: {result:?}"),
                result = async {
                    tokio::join!(candidate.start(), async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        peer
                    })
                } => result,
            }
        }).await.unwrap();
        peers.push(peer);
        assert_eq!(started, NodeStartup::Ready);
        // Re-poll the actual future: the passive prior-Pending marker alone
        // cannot establish physical overlap at the completed AUTH boundary.
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }).await;
        assert!(pending.lock().unwrap().iter().any(|id| *id == Some(old_id)));
        let new = candidate_sample.sample().unwrap();
        let ids: Vec<_> = new.connections.iter().flatten().filter_map(|c| c.actual_id).collect();
        assert_eq!(ids.len(), 1);
        assert_ne!(ids[0], old_id);
        assert!(new.connections.iter().flatten().all(|c| !c.published));
        assert_eq!(candidate.udp_cold_observation_for_test(), (false, false, false));
        assert_eq!(*saved.deadline.lock().unwrap(), None);
        assert!(!saved.stop.stopping.load(Ordering::Acquire));
        assert!(!budget.quarantined());
        let overlap = budget.used();
        assert!(overlap > old_tail && overlap <= LIMIT);
        let roots = [old_parent.root_layout_for_test(), new_parent.root_layout_for_test()];
        for root in roots {
            assert_eq!((root.slot_len, root.slot_capacity, root.rows, root.row_bytes), (4224, 4224, 0, 0));
        }
        let old_instances = old_sample.sample().unwrap().instances;
        assert_eq!((old_instances.endpoint.0, old_instances.endpoint.2, old_instances.endpoint.3), (true, true, false));
        assert_eq!((new.instances.connection_records, new.instances.stats_started), (1, 1));
        eprintln!("[N1-capacity-overlap-control] actual_roots={roots:?} old_node={old_instances:?} candidate_node={:?} session_payload_each={} service_payload={} old_actual_drain_pending=true candidate_DATA=false preconstructed_parents=true", new.instances,
            std::alloc::Layout::for_value(old_parent.as_ref()).size(), std::alloc::Layout::for_value(saved.as_ref()).size());
        eprintln!("[N1-capacity-overlap] auth={auth_cost} original_full_cold={full_cold} old_actual_pending={old_tail} old_plus_candidate_auth={overlap} limit={LIMIT} old_id={old_id} candidate_id={} deadline={deadline:?} candidate_DATA=false full_partition_not_claimed=true", ids[0]);
        let finished = tokio::time::timeout_at(deadline, &mut drain).await.unwrap();
        assert_eq!(finished, NodeShutdown::RunningResourcesSettled);
        drop(drain);
        assert_eq!(old.shutdown_deadline_for_test(), Some(original_deadline));
    }).await;
    // Every failure still drives both original saved roots within their own
    // already-installed absolute deadline, without adding a cleanup allowance.
    let mut cleanup = Vec::new();
    for host in [&mut old, &mut candidate] {
        host.request_stop();
        let deadline = host.shutdown_deadline_for_test().unwrap();
        cleanup.push(tokio::time::timeout_at(deadline, host.drive_shutdown()).await);
    }
    eprintln!("[N1-capacity-overlap-cleanup] actual={cleanup:?} charged={} Q={}", budget.used(), budget.quarantined());
    if scenario.is_ok() {
        assert_eq!(cleanup[0], Ok(NodeShutdown::RunningResourcesSettled));
        assert_eq!(cleanup[1], Ok(NodeShutdown::StartupResourcesSettled));
    }
    drop((old, candidate, old_sink, new_sink, old_parent, new_parent, saved));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    for peer in peers { peer.close(0u32.into(), b"capacity preflight complete"); }
    server.close(0u32.into(), b"capacity preflight complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle()).await.unwrap();
    if let Err(payload) = scenario { std::panic::resume_unwind(payload); }
}

#[tokio::test(flavor = "current_thread")]
async fn full_shared_budget_rejects_candidate_preserves_old_and_recovers_real_data() {
    use crate::upstream::ProxyUpstream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let identity = PublicCa::new();
    let server = quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(LIMIT).unwrap();
    let saved = control_with_budget(true, Some(budget.clone()));
    let meter = saved.app_meter.as_ref().unwrap().clone();
    let mut old = NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let mut candidate = NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let sample = old.native_weak_observer_for_test();
    let mut peer = None;
    let mut data = None;
    let mut pressure = None;
    let scenario = capture(async {
        let (started, remote) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(old.start(), async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, &cfg).await;
                peer
            })
        }).await.unwrap();
        peer = Some(remote);
        assert_eq!(started, NodeStartup::Ready);
        data = Some(old.data().unwrap());
        let before = sample.sample().unwrap();
        let ids: Vec<_> = before.connections.iter().flatten().map(|c| c.actual_id).collect();
        let old_stock = budget.used();
        let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
        pressure = Some(bytes::BytesMut::try_zeroed_metered(LIMIT - old_stock - header, &meter).unwrap());
        assert_eq!(budget.used(), LIMIT, "actual backing fills this same original budget");
        assert_eq!(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), candidate.start()).await.unwrap(), NodeStartup::Failed);
        let first = candidate.assert_cold_full_refusal_for_test();
        assert_eq!(budget.used(), LIMIT);
        let after = sample.sample().unwrap();
        assert_eq!(after.connections.iter().flatten().map(|c| c.actual_id).collect::<Vec<_>>(), ids);
        assert_eq!(after.backing, before.backing);
        assert!(after.connections.iter().flatten().all(|c| c.published));
        assert!(!budget.quarantined() && peer.as_ref().unwrap().close_reason().is_none());
        candidate.request_stop();
        let deadline = candidate.shutdown_deadline_for_test().unwrap();
        assert_eq!(tokio::time::timeout_at(deadline, candidate.drive_shutdown()).await.unwrap(), NodeShutdown::FailedSettled);
        assert_eq!(candidate.shutdown_deadline_for_test(), Some(deadline));
        let retained = candidate.assert_cold_full_refusal_for_test();
        assert_eq!(retained.facts(), first.facts());
        assert_eq!(retained.text_bytes(), first.text_bytes());
        assert!(!budget.quarantined());
        drop(pressure.take());
        assert_eq!(budget.used(), old_stock, "actual pressure Drop releases only its original allocation");
        let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
        let payload = b"old Node remains usable after rejected candidate";
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(async {
                let mut stream = data.as_ref().unwrap().open_tcp(&target).await.unwrap();
                stream.write_all(payload).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert_eq!(received, payload);
            }, async {
                let (mut send, mut recv) = peer.as_ref().unwrap().accept_bi().await.unwrap();
                let mut expected = encode_connect(&target);
                expected.extend_from_slice(payload);
                assert_eq!(recv.read_to_end(expected.len()).await.unwrap(), expected);
                send.write_all(payload).await.unwrap();
                send.finish().unwrap();
            });
        }).await.unwrap();
        eprintln!("[N1-capacity-full-refusal] limit={LIMIT} original_cold={old_stock} candidate_endpoint_registered=false first_class={:?} old_real_DATA_recovered=true", first.facts().class);
    }).await;
    drop((pressure.take(), data.take()));
    let mut cleanup = Vec::new();
    for host in [&mut old, &mut candidate] {
        host.request_stop();
        cleanup.push(tokio::time::timeout_at(host.shutdown_deadline_for_test().unwrap(), host.drive_shutdown()).await);
    }
    eprintln!("[N1-capacity-full-cleanup] actual={cleanup:?} charged={} Q={}", budget.used(), budget.quarantined());
    drop((old, candidate, saved, meter));
    if let Some(peer) = peer { peer.close(0u32.into(), b"capacity refusal complete"); }
    server.close(0u32.into(), b"capacity refusal complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle()).await.unwrap();
    if let Err(payload) = scenario { std::panic::resume_unwind(payload); }
    assert_eq!(cleanup, [Ok(NodeShutdown::RunningResourcesSettled), Ok(NodeShutdown::FailedSettled)]);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
