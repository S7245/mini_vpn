//! True Service partial TUN flush: the original device delivers one TCP packet,
//! keeps its queued tail, and returns EIO to the original ledger/Root path.
use super::*;
mod custody;
const REPLY: [u8; 65536] = [0x5a; 65536];
use crate::client_tun::downlink_custody::{EgressLedgerEventKind, EgressRefundSource};
use crate::upstream::{NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources};
use fixture::{Apps, auth, capture, config};

#[tokio::test(flavor = "current_thread")]
async fn native_permit_partial_actual_flush_refunds_only_proved_prefix_and_retains_tail() {
    partial_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_permit_partial_actual_flush_preserves_original_eio_prefix_and_tail() {
    partial_case(true).await;
}

async fn partial_case(hard: bool) {
    let profile = fixture::ServiceProfile::new(hard);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: Some("1"),
        ..Default::default()
    }));
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::PermitPump);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let slots = if hard { 64 } else { 128 };
    let inbound = PacketLink::bounded(slots);
    let outbound = PacketLink::bounded(slots);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let partial = Arc::new(crate::harness::PartialFlushProbe::default());
    let mut service = profile.service_with_partial_probe(
        inbound,
        outbound.clone(),
        cfg.clone(),
        runtime,
        Some(partial.clone()),
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    if hard {
        assert!(saved.backing_budget.is_some() && saved.app_meter.is_some());
    }
    let mut peer = None;
    let mut stream = None;
    let mut held_recv = None;
    let mut original_queue = None;
    let mut backing = None;
    let mut writer = None;
    let mut first_identity = None;
    let mut inflight_after_fault = None;
    let mut original_owner = None;
    let mut original_id = None;
    let mut original_session_deadline = None;
    let mut retained_inventory = None;
    let mut retained_leased = None;
    let mut cut_sample = None;
    let mut cut_packet = None;
    let mut wire_total = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream = Some((send, recv));
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 5 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                original_id = Some(contracts[0].id);
                                original_owner = Some(owner.clone());
                                original_queue = owner.flow_native_permit_queue();
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Permit Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let queue = original_queue.as_ref().unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let bridge = owner.relay_event_test_bridge().unwrap();
        if hard { bridge.arm_distinct_flush_probe(queue.clone()); }
        else { bridge.arm_egress_probe(queue.clone()); }
        assert_eq!(queue.custodied_released_bytes_for_test(), Some(0));
        assert!(app.tcp_reply.is_empty());
        partial.arm();
        let (send, recv) = stream.take().unwrap();
        writer = Some(tokio::spawn(async move {
            let mut send = send;
            send.write_all(&REPLY).await.unwrap();
            send
        }));
        held_recv = Some(recv);

        let (flushed, remaining_packets) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                std::future::poll_fn(|cx| {
                    let mut run = std::pin::pin!(service.run());
                    assert!(std::future::Future::poll(run.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                }).await;
                if let Some(witness) = partial.witness() {
                    let (samples, overflow, ambiguous) = bridge.egress_probe_samples();
                    eprintln!("[N1-partial-witness] samples={} overflow={overflow} ambiguous={ambiguous} first={:?} last={:?}", samples.len(), samples.first(), samples.last());
                    break witness;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("real partial TCP payload flush did not fire");
        assert!(flushed.payload_bytes > 0 && remaining_packets > 0);
        assert!(outbound.packets_for_test().iter()
            .filter_map(|packet| crate::device::summarize_flushed_ip_tcp_packet(packet))
            .any(|packet| packet == flushed));

        let observed = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                // The actual IO result/capture may already be committed in the
                // witness's poll. Inspect it before driving unrelated later turns.
                let (samples, overflow, ambiguous) = bridge.egress_probe_samples();
                assert!(!overflow && !ambiguous,
                    "original egress probe: samples={} overflow={overflow} ambiguous={ambiguous} first={:?} last={:?}",
                    samples.len(), samples.first(), samples.last());
                if let Some(sample) = samples.last() {
                    if sample.ledger.quarantined && sample.ledger.refunded > 0 {
                        break *sample;
                    }
                }
                std::future::poll_fn(|cx| {
                    let mut run = std::pin::pin!(service.run());
                    assert!(std::future::Future::poll(run.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                }).await;
                tokio::task::yield_now().await;
            }
        }).await.expect("original ledger did not retain partial flush proof");
        assert_eq!(observed.mirror_receipt, Some(false));
        let ledger = observed.ledger;
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            while owner.flow_observation().unwrap().joined != 5 {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
            }
        }).await.expect("original five roles did not join after partial flush");
        let audit = queue.permit_release_audit_for_test().unwrap();
        let physical = hard.then(|| {
            let id = original_id.unwrap();
            assert_eq!(observed.handle, id.handle);
            let identity = crate::tcp_downlink_pump::PermitQueueIdentity::from_queue(queue);
            let dispatcher = owner.flow_retained_permits_for_test(id, &identity).unwrap();
            custody::inspect(&owner, &bridge, queue, (id.handle,id.epoch), observed.ledger, dispatcher)
        });
        let proved = if hard { custody::wire_prefix(&outbound, flushed, ledger.origin, partial.native_cut_index().unwrap()) }
            else { flushed.payload_bytes };
        retained_inventory = physical;
        retained_leased = Some(audit.leased);
        cut_sample = Some(observed);
        cut_packet = Some(flushed);
        wire_total = Some(proved);
        assert!(!audit.interrupted);
        assert_eq!(audit.consumed, audit.released + audit.leased);
        assert_eq!(audit.emitted, audit.consumed + audit.discarded + audit.queued);
        assert_eq!(audit.released,
            audit.packet_flush + audit.ack_drain + audit.explicit_other + audit.dropped);
        assert_eq!(ledger.refunded, proved as u64);
        assert!(ledger.admitted > ledger.refunded);
        assert_eq!(audit.packet_flush, proved);
        assert_eq!(audit.ack_drain, 0);
        if !hard { assert_eq!(audit.leased as u64, ledger.admitted - ledger.refunded); }
        else { assert_eq!(audit.leased, physical.unwrap().total()); }
        inflight_after_fault = Some(ledger.admitted - ledger.refunded);
        let outside_main = physical.map_or(0, |p| p.total().checked_sub(p.main_inflight).unwrap());
        assert_eq!(audit.explicit_other + audit.dropped,
            audit.consumed.checked_sub(ledger.admitted as usize).unwrap().checked_sub(outside_main).unwrap());
        assert!(queue.snapshot_now().leased_bytes > 0);
        assert!(backing.as_ref().unwrap().snapshot().payload_bytes > 0);
        assert!(!ledger.event_overflow);
        if hard { custody::refund_prefix(ledger, proved); }
        else {
        assert!(ledger.events[..ledger.event_count].iter().flatten().any(|event|
            event.kind == EgressLedgerEventKind::Refund
                && event.source == Some(EgressRefundSource::PacketFlush)
                && event.start == 0 && event.end == ledger.refunded));
        }
        let owner_ledger = owner.ledger();
        let first = owner_ledger.first_failure.as_ref().unwrap();
        assert_eq!(first.facts().origin.phase, FailurePhase::DeviceWrite);
        assert_eq!(first.facts().raw_os_code, Some(5));
        if hard {
            assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
            assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
            assert_eq!(first.facts().origin, SessionControl::origin(FailurePhase::DeviceWrite));
            assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
            assert_eq!(first.facts().io_kind, Some(std::io::Error::from_raw_os_error(5).kind()));
            original_session_deadline = owner_ledger.shutdown_deadline;
            assert!(original_session_deadline.is_some());
        }
        first_identity = Some((first as *const _ as usize,
            format!("{:?}", first.facts()), first.text_bytes().to_vec()));
        println!("Permit partial flush epoch={} origin={} proved={} admitted={} queued_tail={} queue_audit={audit:?} first={:?}",
            ledger.epoch, ledger.origin, ledger.refunded, ledger.admitted, remaining_packets,
            first.facts());
    }).await;

    if scenario.is_err() {
        if let Some(writer) = writer.take() {
            writer.abort();
        }
    }
    handle.request_stop();
    let requested_deadline = saved.deadline.lock().unwrap().unwrap();
    let original_deadline = if hard {
        original_session_deadline.map_or(requested_deadline, |old| old.min(requested_deadline))
    } else {
        requested_deadline
    };
    let cleanup = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, original_queue.as_ref()).await;
            return;
        }
        let mut observer = Box::pin(handle.observe_shutdown());
        let phase = tokio::time::timeout_at(original_deadline, async {
            tokio::select! {
                phase = observer.as_mut() => phase,
                report = service.run() => panic!("unproved Permit tail falsely settled: {report:?}"),
            }
        }).await.unwrap();
        assert!(matches!(phase, Phase::Incomplete | Phase::Quarantined));
        if phase == Phase::Incomplete {
            tokio::time::timeout(Duration::from_secs(1), async {
                while handle.phase() != Phase::Quarantined {
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.unwrap();
        }
        if tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("partial flush debt falsely settled before original deadline: {report:?}"),
            }
        }
        assert!(tokio::time::Instant::now() >= original_deadline);
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert!(service.completion.is_none());
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined, flow.f), (1, 0, 5, 1));
        assert!(flow.last_permit_reset.is_none() && flow.last_data_disposition.is_none());
        assert!(owner.ledger().quarantined);
        let owner_ledger = owner.ledger();
        let first = owner_ledger.first_failure.as_ref().unwrap();
        assert_eq!(
            (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec()),
            first_identity.as_ref().unwrap().clone(),
        );
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let queue = original_queue.as_ref().unwrap();
        if !hard { assert_eq!(queue.snapshot_now().leased_bytes as u64, inflight_after_fault.unwrap()); }
        else {
            let bridge = owner.relay_event_test_bridge().unwrap();
            let id = original_id.unwrap();
            let identity = crate::tcp_downlink_pump::PermitQueueIdentity::from_queue(queue);
            let dispatcher = owner.flow_retained_permits_for_test(id, &identity).unwrap();
            let actual = custody::inspect(&owner, &bridge, queue, (id.handle,id.epoch), cut_sample.unwrap().ledger, dispatcher);
            assert_eq!(Some(actual), retained_inventory);
            assert_eq!(queue.snapshot_now().leased_bytes, retained_leased.unwrap());
            assert_eq!(Some(custody::wire_prefix(&outbound, cut_packet.unwrap(), cut_sample.unwrap().ledger.origin, partial.native_cut_index().unwrap())), wire_total);
        }
        assert!(backing.as_ref().unwrap().snapshot().payload_bytes > 0);
        if hard {
            assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
            assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
            assert_eq!(owner_ledger.shutdown_deadline, original_session_deadline);
            assert_eq!(Some(original_deadline), original_session_deadline);
            let budget = saved.backing_budget.as_ref().unwrap();
            assert!(budget.quarantined() && budget.used() > 0);
        }
    }).await;
    if let Some(writer) = writer.take() {
        writer.abort();
    }
    drop((stream.take(), held_recv.take()));
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
    assert!(drained.unwrap());
}
