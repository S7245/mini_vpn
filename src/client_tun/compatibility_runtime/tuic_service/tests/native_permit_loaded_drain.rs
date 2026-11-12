//! Real Service Native Permit DATA charge, App/TUN drain, and original Root release.
//! The peer and App drive the wire; Root owns F, five roles, queue and Node meter.

use super::*;
use crate::client_tun::downlink_custody::{EgressLedgerEventKind, EgressRefundSource};
use crate::client_tun::legacy_event_loop::relay_events::EgressProbePhase;
use crate::harness::LoopbackReceiveWatch;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[tokio::test(flavor = "current_thread")]
async fn native_permit_loaded_data_drains_through_app_before_clean_join() {
    const REPLY_BYTES: usize = 8192;
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::PermitPump);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let inbound_tap = inbound.clone();
    let outbound_tap = outbound.clone();
    let receive_watch = Arc::new(std::sync::Mutex::new(LoopbackReceiveWatch::default()));
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound).with_receive_watch(receive_watch.clone()),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let mut stream = None;
    let mut original_queue = None;
    let mut backing = None;

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
                                original_queue = owner.flow_native_permit_queue();
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Native Permit Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let queue = original_queue.as_ref().expect("original Native Permit queue");
        let meter = backing.as_ref().expect("original Node backing meter");
        assert_eq!(queue.custodied_release_mode(),
            Some(crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain));
        assert_eq!(queue.custodied_released_bytes_for_test(), Some(0));
        assert!(app.tcp_reply.is_empty());
        let egress_bridge = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap()
            .relay_event_test_bridge().unwrap();
        egress_bridge.arm_egress_probe(queue.clone());
        let payload: Vec<u8> = (0..REPLY_BYTES).map(|index| (index % 251) as u8).collect();

        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    stream.as_mut().unwrap().0.write_all(&payload).await.unwrap();
                    loop {
                        let physical = queue.snapshot_now();
                        if physical.queued_bytes + physical.leased_bytes > 0 {
                            assert!(physical.owned_bytes() <= physical.capacity_bytes);
                            assert!(meter.snapshot().payload_bytes > 0);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before charged DATA: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());
        let peer_ref = peer.as_ref().unwrap();
        let udp_request = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), peer_ref.read_datagram())
            .await.unwrap().unwrap();
        let assoc = u16::from_be_bytes([udp_request[2], udp_request[3]]);
        let udp_target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
        assert_eq!(&udp_request[..], encode_packet(assoc, &udp_target, b"service-udp"));
        peer_ref.send_datagram(encode_packet(assoc, &udp_target, b"service-udp-reply").into()).unwrap();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    loop {
                        let (samples, overflow, ambiguous) = egress_bridge.egress_probe_samples();
                        assert!(!overflow && !ambiguous);
                        if samples.windows(2).any(|pair|
                            pair[0].phase == EgressProbePhase::BeforeUdpFlush
                                && pair[1].phase == EgressProbePhase::AfterUdpFlush
                                && pair[0].handle == pair[1].handle
                                && pair[0].ledger.epoch == pair[1].ledger.epoch
                                && pair[0].ledger.admitted > pair[0].ledger.refunded
                        ) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before charged UDP flush: {report:?}"),
            }
        }).await.unwrap();
        let (charged_samples, overflow, ambiguous) = egress_bridge.egress_probe_samples();
        assert!(!overflow && !ambiguous);
        let charged_udp = charged_samples.windows(2).find(|pair|
            pair[0].phase == EgressProbePhase::BeforeUdpFlush
                && pair[1].phase == EgressProbePhase::AfterUdpFlush
                && pair[0].handle == pair[1].handle
                && pair[0].ledger.epoch == pair[1].ledger.epoch
                && pair[0].ledger.admitted > pair[0].ledger.refunded
        ).expect("actual UDP flush with original TCP cap charged");
        assert_eq!(charged_udp[0].ledger.refunded, charged_udp[1].ledger.refunded);
        assert_eq!(charged_udp[0].ledger.event_count, charged_udp[1].ledger.event_count);
        assert_eq!(charged_udp[0].ledger.events, charged_udp[1].ledger.events);

        let first_wire = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                packet = async {
                    loop {
                        if let Some(packet) = outbound_tap.packets_for_test().iter()
                            .filter_map(|bytes| crate::device::summarize_flushed_ip_tcp_packet(bytes))
                            .find(|packet| packet.payload_bytes > 0) {
                            break packet;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => packet,
                report = service.run() => panic!("Native Permit Service ended before original TUN payload: {report:?}"),
            }
        }).await.unwrap();
        let ack_target = first_wire.payload_start_seq.wrapping_add(first_wire.payload_bytes as u32);
        receive_watch.lock().unwrap().arm_ack(
            first_wire,
            (REPLY_BYTES - first_wire.payload_bytes) as u32,
        );

        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    loop {
                        app.turn().await;
                        assert!(app.tcp_reply.len() <= REPLY_BYTES);
                        let physical = queue.snapshot_now();
                        if app.tcp_reply.len() == REPLY_BYTES
                            && app.udp_reply == b"service-udp-reply"
                            && physical.queued_bytes == 0 && physical.leased_bytes == 0 {
                            assert_eq!(app.tcp_reply, payload);
                            assert_eq!(queue.custodied_released_bytes_for_test(), Some(REPLY_BYTES));
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before App ACK: {report:?}"),
            }
        }).await.unwrap_or_else(|elapsed| {
            eprintln!("Native Permit drain: app={} queue={:?} meter={:?}",
                app.tcp_reply.len(), queue.snapshot_now(), meter.snapshot());
            panic!("Native Permit App ACK drain: {elapsed}");
        });
        assert!(saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap().ledger().first_failure.is_none());
        let original_ack = receive_watch.lock().unwrap().captured_ack()
            .unwrap_or_else(|| panic!("original smoltcp ingress ACK absent for {ack_target} {first_wire:?}"));
        assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
        assert!(!inbound_tap.packets_for_test().iter().any(|packet| packet == &original_ack),
            "original wire ACK left the shared inbound link");
        let before_replay = receive_watch.lock().unwrap().arm_replay(original_ack.clone());
        let (prior_egress, prior_overflow, prior_ambiguous) = egress_bridge.egress_probe_samples();
        assert!(!prior_overflow && !prior_ambiguous);
        let same_cap_suffix = prior_egress.windows(2).find(|pair| {
            let before = pair[0];
            let after = pair[1];
            before.phase == EgressProbePhase::BeforePoll
                && after.phase == EgressProbePhase::AfterFlush
                && before.handle == after.handle
                && before.ledger.epoch == after.ledger.epoch
                && before.ledger.origin == after.ledger.origin
                && before.ledger.first_cap_remaining > 0
                && before.ledger.cap_count > after.ledger.cap_count
                && after.ledger.refunded.checked_sub(before.ledger.refunded)
                    .is_some_and(|n| n >= before.ledger.first_cap_remaining as u64)
        });
        assert!(same_cap_suffix.is_some(), "original poll/flush refunded an identified FIFO cap suffix");
        let prior_refunded = prior_egress.last().unwrap().ledger.refunded;
        assert_eq!(prior_refunded, REPLY_BYTES as u64);
        let ledger = prior_egress.last().unwrap().ledger;
        assert!(!ledger.event_overflow && ledger.event_count > 0);
        let mut admitted_end = 0u64;
        let mut refunded_end = 0u64;
        let mut last_cap = 0u64;
        let mut refund_events = 0usize;
        let mut flush_refunded = 0u64;
        let mut ack_refunded = 0u64;
        let mut caps = [None; 32];
        let mut admitted_caps = 0usize;
        for event in ledger.events[..ledger.event_count].iter().flatten() {
            assert_eq!(event.epoch, ledger.epoch);
            match event.kind {
                EgressLedgerEventKind::Admit => {
                    assert_eq!(event.source, None);
                    assert_eq!(event.start, admitted_end);
                    assert_eq!(event.cap_ordinal, admitted_caps as u64);
                    assert_eq!(event.cap_before, (event.end - event.start) as usize);
                    assert_eq!(event.cap_after, event.cap_before);
                    caps[admitted_caps] = Some((event.start, event.end, event.cap_before));
                    admitted_caps += 1;
                    admitted_end = event.end;
                }
                EgressLedgerEventKind::Refund => {
                    match event.source.expect("original refund proof source") {
                        EgressRefundSource::PacketFlush => flush_refunded += event.end - event.start,
                        EgressRefundSource::AckDrain => ack_refunded += event.end - event.start,
                    }
                    assert_eq!(event.start, refunded_end);
                    assert_eq!(event.cap_ordinal, last_cap);
                    let cap = caps[event.cap_ordinal as usize].as_mut().unwrap();
                    assert_eq!(event.cap_before, cap.2);
                    assert!(event.start >= cap.0 && event.end <= cap.1);
                    assert_eq!(event.cap_before - event.cap_after, (event.end - event.start) as usize);
                    cap.2 = event.cap_after;
                    refunded_end = event.end;
                    refund_events += 1;
                    if event.cap_after == 0 { last_cap += 1; }
                }
            }
        }
        assert!(refund_events > 0 && ledger.events[0].unwrap().kind == EgressLedgerEventKind::Admit);
        assert_eq!((admitted_end, refunded_end), (REPLY_BYTES as u64, REPLY_BYTES as u64));
        assert!(caps[..admitted_caps].iter().all(|cap| cap.unwrap().2 == 0));
        assert!(flush_refunded > 0, "original poll/flush must prove a TCP cap prefix");
        assert_eq!(flush_refunded + ack_refunded, REPLY_BYTES as u64,
            "every actual refund must belong to original TCP packet flush or ACK proof");
        println!("original Native Permit ledger events={:?}", &ledger.events[..ledger.event_count]);
        assert!(inbound_tap.push(original_ack.clone()));
        assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
        assert!(!inbound_tap.packets_for_test().iter().any(|packet| packet == &original_ack),
            "replayed wire ACK left the shared inbound link");
        assert!(receive_watch.lock().unwrap().replay_count() > before_replay,
            "an original-App-ACK-equivalent packet reached smoltcp receive after replay");
        assert_eq!(queue.custodied_released_bytes_for_test(), Some(REPLY_BYTES));
        assert_eq!((queue.snapshot_now().queued_bytes, queue.snapshot_now().leased_bytes), (0, 0));
        let (egress_samples, overflow, ambiguous) = egress_bridge.egress_probe_samples();
        assert!(!overflow && !ambiguous && egress_samples.len() > prior_egress.len());
        assert!(egress_samples.windows(2).any(|pair|
            pair[0].phase == EgressProbePhase::BeforeUdpFlush
                && pair[1].phase == EgressProbePhase::AfterUdpFlush
                && pair[0].handle == pair[1].handle
                && pair[0].ledger.epoch == pair[1].ledger.epoch
                && pair[0].ledger.admitted > pair[0].ledger.refunded));
        assert!(egress_samples[prior_egress.len()..].iter().all(|sample|
            sample.ledger.refunded == prior_refunded && !sample.ledger.quarantined
                && sample.ledger.event_count == ledger.event_count
                && sample.ledger.events == ledger.events));

        let (mut send, mut recv) = stream.take().unwrap();
        send.finish().unwrap();
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.published, flow.released, flow.f), (1, 1, 0, 1));
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        assert_eq!((queue.snapshot_now().queued_bytes, queue.snapshot_now().leased_bytes), (0, 0));
    }).await;

    let scenario_failed = scenario.is_err();
    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario_failed {
            fixture::observe_failed_native_stop(
                service.run(),
                &handle,
                original_deadline,
                original_queue.as_ref(),
            )
            .await;
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (flow.live, flow.published, flow.released, flow.joined),
            (0, 1, 1, 5)
        );
        assert_eq!(flow.f, 0);
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        let queue = original_queue.as_ref().unwrap();
        assert!(queue.physical_zero());
        let audit = queue.permit_release_audit_for_test().unwrap();
        assert!(!audit.interrupted);
        assert_eq!(
            (audit.emitted, audit.consumed, audit.discarded),
            (REPLY_BYTES, REPLY_BYTES, 0)
        );
        assert_eq!(
            (audit.queued, audit.leased, audit.released),
            (0, 0, REPLY_BYTES)
        );
        assert_eq!(
            (
                audit.packet_flush,
                audit.ack_drain,
                audit.explicit_other,
                audit.dropped
            ),
            (REPLY_BYTES, 0, 0, 0)
        );
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (
                physical.allocations,
                physical.payload_bytes,
                physical.physical_bytes,
                physical.quarantined
            ),
            (0, 0, 0, false)
        );
    })
    .await;
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
