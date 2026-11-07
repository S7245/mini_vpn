//! Native direct reader gap recovery through real Quinn and the saved Service.
//! Owns only peer writes and the existing bounded UDP ciphertext hold/restore;
//! Root owns reader reassembly, physical backing, roles and final settlement.

use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride};

#[tokio::test(flavor = "current_thread")]
async fn actual_native_direct_wire_gap_restores_exact_tcp_and_joins_original_root() {
    const SUFFIX: usize = crate::client_tun::NATIVE_TCP_REASSEMBLY_MAX_BYTES - 65536;
    let mut server_config = UdpPair::server_config(8, 65536, 100, true);
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(8 * 1024 * 1024);
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .congestion_controller_factory(Arc::new(cubic));
    let (mut server, fault) = udp_fault::server(server_config);
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeChunkPump
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut peer = None;
    let mut halves = None;

    wire::drive_until(&mut service, until, async {
        let remote = server.accept().await.unwrap().await.unwrap();
        wire::auth(&remote, &cfg).await;
        peer = Some(remote.clone());
        let remote_open = async {
            let (send, mut recv) = remote.accept_bi().await.unwrap();
            let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
            want.extend_from_slice(b"service-tcp");
            let mut actual = vec![0; want.len()];
            recv.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, want);
            halves = Some((send, recv));
        };
        let app_open = async {
            loop {
                app.turn().await;
                let owner = original_owner(&saved);
                let flow = owner.flow_observation().unwrap();
                if flow.published == 1 && flow.running == 4 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::join!(remote_open, app_open);
    })
    .await;
    let owner = original_owner(&saved);
    let meter = owner.flow_native_backing_meter().unwrap();
    let baseline = meter.snapshot();
    assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
    assert!(app.tcp_reply.is_empty());

    fault.prefix().unwrap();
    let sent_before = peer.as_ref().unwrap().stats().frame_tx.stream;
    wire::drive_until(&mut service, until, async {
        halves
            .as_mut()
            .unwrap()
            .0
            .write_all(&[0x41; 1024])
            .await
            .unwrap();
        loop {
            let wire = fault.snapshot();
            if wire.counts.captured == 1
                && peer.as_ref().unwrap().stats().frame_tx.stream > sent_before
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let held = fault.snapshot();
    assert!(held.held_bytes > 0 && held.counts.restored == 0);
    fault.flood().unwrap();
    wire::drive_until(&mut service, until, async {
        halves
            .as_mut()
            .unwrap()
            .0
            .write_all(&vec![0x5a; SUFFIX])
            .await
            .unwrap();
        loop {
            app.turn().await;
            let wire = fault.snapshot();
            let physical = meter.snapshot();
            if wire.mode == udp_fault::Mode::Flood
                && physical.payload_bytes >= baseline.payload_bytes + SUFFIX
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        app.tcp_reply.is_empty(),
        "held original prefix blocks App DATA"
    );
    println!(
        "N1 Native actual wire gap held={held:?} current={:?} physical={:?} baseline={baseline:?}",
        fault.snapshot(),
        meter.snapshot()
    );
    assert_eq!(fault.snapshot().held_pointer, held.held_pointer);
    assert!(!owner.ledger().quarantined);

    wire::drive_until(&mut service, until, fault.restore())
        .await
        .unwrap();
    assert_eq!(fault.snapshot().counts.restored, 1);
    halves.as_mut().unwrap().0.finish().unwrap();
    wire::drive_until(&mut service, until, async {
        while app.tcp_reply.len() < 1024 + SUFFIX {
            app.turn().await;
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(app.tcp_reply.len(), 1024 + SUFFIX);
    assert_eq!(&app.tcp_reply[..1024], &[0x41; 1024]);
    assert_eq!(&app.tcp_reply[1024..], &vec![0x5a; SUFFIX]);
    assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);

    drop(halves.take());
    handle.request_stop();
    let original_deadline = parent_deadline(&saved).unwrap();
    let report = tokio::time::timeout_at(original_deadline, service.run())
        .await
        .unwrap();
    assert_eq!(report.session, SessionDeviceState::DeviceSettled);
    assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
    let flow = owner.flow_observation().unwrap();
    assert_eq!((flow.published, flow.released, flow.joined), (1, 1, 4));
    assert_eq!((flow.live, flow.w, flow.f, flow.running), (0, 0, 0, 0));
    assert!(flow.audit);
    let physical = meter.snapshot();
    assert_eq!(
        (
            physical.allocations,
            physical.payload_bytes,
            physical.physical_bytes,
            physical.quarantined
        ),
        (0, 0, 0, false)
    );
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_direct_wire_gap_cap_keeps_aggregate_charge_and_q() {
    const CAP: usize = crate::client_tun::NATIVE_TCP_REASSEMBLY_MAX_BYTES;
    let mut server_config = UdpPair::server_config(8, 65536, 100, true);
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(8 * 1024 * 1024);
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .congestion_controller_factory(Arc::new(cubic));
    let (mut server, fault) = udp_fault::server(server_config);
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeChunkPump
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut peer = None;
    let mut halves = None;

    wire::drive_until(&mut service, until, async {
        let remote = server.accept().await.unwrap().await.unwrap();
        wire::auth(&remote, &cfg).await;
        peer = Some(remote.clone());
        let remote_open = async {
            let (send, mut recv) = remote.accept_bi().await.unwrap();
            let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
            want.extend_from_slice(b"service-tcp");
            let mut actual = vec![0; want.len()];
            recv.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, want);
            halves = Some((send, recv));
        };
        let app_open = async {
            loop {
                app.turn().await;
                let owner = original_owner(&saved);
                let flow = owner.flow_observation().unwrap();
                if flow.published == 1 && flow.running == 4 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::join!(remote_open, app_open);
    })
    .await;
    let owner = original_owner(&saved);
    let meter = owner.flow_native_backing_meter().unwrap();
    let baseline = meter.snapshot();
    assert!(app.tcp_reply.is_empty());
    fault.prefix().unwrap();
    let sent_before = peer.as_ref().unwrap().stats().frame_tx.stream;
    wire::drive_until(&mut service, until, async {
        halves
            .as_mut()
            .unwrap()
            .0
            .write_all(&[0x41; 1024])
            .await
            .unwrap();
        loop {
            let wire = fault.snapshot();
            if wire.counts.captured == 1
                && peer.as_ref().unwrap().stats().frame_tx.stream > sent_before
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let held = fault.snapshot();
    assert!(held.held_bytes > 0 && held.counts.restored == 0);
    fault.flood().unwrap();
    wire::drive_until(&mut service, until, async {
        halves
            .as_mut()
            .unwrap()
            .0
            .write_all(&vec![0x5a; CAP + 65536])
            .await
            .unwrap();
        halves.as_mut().unwrap().0.finish().unwrap();
        loop {
            let physical = meter.snapshot();
            if physical.payload_bytes >= baseline.payload_bytes + CAP && owner.ledger().quarantined
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let physical = meter.snapshot();
    let wire = fault.snapshot();
    println!(
        "N1 Native actual wire cap physical={physical:?} wire={wire:?} root={:?}",
        owner.flow_observation()
    );
    assert_eq!(wire.mode, udp_fault::Mode::Flood);
    assert_eq!(
        (wire.held_pointer, wire.held_bytes),
        (held.held_pointer, held.held_bytes)
    );
    assert_eq!(wire.counts.restored, 0);
    assert!(wire.counts.rx_dropped_datagrams > 0);
    assert!(physical.payload_bytes >= baseline.payload_bytes + CAP);
    assert!(owner.ledger().quarantined);
    assert!(owner
        .flow_debug()
        .iter()
        .any(|line| line.contains("native_reassembly_cap")));
    assert!(app.tcp_reply.is_empty());
    let flow = owner.flow_observation().unwrap();
    assert_eq!((flow.published, flow.released, flow.f), (1, 0, 1));
    assert!(service.completion.is_none());
    let first = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|failure| {
            (
                failure as *const _ as usize,
                format!("{:?}", failure.facts()),
                failure.text_bytes().to_vec(),
            )
        })
    };

    handle.request_stop();
    let original_deadline = parent_deadline(&saved).unwrap();
    let mut observer = Box::pin(handle.observe_shutdown());
    let phase = tokio::time::timeout_at(original_deadline, async {
        tokio::select! {
            phase = observer.as_mut() => phase,
            report = service.run() => panic!("charged cap falsely settled: {report:?}"),
        }
    })
    .await
    .unwrap();
    assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
    assert!(owner.ledger().quarantined);
    assert_eq!(parent_deadline(&saved), Some(original_deadline));
    tokio::time::timeout_at(original_deadline, async {
        while owner.flow_observation().unwrap().joined < 4 {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
        }
    })
    .await
    .unwrap();
    println!(
        "N1 Native charged cap after stop={:?}",
        owner.flow_observation()
    );
    assert_eq!(
        (
            owner.flow_observation().unwrap().f,
            owner.flow_observation().unwrap().released
        ),
        (1, 0)
    );
    assert!(meter.snapshot().payload_bytes >= CAP);
    assert!(service.completion.is_none());
    let after_first = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|failure| {
            (
                failure as *const _ as usize,
                format!("{:?}", failure.facts()),
                failure.text_bytes().to_vec(),
            )
        })
    };
    assert_eq!(
        after_first, first,
        "stop cannot replace the original first error"
    );
    if tokio::time::Instant::now() < original_deadline {
        tokio::select! {
            _ = tokio::time::sleep_until(original_deadline) => {},
            report = service.run() => panic!("charged direct cap falsely settled before parent deadline: {report:?}"),
        }
    }
    assert!(tokio::time::Instant::now() >= original_deadline);
    assert_eq!(parent_deadline(&saved), Some(original_deadline));
    assert_eq!(handle.phase(), Phase::Quarantined);
    let terminal = owner.flow_observation().unwrap();
    assert_eq!(
        (
            terminal.published,
            terminal.released,
            terminal.joined,
            terminal.f
        ),
        (1, 0, 4, 1)
    );
    assert!(owner.ledger().quarantined);
    assert!(meter.snapshot().payload_bytes >= baseline.payload_bytes + CAP);
    assert!(service.completion.is_none());
    let terminal_first = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|failure| {
            (
                failure as *const _ as usize,
                format!("{:?}", failure.facts()),
                failure.text_bytes().to_vec(),
            )
        })
    };
    assert_eq!(terminal_first, first);

    drop(halves.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
}
