//! D72 tracer: a hard Native remote EOF that wins the race against its own last
//! DATA still queued in the original relay lane. The lane is withheld
//! (harness-only receive hold, no event dropped) until the reader's EOF has
//! closed the Flow, then released: the App must receive the exact reply bytes
//! and the Flow must settle without quarantine. Real Service/QUIC/Node/budget.
//! D75: the same overtake for the reader's `remote_read_failed` terminal (peer
//! RESET after the reader already published DATA into the lane).
//! D191: the non-hard profile has no producer custody, so its withheld DATA
//! meets a closed relay; the seam settles it as discarded (one disposition per
//! begun event) instead of leaving it unaccounted (mirror Q, F3).

use super::*;
use fixture::{auth, capture, window};
use native_rebuild_fatal::{close, rig, Rig};

#[tokio::test(flavor = "current_thread")]
async fn hard_native_remote_eof_before_lane_data_delivers_reply_without_q() {
    lane_order_case(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_remote_reset_before_lane_data_delivers_reply_without_q() {
    lane_order_case(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn nonhard_native_remote_reset_before_lane_data_discards_it_without_q() {
    let profile = fixture::ServiceProfile::new(false);
    let server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = native_rebuild::native_ordered_chunk();
    let cfg = fixture::config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap().with_relay_policy(policy);
    let (inbound, outbound) = (PacketLink::bounded(64), PacketLink::bounded(64));
    let mut apps = fixture::Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let (mut peers, mut stream, mut bridge) = (Vec::new(), None, None);
    let scenario = capture(async {
        let tcp_target = TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, 10000)));
        window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer.clone());
                    let remote_open = async {
                        let (tx, mut rx) = peer.accept_bi().await.unwrap();
                        let mut want = encode_connect(&tcp_target);
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        rx.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream = Some((tx, rx));
                    };
                    let app_open = async {
                        loop {
                            apps.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            if owner.flow_observation().is_some_and(|flow| flow.published == 1) {
                                bridge = owner.relay_event_test_bridge();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Service ended before publication: {report:?}"),
            }
        }).await.expect("publication within 5s");
        let lane = bridge.clone().expect("harness relay-event bridge");
        lane.hold_relay_receive(true);
        window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let tx = &mut stream.as_mut().unwrap().0;
                    tx.write_all(b"tcp-reply").await.unwrap();
                    while lane.queued_len() == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    tx.reset(77u32.into()).unwrap();
                } => {},
                report = service.run() => panic!("Service ended before peer DATA/RESET: {report:?}"),
            }
        }).await.expect("peer DATA/RESET within 5s");
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        window(Duration::from_secs(5), async {
            loop {
                apps.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                if owner.flow_close_for_test().is_some_and(|(_, reason, _)|
                    reason == Some(("remote_to_local", "remote_read_failed"))) {
                    break;
                }
            }
        }).await.expect("reader terminal closes the Flow while its DATA is withheld");
        assert!(apps.tcp_reply.is_empty() && lane.queued_len() >= 1, "precondition");
        lane.hold_relay_receive(false);
        for _ in 0..20 {
            apps.turn().await;
            assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
        }
        assert_eq!(lane.queued_len(), 0, "withheld DATA was not consumed");
        assert!(!owner.ledger().quarantined(), "late DATA after the terminal left the mirror unaccounted");
    }).await;
    if let Some(lane) = &bridge {
        lane.hold_relay_receive(false);
    }
    let cleanup = capture(async {
        handle.request_stop();
        let report = window(Duration::from_secs(15), service.run()).await.unwrap();
        assert!(report.device_settled() && report.device_failed(), "{report:?}");
        let current = &service.completion.as_ref().unwrap().control;
        assert!(!current.ledger().quarantined());
        let flow = current.flow_observation().unwrap();
        assert!(flow.audit && flow.released == 1 && flow.f == 0, "{flow:?}");
        let (messages, bytes, transferred, discarded, _) =
            flow.last_data_disposition.expect("original physical receipt");
        assert_eq!((messages, bytes, transferred, discarded), (1, 9, 0, 9));
    }).await;
    drop(stream.take());
    close(server, peers).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

async fn lane_order_case(reset: bool) {
    let terminal = if reset { "remote_read_failed" } else { "remote_eof" };
    let mut rig = rig(|_| {});
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut stream = None;
    let mut bridge = None;
    let scenario = capture(async {
        let Rig { server, cfg, service, handle, apps, .. } = &mut rig;
        let tcp_target = TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, 10000)));
        window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, cfg).await;
                    peers.push(peer.clone());
                    let remote_open = async {
                        let (tx, mut rx) = peer.accept_bi().await.unwrap();
                        let mut want = encode_connect(&tcp_target);
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        rx.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream = Some((tx, rx));
                    };
                    let app_open = async {
                        loop {
                            apps.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            if owner.flow_observation().is_some_and(|flow| flow.published == 1) {
                                bridge = owner.relay_event_test_bridge();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Service ended before publication: {report:?}"),
            }
        }).await.expect("publication within 5s");
        let lane = bridge.clone().expect("harness relay-event bridge");
        lane.hold_relay_receive(true);
        window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let tx = &mut stream.as_mut().unwrap().0;
                    tx.write_all(b"tcp-reply").await.unwrap();
                    if reset {
                        // RESET only after the reader published DATA into the lane;
                        // an earlier RESET may legally discard unread stream bytes.
                        while lane.queued_len() == 0 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        tx.reset(77u32.into()).unwrap();
                    } else {
                        tx.finish().unwrap();
                    }
                } => {},
                report = service.run() => panic!("Service ended before peer DATA/EOF: {report:?}"),
            }
        }).await.expect("peer DATA/EOF within 5s");
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        // The reader's EOF wins: the Flow closes while its DATA is still withheld.
        window(Duration::from_secs(5), async {
            loop {
                apps.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                if owner.flow_close_for_test().is_some_and(|(_, reason, _)|
                    reason == Some(("remote_to_local", terminal))) {
                    break;
                }
            }
        }).await.expect("reader terminal closes the Flow while its DATA is withheld");
        assert!(apps.tcp_reply.is_empty() && lane.queued_len() >= 1, "precondition: app={} queued={} close={:?} q={}",
            apps.tcp_reply.len(), lane.queued_len(), owner.flow_close_for_test(), owner.ledger().quarantined());
        // Give the Closing seam many original session turns before releasing.
        for _ in 0..10 {
            apps.turn().await;
            assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
        }
        let held = owner.flow_close_for_test();
        lane.hold_relay_receive(false);
        let delivered = window(Duration::from_secs(5), async {
            loop {
                apps.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                if apps.tcp_reply.len() >= 9 { break; }
            }
        }).await;
        if delivered.is_err() {
            eprintln!("D72 lane-order deferred: held={held:?} app={} flow={:?} q={}",
                apps.tcp_reply.len(), owner.flow_observation(), owner.ledger().quarantined());
        }
        delivered.expect("reply DATA withheld behind remote EOF reaches the App");
        assert_eq!(apps.tcp_reply, b"tcp-reply");
        assert!(!owner.ledger().quarantined());
        assert_eq!(owner.ledger().first_failure.as_ref().map(|first| first.facts().class == crate::shared::errors::FailureClass::Io),
            reset.then_some(true));
    }).await;
    if let Some(lane) = &bridge {
        lane.hold_relay_receive(false);
    }
    let cleanup = capture(async {
        rig.handle.request_stop();
        let report = window(Duration::from_secs(15), rig.service.run()).await.unwrap();
        assert!(report.device_settled() && report.device_failed() == reset, "{report:?}");
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        let current = &rig.service.completion.as_ref().unwrap().control;
        assert!(!current.ledger().quarantined());
        assert_eq!(current.ledger().first_failure.is_some(), reset);
        let flow = current.flow_observation().unwrap();
        assert!(flow.audit && flow.released == 1 && flow.f == 0, "{flow:?}");
        let (messages, bytes, transferred, discarded, _) =
            flow.last_data_disposition.expect("original native physical receipt");
        assert_eq!((messages, bytes, transferred, discarded), (1, 9, 9, 0), "{terminal}");
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0);
    }).await;
    drop(stream.take());
    close(rig.server, peers).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}
