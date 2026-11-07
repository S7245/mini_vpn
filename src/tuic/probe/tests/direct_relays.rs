use super::super::*;
use super::direct::command;
use super::lifecycle::{auth, close, config, server};
use crate::tuic::encode_connect;

async fn header(recv: &mut quinn::RecvStream, target: &TargetAddr) {
    let expected = encode_connect(target);
    let mut bytes = vec![0; expected.len()];
    recv.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn terminal_reset_continues_accept_and_preserves_real_two_connection_one_reset() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let child = command(
        "import socket,sys,os; p=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); s.sendall(b'reset'); s.shutdown(socket.SHUT_WR)\ntry: s.recv(1024)\nexcept ConnectionResetError: pass\ns.close(); s=socket.create_connection(('127.0.0.1',p)); s.sendall(b'next'); s.shutdown(socket.SHUT_WR); r=b''\nwhile True:\n b=s.recv(1024)\n if not b: break\n r+=b\nassert r==b'useful-next',r\ns.close()",
    );
    let mut probe = prepare_direct(
        config.clone(),
        target.clone(),
        1,
        2,
        child,
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let work = async {
        let result = probe.run().await;
        assert_eq!(result.phase, Phase::Complete);
        let report = probe.direct_report().unwrap().clone();
        let bridge = report.bridge;
        assert_eq!(
            (
                bridge.accepted_connections,
                bridge.completed_connections,
                bridge.terminal_reset_connections,
                bridge.failed_connections
            ),
            (2, 1, 1, 0)
        );
        assert!(bridge.first_terminal_reset.is_some());
        assert!(bridge.first_failure.is_none());
        assert!(
            report.child.joined
                && report.child.waited
                && report.child.stdout_eof
                && report.child.stderr_eof
        );
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        header(&mut recv, &target).await;
        assert_eq!(recv.read_to_end(1024).await.unwrap(), b"reset");
        send.reset(19u32.into()).unwrap();
        drop(send);
        drop(recv);
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        header(&mut recv, &target).await;
        assert_eq!(recv.read_to_end(1024).await.unwrap(), b"next");
        send.write_all(b"useful-next").await.unwrap();
        send.finish().unwrap();
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(work, peer);
    })
    .await
    .unwrap();
    close(server).await;
}

async fn isolated_failure(fault: u8) {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let child = command(
        "import socket,sys,os; p=int(sys.argv[sys.argv.index('-p')+1]); a=socket.create_connection(('127.0.0.1',p)); b=socket.create_connection(('127.0.0.1',p)); a.sendall(b'bad'); a.shutdown(socket.SHUT_WR); b.sendall(b'good'); b.shutdown(socket.SHUT_WR); r=b''\nwhile len(r)<6: r+=b.recv(6-len(r))\nc=socket.create_connection(('127.0.0.1',p)); c.sendall(b'third-must-not-open'); c.shutdown(socket.SHUT_WR); os.write(2,b'third-connected')\nwhile True:\n x=b.recv(1024)\n if not x: break\n r+=x\nassert r==b'ready-survivor:good',r\na.close(); b.close(); print('survived')",
    );
    let mut probe = prepare_direct(
        config.clone(),
        target.clone(),
        1,
        2,
        child,
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let control = probe.control.clone();
    let facts = probe.control.facts.clone();
    facts.fault.store(fault, Ordering::Release);
    let completed = Arc::new(Notify::new());
    let notify = completed.clone();
    let work = async {
        let result = probe.run().await;
        assert_eq!(
            result.phase,
            if fault == 1 {
                Phase::Quarantined
            } else {
                Phase::Failed
            }
        );
        let report = facts.direct.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.bridge.accepted_connections, 2);
        assert_eq!(report.bridge.completed_connections, 1);
        assert_eq!(report.bridge.remote_to_local_bytes, 19);
        assert!(
            report.child.waited
                && report.child.stdout_eof
                && report.child.stderr_eof
                && report.child.joined
        );
        assert_eq!(facts.direct.joins.load(Ordering::Acquire), 1);
        if fault == 1 {
            assert_eq!(facts.direct.retained.load(Ordering::Acquire), 1);
            let data = facts.data.lock().unwrap().upgrade().unwrap();
            assert_eq!(data.tcp_pool_admission.active_total(), 1);
            let before = facts.before.lock().unwrap().unwrap();
            let after = facts.after.lock().unwrap().unwrap();
            assert!(before.lease_held && before.retirement_pending);
            assert!(after.lease_held && after.retirement_pending);
            assert_eq!(
                facts.attempt_drops.load(Ordering::Acquire),
                1,
                "only useful sibling's actual provider dropped"
            );
            assert_eq!(facts.payload_drops.load(Ordering::Acquire), 0);
        } else {
            assert_eq!(report.bridge.failed_connections, 1);
            assert_eq!(facts.direct.retained.load(Ordering::Acquire), 1);
            assert!(
                matches!(probe.failure(),Some(ClientError::Io(error)) if error.to_string().contains("original carrier read failure"))
            );
        }
        completed.notify_one();
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut bad_send, mut bad_recv) = peer.accept_bi().await.unwrap();
        header(&mut bad_recv, &target).await;
        let (mut good_send, mut good_recv) = peer.accept_bi().await.unwrap();
        header(&mut good_recv, &target).await;
        assert_eq!(bad_recv.read_to_end(1024).await.unwrap(), b"bad");
        assert_eq!(good_recv.read_to_end(1024).await.unwrap(), b"good");
        bad_send.write_all(b"trigger-original-read").await.unwrap();
        bad_send.finish().unwrap();
        // The original first relay must be observed failed while its useful sibling remains alive.
        while facts.direct.failures.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        assert_ne!(control.phase(), Phase::Quarantined);
        good_send.write_all(b"ready-").await.unwrap();
        while facts.direct.stderr_bytes.load(Ordering::Acquire) < 15 {
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), peer.accept_bi())
                .await
                .is_err(),
            "third physical local connection must not cause new TUIC CONNECT"
        );
        good_send.write_all(b"survivor:good").await.unwrap();
        good_send.finish().unwrap();
        drop(bad_send);
        drop(bad_recv);
        drop(good_send);
        drop(good_recv);
        notify.notified().await;
        peer.close(0u32.into(), b"local test complete");
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(work, peer);
    })
    .await
    .unwrap();
    close(server).await;
}
#[tokio::test]
async fn actual_relay_unknown_q_keeps_original_open_and_useful_sibling_until_aggregate() {
    isolated_failure(1).await;
}
#[tokio::test]
async fn actual_unexpected_read_failure_closes_new_accept_without_aborting_existing_sibling() {
    isolated_failure(4).await;
}

#[tokio::test]
async fn bridge_first5_failure_survives_four_cancelled_run_observations_without_abort_as_join() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let child = command(
        "import socket,sys; p=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); s.sendall(b'hold-remote-eof'); s.shutdown(socket.SHUT_WR); s.close()",
    );
    let mut probe = prepare_direct(
        config.clone(),
        target.clone(),
        1,
        1,
        child,
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let control = probe.control.clone();
    let facts = control.facts.clone();
    let work = async {
        let start = std::time::Instant::now();
        let mut deadline = None;
        let mut bridge_deadline = None;
        for _ in 0..4 {
            assert!(
                tokio::time::timeout(Duration::from_secs(1), probe.run())
                    .await
                    .is_err()
            );
            let bridge_actual = *facts.direct.bridge_deadline.lock().unwrap();
            assert!(bridge_actual.is_some());
            if let Some(first) = bridge_deadline {
                assert_eq!(bridge_actual, Some(first));
            } else {
                bridge_deadline = bridge_actual;
            }
            let actual = *control.deadline.lock().unwrap();
            assert!(actual.is_some());
            if let Some(first) = deadline {
                assert_eq!(actual, Some(first));
            } else {
                deadline = actual;
            }
        }
        let result = probe.run().await;
        assert_eq!(result.phase, Phase::Failed);
        assert_eq!(result.node, Some(NodeShutdown::RunningResourcesSettled));
        assert!(start.elapsed() >= Duration::from_secs(5));
        assert_eq!(*control.deadline.lock().unwrap(), deadline);
        assert!(format!("{:?}", probe.failure()).contains("drain timed out after 5000ms"));
        assert_eq!(facts.direct.joins.load(Ordering::Acquire), 1);
        let report = probe.direct_report().unwrap().clone();
        assert!(
            report.child.waited
                && report.child.stdout_eof
                && report.child.stderr_eof
                && report.child.joined
        );
        assert_eq!(report.bridge.failed_connections, 1);
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        header(&mut recv, &target).await;
        assert_eq!(recv.read_to_end(1024).await.unwrap(), b"hold-remote-eof");
        while facts.direct.bridge_expired.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(facts.direct.bridge_expired.load(Ordering::Acquire), 1);
        assert_eq!(facts.direct.joins.load(Ordering::Acquire), 1);
        let child = facts.direct.child.lock().unwrap().clone().unwrap();
        assert!(child.waited && child.stdout_eof && child.stderr_eof);
        assert!(
            peer.close_reason().is_none(),
            "Node must remain alive until real business settlement"
        );
        let _actual_finish = send.finish();
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(9), async {
        tokio::join!(work, peer);
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test]
async fn original_request20_stream_credit_rejection_retains_provider_and_useful_existing_relay() {
    let server = quinn::Endpoint::server(
        crate::tuic::tcp_fixture::UdpPair::server_config(8, 65536, 1, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let child = command(
        "import socket,sys; p=int(sys.argv[sys.argv.index('-p')+1]); a=socket.create_connection(('127.0.0.1',p)); a.sendall(b'good'); a.shutdown(socket.SHUT_WR); r=a.recv(6); assert r==b'ready-',r; b=socket.create_connection(('127.0.0.1',p)); b.sendall(b'blocked'); b.shutdown(socket.SHUT_WR)\ntry: b.recv(1024)\nexcept ConnectionResetError: pass\nb.close()\nwhile True:\n x=a.recv(1024)\n if not x: break\n r+=x\nassert r==b'ready-survivor',r\na.close()",
    );
    let mut probe = prepare_direct(
        config.clone(),
        target.clone(),
        1,
        2,
        child,
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let facts = probe.control.facts.clone();
    let control = probe.control.clone();
    let work = async {
        let start = std::time::Instant::now();
        let result = probe.run().await;
        assert_eq!(result.phase, Phase::Failed);
        assert_eq!(result.node, Some(NodeShutdown::RunningResourcesSettled));
        assert!(start.elapsed() >= Duration::from_secs(20));
        let report = probe.direct_report().unwrap().clone();
        assert_eq!(report.bridge.accepted_connections, 2);
        assert_eq!(report.bridge.completed_connections, 1);
        assert_eq!(report.bridge.failed_connections, 1);
        assert!(
            report
                .bridge
                .first_failure
                .as_ref()
                .unwrap()
                .contains("open target")
        );
        assert!(
            probe.failure().is_some(),
            "borrow original known provider failure"
        );
        assert_eq!(facts.direct.retained.load(Ordering::Acquire), 1);
        assert!(
            report.child.joined
                && report.child.waited
                && report.child.stdout_eof
                && report.child.stderr_eof
        );
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        header(&mut recv, &target).await;
        assert_eq!(recv.read_to_end(1024).await.unwrap(), b"good");
        send.write_all(b"ready-").await.unwrap();
        while facts.direct.failures.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        assert_ne!(control.phase(), Phase::Quarantined);
        assert!(peer.close_reason().is_none());
        assert_eq!(
            facts
                .data
                .lock()
                .unwrap()
                .upgrade()
                .unwrap()
                .tcp_pool_admission
                .active_total(),
            1
        );
        send.write_all(b"survivor").await.unwrap();
        send.finish().unwrap();
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(28), async {
        tokio::join!(work, peer);
    })
    .await
    .unwrap();
    close(server).await;
}
