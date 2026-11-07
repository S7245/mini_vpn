#[derive(Default)]
pub(in crate::tuic::probe) struct Facts {
    pub(in crate::tuic::probe) bridge_deadline: Mutex<Option<tokio::time::Instant>>,
    pub(in crate::tuic::probe) bridge_expired: std::sync::atomic::AtomicUsize,
    pub(in crate::tuic::probe) child_waited: AtomicBool,
    pub(in crate::tuic::probe) report: Mutex<Option<direct::Report>>,
    pub(in crate::tuic::probe) child: Mutex<Option<child::Report>>,
    pub(in crate::tuic::probe) stderr_bytes: std::sync::atomic::AtomicUsize,
    pub(in crate::tuic::probe) failures: std::sync::atomic::AtomicUsize,
    pub(in crate::tuic::probe) retained: std::sync::atomic::AtomicUsize,
    pub(in crate::tuic::probe) joins: std::sync::atomic::AtomicUsize,
    pub(in crate::tuic::probe) actor: Mutex<Option<tokio::task::AbortHandle>>,
    pub(in crate::tuic::probe) pid: std::sync::atomic::AtomicU32,
}
use super::super::*;
use super::lifecycle::{auth, close, config, server};
use crate::tuic::encode_connect;

pub(super) fn command(script: &str) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("/usr/bin/python3");
    command.arg("-c").arg(script);
    command
}

#[tokio::test(flavor = "current_thread")]
async fn actual_direct_payload_eof_child_wait_pipes_and_join_precede_node_cleanup() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_direct(
        config.clone(), target.clone(), 1, 2,
        command("import socket,sys; p=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); s.sendall(b'local-payload'); s.shutdown(socket.SHUT_WR); r=b''\nwhile True:\n b=s.recv(1024)\n if not b: break\n r+=b\nassert r==b'remote:local-payload',r\ns.close(); print('actual-child-eof')"),
        Arc::new(Metrics::default()),
        None,
    ).unwrap();
    let work = async {
        let result = probe.run().await;
        assert_eq!(result.phase, Phase::Complete);
        assert_eq!(result.node, Some(NodeShutdown::RunningResourcesSettled));
        let direct = probe.direct_report().unwrap().clone();
        assert_eq!(direct.bridge.accepted_connections, 1);
        assert_eq!(direct.bridge.completed_connections, 1);
        assert_eq!(direct.bridge.local_to_remote_bytes, 13);
        assert_eq!(direct.bridge.remote_to_local_bytes, 20);
        assert!(direct.child.waited && direct.child.stdout_eof && direct.child.stderr_eof);
        assert!(direct.child.joined && direct.child.success && !direct.child.killed);
        assert_eq!(direct.child.stdout.as_slice(), b"actual-child-eof\n");
        assert!(probe.failure().is_none());
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        assert_eq!(recv.read_to_end(1024).await.unwrap(), b"local-payload");
        send.write_all(b"remote:local-payload").await.unwrap();
        send.finish().unwrap();
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(work, peer);
    })
    .await
    .unwrap();
    close(server).await;
}

async fn child_case(command: tokio::process::Command, expected: Phase) -> child::Report {
    let server = server();
    let config = config(&server);
    let mut probe = prepare_direct(
        config.clone(),
        TargetAddr::parse("192.0.2.1:443").unwrap(),
        1,
        2,
        command,
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let work = async {
        let result = probe.run().await;
        assert_eq!(result.phase, expected);
        assert_eq!(result.node, Some(NodeShutdown::RunningResourcesSettled));
        probe.direct_report().unwrap().clone().child
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        peer.closed().await;
    };
    let (report, ()) =
        tokio::time::timeout(Duration::from_secs(8), async { tokio::join!(work, peer) })
            .await
            .unwrap();
    close(server).await;
    report
}

#[tokio::test]
async fn child_nonzero_waits_and_spawn_rejection_is_known_without_fake_wait() {
    let report = child_case(
        command("import sys; print('original'); sys.stderr.write('cause'); sys.exit(7)"),
        Phase::Failed,
    )
    .await;
    assert!(
        report.joined && report.waited && report.stdout_eof && report.stderr_eof && !report.success
    );
    assert_eq!(report.stdout.as_slice(), b"original\n");
    assert_eq!(report.stderr.as_slice(), b"cause");
    let report = child_case(
        tokio::process::Command::new("/mini-vpn-nonexistent-child"),
        Phase::Failed,
    )
    .await;
    assert!(report.joined && !report.waited && !report.stdout_eof && !report.stderr_eof);
    assert!(report.error.unwrap().contains("run iperf3"));
}

#[tokio::test]
async fn stdout_and_stderr_overflow_kill_wait_and_boundedly_drain_both_pipes() {
    for (fd, limit) in [(1, 1048576), (2, 65536)] {
        let report = child_case(
            command(&format!(
                "import os,time\nwhile True: os.write({fd},b'x'*8192)"
            )),
            Phase::Failed,
        )
        .await;
        assert!(
            report.killed
                && report.waited
                && report.stdout_eof
                && report.stderr_eof
                && report.joined
        );
        assert!(report.stdout.len() <= 1048576 && report.stderr.len() <= 65536);
        assert_eq!(
            if fd == 1 {
                report.stdout.len()
            } else {
                report.stderr.len()
            },
            limit
        );
        assert!(report.error.unwrap().contains("overflow"));
    }
}
