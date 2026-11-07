use super::super::*;
use super::direct::command;
use super::lifecycle::{auth, close, config, server};

async fn start_child(
    probe: &mut Probe,
    server: &quinn::Endpoint,
    config: &TuicClientConfig,
) -> quinn::Connection {
    let facts = probe.control.facts.clone();
    let mut run = Box::pin(probe.run());
    let accepted = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, config).await;
        while facts.direct.pid.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        peer
    };
    tokio::select! {biased; value=&mut run=>panic!("early completion {value:?}"),peer=accepted=>peer}
}
fn sleeper() -> tokio::process::Command {
    command("import time; time.sleep(60)")
}

#[tokio::test]
async fn cancelled_run_and_stop_observer_reuse_original_child_join_and_first15() {
    let server = server();
    let config = config(&server);
    let mut probe = prepare_direct(
        config.clone(),
        TargetAddr::parse("192.0.2.1:443").unwrap(),
        1,
        2,
        sleeper(),
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let handle = probe.take_handle().unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        let peer = start_child(&mut probe, &server, &config).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), handle.observe_shutdown())
                .await
                .is_err()
        );
        let deadline = *probe.control.deadline.lock().unwrap();
        let (result, ()) = tokio::join!(probe.run(), async {
            peer.closed().await;
        });
        assert_eq!(result.phase, Phase::Stopped);
        let child = probe.direct_report().unwrap().clone().child;
        assert!(
            child.killed && child.waited && child.stdout_eof && child.stderr_eof && child.joined
        );
        assert_eq!(handle.observe_shutdown().await, Phase::Stopped);
        assert_eq!(*probe.control.deadline.lock().unwrap(), deadline);
        assert_eq!(probe.control.facts.direct.joins.load(Ordering::Acquire), 1);
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test]
async fn forced_probe_drop_outer_stop_really_kills_waits_and_drains_without_consuming_join() {
    let server = server();
    let config = config(&server);
    let mut probe = prepare_direct(
        config.clone(),
        TargetAddr::parse("192.0.2.1:443").unwrap(),
        1,
        2,
        sleeper(),
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let facts = probe.control.facts.clone();
    let control = probe.control.clone();
    tokio::time::timeout(Duration::from_secs(8), async {
        let peer = start_child(&mut probe, &server, &config).await;
        let pid = facts.direct.pid.load(Ordering::Acquire);
        assert_ne!(pid, 0);
        drop(probe);
        loop {
            if facts.direct.child.lock().unwrap().is_some()
                && facts
                    .direct
                    .actor
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .is_finished()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let child = facts.direct.child.lock().unwrap().clone().unwrap();
        assert!(child.killed && child.waited && child.stdout_eof && child.stderr_eof);
        assert!(!child.joined);
        assert_eq!(facts.direct.joins.load(Ordering::Acquire), 0);
        assert_eq!(control.phase(), Phase::Quarantined);
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        peer.close(0u32.into(), b"charged original Node retained");
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test]
async fn real_node_stats_actor_fatal_still_waits_for_child_then_keeps_original_node_q() {
    let server = server();
    let config = config(&server);
    let mut probe = prepare_direct(
        config.clone(),
        TargetAddr::parse("192.0.2.1:443").unwrap(),
        1,
        2,
        sleeper(),
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let facts = probe.control.facts.clone();
    tokio::time::timeout(Duration::from_secs(8), async {
        let peer = start_child(&mut probe, &server, &config).await;
        facts.stats_abort.lock().unwrap().as_ref().unwrap().abort();
        let (result, ()) = tokio::join!(probe.run(), async {
            peer.closed().await;
        });
        assert_eq!(result.phase, Phase::Quarantined);
        assert_eq!(result.node, Some(NodeShutdown::Quarantined));
        let child = probe.direct_report().unwrap().clone().child;
        assert!(
            child.killed && child.waited && child.stdout_eof && child.stderr_eof && child.joined
        );
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test]
async fn actual_child_wait_is_not_pipe_eof_or_original_actor_join() {
    let server = server();
    let config = config(&server);
    let child = command(
        "import os,time\npid=os.fork()\nif pid==0:\n time.sleep(.4); os._exit(0)\nos._exit(0)",
    );
    let mut probe = prepare_direct(
        config.clone(),
        TargetAddr::parse("192.0.2.1:443").unwrap(),
        1,
        2,
        child,
        Arc::new(Metrics::default()),
        None,
    )
    .unwrap();
    let facts = probe.control.facts.clone();
    tokio::time::timeout(Duration::from_secs(8),async {
        let peer=start_child(&mut probe,&server,&config).await;
        let mut run=Box::pin(probe.run());
        tokio::select! {biased; result=&mut run=>panic!("early pipe EOF/join {result:?}"),_=async{while !facts.direct.child_waited.load(Ordering::Acquire){tokio::task::yield_now().await;}}=>{}}
        drop(run);
        let first_deadline=*probe.control.deadline.lock().unwrap();
        assert!(first_deadline.is_some(),"actual wait locks parentfirst15 before inherited pipe EOF");
        assert!(facts.direct.child.lock().unwrap().is_none());
        assert_eq!(facts.direct.joins.load(Ordering::Acquire),0);
        assert!(tokio::time::timeout(Duration::from_millis(40),probe.run()).await.is_err());
        assert!(facts.data.lock().unwrap().upgrade().is_some());
        let (result,())=tokio::join!(probe.run(),async{peer.closed().await;});
        assert_eq!(result.phase,Phase::Complete);
        assert_eq!(*probe.control.deadline.lock().unwrap(),first_deadline);
        let report=probe.direct_report().unwrap().clone().child;
        assert!(report.waited && report.stdout_eof && report.stderr_eof && report.joined);
    }).await.unwrap();
    close(server).await;
}

#[tokio::test(start_paused = true)]
async fn unique_child_original_duration_plus30_is_not_reset_by_observation() {
    let control = Control::local();
    let mut actor = child::spawn(sleeper(), 1, control.clone());
    while control.facts.direct.pid.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    assert!(!actor.join.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    let result = (&mut actor.join).await.unwrap();
    assert!(
        result.report.killed
            && result.report.waited
            && result.report.stdout_eof
            && result.report.stderr_eof
    );
    assert!(result.report.error.unwrap().contains("timed out"));
}
