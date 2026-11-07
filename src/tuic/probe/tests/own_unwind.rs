//! Aggregate unwind tracer wraps the original saved production driver, retaining
//! its real Node/Open/socket owners. No replacement provider or process actor.
use super::super::*;
use super::direct::command;
use super::lifecycle::{auth, close, config, server};
use crate::tuic::encode_connect;
use std::sync::atomic::AtomicUsize;

struct OriginalFault(Arc<AtomicUsize>);
impl Drop for OriginalFault {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

fn io_identity(error: &ClientError) -> usize {
    match error {
        ClientError::Io(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            error.get_ref().unwrap() as *const _ as *const () as usize
        }
        _ => panic!("expected original carrier IO error: {error:?}"),
    }
}
struct OwnDrop {
    inner: Option<Saved>,
    fault: Option<Box<OriginalFault>>,
    drops: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    ready_error: Arc<AtomicUsize>,
    ready_deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
    ready_child: Arc<Mutex<Option<child::Report>>>,
    control: Arc<Control>,
}
impl Future for OwnDrop {
    type Output = Completion;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Completion> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        match self.inner.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(done) => {
                assert_eq!(done.observation.phase, Phase::Failed);
                assert_eq!(
                    done.observation.node,
                    Some(NodeShutdown::RunningResourcesSettled)
                );
                let job = done.job.as_ref().unwrap();
                assert!(job.settled());
                self.ready_error
                    .store(io_identity(job.failure().unwrap()), Ordering::Release);
                *self.ready_deadline.lock().unwrap() = *self.control.deadline.lock().unwrap();
                *self.ready_child.lock().unwrap() =
                    Some(job.direct.as_ref().unwrap().report.child.clone());
                Poll::Ready(done)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for OwnDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        drop(self.inner.take());
        std::panic::panic_any(self.fault.take().unwrap());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn own_driver_drop_unwind_keeps_actual_first_io_and_child_join_before_node_settlement() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_direct(
        config.clone(), target.clone(), 1, 2,
        command("import socket,sys,os\np=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); os.write(1,b'child-output'); os.write(2,b'child-stderr'); s.sendall(b'original'); assert s.recv(1)==b''; sys.exit(7)"),
        Arc::new(Metrics::default()), None,
    ).unwrap();
    let control = probe.control.clone();
    let facts = control.facts.clone();
    facts.fault.store(4, Ordering::Release);
    let fault_drops = Arc::new(AtomicUsize::new(0));
    let fault = Box::new(OriginalFault(fault_drops.clone()));
    let identity = &*fault as *const OriginalFault as usize;
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let ready_error = Arc::new(AtomicUsize::new(0));
    let ready_deadline = Arc::new(Mutex::new(None));
    let ready_child = Arc::new(Mutex::new(None));
    probe.driver = Some(Box::pin(OwnDrop {
        inner: probe.driver.take(),
        fault: Some(fault),
        drops: drops.clone(),
        polls: polls.clone(),
        ready_error: ready_error.clone(),
        ready_deadline: ready_deadline.clone(),
        ready_child: ready_child.clone(),
        control: control.clone(),
    }));
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let header = encode_connect(&target);
        let mut actual = vec![0; header.len()];
        recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        let mut payload = [0; 8];
        recv.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"original");
        send.write_all(b"real-carrier-read-failure").await.unwrap();
        send.finish().unwrap();
        let _ = recv.read_to_end(64).await;
        drop((send, recv));
        peer.closed().await;
    };
    let (observed, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(probe.run(), peer)
    })
    .await
    .unwrap();
    assert_eq!(observed.phase, Phase::Quarantined);
    assert_eq!(observed.node, Some(NodeShutdown::RunningResourcesSettled));
    assert!(probe.driver.is_none());
    assert!(probe.completion.is_some());
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_ne!(ready_error.load(Ordering::Acquire), 0);
    assert_eq!(
        io_identity(probe.failure().unwrap()),
        ready_error.load(Ordering::Acquire)
    );
    assert!(
        probe
            .failure()
            .unwrap()
            .to_string()
            .contains("injected original carrier read failure")
    );
    let original = probe
        .panic
        .as_ref()
        .unwrap()
        .downcast_ref::<Box<OriginalFault>>()
        .unwrap();
    assert_eq!(&**original as *const OriginalFault as usize, identity);
    let direct = probe.direct_report().unwrap();
    assert_eq!(direct.bridge.failed_connections, 1);
    assert!(
        direct.child.waited
            && direct.child.stdout_eof
            && direct.child.stderr_eof
            && direct.child.joined
    );
    assert!(!direct.child.success && direct.child.status.is_some());
    assert_eq!(direct.child, ready_child.lock().unwrap().clone().unwrap());
    assert_eq!(direct.child.stdout.as_slice(), b"child-output");
    assert_eq!(direct.child.stderr.as_slice(), b"child-stderr");
    assert_eq!(facts.direct.joins.load(Ordering::Acquire), 1);
    assert!(facts.data.lock().unwrap().upgrade().is_none());
    assert!(control.stopped.load(Ordering::Acquire));
    let first_deadline = ready_deadline.lock().unwrap().unwrap();
    assert_eq!(*control.deadline.lock().unwrap(), Some(first_deadline));
    let completed_polls = polls.load(Ordering::Acquire);
    assert_ne!(completed_polls, 0);
    assert_eq!(probe.run().await, observed);
    assert_eq!(polls.load(Ordering::Acquire), completed_polls);
    assert_eq!(
        io_identity(probe.failure().unwrap()),
        ready_error.load(Ordering::Acquire)
    );
    drop(probe);
    assert_eq!(fault_drops.load(Ordering::Acquire), 0);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(control.phase(), Phase::Quarantined);
    close(server).await;
}
struct OwnPoll {
    _inner: Saved,
    fault: Option<Box<OriginalFault>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Drop for OwnPoll {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
impl Future for OwnPoll {
    type Output = Completion;
    fn poll(mut self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Completion> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        std::panic::panic_any(self.fault.take().unwrap());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn own_poll_unwind_stops_actual_child_but_retains_original_unknown_driver_and_join() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_direct(
        config.clone(), target.clone(), 1, 2,
        command("import socket,sys,os,time\np=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); s.sendall(b'aggregate'); assert s.recv(5)==b'ready'; os.write(1,b'original-output'); os.write(2,b'original-stderr'); time.sleep(60)"),
        Arc::new(Metrics::default()), None,
    ).unwrap();
    let control = probe.control.clone();
    let facts = control.facts.clone();
    let (peer, send, recv) = tokio::time::timeout(Duration::from_secs(5), async {
        let mut run = Box::pin(probe.run());
        let connected = async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &config).await;
            let (mut send, mut recv) = peer.accept_bi().await.unwrap();
            let header = encode_connect(&target);
            let mut actual = vec![0; header.len()];
            recv.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, header);
            let mut payload = [0; 9];
            recv.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"aggregate");
            send.write_all(b"ready").await.unwrap();
            while facts.direct.stderr_bytes.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            (peer, send, recv)
        };
        tokio::select! {biased; value=&mut run=>panic!("early completion {value:?}"), pair=connected=>pair}
    }).await.unwrap();
    let pid = facts.direct.pid.load(Ordering::Acquire);
    assert_ne!(pid, 0);
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, 0);
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
    assert!(control.deadline.lock().unwrap().is_none());
    let drops = Arc::new(AtomicUsize::new(0));
    let fault = Box::new(OriginalFault(drops.clone()));
    let identity = &*fault as *const OriginalFault as usize;
    let polls = Arc::new(AtomicUsize::new(0));
    let driver_drops = Arc::new(AtomicUsize::new(0));
    probe.driver = Some(Box::pin(OwnPoll {
        _inner: probe.driver.take().unwrap(),
        fault: Some(fault),
        polls: polls.clone(),
        drops: driver_drops.clone(),
    }));
    let before = tokio::time::Instant::now();
    let first = probe.run().await;
    let after = tokio::time::Instant::now();
    let deadline = control.deadline.lock().unwrap().unwrap();
    let original = probe
        .panic
        .as_ref()
        .unwrap()
        .downcast_ref::<Box<OriginalFault>>()
        .unwrap();
    let retained_identity = &**original as *const OriginalFault as usize;
    let second = probe.run().await;
    // This does not request stop, poll the poisoned driver, consume its actor
    // JoinHandle, or drop the Probe. Only the original outer stop can wake Child.
    let before_drop = tokio::time::timeout(Duration::from_secs(3), async {
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
    })
    .await
    .is_ok();
    let stopped_before_drop = control.stopped.load(Ordering::Acquire);
    let charged = facts
        .data
        .lock()
        .unwrap()
        .upgrade()
        .unwrap()
        .tcp_pool_admission
        .active_total();
    let no_completion = probe.completion.is_none() && probe.driver.is_some();
    let original_peer_open = peer.close_reason().is_none();
    let unchanged_deadline = *control.deadline.lock().unwrap() == Some(deadline);
    // Finally always request the original stop through forced owner Drop, even
    // for the RED. Observe real actor/process/pipe cleanup in this living runtime.
    drop(probe);
    tokio::time::timeout(Duration::from_secs(3), async {
        while facts.direct.child.lock().unwrap().is_none()
            || !facts
                .direct
                .actor
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_finished()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let child = facts.direct.child.lock().unwrap().clone().unwrap();
    let gone = unsafe { libc::kill(pid as i32, 0) };
    let errno = std::io::Error::last_os_error().raw_os_error();
    peer.close(0u32.into(), b"aggregate unknown Node remains charged");
    drop((send, recv, peer));
    close(server).await;
    assert_eq!(first.phase, Phase::Quarantined);
    assert_eq!(first.node, None);
    assert_eq!(second, first);
    assert_eq!(retained_identity, identity);
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert_eq!(driver_drops.load(Ordering::Acquire), 0);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert!(deadline >= before + Duration::from_secs(15));
    assert!(deadline <= after + Duration::from_secs(15));
    assert!(unchanged_deadline);
    assert!(no_completion);
    assert!(original_peer_open);
    assert_eq!(charged, 1);
    assert_eq!(control.phase(), Phase::Quarantined);
    assert!(child.killed && child.waited && child.stdout_eof && child.stderr_eof);
    assert_eq!(child.stdout.as_slice(), b"original-output");
    assert_eq!(child.stderr.as_slice(), b"original-stderr");
    assert!(!child.joined);
    assert_eq!(facts.direct.joins.load(Ordering::Acquire), 0);
    assert_eq!(gone, -1);
    assert_eq!(errno, Some(libc::ESRCH));
    eprintln!(
        "actual child finally killed/waited/drained, original join0, charged Open1; before Drop: stopped={stopped_before_drop}, actorFinished={before_drop}"
    );
    assert!(
        before_drop && stopped_before_drop,
        "aggregate own poll unwind must stop the actual Child before forced Probe Drop"
    );
}
