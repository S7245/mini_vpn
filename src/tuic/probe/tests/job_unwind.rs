//! Whole-job unwind boundary, using the original real direct job Future and
//! physical owners. Test-only wrappers never supply a replacement Finished.
use super::super::*;
use super::direct::command;
use super::lifecycle::{auth, close, config, server};
use crate::tuic::encode_connect;
use std::sync::atomic::{AtomicU8, AtomicUsize};

#[derive(Default)]
pub(in crate::tuic::probe) struct Facts {
    mode: AtomicU8,
    armed: AtomicBool,
    polls: AtomicUsize,
    drops: AtomicUsize,
    payload: Mutex<Option<Box<OriginalFault>>>,
    payload_drops: Arc<AtomicUsize>,
    retained_payload: AtomicUsize,
    ready_io: AtomicUsize,
    retained_io: AtomicUsize,
    ready_child: Mutex<Option<child::Report>>,
    retained_child: Mutex<Option<child::Report>>,
    deadline: Mutex<Option<tokio::time::Instant>>,
}
struct OriginalFault(Arc<AtomicUsize>);
impl Drop for OriginalFault {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
fn io_identity(done: &Finished) -> usize {
    let ClientError::Io(error) = done.failure().unwrap() else {
        panic!("original IO absent")
    };
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(error.to_string(), "injected original carrier read failure");
    error.get_ref().unwrap() as *const _ as *const () as usize
}
pub(in crate::tuic::probe) fn retained(
    control: &Control,
    done: Option<&Finished>,
    payload: &Box<dyn std::any::Any + Send>,
) {
    let facts = &control.facts.aggregate;
    if facts.mode.load(Ordering::Acquire) == 0 {
        return;
    }
    let fault = payload.downcast_ref::<Box<OriginalFault>>().unwrap();
    facts
        .retained_payload
        .store(&**fault as *const _ as usize, Ordering::Release);
    if let Some(done) = done {
        facts
            .retained_io
            .store(io_identity(done), Ordering::Release);
        *facts.retained_child.lock().unwrap() =
            Some(done.direct.as_ref().unwrap().report.child.clone());
    }
}
struct JobWork<F> {
    inner: Option<Pin<Box<F>>>,
    control: Arc<Control>,
}
pub(in crate::tuic::probe) fn wrap<F: Future<Output = Finished> + Send>(
    work: F,
    control: Arc<Control>,
) -> impl Future<Output = Finished> + Send {
    JobWork {
        inner: Some(Box::pin(work)),
        control,
    }
}
impl<F: Future<Output = Finished>> Future for JobWork<F> {
    type Output = Finished;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Finished> {
        let facts = &self.control.facts.aggregate;
        facts.polls.fetch_add(1, Ordering::AcqRel);
        if facts.mode.load(Ordering::Acquire) == 1 && facts.armed.swap(false, Ordering::AcqRel) {
            let payload = facts.payload.lock().unwrap().take().unwrap();
            std::panic::panic_any(payload);
        }
        match self.inner.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(done) => {
                let facts = &self.control.facts.aggregate;
                if facts.mode.load(Ordering::Acquire) == 2 {
                    assert!(done.settled());
                    facts.ready_io.store(io_identity(&done), Ordering::Release);
                    *facts.ready_child.lock().unwrap() =
                        Some(done.direct.as_ref().unwrap().report.child.clone());
                    *facts.deadline.lock().unwrap() = *self.control.deadline.lock().unwrap();
                }
                Poll::Ready(done)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<F> Drop for JobWork<F> {
    fn drop(&mut self) {
        let facts = &self.control.facts.aggregate;
        facts.drops.fetch_add(1, Ordering::AcqRel);
        drop(self.inner.take());
        if facts.mode.load(Ordering::Acquire) == 2 {
            let payload = facts.payload.lock().unwrap().take().unwrap();
            std::panic::panic_any(payload);
        }
    }
}
async fn child_finished(facts: &super::faults::Facts) {
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
}
fn arm(control: &Control, mode: u8) -> usize {
    let facts = &control.facts.aggregate;
    facts.mode.store(mode, Ordering::Release);
    let payload = Box::new(OriginalFault(facts.payload_drops.clone()));
    let identity = &*payload as *const _ as usize;
    *facts.payload.lock().unwrap() = Some(payload);
    identity
}
fn charged(probe: &Probe, identity: usize, drops: usize) {
    let facts = &probe.control.facts.aggregate;
    assert!(probe.driver.is_some() && probe.completion.is_none() && probe.panic.is_none());
    assert_eq!(probe.control.phase(), Phase::Quarantined);
    assert_eq!(facts.retained_payload.load(Ordering::Acquire), identity);
    assert_eq!(facts.payload_drops.load(Ordering::Acquire), 0);
    assert_eq!(facts.drops.load(Ordering::Acquire), drops);
    assert!(probe.control.facts.data.lock().unwrap().upgrade().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn whole_job_poll_unwind_stops_child_with_original_open_and_node_still_charged() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_direct(
        config.clone(), target.clone(), 1, 2,
        command("import socket,sys,os,time\np=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); s.sendall(b'aggregate'); assert s.recv(5)==b'ready'; os.write(1,b'job-output'); os.write(2,b'job-stderr'); time.sleep(60)"),
        Arc::new(Metrics::default()), None,
    ).unwrap();
    let control = probe.control.clone();
    let facts = control.facts.clone();
    let identity = arm(&control, 1);
    let (peer, send, recv) = tokio::time::timeout(Duration::from_secs(5), async {
        let mut run = Box::pin(probe.run());
        let peer = async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &config).await;
            let (mut send, mut recv) = peer.accept_bi().await.unwrap();
            let header = encode_connect(&target);
            let mut actual = vec![0; header.len()];
            recv.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, header);
            let mut bytes = [0; 9];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"aggregate");
            send.write_all(b"ready").await.unwrap();
            while facts.direct.stderr_bytes.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            (peer, send, recv)
        };
        tokio::select! {biased; value=&mut run=>panic!("early {value:?}"), pair=peer=>pair}
    })
    .await
    .unwrap();
    let pid = facts.direct.pid.load(Ordering::Acquire);
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, 0);
    assert!(control.deadline.lock().unwrap().is_none());
    facts.aggregate.armed.store(true, Ordering::Release);
    let before = tokio::time::Instant::now();
    let first = probe.run().await;
    let after = tokio::time::Instant::now();
    let deadline = control.deadline.lock().unwrap().unwrap();
    let prompt = tokio::time::timeout(Duration::from_secs(3), async {
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
    .is_ok();
    let stopped = control.stopped.load(Ordering::Acquire);
    charged(&probe, identity, 0);
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
    let polls = facts.aggregate.polls.load(Ordering::Acquire);
    assert_eq!(probe.run().await, first);
    assert_eq!(facts.aggregate.polls.load(Ordering::Acquire), polls);
    assert_eq!(*control.deadline.lock().unwrap(), Some(deadline));
    assert!(
        deadline >= before + Duration::from_secs(15) && deadline <= after + Duration::from_secs(15)
    );
    drop(probe);
    child_finished(&facts).await;
    let child = facts.direct.child.lock().unwrap().clone().unwrap();
    let gone = unsafe { libc::kill(pid as i32, 0) };
    let errno = std::io::Error::last_os_error().raw_os_error();
    peer.close(0u32.into(), b"charged original job/Node retained");
    drop((send, recv, peer));
    close(server).await;
    assert!(child.killed && child.waited && child.stdout_eof && child.stderr_eof && !child.joined);
    assert_eq!(child.stdout.as_slice(), b"job-output");
    assert_eq!(child.stderr.as_slice(), b"job-stderr");
    assert_eq!(facts.direct.joins.load(Ordering::Acquire), 0);
    assert_eq!(gone, -1);
    assert_eq!(errno, Some(libc::ESRCH));
    assert_eq!(facts.aggregate.drops.load(Ordering::Acquire), 0);
    assert_eq!(facts.aggregate.payload_drops.load(Ordering::Acquire), 0);
    eprintln!(
        "wholeJob poll: beforeDrop stop={stopped}, actorFinished={prompt}; finallyactualkillwaitdualEOF/join0/Open1"
    );
    assert!(
        stopped && prompt,
        "wholeJob poll uncertainty must stop actual Child before Probe Drop"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn whole_job_drop_unwind_keeps_first_io_and_settled_child_but_does_not_clean_node() {
    let server = server();
    let config = config(&server);
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_direct(
        config.clone(), target.clone(), 1, 2,
        command("import socket,sys,os\np=int(sys.argv[sys.argv.index('-p')+1]); s=socket.create_connection(('127.0.0.1',p)); os.write(1,b'job-output'); os.write(2,b'job-stderr'); s.sendall(b'original'); s.recv(1); sys.exit(7)"),
        Arc::new(Metrics::default()), None,
    ).unwrap();
    let control = probe.control.clone();
    let facts = control.facts.clone();
    let identity = arm(&control, 2);
    facts.fault.store(4, Ordering::Release);
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let header = encode_connect(&target);
        let mut actual = vec![0; header.len()];
        recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        let mut bytes = [0; 8];
        recv.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"original");
        send.write_all(b"original-read-fault").await.unwrap();
        send.finish().unwrap();
        let _ = recv.read_to_end(64).await;
        drop((send, recv));
        peer
    };
    let (first, peer) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(probe.run(), peer)
    })
    .await
    .unwrap();
    let stopped = control.stopped.load(Ordering::Acquire);
    charged(&probe, identity, 1);
    assert_eq!(first.phase, Phase::Quarantined);
    assert_eq!(first.node, None);
    assert!(peer.close_reason().is_none());
    assert_ne!(facts.aggregate.ready_io.load(Ordering::Acquire), 0);
    assert_eq!(
        facts.aggregate.ready_io.load(Ordering::Acquire),
        facts.aggregate.retained_io.load(Ordering::Acquire)
    );
    let child = facts.aggregate.ready_child.lock().unwrap().clone().unwrap();
    assert_eq!(
        facts.aggregate.retained_child.lock().unwrap().clone(),
        Some(child.clone())
    );
    assert!(child.waited && child.stdout_eof && child.stderr_eof && child.joined && !child.success);
    assert_eq!(child.stdout.as_slice(), b"job-output");
    assert_eq!(child.stderr.as_slice(), b"job-stderr");
    assert_eq!(facts.direct.joins.load(Ordering::Acquire), 1);
    assert_eq!(
        facts
            .data
            .lock()
            .unwrap()
            .upgrade()
            .unwrap()
            .tcp_pool_admission
            .active_total(),
        0
    );
    let deadline = *facts.aggregate.deadline.lock().unwrap();
    let polls = facts.aggregate.polls.load(Ordering::Acquire);
    assert_eq!(probe.run().await, first);
    assert_eq!(facts.aggregate.polls.load(Ordering::Acquire), polls);
    assert_eq!(*control.deadline.lock().unwrap(), deadline);
    drop(probe);
    child_finished(&facts).await;
    let pid = facts.direct.pid.load(Ordering::Acquire);
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    peer.close(0u32.into(), b"actualNodecleanup forbidden byunknownJobDrop");
    drop(peer);
    close(server).await;
    assert_eq!(facts.aggregate.payload_drops.load(Ordering::Acquire), 0);
    assert_eq!(facts.aggregate.drops.load(Ordering::Acquire), 1);
    eprintln!(
        "wholeJob Drop: originalIO retained/Childactualjoin1/NodeDATAretained/stop={stopped}"
    );
    assert!(
        stopped,
        "wholeJob Drop uncertainty must set original outer stop while retaining Node"
    );
}
