//! Explicit faults wrap an ORIGINAL actual QUIC carrier; never a success stub.
use super::super::*;
use super::lifecycle::{auth, close, config, server};
use crate::tuic::encode_connect;
use crate::upstream::{OpenedTcpRelay, RelayStream, TcpOpenView};
use std::sync::Weak;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
pub(in crate::tuic::probe) struct Facts {
    pub(in crate::tuic::probe) aggregate: super::job_unwind::Facts,
    pub(in crate::tuic::probe) direct: super::direct::Facts,
    pub(in crate::tuic::probe) data: Mutex<Weak<TuicUpstream>>,
    pub(in crate::tuic::probe) opening: Mutex<Option<(usize, std::time::Instant, TcpOpenView)>>,
    pub(in crate::tuic::probe) stats_abort: Mutex<Option<tokio::task::AbortHandle>>,
    pub(in crate::tuic::probe) fault: AtomicU8,
    polls: AtomicUsize,
    drops: AtomicUsize,
    pub(in crate::tuic::probe) attempt_drops: AtomicUsize,
    settled_view: Mutex<Option<TcpOpenView>>,
    pub(in crate::tuic::probe) payload_drops: Arc<AtomicUsize>,
    pub(in crate::tuic::probe) before: Mutex<Option<TcpOpenView>>,
    pub(in crate::tuic::probe) after: Mutex<Option<TcpOpenView>>,
    pub(in crate::tuic::probe) hold: AtomicBool,
    pub(in crate::tuic::probe) entered: Notify,
    pub(in crate::tuic::probe) release: Notify,
    pub(in crate::tuic::probe) known_cleanup_failure: AtomicBool,
}
pub(in crate::tuic::probe) async fn gate(control: &Control) {
    if control.facts.hold.load(Ordering::Acquire) {
        control.facts.entered.notify_one();
        tokio::select! {biased; _=control.stop()=>{},_=control.facts.release.notified()=>{}}
    }
}
struct Payload(Arc<AtomicUsize>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unknown payload destructor must never be retried");
    }
}
struct ActualCarrier {
    inner: RelayStream,
    facts: Arc<Facts>,
}
impl AsyncRead for ActualCarrier {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.facts.polls.fetch_add(1, Ordering::SeqCst);
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(())))
            && !buf.filled().is_empty()
            && self
                .facts
                .fault
                .compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Poll::Ready(Err(std::io::Error::other(
                "injected original carrier read failure",
            )));
        }
        if matches!(result, Poll::Ready(Ok(())))
            && !buf.filled().is_empty()
            && self
                .facts
                .fault
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            std::panic::panic_any(Payload(self.facts.payload_drops.clone()));
        }
        result
    }
}
impl AsyncWrite for ActualCarrier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        b: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl Drop for ActualCarrier {
    fn drop(&mut self) {
        self.facts.drops.fetch_add(1, Ordering::SeqCst);
        if self
            .facts
            .fault
            .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            std::panic::panic_any(Payload(self.facts.payload_drops.clone()));
        }
    }
}
pub(in crate::tuic::probe) fn wrap(
    carrier: OpenedTcpRelay,
    control: &Control,
    view: TcpOpenView,
) -> OpenedTcpRelay {
    if control.facts.fault.load(Ordering::Acquire) == 0 {
        return carrier;
    }
    *control.facts.before.lock().unwrap() = Some(view);
    match carrier {
        OpenedTcpRelay::Generic(inner) => OpenedTcpRelay::Generic(Box::new(ActualCarrier {
            inner,
            facts: control.facts.clone(),
        })),
        other => other,
    }
}

async fn actual_fault(kind: u8) {
    let server = server();
    let config = config(&server);
    let mut probe = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = probe.control.facts.clone();
    facts.fault.store(kind, Ordering::SeqCst);
    let work = async {
        let observed = probe.run().await;
        assert_eq!(observed.phase, Phase::Quarantined);
        let original_deadline = (*probe.control.deadline.lock().unwrap())
            .expect("actual Q must start original first15 clock before an observer");
        let before = facts.before.lock().unwrap().unwrap();
        assert!(before.lease_held && before.retirement_pending);
        let data = facts
            .data
            .lock()
            .unwrap()
            .upgrade()
            .expect("original Node DATA must remain occupied");
        assert_eq!(
            data.tcp_pool_admission.active_total(),
            1,
            "original attempt lease must remain occupied"
        );
        let after = facts
            .after
            .lock()
            .unwrap()
            .expect("original attempt must be read after the caught fault");
        assert!(after.lease_held && after.retirement_pending);
        assert_eq!(
            facts.drops.load(Ordering::SeqCst),
            if kind == 1 { 0 } else { 1 }
        );
        let polls = facts.polls.load(Ordering::SeqCst);
        assert_eq!(probe.run().await.phase, Phase::Quarantined);
        let observer = probe.take_handle().unwrap();
        assert_eq!(observer.observe_shutdown().await, Phase::Quarantined);
        assert_eq!(
            *probe.control.deadline.lock().unwrap(),
            Some(original_deadline)
        );
        assert_eq!(facts.polls.load(Ordering::SeqCst), polls);
        assert_eq!(facts.payload_drops.load(Ordering::SeqCst), 0);
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let mut header =
            vec![0; encode_connect(&TargetAddr::parse("192.0.2.1:443").unwrap()).len()];
        recv.read_exact(&mut header).await.unwrap();
        send.write_all(b"actual-fault").await.unwrap();
        send.finish().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            peer.close_reason().is_none(),
            "root must not close Node after unknown job fault"
        );
        peer.close(0u32.into(), b"forced Q test exit, not clean acceptance");
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(work, peer);
    })
    .await
    .unwrap();
    drop(probe);
    assert_eq!(facts.payload_drops.load(Ordering::SeqCst), 0);
    close(server).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_carrier_read_panic_keeps_original_open_and_node_occupied() {
    actual_fault(1).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_carrier_drop_panic_keeps_remaining_open_and_node_occupied() {
    actual_fault(2).await;
}

type OriginalAttempt = Box<dyn crate::upstream::TcpOpenAttempt<OpenedTcpRelay>>;
struct ActualAttempt {
    inner: Option<OriginalAttempt>,
    facts: Arc<Facts>,
}
impl crate::upstream::TcpOpenAttempt<OpenedTcpRelay> for ActualAttempt {
    fn step(&mut self, cx: &mut std::task::Context<'_>, now: std::time::Instant) {
        self.inner.as_mut().unwrap().step(cx, now);
    }
    fn view(&self) -> TcpOpenView {
        self.inner.as_ref().unwrap().view()
    }
    fn cancel(&mut self, reason: crate::upstream::TcpOpenCancel, deadline: std::time::Instant) {
        self.inner.as_mut().unwrap().cancel(reason, deadline);
    }
    fn take_carrier(&mut self, now: std::time::Instant) -> Option<OpenedTcpRelay> {
        self.inner.as_mut().unwrap().take_carrier(now)
    }
    fn settle(
        &mut self,
        cx: &mut std::task::Context<'_>,
        now: std::time::Instant,
        deadline: std::time::Instant,
    ) {
        self.inner.as_mut().unwrap().settle(cx, now, deadline);
    }
    fn failure(&self) -> Option<&ClientError> {
        self.inner.as_ref().unwrap().failure()
    }
}
impl Drop for ActualAttempt {
    fn drop(&mut self) {
        *self.facts.settled_view.lock().unwrap() = Some(self.inner.as_ref().unwrap().view());
        drop(self.inner.take());
        self.facts.attempt_drops.fetch_add(1, Ordering::SeqCst);
        if self
            .facts
            .fault
            .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            std::panic::panic_any(Payload(self.facts.payload_drops.clone()));
        }
    }
}
pub(in crate::tuic::probe) fn wrap_attempt(
    inner: OriginalAttempt,
    control: &Control,
) -> OriginalAttempt {
    if !matches!(control.facts.fault.load(Ordering::SeqCst), 1 | 3 | 4) {
        return inner;
    }
    Box::new(ActualAttempt {
        inner: Some(inner),
        facts: control.facts.clone(),
    })
}
#[tokio::test(flavor = "current_thread")]
async fn actual_settled_provider_drop_panic_precedes_node_cleanup_and_success_publication() {
    let server = server();
    let config = config(&server);
    let mut probe = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = probe.control.facts.clone();
    facts.fault.store(3, Ordering::SeqCst);
    let job = async {
        assert_eq!(probe.run().await.phase, Phase::Quarantined);
        let view = facts.settled_view.lock().unwrap().unwrap();
        assert_eq!(view.phase, crate::upstream::TcpOpenPhase::Settled);
        assert!(!view.lease_held && !view.retirement_pending);
        assert_eq!(facts.attempt_drops.load(Ordering::SeqCst), 1);
        assert!(
            facts.data.lock().unwrap().upgrade().is_some(),
            "remaining original Node must stay occupied"
        );
        assert_eq!(probe.run().await.phase, Phase::Quarantined);
        assert_eq!(facts.attempt_drops.load(Ordering::SeqCst), 1);
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let mut header =
            vec![0; encode_connect(&TargetAddr::parse("192.0.2.1:443").unwrap()).len()];
        recv.read_exact(&mut header).await.unwrap();
        send.write_all(b"provider-dtor").await.unwrap();
        send.finish().unwrap();
        let _ = recv.read_to_end(1024).await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(peer.close_reason().is_none());
        peer.close(0u32.into(), b"forced Q exit, not clean");
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(job, peer);
    })
    .await
    .unwrap();
    drop(probe);
    assert_eq!(facts.payload_drops.load(Ordering::SeqCst), 0);
    close(server).await;
}
