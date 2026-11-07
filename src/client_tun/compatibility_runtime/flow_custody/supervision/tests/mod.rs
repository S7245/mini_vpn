//! Real role gates, joins, queued command disposal and source-error order.
use super::*;
use smoltcp::iface::SocketSet;
use smoltcp::socket::tcp::{Socket, SocketBuffer};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
mod native_start;
mod start_gate;

#[derive(Default)]
struct Probe {
    reads: AtomicUsize,
    writes: AtomicUsize,
    shutdowns: AtomicUsize,
    drops: AtomicUsize,
    fail_at_write: AtomicUsize,
}
struct Stream {
    probe: Arc<Probe>,
    fail: bool,
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.probe.reads.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Poll::Ready(Err(std::io::Error::from_raw_os_error(54)))
        } else {
            Poll::Pending
        }
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let write = self.probe.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail || self.probe.fail_at_write.load(Ordering::SeqCst) == write {
            Poll::Ready(Err(std::io::Error::from_raw_os_error(32)))
        } else {
            Poll::Ready(Ok(bytes.len()))
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.probe.shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Poll::Ready(Err(std::io::Error::from_raw_os_error(5)))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Fixture {
    roles: FlowRoles,
    tx: crate::client_tun::uplink::Sender,
    _back: crate::client_tun::relay_channel::Receiver<(SocketHandle, RelayEvent)>,
    _credit: watch::Sender<RelayReadCredit>,
    probe: Arc<Probe>,
    _window: super::super::super::receive_window::ReceiveWindow,
}
fn fixture(fail: bool) -> Fixture {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let probe = Arc::new(Probe::default());
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, back) = crate::client_tun::relay_channel::channel(2);
    let (credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let mut roles = FlowRoles::new(GenericLaunchRequest {
        engine: TcpRelayEngine::Legacy,
        handle,
        epoch: 7,
        stream: Box::new(Stream {
            probe: probe.clone(),
            fail,
        }),
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    });
    let mut window = super::super::super::receive_window::ReceiveWindow::new();
    roles.prepare(window.take_reader().unwrap());
    Fixture {
        roles,
        tx,
        _back: back,
        _credit: credit,
        _window: window,
        probe,
    }
}
// Actor-only fixtures run the same bounded physical disposition outside any
// Session lock. Full identity/turn-loss tests use original OpenDispatch instead.
fn poll_role_fixture(roles: &mut FlowRoles, cx: &mut Context<'_>, stopped: bool) {
    roles.poll(cx, stopped);
    if let Some(native) = &mut roles.native {
        if let Some(mut work) = native.take_disposal(stopped) {
            work.run(cx);
            assert!(work.matches(native));
            work.restore(native);
            cx.waker().wake_by_ref();
        }
    }
}
async fn until(roles: &mut FlowRoles, stopped: bool, done: impl Fn(&FlowRoles) -> bool) {
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        std::future::poll_fn(|cx| {
            poll_role_fixture(roles, cx, stopped);
            if done(roles) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn dropped_start_gates_join_without_io_and_keep_outstanding_command_permit() {
    let mut f = fixture(false);
    f.tx.try_send(RelayCommand::Data(vec![1, 2, 3])).unwrap();
    let permit = f.tx.legacy_sender_for_test().expect("original Legacy Tokio permit").clone().reserve_owned().await.unwrap();
    drop(f.tx);
    f.roles.stop();
    until(&mut f.roles, false, |roles| roles.join_count() == 3).await;
    assert_eq!(f.probe.reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.probe.writes.load(Ordering::SeqCst), 0);
    assert_eq!(f.probe.shutdowns.load(Ordering::SeqCst), 0);
    assert_eq!(f.probe.drops.load(Ordering::SeqCst), 1);
    assert!(!f.roles.quarantined());
    assert!(!f.roles.settled());
    f.roles
        .poll(&mut Context::from_waker(std::task::Waker::noop()), true);
    assert!(
        f.roles.writer_receipt.is_none(),
        "outstanding actual permit is still live"
    );
    drop(permit);
    until(&mut f.roles, true, FlowRoles::settled).await;
    let receipt = f.roles.writer_receipt.unwrap();
    assert_eq!((receipt.queued.messages, receipt.queued.bytes), (1, 3));
    assert_eq!(f.roles.reader_receipt.unwrap().unpublished_bytes, 0);
}

#[tokio::test]
async fn missing_one_start_gate_after_publication_cannot_settle_clean() {
    let mut f = fixture(false);
    drop(f.roles.starts[1].take());
    f.roles.start();
    drop(f.tx);
    until(&mut f.roles, false, FlowRoles::quarantined).await;
    f.roles.stop();
    until(&mut f.roles, false, |roles| roles.join_count() == 3).await;
    assert_eq!(f.probe.reads.load(Ordering::SeqCst), 0);
    assert!(f.roles.quarantined());
    assert!(!f.roles.settled());
    assert!(
        f.roles.writer_receipt.is_none(),
        "unknown producer has no disposal receipt"
    );
    assert!(f.roles.reader_receipt.is_none());
}

#[tokio::test]
async fn actual_reader_error_precedes_writer_and_shutdown_errors() {
    let mut f = fixture(true);
    f.tx.try_send(RelayCommand::Data(vec![7])).unwrap();
    drop(f.tx);
    f.roles.starts[1].take().unwrap().send(()).unwrap();
    until(&mut f.roles, false, |roles| {
        matches!(roles.reader, Role::Joined(_))
    })
    .await;
    f.roles.start();
    until(&mut f.roles, false, |roles| roles.join_count() == 3).await;
    f.roles.stop();
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.take_error().unwrap().raw_os_error(), Some(54));
    assert_eq!(f.roles.take_error().unwrap().raw_os_error(), Some(32));
    assert_eq!(f.roles.take_error().unwrap().raw_os_error(), Some(5));
    assert!(f.roles.take_error().is_none());
    assert_eq!(f.roles.failures.load(Ordering::SeqCst), 3);
}

mod notification_fatal;
