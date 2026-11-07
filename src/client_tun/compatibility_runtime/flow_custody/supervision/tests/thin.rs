//! Actual Thin role/window/queue dispositions. These deterministic module tests
//! complement the real public QUIC tracer; they do not claim physical TUN ACKs.
use super::super::super::receive_window::{ReceiveWindow, WINDOW_BYTES};
use super::super::thin;
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Wake, Waker};
use tokio::io::AsyncWriteExt;

fn handle() -> SocketHandle {
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    sockets.add(smoltcp::socket::tcp::Socket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
    ))
}
async fn until(roles: &mut FlowRoles, done: impl Fn(&FlowRoles) -> bool) {
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| {
            roles.poll(cx, true);
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
fn data(event: (SocketHandle, RelayEvent)) -> Vec<u8> {
    match event.1 {
        RelayEvent::Data {
            epoch: 7, bytes, ..
        } => bytes.to_vec(),
        _ => panic!("actual DATA"),
    }
}
#[tokio::test]
async fn fifo_eof_waits_for_both_main_windows_before_four_original_joins() {
    let (local, mut remote) = tokio::io::duplex(256 * 1024);
    let expected: Vec<u8> = (0..128 * 1024).map(|n| (n % 251) as u8).collect();
    remote.write_all(&expected).await.unwrap();
    remote.shutdown().await.unwrap();
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, mut back) = crate::client_tun::relay_channel::channel(4);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit::default());
    let mut window = ReceiveWindow::new();
    let mut roles = FlowRoles::new(GenericLaunchRequest {
        engine: TcpRelayEngine::ThinStaging,
        handle: handle(),
        epoch: 7,
        stream: Box::new(local),
        rx,
        back_tx,
        read_credit_rx: credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    });
    roles.prepare(window.take_reader().unwrap());
    roles.start();
    let first = data(
        tokio::time::timeout(std::time::Duration::from_secs(2), back.recv())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(first, expected[..WINDOW_BYTES]);
    until(&mut roles, |r| matches!(r.reader, Role::Joined(_))).await;
    for _ in 0..4 {
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(2),
            std::future::poll_fn(|cx| {
                roles.poll(cx, true);
                assert!(roles.closed().is_none() && !roles.quarantined());
                assert!(back.try_recv().is_err());
                Poll::<()>::Pending
            }),
        )
        .await;
        assert!(pending.is_err());
    }
    let Role::Joined(reader) = &roles.reader else {
        unreachable!()
    };
    assert_eq!(reader.remainder.remote_total(), expected.len());
    assert_eq!(
        reader.remainder.published(),
        PublishedReceipt {
            messages: 1,
            bytes: expected.len()
        }
    );
    assert!(reader.notification.closed_sent);
    window.admit(WINDOW_BYTES, 0, first.len());
    window.publish();
    let second = data(
        tokio::time::timeout(std::time::Duration::from_secs(2), back.recv())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(second, expected[WINDOW_BYTES..]);
    until(&mut roles, |r| {
        r.closed() == Some(("remote_to_local", "remote_eof"))
    })
    .await;
    roles.stop();
    drop(tx);
    until(&mut roles, FlowRoles::settled).await;
    assert_eq!(roles.join_count(), 4);
    assert_eq!(
        roles.published(),
        Some(PublishedReceipt {
            messages: 2,
            bytes: expected.len()
        })
    );
    let receipt = roles.reader_receipt.unwrap();
    assert_eq!(receipt.unpublished_bytes, 0);
    assert_eq!(receipt.scratch_capacity, WINDOW_BYTES);
    assert!(
        receipt.batch_capacity >= receipt.batch_bytes
            && receipt.initial_capacity >= receipt.initial_bytes
    );
    assert!(roles.take_error().is_none());
}

struct Dispatcher {
    tx: mpsc::Sender<thin::StageItem>,
    back: crate::client_tun::relay_channel::Receiver<(SocketHandle, RelayEvent)>,
    _signals: mpsc::Receiver<RelayReaderSignal>,
    stop: oneshot::Sender<()>,
    gate: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<thin::Outcome>,
    _window: ReceiveWindow,
}
fn dispatcher(capacity: usize) -> Dispatcher {
    let (tx, rx) = mpsc::channel(THIN_TCP_RELAY_STAGING_CAPACITY);
    let (back_tx, back) = crate::client_tun::relay_channel::channel(capacity);
    let (signals, signal_rx) = mpsc::channel(2);
    let (stop, stop_rx) = oneshot::channel();
    let (gate, gate_rx) = oneshot::channel();
    let mut window = ReceiveWindow::new();
    let request = thin::Request {
        handle: handle(),
        epoch: 7,
        rx,
        back: Some(back_tx),
        signals: Some(signals),
        window: window.take_reader().unwrap(),
        stop: Some(stop_rx),
        gate: gate_rx,
    };
    Dispatcher {
        tx,
        back,
        _signals: signal_rx,
        stop,
        gate,
        task: tokio::spawn(thin::run(request)),
        _window: window,
    }
}
fn payload() -> thin::StageItem {
    let mut payload = Vec::with_capacity(192 * 1024);
    payload.resize(128 * 1024, 9);
    thin::StageItem::Data {
        epoch: 7,
        payload: payload.into(),
        window: Default::default(),
    }
}
#[tokio::test]
async fn full_64_slot_stage_and_half_held_item_dispose_after_actual_join() {
    let mut f = dispatcher(1);
    for _ in 0..64 {
        f.tx.try_send(payload())
            .unwrap_or_else(|_| panic!("64 original slots"));
    }
    assert!(matches!(
        f.tx.try_send(payload()),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    f.gate.send(()).unwrap();
    let published = data(
        tokio::time::timeout(std::time::Duration::from_secs(2), f.back.recv())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(published.len(), WINDOW_BYTES);
    // One real take has made exactly one stage slot available. Fill it again.
    f.tx.try_send(payload())
        .unwrap_or_else(|_| panic!("actual freed slot"));
    assert!(matches!(
        f.tx.try_send(payload()),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    f.stop.send(()).unwrap();
    drop(f.tx);
    let mut out = f.task.await.unwrap();
    assert!(out.started && !out.quarantined());
    assert_eq!(
        out.consumed,
        PublishedReceipt {
            messages: 1,
            bytes: 128 * 1024
        }
    );
    assert_eq!(
        out.published,
        PublishedReceipt {
            messages: 1,
            bytes: WINDOW_BYTES
        }
    );
    std::future::poll_fn(|cx| {
        out.dispose(cx);
        if out.receipt.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    let receipt = out.receipt.unwrap();
    assert_eq!(
        receipt.queued,
        PublishedReceipt {
            messages: 64,
            bytes: 8 * 1024 * 1024
        }
    );
    assert_eq!(receipt.queued_capacity, 64 * 192 * 1024);
    assert_eq!(
        (
            receipt.held_bytes,
            receipt.held_capacity,
            receipt.unpublished
        ),
        (128 * 1024, 192 * 1024, WINDOW_BYTES)
    );
    assert_eq!((receipt.segment_bytes, receipt.segment_capacity), (0, 0));
    assert!(out.audit(
        PublishedReceipt {
            messages: 65,
            bytes: 65 * 128 * 1024
        },
        false
    ));
    out.verify(
        PublishedReceipt {
            messages: 64,
            bytes: 65 * 128 * 1024,
        },
        false,
    );
    assert!(
        out.quarantined(),
        "mismatched independent message count stays charged Q"
    );
}
struct PanicWake(AtomicBool);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.0.swap(false, Ordering::SeqCst) {
            panic!("actual main send wake");
        }
    }
}
#[tokio::test]
async fn committed_global_send_wake_panic_retains_unknown_q_without_republish() {
    let mut f = dispatcher(1);
    f.tx.try_send(payload())
        .unwrap_or_else(|_| panic!("original DATA"));
    let panic = Arc::new(PanicWake(AtomicBool::new(true)));
    let waker = Waker::from(panic);
    let mut cx = Context::from_waker(&waker);
    assert!(f.back.poll_recv(&mut cx).is_pending());
    f.gate.send(()).unwrap();
    let mut out = tokio::time::timeout(std::time::Duration::from_secs(2), f.task)
        .await
        .unwrap()
        .unwrap();
    assert!(out.quarantined() && out.panics[0].is_some());
    assert_eq!(out.window_read(), WINDOW_BYTES);
    assert_eq!(
        out.published.bytes, 0,
        "send disposition interrupted before counter commit"
    );
    assert_eq!(
        data(f.back.try_recv().unwrap()).len(),
        WINDOW_BYTES,
        "physical DATA did enter original lane"
    );
    assert!(f.back.try_recv().is_err());
    drop(f.tx);
    drop(f.stop);
    out.dispose(&mut cx);
    assert!(out.receipt.is_none() && out.quarantined());
}
