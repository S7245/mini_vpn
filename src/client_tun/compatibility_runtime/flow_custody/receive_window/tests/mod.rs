use super::*;

#[test]
fn receive_window_exact_absolute_credit_and_admission() {
    let mut owner = ReceiveWindow::new();
    let mut reader = owner.take_reader().unwrap();
    assert!(owner.take_reader().is_none());
    assert_eq!(reader.remaining(), Some(WINDOW_BYTES));
    assert!(reader.debit(WINDOW_BYTES));
    for _ in 0..3 {
        owner.publish();
        assert_eq!(reader.remaining(), Some(0));
    }
    owner.admit(WINDOW_BYTES, WINDOW_BYTES - 7, 7);
    owner.audit_storage(WINDOW_BYTES, WINDOW_BYTES - 7, 0);
    owner.publish();
    assert_eq!(reader.remaining(), Some(7));
    assert!(reader.debit(7));
    owner.audit_reader(reader.read());
    assert_eq!(reader.remaining(), Some(0));
    assert!(!owner.failed());
}

#[test]
fn receive_window_unknown_clear_stops_without_refill() {
    let mut owner = ReceiveWindow::new();
    let mut reader = owner.take_reader().unwrap();
    assert!(reader.debit(33));
    owner.audit_storage(33, 0, 0);
    assert!(owner.take_error().is_some());
    owner.admit(33, 0, 33);
    owner.publish();
    assert_eq!((owner.admitted, owner.issued), (0, WINDOW_BYTES));
    assert_eq!(reader.remaining(), Some(0));
    assert!(reader.stopped());
    assert!(!reader.failed());
    assert!(owner.take_error().is_none());
}

#[test]
fn receive_window_bad_admission_overflow_and_duplication_fail_closed() {
    let mut owner = ReceiveWindow::new();
    owner.admit(10, 5, 4);
    assert!(owner.failed());
    assert_eq!(owner.admitted, 0);
    let mut owner = ReceiveWindow::new();
    owner.admitted = usize::MAX;
    owner.publish();
    assert!(owner.failed());
    let mut owner = ReceiveWindow::new();
    owner.tx.send_replace(Some(WINDOW_BYTES + 1));
    owner.publish();
    assert!(owner.failed());
    let mut owner = ReceiveWindow::new();
    owner.audit_storage(10, 6, 5);
    assert!(owner.failed());
    let mut owner = ReceiveWindow::new();
    owner.audit_reader(WINDOW_BYTES + 1);
    assert!(owner.failed());
}

#[test]
fn receive_window_known_abandonment_is_not_admission() {
    let mut owner = ReceiveWindow::new();
    let mut reader = owner.take_reader().unwrap();
    assert!(reader.debit(33));
    owner.audit_storage(33, 0, 33);
    owner.publish();
    assert!(!owner.failed());
    assert_eq!(owner.admitted, 0);
    assert_eq!(reader.remaining(), Some(WINDOW_BYTES - 33));
}

#[tokio::test]
async fn receive_window_receiver_drop_panic_is_retained_by_actual_reader_guard() {
    use crate::client_tun::compatibility_runtime::flow_custody::relay_reader::{
        ReaderRequest, run_custodied_relay_reader_gated,
    };
    use crate::client_tun::{RelayReadCredit, RelayStream};
    use std::future::Future;
    use std::sync::{Arc, atomic::AtomicBool};
    use std::task::{Context, Wake, Waker};
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("window receiver destructor wake");
        }
    }
    let mut owner = ReceiveWindow::new();
    let reader_window = owner.take_reader().unwrap();
    let mut closed = std::pin::pin!(owner.tx.closed());
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(
        closed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let (stream, peer) = tokio::io::duplex(8);
    let stream: RelayStream = Box::new(stream);
    let (remote_reader, _writer) = tokio::io::split(stream);
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
    ));
    let (back_tx, _back) = tokio::sync::mpsc::channel(1);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let (signal_tx, _signals) = tokio::sync::mpsc::channel(1);
    let (stop, stop_rx) = tokio::sync::oneshot::channel();
    stop.send(()).unwrap();
    let task = tokio::spawn(run_custodied_relay_reader_gated(
        ReaderRequest {
            handle,
            epoch: 1,
            remote_reader,
            back_tx,
            read_credit_rx,
            signal_tx,
            local_finish_seen: Arc::new(AtomicBool::new(false)),
            stop_rx,
        },
        None,
        None,
        Some(reader_window),
    ));
    let mut outcome = task.await.unwrap();
    assert!(outcome.panics[7].is_some());
    assert!(outcome.remainder.quarantined());
    assert!(outcome.remainder.discard_local().is_none());
    assert_eq!(outcome.remainder.read_total(), Some(0));
    assert_eq!((owner.admitted, owner.issued), (0, WINDOW_BYTES));
    drop(peer);
}
