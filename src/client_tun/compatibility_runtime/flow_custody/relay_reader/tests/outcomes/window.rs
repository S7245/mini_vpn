use super::*;
use crate::client_tun::compatibility_runtime::flow_custody::receive_window::{
    ReceiveWindow, WINDOW_BYTES,
};

#[tokio::test]
async fn receive_window_failure_interrupts_wait_for_remote_bytes() {
    let mut f = fixture(vec![Read::Data(vec![1; 3]), Read::Pending], 8, false);
    let mut owner = ReceiveWindow::new();
    let mut task = Box::pin(run_custodied_relay_reader_gated(
        f.request,
        None,
        None,
        owner.take_reader(),
    ));
    assert!(
        task.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(data(f.back.try_recv().unwrap()).len(), 3);
    assert!(f.probe.reads.load(Ordering::SeqCst) >= 3);
    owner.audit_storage(3, 0, 0);
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), &mut task).await;
    if result.is_err() {
        f.stop.send(()).unwrap();
        let outcome = task.await;
        assert_eq!(outcome.terminal, ReaderTerminal::Stopped);
        panic!("issuer failure did not close original pending reader");
    }
    let mut outcome = result.unwrap();
    assert_eq!(outcome.terminal, ReaderTerminal::CreditClosed);
    assert_eq!(outcome.remainder.read_total(), Some(3));
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        0
    );
    assert!(f.probe.dropped.load(Ordering::SeqCst));
    let mut closed = false;
    while let Ok(signal) = f.signals.try_recv() {
        closed |= matches!(
            signal,
            RelayReaderSignal::Closed {
                reason: "receive_window_failed",
                ..
            }
        );
    }
    assert!(closed);
}

#[tokio::test]
async fn receive_window_real_gather_exhaustion_refill_and_join() {
    let mut f = fixture(
        vec![
            Read::Data(vec![1; 1]),
            Read::Data(vec![2; WINDOW_BYTES * 2]),
        ],
        8,
        false,
    );
    let mut owner = ReceiveWindow::new();
    let mut task = Box::pin(run_custodied_relay_reader_gated(
        f.request,
        None,
        None,
        owner.take_reader(),
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(task.as_mut().poll(&mut cx).is_pending());
    assert_eq!(data(f.back.try_recv().unwrap()).len(), WINDOW_BYTES);
    let reads = f.probe.reads.load(Ordering::SeqCst);
    for _ in 0..3 {
        f.credit.send(RelayReadCredit::default()).unwrap();
        owner.publish();
        assert!(task.as_mut().poll(&mut cx).is_pending());
        assert_eq!(f.probe.reads.load(Ordering::SeqCst), reads);
        assert!(f.back.try_recv().is_err());
    }
    owner.admit(WINDOW_BYTES, WINDOW_BYTES - 3, 3);
    owner.audit_storage(WINDOW_BYTES, WINDOW_BYTES - 3, 0);
    owner.publish();
    assert!(task.as_mut().poll(&mut cx).is_pending());
    assert_eq!(data(f.back.try_recv().unwrap()).len(), 3);
    assert!(f.back.try_recv().is_err());
    f.stop.send(()).unwrap();
    let mut outcome = task.await;
    assert_eq!(outcome.terminal, ReaderTerminal::Stopped);
    assert!(outcome.panics.iter().all(Option::is_none));
    assert_eq!(outcome.remainder.read_total(), Some(WINDOW_BYTES + 3));
    assert_eq!(outcome.remainder.published().bytes, WINDOW_BYTES + 3);
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        0
    );
    assert!(f.probe.dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn receive_window_issuer_failure_uses_real_closed_signal() {
    let mut f = fixture(vec![Read::Data(vec![7; WINDOW_BYTES * 2])], 8, false);
    let mut owner = ReceiveWindow::new();
    let mut task = Box::pin(run_custodied_relay_reader_gated(
        f.request,
        None,
        None,
        owner.take_reader(),
    ));
    assert!(
        task.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(data(f.back.try_recv().unwrap()).len(), WINDOW_BYTES);
    owner.audit_storage(WINDOW_BYTES, 0, 0);
    let mut outcome = task.await;
    assert_eq!(outcome.terminal, ReaderTerminal::CreditClosed);
    assert!(!outcome.remainder.quarantined());
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        0
    );
    let mut closed = false;
    while let Ok(signal) = f.signals.try_recv() {
        closed |= matches!(
            signal,
            RelayReaderSignal::Closed {
                reason: "receive_window_failed",
                ..
            }
        );
    }
    assert!(closed);
    assert_eq!(outcome.remainder.read_total(), Some(WINDOW_BYTES));
}

#[tokio::test]
async fn receive_window_read_error_and_panic_retain_original_outcome() {
    for panic in [false, true] {
        let mut f = fixture(
            vec![
                Read::Data(vec![9; 3]),
                if panic { Read::Panic } else { Read::Error(54) },
            ],
            8,
            false,
        );
        let mut owner = ReceiveWindow::new();
        let mut outcome =
            run_custodied_relay_reader_gated(f.request, None, None, owner.take_reader()).await;
        assert_eq!(outcome.remainder.read_total(), Some(3));
        if panic {
            assert!(outcome.panics[0].is_some());
            assert!(outcome.remainder.discard_local().is_none());
        } else {
            assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
            assert_eq!(data(f.back.try_recv().unwrap()), vec![9; 3]);
            assert_eq!(
                outcome.remainder.discard_local().unwrap().unpublished_bytes,
                0
            );
        }
    }
}
