//! Actual RelayEvent -> original leased permit -> original Notify counterexample.
use super::*;
use crate::{
    client_tun::{RelayEvent, SocketHandle, StreamServiceWindow},
    tcp_downlink_pump::*,
};
use std::future::Future;
#[test]
fn actual_event_permit_notify_retains_both_original_panics() {
    const CHILD: &str = "N1_GLOBAL_CORE_ACTUAL_PERMIT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let budget = bytes::BackingBudget::new(1 << 22).unwrap();
                let meter = BackingMeter::with_budget(&budget);
                let (clock, facts) = clock(&meter);
                clock.io(&std::io::Error::from_raw_os_error(5));
                let queue = AsyncLeasedByteFlowQueue::new_custodied_with_mode(
                    3,
                    DownstreamPermitReleaseMode::OnEgressDrain,
                )
                .unwrap();
                queue.bind_native_meter(Some(meter.clone())).unwrap();
                assert!(queue.bind_native_failures(clock.clone()));
                let mut pending = Some(OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(b"abc", &meter).unwrap()).into()));
                queue.push_custodied(&mut pending).await.unwrap();
                let mut batch = None;
                assert!(queue.recv_custodied(3, &mut batch).await.unwrap());
                let batch = batch.unwrap();
                let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
                let handle: SocketHandle = sockets.add(smoltcp::socket::tcp::Socket::new(
                    smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
                    smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
                ));
                let (tx, rx, observer) =
                    native_channel::<(SocketHandle, RelayEvent)>(1, &meter, clock).unwrap();
                tx.reserve().await.unwrap().send((
                    handle,
                    RelayEvent::Data {
                        epoch: 1,
                        bytes: OwnedPayload::Native((batch.data.bytes).into()),
                        stream_service_window: StreamServiceWindow::default(),
                        downstream_permit: Some(batch.permit),
                    },
                ));
                let mut wait = Box::pin(queue.wait_read_len(1));
                let waker = Waker::from(Arc::new(Twice));
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                drop(waker);
                drop(rx); // Actual same Core's closed drain, no synthetic drop surrogate.
                assert!(meter.is_quarantined());
                let ledger = facts.0.lock().unwrap();
                assert_eq!(ledger.len(), 3);
                assert_eq!(ledger[0].facts().raw_os_code, Some(5));
                assert_eq!(ledger[1].facts().class, FailureClass::Panic);
                assert_eq!(ledger[2].facts().class, FailureClass::Panic);
                drop(ledger);
                assert!(observer.held_for_test());
                assert!(matches!(
                    wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Err(AsyncByteQueueWait::Quarantined))
                ));
                drop(wait);
                drop(queue);
                drop(tx);
                drop(observer);
                assert!(budget.used() > 1);
            });
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "client_tun::relay_channel::tests::permit_drop::actual_event_permit_notify_retains_both_original_panics", "--nocapture"])
        .env(CHILD, "1").env("RUST_BACKTRACE", "0").output().unwrap();
    eprintln!(
        "actual event child: {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(output.status.signal(), None);
        assert!(
            output.status.success(),
            "child must actually verify original first/Box facts/custody/Q"
        );
    }
}
