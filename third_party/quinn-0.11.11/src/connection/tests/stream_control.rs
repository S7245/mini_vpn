//! Actual host lock, protocol Root, first-error publication and driver wake.
//! Authority Q is injected after real bidirectional traffic; this is not Full.
use super::{super::*, retirement::Loopback};
use std::task::{Wake, Waker};
use tokio::time::timeout;

struct ForwardWake {
    original: Waker,
    count: Arc<std::sync::atomic::AtomicUsize>,
}
impl Wake for ForwardWake {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.original.wake_by_ref();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_local_stream_failure_publishes_first_and_wakes_original_driver() {
    for action in 0..8 {
        let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut pair = Loopback::with_meter(meter.clone()).await;
        let driver = pair.endpoint.take_driver_drain_handle().unwrap();
        let (mut send, mut recv) = pair.connection.open_bi().await.unwrap();
        send.write_all(b"request").await.unwrap();
        let (mut peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
        peer_recv.read_exact(&mut [0; 7]).await.unwrap();
        peer_send.write_all(b"response").await.unwrap();
        recv.read_exact(&mut [0; 1]).await.unwrap();
        let wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        timeout(Duration::from_secs(2), async {
            loop {
                {
                    let mut state = pair
                        .connection
                        .0
                        .state
                        .lock("install forwarding wake counter");
                    if let Some(original) = state.driver.take() {
                        state.driver = Some(Waker::from(Arc::new(ForwardWake {
                            original,
                            count: wakes.clone(),
                        })));
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // No await/task turn between Q and each synchronous first operation;
        // read/write futures must return their failure on their first poll.
        meter.quarantine();
        let mut send = Some(send);
        let mut recv = Some(recv);
        let rejected = match action {
            0 => send.as_mut().unwrap().reset(42u32.into()).is_err(),
            1 => recv.as_mut().unwrap().stop(42u32.into()).is_err(),
            2 => matches!(
                recv.as_mut().unwrap().read_chunk(1, true).await,
                Err(crate::ReadError::ConnectionLost(_))
            ),
            3 => {
                drop(send.take());
                true
            }
            4 => {
                drop(recv.take());
                true
            }
            5 => matches!(
                send.as_mut().unwrap().write(b"must refuse").await,
                Err(crate::WriteError::Admission(bytes::MeteredCopyError::Quarantined))
            ),
            6 => send.as_mut().unwrap().finish().is_err(),
            7 => recv.as_mut().unwrap().received_reset().await.is_err(),
            _ => unreachable!(),
        };
        let woke = wakes.load(Ordering::Relaxed) > 0;
        let first = pair.connection.close_reason();
        let typed = matches!(&first, Some(ConnectionError::TransportError(error))
            if error.code == proto::TransportErrorCode::INTERNAL_ERROR);
        let preserved = {
            let mut state = pair.connection.0.state.lock("later failure keeps first");
            // Explicit later-failure fault at the original sole host setter.
            state.terminate(ConnectionError::LocallyClosed, &pair.connection.0.shared);
            state.error == first
        };
        // Capture evidence before cleanup; cleanup cannot replace an earlier
        // failed assertion with a second panic or promote Q to clean.
        drop((send, recv, peer_send, peer_recv));
        pair.connection.close(0u32.into(), b"test stop");
        pair.peer.close(0u32.into(), b"test stop");
        pair.endpoint.close(0u32.into(), b"test stop");
        drop(pair);
        let release = timeout(Duration::from_secs(2), driver.drained()).await;
        assert!(rejected, "action {action} did not reject after Q");
        assert!(
            woke,
            "action {action} failed without waking the original driver"
        );
        assert!(
            typed && preserved,
            "action {action}: first={first:?} preserved={preserved}"
        );
        assert!(budget.quarantined() && budget.used() > 0);
        eprintln!("real-host-stream-Q action={action} driver_wakes={} first={first:?} later_first_preserved={preserved} endpoint_release={release:?} charged={} Q=true; natural_Full=false clean_join=false", wakes.load(Ordering::Relaxed), budget.used());
    }
}
