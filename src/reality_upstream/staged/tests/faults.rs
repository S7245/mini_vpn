use super::*;
use crate::upstream::TcpOpenCancel;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[tokio::test]
async fn concurrent_actual_carrier_return_between_observations_cannot_false_quarantine() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let (mut open, peer) = authenticated_peer().await;
    until(&mut open, TcpOpenPhase::Ready).await;
    let carrier = open.take_carrier(Instant::now()).unwrap();
    let (start, started) = std::sync::mpsc::sync_channel(1);
    let (finished, done) = std::sync::mpsc::sync_channel(1);
    open.return_observation = Some(ReturnObservation {
        start,
        finished: done,
    });
    let original_return = std::thread::spawn(move || {
        started.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(carrier); // actual inner socket halves, marker Arc and original wake
        finished.send(()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    settle_at(&mut open, Instant::now(), deadline);
    original_return.join().unwrap();
    assert_eq!(
        open.view().phase,
        TcpOpenPhase::Retiring,
        "concurrent return must be pending for one turn"
    );
    assert!(open.witness.is_some());
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    joined(peer).await;
}

#[tokio::test]
async fn actual_handshake_future_destruction_panic_retains_physical_witness() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    until(&mut open, TcpOpenPhase::Preparing).await;
    let (mut peer, _) = listener.accept().await.unwrap();
    turn(&mut open);
    let mut hello = [0; 1];
    peer.read_exact(&mut hello).await.unwrap();
    assert_eq!(hello[0], 0x16);
    struct DestructionFault(Option<super::super::Handshake>);
    impl Future for DestructionFault {
        type Output = Result<
            (
                tokio::net::TcpStream,
                crate::reality::handshake::HandshakeOutput,
            ),
            crate::shared::ClientError,
        >;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.0.as_mut().unwrap().as_mut().poll(cx)
        }
    }
    impl Drop for DestructionFault {
        fn drop(&mut self) {
            drop(self.0.take()); // actual original stream and establish borrow
            panic!("injected original handshake destructor fault");
        }
    }
    let super::super::State::Handshake(original) =
        std::mem::replace(&mut open.state, super::super::State::Retiring)
    else {
        panic!("actual saved handshake");
    };
    open.state = super::super::State::Handshake(Box::pin(DestructionFault(Some(original))));
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    let first = open.failure().unwrap().to_string();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        settle_at(&mut open, Instant::now(), deadline);
    }));
    assert!(result.is_err());
    assert!(open.interrupted && open.witness.is_some());
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.view().lease_held && open.view().retirement_pending);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.failure().unwrap().to_string(), first);
    assert!(open.quarantined && open.witness.is_some());
    let mut rest = Vec::new();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.read_to_end(&mut rest))
            .await
            .is_err()
    );
    drop(open); // force-close retained witness; not an async-clean receipt
    tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_receipt_fault_retains_actual_witness_and_original_error() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(listener.local_addr().unwrap().to_string());
    until(&mut open, TcpOpenPhase::Connecting).await;
    let (mut peer, _) = listener.accept().await.unwrap();
    open.shutdown_fault = true; // control fault injection, not a real OS failure claim
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    let first = open.failure().unwrap().to_string();
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.witness.is_some());
    assert!(
        open.shutdown
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("injected")
    );
    assert_eq!(open.failure().unwrap().to_string(), first);
    open.shutdown_fault = false;
    settle_at(&mut open, Instant::now(), deadline);
    assert!(open.witness.is_some() && open.view().phase == TcpOpenPhase::Quarantined);
    let mut first_byte = [0; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.read(&mut first_byte))
            .await
            .is_err()
    );
    drop(open); // explicit forced Q destruction
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peer.read(&mut first_byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn altered_transferred_marker_cannot_forge_carrier_release() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let (mut open, peer) = authenticated_peer().await;
    until(&mut open, TcpOpenPhase::Ready).await;
    let carrier = open.take_carrier(Instant::now()).unwrap();
    open.release
        .phase
        .store(super::super::RELEASED, std::sync::atomic::Ordering::Release);
    drop(carrier);
    assert_eq!(
        open.release
            .phase
            .load(std::sync::atomic::Ordering::Acquire),
        super::super::BROKEN
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.witness.is_some() && open.view().retirement_pending);
    drop(open); // no clean claim for a corrupted release receipt
    joined(peer).await;
}
