//! StateLock interruption is decided per critical section (std Mutex rule):
//! a guard taken while a foreign panic is already unwinding (a stream Drop)
//! completes its section and must not quarantine the Connection; a panic that
//! starts inside a held section still must.
use super::{super::*, retirement::Loopback};
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
async fn stream_dropped_by_foreign_unwind_keeps_connection_live() {
    let pair = Loopback::new().await;
    let mut send = pair.connection.open_uni().await.unwrap();
    send.write_all(b"prefix").await.unwrap();
    let unwound = catch_unwind(AssertUnwindSafe(move || {
        let _held = send; // Drop locks State during this foreign unwind.
        panic!("foreign panic outside every State section");
    }));
    assert!(unwound.is_err());
    assert!(!pair.connection.0.state.lock("observe").poisoned);
    assert!(pair.connection.close_reason().is_none());
    let mut next = pair.connection.open_uni().await.unwrap();
    next.write_all(b"live").await.unwrap();
    next.finish().unwrap();
    let read = timeout(Duration::from_secs(2), async {
        loop {
            let mut recv = pair.peer.accept_uni().await.unwrap();
            if recv.id() == next.id() {
                return recv.read_to_end(16).await.unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(read, b"live");
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn panic_inside_state_section_still_quarantines_connection() {
    let pair = Loopback::new().await;
    let interrupted = catch_unwind(AssertUnwindSafe(|| {
        let _state = pair.connection.0.state.lock("interrupted section");
        panic!("panic inside the State critical section");
    }));
    assert!(interrupted.is_err());
    assert!(pair.connection.0.state.lock("observe").poisoned);
    assert!(pair.connection.close_reason().is_some());
}
