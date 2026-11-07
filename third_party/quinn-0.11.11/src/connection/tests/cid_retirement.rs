//! Real driver Rebind refusal retains the original socket and protocol first.
use super::{super::*, retirement::Loopback};
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
async fn original_rebind_full_keeps_old_socket_first_and_actual_driver_release() {
    let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut pair = Loopback::with_control_limit(meter, 1).await;
    let driver = pair.endpoint.take_driver_drain_handle().unwrap();
    // Real traffic ensures the peer's unused remote CIDs have reached this Conn.
    let (mut send, mut recv) = pair.connection.open_bi().await.unwrap();
    send.write_all(b"before").await.unwrap();
    let (mut peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
    peer_recv.read_exact(&mut [0; 6]).await.unwrap();
    peer_send.write_all(b"ready").await.unwrap();
    recv.read_exact(&mut [0; 5]).await.unwrap();
    let old_socket = pair
        .connection
        .0
        .state
        .lock("original socket")
        .socket
        .clone();
    // No task turn: one real RESET holds the sole control node; Rebind must be
    // processed before the original driver transmits/receives its ACK.
    send.reset(7u32.into()).unwrap();
    pair.endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let first = timeout(Duration::from_secs(2), pair.connection.closed())
        .await
        .unwrap();
    assert!(
        matches!(&first, ConnectionError::TransportError(e) if e.code == proto::TransportErrorCode::INTERNAL_ERROR)
    );
    let (original_deadline, host_deadline, was_drained, same_socket, retained, facade, later) = {
        let mut state = pair
            .connection
            .0
            .state
            .lock("rebind rejected before socket commit");
        let deadline = state.inner.poll_timeout();
        let snapshot = (
            deadline,
            state.timer_deadline,
            state.inner.is_drained(),
            Arc::ptr_eq(&state.socket, &old_socket),
            state.inner.retained_error(),
            state.error.clone(),
        );
        state.terminate(ConnectionError::LocallyClosed, &pair.connection.0.shared);
        (
            snapshot.0,
            snapshot.1,
            snapshot.2,
            snapshot.3,
            snapshot.4,
            snapshot.5,
            state.error.clone(),
        )
    };
    // Read snapshots outside the lock so a RED cannot poison the original host.
    assert!(original_deadline.is_some());
    assert_eq!(
        host_deadline, original_deadline,
        "original driver must actually install Close timer before test cleanup"
    );
    assert!(!was_drained && same_socket);
    // The original driver has forwarded Connection::poll's one-shot error.
    // State now owns first; retaining a second Proto error would change API semantics.
    assert_eq!(retained, None);
    assert_eq!(facade, Some(first.clone()));
    assert_eq!(later, Some(first));
    let original_deadline = original_deadline.unwrap();
    let peer_first = timeout(Duration::from_secs(2), pair.peer.closed())
        .await
        .unwrap();
    assert!(
        matches!(peer_first, ConnectionError::ConnectionClosed(e) if e.error_code == proto::TransportErrorCode::INTERNAL_ERROR),
        "actual peer must decrypt original capacity Close"
    );
    // Another ordinary Rebind during Closing must not terminate the driver.
    pair.endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let (drained, now) = {
                let state = pair.connection.0.state.lock("actual original timed drain");
                (state.inner.is_drained(), state.runtime.now())
            };
            if drained {
                assert!(now >= original_deadline);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // Keep both connection refs and socket custody alive during actual drain.
    pair.peer.close(0u32.into(), b"peer known cleanup");
    timeout(Duration::from_secs(2), pair.endpoint.wait_idle())
        .await
        .unwrap();
    assert!(
        budget.used() > 0,
        "live refs still retain their exact cold owners"
    );
    drop((send, recv, peer_send, peer_recv, old_socket));
    pair.connection.close(0u32.into(), b"known cleanup");
    pair.peer.close(0u32.into(), b"known cleanup");
    pair.endpoint.close(0u32.into(), b"known cleanup");
    drop(pair);
    assert!(timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
