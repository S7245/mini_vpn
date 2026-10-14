//! Exact two-sided stream retirement through real local Quinn endpoints.
use super::super::*;
use super::retirement::Loopback;
use tokio::time::timeout;

async fn retired(ticket: &BiStreamRetirement) {
    timeout(Duration::from_secs(2), async {
        while !ticket.is_retired() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both stream maps must actually retire");
}

fn send_retired(connection: &Connection, id: StreamId) -> bool {
    connection
        .0
        .state
        .lock("test send-side inventory")
        .inner
        .send_stream(id)
        .stopped()
        .is_err()
}

#[tokio::test(flavor = "current_thread")]
async fn both_half_orders_keep_custody_until_the_other_half_retires() {
    for send_first in [true, false] {
        let pair = Loopback::new().await;
        let (mut send, mut recv, ticket) =
            pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
        assert_eq!(send.id(), recv.id());
        assert_eq!(ticket.stream, send.id());
        assert!(Arc::ptr_eq(
            &ticket.connection.upgrade().unwrap(),
            &pair.connection.0 .0
        ));
        assert!(
            !ticket.is_retired(),
            "unmaterialized map entries still own the stream"
        );
        send.write_all(b"request").await.unwrap();
        let (mut peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
        if send_first {
            send.finish().unwrap();
            assert_eq!(peer_recv.read_to_end(7).await.unwrap(), b"request");
            timeout(Duration::from_secs(2), async {
                while !send_retired(&pair.connection, send.id()) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(
                !ticket.is_retired(),
                "send absence cannot retire the still-lazy recv entry"
            );
            peer_send.finish().unwrap();
            assert!(recv.read_to_end(0).await.unwrap().is_empty());
        } else {
            peer_send.write_all(b"response").await.unwrap();
            peer_send.finish().unwrap();
            assert_eq!(recv.read_to_end(8).await.unwrap(), b"response");
            assert!(!send_retired(&pair.connection, send.id()));
            assert!(
                !ticket.is_retired(),
                "recv absence cannot retire the still-open send entry"
            );
            send.finish().unwrap();
            assert_eq!(peer_recv.read_to_end(7).await.unwrap(), b"request");
        }
        retired(&ticket).await;
        assert!(pair.connection.close_reason().is_none());
        assert!(pair.peer.close_reason().is_none());
        drop((send, recv, peer_send, peer_recv));

        // A different flow still has a usable shared connection after exact retirement.
        let (mut next_send, mut next_recv, next_ticket) =
            pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
        next_send.write_all(b"next").await.unwrap();
        next_send.finish().unwrap();
        let (mut reply, mut request) = pair.peer.accept_bi().await.unwrap();
        assert_eq!(request.read_to_end(4).await.unwrap(), b"next");
        reply.write_all(b"ok").await.unwrap();
        reply.finish().unwrap();
        assert_eq!(next_recv.read_to_end(2).await.unwrap(), b"ok");
        retired(&next_ticket).await;
        drop((next_send, next_recv, reply, request));
        pair.close().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reset_and_stop_requests_do_not_mean_both_maps_are_retired() {
    let pair = Loopback::new().await;
    let (mut send, mut recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
    send.write_all(b"started").await.unwrap();
    let (mut peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
    send.reset(1u32.into()).unwrap();
    recv.stop(2u32.into()).unwrap();
    assert!(
        !ticket.is_retired(),
        "no driver turn or peer final-offset evidence has occurred after cancel"
    );
    drop((send, recv));
    assert!(peer_recv.read_to_end(7).await.is_err());
    peer_send.reset(3u32.into()).unwrap();
    retired(&ticket).await;
    assert!(pair.connection.close_reason().is_none());
    drop((peer_send, peer_recv));
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn exhausted_credit_and_closed_admission_do_not_consume_stream_ids() {
    let pair = Loopback::with_bi_limit(Some(1)).await;
    let (mut send, mut recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
    assert_eq!(send.id().index(), 0);
    for _ in 0..257 {
        assert!(pair.connection.try_open_bi_1rtt_with_retirement().is_none());
    }
    assert!(pair
        .connection
        .0
        .state
        .lock("no stopped waiters")
        .stopped
        .is_empty());
    send.write_all(b"x").await.unwrap();
    send.finish().unwrap();
    let (mut peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
    assert_eq!(peer_recv.read_to_end(1).await.unwrap(), b"x");
    peer_send.finish().unwrap();
    assert!(recv.read_to_end(0).await.unwrap().is_empty());
    retired(&ticket).await;
    drop((send, recv, peer_send, peer_recv));
    let (mut next_send, mut next_recv, next_ticket) = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(opened) = pair.connection.try_open_bi_1rtt_with_retirement() {
                break opened;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(next_send.id().index(), 1);
    next_send.reset(0u32.into()).unwrap();
    next_recv.stop(0u32.into()).unwrap();
    drop((next_send, next_recv));
    pair.connection.close(0u32.into(), b"test closed admission");
    let before = pair
        .connection
        .0
        .state
        .lock("before closed tries")
        .inner
        .streams()
        .send_streams();
    for _ in 0..257 {
        assert!(pair.connection.try_open_bi_1rtt_with_retirement().is_none());
    }
    assert_eq!(
        pair.connection
            .0
            .state
            .lock("after closed tries")
            .inner
            .streams()
            .send_streams(),
        before
    );
    assert!(pair
        .connection
        .0
        .state
        .lock("no closed waiters")
        .stopped
        .is_empty());
    drop(next_ticket);
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn bi_atomic_admission_rejects_real_zero_rtt_without_consuming_an_id() {
    let pair = Loopback::new().await;
    let mut marker = pair.peer.open_uni().await.unwrap();
    marker.write_all(b"ticket").await.unwrap();
    marker.finish().unwrap();
    assert_eq!(
        pair.connection
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(6)
            .await
            .unwrap(),
        b"ticket"
    );
    drop(marker);
    let (early, accepted) = pair
        .endpoint
        .connect(pair.endpoint.local_addr().unwrap(), "localhost")
        .unwrap()
        .into_0rtt()
        .unwrap();
    assert!(early.0.state.lock("early bi").inner.is_handshaking());
    for _ in 0..257 {
        assert!(early.try_open_bi_1rtt_with_retirement().is_none());
    }
    assert!(early.0.state.lock("early no waiters").stopped.is_empty());
    let (mut probe_send, mut probe_recv) = early.open_bi().await.unwrap();
    assert_eq!(probe_send.id().index(), 0);
    probe_send.reset(0u32.into()).unwrap();
    probe_recv.stop(0u32.into()).unwrap();
    drop((probe_send, probe_recv));
    let (was_accepted, next_peer) = tokio::join!(accepted, async {
        pair.endpoint.accept().await.unwrap().await.unwrap()
    });
    assert!(was_accepted);
    let (mut established_send, mut established_recv, ticket) =
        early.try_open_bi_1rtt_with_retirement().unwrap();
    assert_eq!(established_send.id().index(), 1);
    assert!(!ticket.is_retired());
    established_send.reset(0u32.into()).unwrap();
    established_recv.stop(0u32.into()).unwrap();
    drop((established_send, established_recv));
    early.close(0u32.into(), b"test complete");
    next_peer.close(0u32.into(), b"test complete");
    drop((early, next_peer));
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn bi_weak_upgrade_failure_waits_for_the_post_storage_destruction_token() {
    let pair = Loopback::new().await;
    let (mut send, mut recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
    send.write_all(b"retained").await.unwrap();
    send.reset(0u32.into()).unwrap();
    recv.stop(0u32.into()).unwrap();
    drop((send, recv));
    assert!(!ticket.is_retired());
    let retained = ticket.connection.upgrade().unwrap();
    pair.close().await;
    timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&retained) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!ticket.is_retired(), "close is not storage destruction");
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    *retained.retirement_barrier.lock("bi destruction window") = Some(super::DestructionBarrier {
        entered: entered.clone(),
        release: release.clone(),
    });
    let destroy = std::thread::spawn(move || drop(retained));
    entered.wait();
    let weak_missing = ticket.connection.upgrade().is_none();
    let token_in_window = ticket.destroyed.load(Ordering::Acquire);
    let retired_in_window = ticket.is_retired();
    release.wait();
    destroy.join().unwrap();
    assert!(weak_missing);
    assert!(!token_in_window);
    assert!(!retired_in_window);
    assert!(ticket.destroyed.load(Ordering::Acquire));
    assert!(ticket.is_retired());
}

#[tokio::test(flavor = "current_thread")]
async fn bi_open_snapshot_observes_credit_without_consuming_it() {
    for limit in [0, 1] {
        let pair = Loopback::with_bi_limit(Some(limit)).await;
        for _ in 0..3 {
            assert_eq!(
                pair.connection.bi_stream_open_snapshot(),
                (0, limit as u64, false, false, false)
            );
        }
        if limit == 0 {
            assert!(pair.connection.try_open_bi_1rtt_with_retirement().is_none());
        } else {
            let (send, recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
            assert_eq!(send.id().index(), 0);
            for _ in 0..3 {
                assert_eq!(
                    pair.connection.bi_stream_open_snapshot(),
                    (1, 1, false, false, false)
                );
                assert!(pair.connection.try_open_bi_1rtt_with_retirement().is_none());
            }
            drop((send, recv, ticket));
        }
        assert_eq!(
            pair.connection.bi_stream_open_snapshot(),
            (limit as u64, limit as u64, false, false, false)
        );
        pair.close().await;
    }
}
