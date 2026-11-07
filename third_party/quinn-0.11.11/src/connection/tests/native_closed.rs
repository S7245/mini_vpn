//! Real half Drop on the original closed Native Connection. Physical maps,
//! retained payload aliases and the old witness are checked independently.
use super::{super::*, retirement::Loopback};
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
async fn native_closed_half_does_not_count_unaccepted_peer_bi_as_accepted() {
    let budget = bytes::BackingBudget::new(8 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let pair = Loopback::with_meter(meter).await;
    let (send, recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
    let (mut remote_send, remote_recv) = pair.peer.open_bi().await.unwrap();
    let before = pair.connection.stats().frame_rx.stream;
    remote_send.write_all(b"x").await.unwrap();
    timeout(Duration::from_secs(2), async {
        while pair.connection.stats().frame_rx.stream == before { tokio::task::yield_now().await; }
    }).await.unwrap();
    // No accept_bi occurs on the local endpoint: received is not accepted.
    pair.connection.close(0u32.into(), b"known close");
    assert!(!ticket.is_retired());
    drop(send);
    let send_only = ticket.is_retired();
    drop(recv);
    let both = ticket.is_retired();
    let q = budget.quarantined();
    pair.peer.close(0u32.into(), b"known close");
    drop((remote_send, remote_recv));
    assert!(close_physical(pair).await);
    assert!(!send_only && both, "only the two actual Drops grant the original witness");
    assert!(!q, "unaccepted remote stream is legal physical inventory");
    assert_eq!(budget.used(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn native_closed_actual_half_orders_stop_reset_and_last_payload_alias() {
    for send_first in [true, false] {
        for stopped in [true, false] {
            let budget = bytes::BackingBudget::new(8 << 20).unwrap();
            let meter = bytes::BackingMeter::with_budget(&budget);
            let pair = Loopback::with_meter(meter.clone()).await;
            let (mut send, mut recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
            let alias = bytes::BytesMut::try_copy_from_slice_metered(&[19; 1024], &meter).unwrap().freeze();
            let identity = alias.allocation_identity().unwrap();
            let layout = bytes::BytesMut::metered_copy_layout_for_len(1024).unwrap();
            send.write_chunk(alias.clone()).await.unwrap();
            let (mut peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
            peer_recv.read_exact(&mut [0; 1]).await.unwrap();
            peer_send.write_all(b"response").await.unwrap();
            recv.read_exact(&mut [0; 1]).await.unwrap();
            if stopped {
                send.reset(17u32.into()).unwrap();
                recv.stop(18u32.into()).unwrap();
                assert!(recv.all_data_read_for_test(), "exercise original stop early-return branch");
            }
            pair.connection.close(0u32.into(), b"known close");
            assert!(!ticket.is_retired());
            if send_first {
                drop(send);
                assert!(!ticket.is_retired(), "other actual half still owns its map");
                drop(recv);
            } else {
                drop(recv);
                assert!(!ticket.is_retired(), "other actual half still owns its map");
                drop(send);
            }
            let retired = ticket.is_retired();
            let q = budget.quarantined();
            assert_eq!(alias.allocation_identity(), Some(identity));
            assert_eq!(&alias[..], &[19; 1024]);
            let charged = budget.used();
            drop(alias); // No task turn: only the final original allocation reference changes.
            let returned = charged - budget.used();
            pair.peer.close(0u32.into(), b"known close");
            drop((peer_send, peer_recv));
            assert!(close_physical(pair).await);
            assert!(retired && !q, "actual halves retire; reset={stopped} send_first={send_first}");
            assert_eq!(returned, layout, "map absence never refunds a live external payload alias");
            assert_eq!(budget.used(), 0);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_closed_q_keeps_actual_half_maps_present() {
    let budget = bytes::BackingBudget::new(8 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let pair = Loopback::with_meter(meter.clone()).await;
    let (mut send, mut recv, ticket) = pair.connection.try_open_bi_1rtt_with_retirement().unwrap();
    send.write_all(b"original").await.unwrap();
    let (peer_send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
    peer_recv.read_exact(&mut [0; 1]).await.unwrap();
    recv.stop(23u32.into()).unwrap();
    assert!(recv.all_data_read_for_test());
    pair.connection.close(0u32.into(), b"closed before Q");
    meter.quarantine();
    let charged = budget.used();
    drop((send, recv));
    assert!(!ticket.is_retired(), "Q cannot certify either original lazy/materialized map absent");
    assert!(budget.used() >= charged && budget.quarantined());
    pair.peer.close(0u32.into(), b"test Q teardown");
    drop((peer_send, peer_recv));
    assert!(!close_physical(pair).await);
    assert!(budget.quarantined() && budget.used() > 0);
}

async fn close_physical(mut pair: Loopback) -> bool {
    let driver = pair.endpoint.take_driver_drain_handle().unwrap();
    pair.close().await;
    timeout(Duration::from_secs(2), driver.drained()).await.unwrap()
}
