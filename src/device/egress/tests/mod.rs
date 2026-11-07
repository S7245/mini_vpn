use super::*;
use std::{
    future::Future,
    task::{Context, Waker},
};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn cancelled_packet_write_keeps_original_selected_packet() {
    let (mut writer, mut peer) = tokio::io::duplex(16);
    writer.write_all(&[9; 16]).await.unwrap();
    let mut owner = Egress::default();
    owner.queue.push_back(BytesMut::from(&b"packet"[..]));
    {
        let mut flush = Box::pin(owner.flush(&mut writer));
        assert!(
            flush
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let retained = owner.owned_bytes();
    let mut prefix = [0; 16];
    peer.read_exact(&mut prefix).await.unwrap();
    assert_eq!(prefix, [9; 16]);
    assert_eq!(
        retained, 6,
        "borrowed flush cancellation must not discard the selected packet"
    );
    owner.flush(&mut writer).await.unwrap();
    let mut packet = [0; 6];
    peer.read_exact(&mut packet).await.unwrap();
    assert_eq!(&packet, b"packet");
    assert_eq!(owner.owned_bytes(), 0);
}

fn layout_bytes(slots: usize) -> usize {
    slots * (std::mem::size_of::<BytesMut>() + std::mem::size_of::<TunFlushedTcpPacket>())
        + callback::layout_bytes()
}
fn native(slots: usize) -> (bytes::BackingBudget, BackingMeter, Egress) {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let owner = Egress::native(slots, 1200, tun_packet_header_len(), &meter).unwrap();
    (budget, meter, owner)
}
fn fill(owner: &mut Egress, payload: &[u8]) {
    owner
        .token(1200, tun_packet_header_len())
        .unwrap()
        .consume(payload.len(), |out| out.copy_from_slice(payload));
}
#[test]
fn cold_layout_is_exact_atomic_and_token_is_precharged() {
    let layout = layout_bytes(2) + BytesMut::metered_copy_layout_for_len(1200).unwrap();
    for limit in [layout - 1, layout] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let owner = Egress::native(2, 1200, 0, &meter);
        if limit < layout {
            assert!(owner.is_err());
        } else {
            let mut owner = owner.unwrap();
            assert_eq!((owner.queue.capacity(), owner.summaries.capacity()), (2, 2));
            assert_eq!(budget.used(), layout);
            assert!(
                owner.token(1200, 0).is_some(),
                "the actual cold packet, not additional budget, admits this token"
            );
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
    let (budget, _, mut owner) = native(1);
    let cold = budget.used();
    let token = owner.token(1200, tun_packet_header_len()).unwrap();
    assert!(budget.used() > cold + 1200);
    drop(token);
    assert_eq!(budget.used(), cold);
    drop(owner);
    assert_eq!(budget.used(), 0);
}
#[tokio::test]
async fn short_packet_write_retains_original_without_summary_or_retry() {
    let (budget, _, mut owner) = native(1);
    fill(&mut owner, b"six123");
    let original = owner.queue.front().unwrap().to_vec();
    let charged = budget.used();
    let (mut writer, mut peer) = tokio::io::duplex(2);
    assert!(owner.flush(&mut writer).await.is_err());
    assert!(budget.quarantined());
    assert_eq!(budget.used(), charged);
    assert_eq!(owner.selected.as_ref().unwrap().as_ref(), original);
    assert!(owner.summaries.is_empty());
    let mut got = [0; 2];
    peer.read_exact(&mut got).await.unwrap();
    assert_eq!(got, original[..2]);
    assert!(owner.flush(&mut writer).await.is_err());
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
}
#[tokio::test]
async fn known_write_error_keeps_original_until_actual_disposal_without_unknown_q() {
    let (budget, _, mut owner) = native(1);
    fill(&mut owner, b"packet");
    let original = owner.owned_bytes();
    let (mut writer, peer) = tokio::io::duplex(128);
    drop(peer);
    assert_eq!(
        owner.flush(&mut writer).await.unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
    assert_eq!(owner.owned_bytes(), original);
    assert!(!budget.quarantined());
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 0);
}
#[test]
fn callback_panic_and_invalidated_token_keep_preparation_in_original_owner() {
    for case in 0..3 {
        let (budget, meter, mut owner) = native(1);
        let token = owner.token(1200, tun_packet_header_len()).unwrap();
        let charged = budget.used();
        if case == 1 {
            meter.quarantine();
        }
        let mut called = false;
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            token.consume(if case == 2 { 1201 } else { 6 }, |_| {
                called = true;
                panic!("original packet encoder");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(called, case == 0);
        assert!(budget.quarantined());
        assert_eq!(budget.used(), charged);
        assert!(owner.prepared.is_some());
        assert!(owner.queue.is_empty());
        owner.discard(&mut (0, 0));
        drop(owner);
        assert_eq!(budget.used(), 1);
    }
}
fn tcp_packet() -> Vec<u8> {
    let builder =
        etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64).tcp(443, 5000, 1, 4096);
    let mut packet = Vec::with_capacity(builder.size(3));
    builder.write(&mut packet, b"tcp").unwrap();
    packet
}
#[tokio::test]
async fn successful_summary_owns_slot_and_cold_backing_until_proof_commits() {
    let (budget, _, mut owner) = native(1);
    let cold = budget.used();
    fill(&mut owner, &tcp_packet());
    let (mut writer, mut peer) = tokio::io::duplex(4096);
    let bytes = owner.owned_bytes();
    owner.flush(&mut writer).await.unwrap();
    let mut observed = vec![0; bytes];
    peer.read_exact(&mut observed).await.unwrap();
    assert_eq!(
        summarize_flushed_tcp_packet(&observed)
            .unwrap()
            .payload_bytes,
        3
    );
    assert_eq!(budget.used(), cold);
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    let pointer = owner.summaries.as_ptr();
    let mut batch = owner.summaries();
    assert_eq!(batch.next().unwrap().payload_bytes, 3);
    assert!(batch.next().is_none());
    batch.commit();
    assert_eq!(owner.summaries.as_ptr(), pointer);
    assert_eq!(owner.summaries.capacity(), 1);
    assert!(owner.token(1200, tun_packet_header_len()).is_some());
    drop(owner);
    assert_eq!(budget.used(), 0);
}
#[tokio::test]
async fn abandoned_summary_even_after_last_pop_is_quarantined() {
    let (budget, _, mut owner) = native(1);
    fill(&mut owner, &tcp_packet());
    let (mut writer, _peer) = tokio::io::duplex(4096);
    owner.flush(&mut writer).await.unwrap();
    let mut batch = owner.summaries();
    assert!(batch.next().is_some());
    assert!(batch.is_empty());
    drop(batch);
    assert!(budget.quarantined());
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    drop(owner);
    assert_eq!(budget.used(), 1);
}

mod injection;

mod protected;
