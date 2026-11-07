//! Actual Full, original allocation identity and known/unknown dispositions.
use super::*;

fn fill_general_budget(budget: &bytes::BackingBudget) -> BytesMut {
    let data = BackingMeter::with_budget(budget);
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let packet =
        BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &data).unwrap();
    assert_eq!(budget.used(), budget.limit());
    packet
}
fn protected_base(owner: &Egress) -> usize {
    owner.cold.as_ref().unwrap().protected.identity.base
}

#[tokio::test]
async fn full_unused_and_complete_write_return_same_zeroed_actual_packet_without_refund() {
    for prefix in [0, 4] {
        let budget = bytes::BackingBudget::new(128 << 10).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut owner = Egress::native(2, 1200, prefix, &meter).unwrap();
        let original = protected_base(&owner);
        let held = fill_general_budget(&budget);
        for _ in 0..2 {
            let token = owner.token(1200, prefix).unwrap();
            let packet = token.owner.prepared.as_ref().unwrap();
            assert_eq!(packet.as_ptr() as usize, original);
            assert!(packet.iter().all(|byte| *byte == 0));
            drop(token);
            assert_eq!(budget.used(), budget.limit());
            assert!(owner.protected_inventory_matches());
        }
        owner
            .token(1200, prefix)
            .unwrap()
            .consume(6, |out| out.copy_from_slice(b"packet"));
        assert_eq!(owner.queue.front().unwrap().as_ptr() as usize, original);
        assert!(
            owner.token(1200, prefix).is_none(),
            "a logical free slot cannot invent a second physical packet"
        );
        let (mut writer, mut peer) = tokio::io::duplex(4096);
        owner.flush(&mut writer).await.unwrap();
        let mut actual = vec![0; 6 + prefix];
        peer.read_exact(&mut actual).await.unwrap();
        assert_eq!(&actual[prefix..], b"packet");
        assert_eq!(
            budget.used(),
            budget.limit(),
            "complete write returns availability, not global budget"
        );
        let token = owner.token(1200, prefix).unwrap();
        let packet = token.owner.prepared.as_ref().unwrap();
        assert_eq!(packet.as_ptr() as usize, original);
        assert!(
            packet.iter().all(|byte| *byte == 0),
            "reuse preserves original zeroed preparation"
        );
        drop(token);
        assert!(owner.protected_inventory_matches());
        drop(held);
        drop(owner);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test]
async fn protected_summary_still_owns_original_logical_slot_until_commit() {
    let (budget, _, mut owner) = native(1);
    let held = fill_general_budget(&budget);
    fill(&mut owner, &tcp_packet());
    let original = protected_base(&owner);
    let (mut writer, mut peer) = tokio::io::duplex(4096);
    let bytes = owner.owned_bytes();
    owner.flush(&mut writer).await.unwrap();
    let mut actual = vec![0; bytes];
    peer.read_exact(&mut actual).await.unwrap();
    assert_eq!(owner.summaries.len(), 1);
    assert!(owner.protected_inventory_matches());
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    let mut proofs = owner.summaries();
    assert_eq!(proofs.next().unwrap().payload_bytes, 3);
    proofs.commit();
    let token = owner.token(1200, tun_packet_header_len()).unwrap();
    assert_eq!(
        token.owner.prepared.as_ref().unwrap().as_ptr() as usize,
        original
    );
    drop(token);
    assert_eq!(budget.used(), budget.limit());
    drop(held);
    drop(owner);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn protected_cancelled_write_keeps_selected_until_explicit_cut_without_refund() {
    let (budget, _, mut owner) = native(2);
    let held = fill_general_budget(&budget);
    fill(&mut owner, b"packet");
    let original = protected_base(&owner);
    let (mut writer, _peer) = tokio::io::duplex(16);
    writer.write_all(&[0; 16]).await.unwrap();
    {
        let mut flush = Box::pin(owner.flush(&mut writer));
        assert!(
            flush
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(owner.selected.as_ref().unwrap().as_ptr() as usize, original);
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    assert!(owner.protected_inventory_matches());
    let mut totals = (0, 0);
    owner.discard(&mut totals);
    assert_eq!(totals, (1, (1200 + tun_packet_header_len()) as u64));
    assert_eq!(
        budget.used(),
        budget.limit(),
        "cut disposes a packet but retains the actual stock"
    );
    let token = owner.token(1200, tun_packet_header_len()).unwrap();
    assert_eq!(
        token.owner.prepared.as_ref().unwrap().as_ptr() as usize,
        original
    );
    drop(token);
    drop(held);
    drop(owner);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn protected_short_and_original_io_failure_never_reuse_selected_or_clear_q() {
    for short in [true, false] {
        let (budget, _, mut owner) = native(2);
        let held = fill_general_budget(&budget);
        fill(&mut owner, b"packet");
        let original = protected_base(&owner);
        let (mut writer, peer) = tokio::io::duplex(if short { 2 } else { 4096 });
        let _peer = if short {
            Some(peer)
        } else {
            drop(peer);
            None
        };
        let error = owner.flush(&mut writer).await.unwrap_err();
        if !short {
            assert_eq!(error.kind(), ErrorKind::BrokenPipe);
        }
        assert_eq!(budget.quarantined(), short);
        assert_eq!(owner.selected.as_ref().unwrap().as_ptr() as usize, original);
        assert!(owner.summaries.is_empty());
        assert!(owner.token(1200, tun_packet_header_len()).is_none());
        owner.discard(&mut (0, 0));
        assert!(owner.protected_inventory_matches());
        assert!(
            owner.token(1200, tun_packet_header_len()).is_none(),
            "known disposal does not reopen a failed owner"
        );
        drop(held);
        drop(owner);
        assert_eq!(budget.used(), usize::from(short));
        assert_eq!(budget.quarantined(), short);
    }
}

#[tokio::test]
async fn protected_zero_poll_token_cancellation_and_owner_loss_have_distinct_dispositions() {
    for lost in [false, true] {
        let (budget, _, mut owner) = native(2);
        let held = fill_general_budget(&budget);
        if lost {
            fill(&mut owner, b"unsettled");
        } else {
            let token = owner.token(1200, tun_packet_header_len()).unwrap();
            drop(async move {
                drop(token);
            });
            assert!(owner.protected_inventory_matches());
            assert_eq!(budget.used(), budget.limit());
        }
        drop(held);
        drop(owner);
        assert_eq!(budget.quarantined(), lost);
        assert_eq!(budget.used(), usize::from(lost));
    }
}

#[tokio::test]
async fn protected_actual_empty_alias_prevents_recycling_and_retains_original_in_q() {
    let (budget, _, mut owner) = native(2);
    let held = fill_general_budget(&budget);
    fill(&mut owner, b"packet");
    let alias = owner.queue.front_mut().unwrap().split_to(0).freeze();
    let original = protected_base(&owner);
    assert_eq!(alias.allocation_identity().unwrap().base, original);
    let (mut writer, _peer) = tokio::io::duplex(4096);
    assert!(owner.flush(&mut writer).await.is_err());
    assert!(budget.quarantined());
    assert!(matches!(&owner.cold.as_ref().unwrap().protected.state,
        ProtectedState::Uncertain(bytes) if bytes.as_ptr() as usize == original && bytes.len() > 0));
    assert_eq!(budget.used(), budget.limit());
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    drop(held);
    owner.discard(&mut (0, 0));
    drop(owner);
    assert!(
        budget.used() > 1,
        "the actual alias still retains its Shared layout"
    );
    drop(alias);
    assert_eq!(budget.used(), 1);
}

#[test]
fn protected_bind_rejects_missing_and_foreign_actual_storage() {
    for foreign in [false, true] {
        let (budget, meter, mut owner) = native(2);
        let cold = owner.cold.as_mut().unwrap();
        let original = std::mem::replace(&mut cold.protected.state, ProtectedState::InFlight);
        if foreign {
            let other = BackingMeter::with_budget(&budget);
            cold.protected.state = ProtectedState::Idle(
                BytesMut::try_zeroed_metered(1200 + tun_packet_header_len(), &other).unwrap(),
            );
        }
        assert!(
            owner
                .bind(&meter, 2, 1200, tun_packet_header_len())
                .is_err()
        );
        assert!(budget.quarantined());
        drop(original);
        drop(owner);
        assert_eq!(budget.used(), 1);
    }
}

struct ZeroPacketWriter;
impl tokio::io::AsyncWrite for ZeroPacketWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(0))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn protected_zero_write_retains_original_and_never_mints_a_summary() {
    let (budget, _, mut owner) = native(2);
    let held = fill_general_budget(&budget);
    fill(&mut owner, b"packet");
    let original = protected_base(&owner);
    let error = owner.flush(&mut ZeroPacketWriter).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteZero);
    assert!(budget.quarantined());
    assert_eq!(owner.selected.as_ref().unwrap().as_ptr() as usize, original);
    assert!(owner.summaries.is_empty());
    assert!(owner.token(1200, tun_packet_header_len()).is_none());
    owner.discard(&mut (0, 0));
    drop(held);
    drop(owner);
    assert_eq!(budget.used(), 1);
}
#[test]
fn protected_duplicate_body_is_rejected_by_actual_unique_recovery() {
    let (budget, meter, mut owner) = native(2);
    let cold = owner.cold.as_mut().unwrap();
    let ProtectedState::Idle(mut packet) =
        std::mem::replace(&mut cold.protected.state, ProtectedState::InFlight)
    else {
        panic!()
    };
    let duplicate = packet.split_to(0);
    cold.protected.state = ProtectedState::Idle(packet);
    // The cold owner cannot treat an out-of-inventory live reference as clean:
    // the actual move-through uniqueness check, not a marker, rejects it.
    let mut slot = cold.protected.take(&meter).unwrap();
    assert!(!cold.protected.return_packet(&mut slot, &meter));
    assert!(slot.is_none());
    assert!(matches!(cold.protected.state, ProtectedState::Uncertain(_)));
    assert!(
        owner
            .bind(&meter, 2, 1200, tun_packet_header_len())
            .is_err()
    );
    assert!(budget.quarantined());
    drop(owner);
    assert!(budget.used() > 1);
    drop(duplicate);
    assert_eq!(budget.used(), 1);
}

#[test]
fn protected_cold_bind_verifies_actual_uniqueness_of_idle_and_queued_stock() {
    for queued in [false, true] {
        let (budget, meter, mut owner) = native(2);
        let held = queued.then(|| fill_general_budget(&budget));
        let duplicate = if queued {
            fill(&mut owner, b"packet");
            owner.queue.front_mut().unwrap().split_to(0)
        } else {
            let ProtectedState::Idle(packet) = &mut owner.cold.as_mut().unwrap().protected.state
            else {
                panic!()
            };
            packet.split_to(0)
        };
        let refused = owner
            .bind(&meter, 2, 1200, tun_packet_header_len())
            .is_err();
        let quarantined = budget.quarantined();
        owner.discard(&mut (0, 0));
        drop(held);
        drop(owner);
        drop(duplicate);
        assert!(
            refused,
            "a zero-length view still owns the actual Shared allocation; a marker-only cold check cannot establish uniqueness"
        );
        assert!(quarantined);
        assert_eq!(budget.used(), 1);
    }
}
