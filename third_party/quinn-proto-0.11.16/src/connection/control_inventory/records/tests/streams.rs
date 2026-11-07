use super::*;

fn ordinary_owner() -> (BackingBudget, Pool, Queue<(StreamId, VarInt)>) {
    let mut cfg = EndpointConfig::default();
    cfg.receive_control_record_limit(1).unwrap();
    cfg.receive_stream_record_limit(2).unwrap();
    cfg.receive_sent_history_limits(1, 1).unwrap();
    let budget = BackingBudget::new(Pool::layout_bytes(&cfg).unwrap()).unwrap();
    cfg.receive_backing_meter(BackingMeter::with_budget(&budget));
    let pool = Pool::prepare(&cfg).unwrap().unwrap();
    let owner = Queue::new(Some(&pool));
    (budget, pool, owner)
}
fn metadata(id: u32) -> frame::StreamMeta {
    frame::StreamMeta {
        id: value(id).0,
        offsets: u64::from(id)..u64::from(id) + 3,
        fin: id != 0,
    }
}
fn encoded(owner: &Queue<(StreamId, VarInt)>) -> StreamFrames {
    let mut pending = PreparedStreams::prepare(owner, true).unwrap();
    for id in [0, 4] {
        let mut credit = pending.next_credit().unwrap().unwrap();
        credit.begin_mutation();
        pending.encoded(credit, metadata(id));
    }
    pending.finish()
}

#[test]
fn ordinary_prepared_cancellation_cannot_take_the_control_reserve() {
    let (budget, pool, owner) = ordinary_owner();
    let a = PreparedStreams::prepare(&owner, true).unwrap();
    let b = PreparedStreams::prepare(&owner, true).unwrap();
    let mut full = PreparedStreams::prepare(&owner, true).unwrap();
    assert!(full.next_credit().unwrap().is_none());
    assert!(!PreparedStreams::ready(&owner));
    assert_eq!(owner.class_stock(1), Some((2, 0, 0)));
    let control = owner.prepare(value(0)).unwrap();
    assert_eq!(owner.cold_stock(), Some((1, 0, 0)));
    drop(a);
    assert!(PreparedStreams::ready(&owner));
    drop(b);
    drop(full);
    drop(control);
    assert_eq!(owner.class_stock(1), Some((2, 2, 0)));
    drop(owner);
    drop(pool);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn ack_loss_callbacks_keep_fifo_and_own_each_credit_through_settlement() {
    let (budget, pool, owner) = ordinary_owner();
    let frames = encoded(&owner);
    assert_eq!(owner.class_stock(1), Some((2, 0, 2)));
    let mut index = 0;
    frames.dispose_with(|frame| {
        assert_eq!(frame.id, value(index * 4).0);
        assert_eq!(
            frame.offsets,
            u64::from(index * 4)..u64::from(index * 4) + 3
        );
        assert_eq!(frame.fin, index != 0);
        // Current callback still owns its slot. Only previous callbacks freed.
        assert_eq!(owner.class_stock(1).unwrap().1, index as usize);
        index += 1;
        super::super::StreamDisposition::Applied
    });
    assert_eq!(index, 2);
    assert_eq!(owner.class_stock(1), Some((2, 2, 0)));
    drop(owner);
    drop(pool);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn interrupted_callback_keeps_q_after_all_known_nodes_are_destroyed() {
    let (budget, pool, owner) = ordinary_owner();
    let frames = encoded(&owner);
    let result = std::panic::catch_unwind(|| {
        frames.dispose_with(|_| panic!("intentional STREAM settlement fault"))
    });
    assert!(result.is_err());
    assert_eq!(owner.class_stock(1), Some((2, 2, 0)));
    assert!(budget.quarantined());
    drop(owner);
    drop(pool);
    assert_eq!(budget.used(), 1);
}

#[test]
fn stream_representation_retains_original_inline_layout_bound() {
    assert!(
        std::mem::size_of::<StreamFrames>()
            <= std::mem::size_of::<tinyvec::TinyVec<[frame::StreamMeta; 1]>>()
    );
}

#[test]
fn legacy_single_frame_stays_inline_and_second_frame_allocates_once() {
    let mut pending = PreparedStreams::default();
    let first = crate::tests::allocations_in(|| {
        let mut credit = pending.next_credit().unwrap().unwrap();
        credit.begin_mutation();
        pending.encoded(credit, metadata(0));
    });
    assert_eq!(first, 0);
    let second = crate::tests::allocations_in(|| {
        let mut credit = pending.next_credit().unwrap().unwrap();
        credit.begin_mutation();
        pending.encoded(credit, metadata(4));
    });
    assert_eq!(second, 1);
    assert_eq!(pending.finish().len(), 2);
}
