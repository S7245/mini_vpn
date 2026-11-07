use super::*;

fn frame(sequence: u64) -> IssuedCid {
    IssuedCid {
        sequence,
        id: ConnectionId::new(&[sequence as u8; 8]),
        reset_token: ResetToken::from([0; 16]),
    }
}
#[test]
fn fixed_native_active_accepts_original_reverse_order_and_rejects_partial_or_excess_batches() {
    let now = Instant::now();
    let mut state = CidState::new_native(8, now, 1).unwrap();
    let allocations =
        crate::tests::allocations_in(|| state.new_cids((1..8).rev().map(frame), now).unwrap());
    assert_eq!(allocations, 0);
    assert_eq!(state.active_seq.len(), 8);
    assert_eq!(state.issued, 8);
    assert!(state.next_timeout().is_none());
    let before = state.active_seq.clone();
    assert!(state.new_cids([frame(8)], now).is_err());
    assert_eq!(state.active_seq, before);
    assert_eq!(state.issued, 8);
    state.mark_encoded(7);
    assert_eq!(state.on_cid_retirement(3, 8).unwrap(), Some(true));
    assert!(state.new_cids([frame(9)], now).is_err());
    assert!(state.new_cids([frame(8), frame(8)], now).is_err());
    assert_eq!(state.issued, 8);
    let allocations = crate::tests::allocations_in(|| state.new_cids([frame(8)], now).unwrap());
    assert_eq!(allocations, 0);
    assert_eq!(state.issued, 9);
    assert_eq!(state.active_seq.len(), 8);
    assert!(!state.active_seq.contains(&3));
    assert!(state.active_seq.contains(&8));
}
