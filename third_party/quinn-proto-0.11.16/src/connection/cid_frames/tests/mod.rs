use super::*;
use crate::{ConnectionId, ResetToken};
use bytes::BackingBudget;

fn value(sequence: u64) -> IssuedCid {
    IssuedCid {
        sequence,
        id: ConnectionId::new(&[sequence as u8; 8]),
        reset_token: ResetToken::from([0; 16]),
    }
}
fn owner() -> (BackingBudget, Pool) {
    let budget = BackingBudget::new(1024 * 1024).unwrap();
    let pool = Pool::new(&BackingMeter::with_budget(&budget)).unwrap();
    (budget, pool)
}
#[test]
fn layout_is_admitted_before_allocation_and_last_owner_releases_it() {
    let layout = Layout::array::<Node>(ORDINARY + LOC_CID_COUNT as usize).unwrap();
    let too_small = BackingBudget::new(layout.size() - 1).unwrap();
    assert!(Pool::new(&BackingMeter::with_budget(&too_small)).is_err());
    assert_eq!(too_small.used(), 0);
    let (budget, pool) = owner();
    assert_eq!(budget.used(), layout.size());
    let endpoint = pool.clone();
    let pending = pool.prepare(1).unwrap();
    assert_eq!(
        pool.0.free.load(Ordering::Acquire),
        ORDINARY + LOC_CID_COUNT as usize - 1
    );
    drop(pending); // Rejection/cancellation before any CID state mutation.
    assert!(!budget.quarantined());
    drop(pool);
    assert_eq!(budget.used(), layout.size());
    drop(endpoint);
    assert_eq!(budget.used(), 0);
}
#[test]
fn full_stock_keeps_new_frames_but_retransmits_the_existing_record() {
    let (budget, pool) = owner();
    let mut first = pool.prepare(1).unwrap();
    first.push(value(0));
    let mut sent = first.finish();
    let frame = sent.pop_sendable().unwrap();
    frame.mark_encoded();
    sent.push(frame);
    let mut rest = pool.prepare(ORDINARY + LOC_CID_COUNT as usize - 1).unwrap();
    for sequence in 1..(ORDINARY + LOC_CID_COUNT as usize) {
        rest.push(value(sequence as u64));
    }
    let mut pending = rest.finish();
    pending.reverse();
    assert_eq!(pool.0.free.load(Ordering::Acquire), 0);
    assert!(pool.prepare(1).is_none());
    assert!(!pending.ready());
    assert!(pending.pop_sendable().is_none());
    // Loss/probe moves the old record, never duplicates or refunds it.
    pending.append(&mut sent);
    assert!(pending.ready());
    let retry = pending.pop_sendable().unwrap();
    assert_eq!(retry.value.sequence, 0);
    assert_eq!(pool.0.free.load(Ordering::Acquire), 0);
    drop(retry); // The final owned packet record can now be ACKed/disposed.
    assert_eq!(pool.0.free.load(Ordering::Acquire), 1);
    assert!(!pending.ready());
    drop(pending);
    drop(sent);
    assert_eq!(
        pool.0.free.load(Ordering::Acquire),
        ORDINARY + LOC_CID_COUNT as usize
    );
    assert!(budget.used() > 0);
    assert!(!budget.quarantined());
    drop(pool);
    assert_eq!(budget.used(), 0);
}
#[test]
fn interrupted_mutation_is_sticky_quarantine_while_layout_remains_charged() {
    let (budget, pool) = owner();
    let result = std::panic::catch_unwind(|| {
        let mut ticket = pool.prepare(1).unwrap();
        ticket.mark_mutated();
        ticket.push(value(1));
        panic!("intentional CID transaction interruption");
    });
    assert!(result.is_err());
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
    drop(pool);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}
#[test]
fn empty_foreign_inventory_cannot_adopt_a_different_authority() {
    let (left_budget, left) = owner();
    let (right_budget, right) = owner();
    let mut pending = Frames::empty(Some(&left));
    let mut reply = right.prepare(1).unwrap();
    reply.push(value(1));
    let mut reply = reply.finish();
    pending.append(&mut reply);
    assert!(left_budget.quarantined());
    assert!(right_budget.quarantined());
    assert!(left.prepare(1).is_none() && right.prepare(1).is_none());
    assert!(!pending.ready() && !reply.ready());
    assert!(pending.is_empty());
    assert_eq!(reply.iter().count(), 1);
    assert!(left_budget.used() > 0 && right_budget.used() > 0);
    drop(pending);
    drop(reply);
    drop(left);
    drop(right);
    assert_eq!(left_budget.used(), 1);
    assert_eq!(right_budget.used(), 1);
}

// Passive inventory only: no slot acquisition, peer mutation or time control.
impl Frames {
    pub(crate) fn cold_stock_for_probe(&self) -> Option<(usize, usize, usize)> {
        match self {
            Self::Cold(chain) => Some((
                chain.pool.0.free.load(Ordering::Acquire),
                chain.len as usize,
                chain.encoded as usize,
            )),
            Self::Legacy(_) => None,
        }
    }
}

#[test]
fn malformed_free_cycle_is_rejected_before_any_node_is_reserved() {
    let (budget, pool) = owner();
    let first_next = {
        let mut state = pool.lock();
        let next = state.nodes[0].next;
        state.nodes[0].next = 0;
        next
    };
    // One node suffices to prove preparation must reject, without asking the
    // old finish/reverse implementation to traverse an intentionally bad cycle.
    let prepared = pool.prepare(1);
    let refused = prepared.is_none();
    // Restore only the injected physical fault so even the RED can clean up.
    {
        let mut state = pool.lock();
        if prepared.is_some() { state.free_head = first_next; }
        else { state.nodes[0].next = first_next; }
    }
    drop(prepared);
    drop(pool);
    assert!(refused, "audit must precede free-list mutation");
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}
