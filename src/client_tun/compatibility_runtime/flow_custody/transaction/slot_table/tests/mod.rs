//! D61/D62 R-a: the occupancy index equals row occupancy in both directions
//! through every mutation API, and `next_occupied` visits exactly the rows.
use super::super::{FLOW_SLOT_LIMIT, OpenPhase, OpenSlot};
use super::{CAPACITY, SlotTable};

fn row() -> OpenSlot {
    OpenSlot {
        handle: Default::default(),
        epoch: 1,
        deadline: tokio::time::Instant::now(),
        phase: OpenPhase::Reserved,
    }
}

fn walk(table: &SlotTable) -> Vec<usize> {
    let mut seen = Vec::new();
    let mut from = 0;
    while let Some(index) = table.next_occupied(from) {
        seen.push(index);
        from = index + 1;
    }
    seen
}

#[test]
fn occupancy_index_tracks_rows_in_both_directions() {
    let mut table = SlotTable::with_len(FLOW_SLOT_LIMIT);
    assert!(CAPACITY >= FLOW_SLOT_LIMIT && table.len() == FLOW_SLOT_LIMIT);
    assert_eq!(walk(&table), Vec::<usize>::new());
    let picks = [0, 63, 64, 127, 4095, FLOW_SLOT_LIMIT - 1];
    for index in picks {
        assert!(matches!(table.put(index, row()), Ok(None)));
    }
    assert!(matches!(table.put(FLOW_SLOT_LIMIT, row()), Err(_)), "out of range is returned");
    assert!(matches!(table.put(64, row()), Ok(Some(_))), "replace keeps the bit");
    assert_eq!(walk(&table), picks);
    assert_eq!(table.occupied().count(), picks.len());
    assert_eq!(table.next_occupied(65), Some(127));
    assert_eq!(table.next_occupied(FLOW_SLOT_LIMIT), None);
    assert!(table.occupancy_consistent());
    table.get_mut(63).expect("occupied").epoch = 7;
    assert!(table.take(63).is_some_and(|slot| slot.epoch == 7));
    assert!(table.take(63).is_none() && table.get_mut(63).is_none());
    assert_eq!(walk(&table), [0, 64, 127, 4095, FLOW_SLOT_LIMIT - 1]);
    assert_eq!(table.occupied_mut().map(|(index, _)| index).collect::<Vec<_>>(), walk(&table));
    assert!(table.occupancy_consistent());
    // Both directions of the census discriminate.
    table.corrupt_bit_for_test(5);
    assert!(!table.occupancy_consistent(), "bit without row");
    table.corrupt_bit_for_test(5);
    table.corrupt_bit_for_test(0);
    assert!(!table.occupancy_consistent(), "row without bit");
    let empty = SlotTable::default();
    assert!(empty.occupancy_consistent() && empty.next_occupied(0).is_none());
}
