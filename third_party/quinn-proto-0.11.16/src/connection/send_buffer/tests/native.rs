use super::*;
use bytes::{BackingBudget, BytesMut};

impl SendBuffer {
    pub(in crate::connection) fn actual_layouts_for_probe(&self) -> Vec<(Option<bytes::AllocationIdentity>, usize)> {
        self.native.as_ref().map_or_else(Vec::new, |native| {
            assert!(native.inventory_matches(self.unacked_len));
            native.actual_layouts_for_probe()
        })
    }
}

fn funded(buffer: &mut SendBuffer, meter: &BackingMeter) {
    let plan = buffer.prepare_write(1, Some(meter)).unwrap();
    let data = BytesMut::try_copy_from_slice_metered(&[7; 64], meter).unwrap().freeze();
    buffer.install_write(plan).unwrap();
    buffer.write(data);
    buffer.poll_transmit(128);
}

#[test]
fn native_write_plan_cannot_cross_buffers_with_same_meter() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut a = SendBuffer::new();
    let mut b = SendBuffer::new();
    funded(&mut a, &meter);
    funded(&mut b, &meter);
    let plan = a.prepare_write(8, Some(&meter)).unwrap();
    assert_eq!(b.install_write(plan), Err(MeteredCopyError::Quarantined));
    assert!(budget.quarantined());
    drop((a, b));
    assert!(budget.used() > 0);
}

#[test]
fn native_ack_plan_cannot_be_used_as_loss() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut buffer = SendBuffer::new();
    funded(&mut buffer, &meter);
    let plan = buffer.prepare_ack(0..16).unwrap();
    assert_eq!(buffer.apply_retransmit(plan), Err(MeteredCopyError::Quarantined));
    assert_eq!(buffer.unacked_len, 64);
    drop(buffer);
    assert!(budget.quarantined() && budget.used() > 0);
}

#[test]
fn native_old_ack_plan_cannot_replace_new_hole_inventory() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut buffer = SendBuffer::new();
    funded(&mut buffer, &meter);
    let old = buffer.prepare_ack(32..40).unwrap();
    let first = buffer.prepare_ack(16..24).unwrap();
    buffer.apply_ack(first).unwrap();
    assert_eq!(buffer.apply_ack(old), Err(MeteredCopyError::Quarantined));
    assert_eq!(buffer.unacked_len, 64);
    drop(buffer);
    assert!(budget.quarantined() && budget.used() > 0);
}

#[test]
fn native_0rtt_cursor_reset_invalidates_old_preparation() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut buffer = SendBuffer::new();
    funded(&mut buffer, &meter);
    let old = buffer.prepare_retransmit(0..64).unwrap();
    buffer.retransmit_all_for_0rtt();
    assert_eq!(buffer.apply_retransmit(old), Err(MeteredCopyError::Quarantined));
    assert_eq!(buffer.unsent, 0);
    assert_eq!(buffer.unacked_len, 64);
}

#[test]
fn native_full_ack_and_loss_preserve_bytes_then_exact_holes_retire() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut buffer = SendBuffer::new();
    funded(&mut buffer, &meter);
    let before = budget.used();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = BytesMut::try_zeroed_metered(budget.limit() - before - header, &meter).unwrap();
    assert!(matches!(buffer.prepare_ack(16..32), Err(MeteredCopyError::BudgetFull)));
    assert!(matches!(buffer.prepare_retransmit(0..64), Err(MeteredCopyError::BudgetFull)));
    assert_eq!(buffer.unacked(), 64);
    assert_eq!(buffer.get(0..64), &[7; 64]);
    assert!(!budget.quarantined());
    drop(pressure);
    let ack = buffer.prepare_ack(16..32).unwrap();
    buffer.apply_ack(ack).unwrap();
    assert_eq!(buffer.unacked(), 48);
    assert_eq!(buffer.unacked_len, 64);
    let hole_cost = budget.used();
    let ack = buffer.prepare_ack(0..16).unwrap();
    buffer.apply_ack(ack).unwrap();
    assert_eq!(buffer.unacked_len, 32);
    assert_eq!(budget.used(), hole_cost, "a slice and cold free node do not refund their backing");
    let ack = buffer.prepare_ack(32..64).unwrap();
    buffer.apply_ack(ack).unwrap();
    assert!(buffer.is_fully_acked());
    assert!(budget.used() > 0, "cold inventories retire with their physical owner");
    drop(buffer);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn cold_metadata_query_rejects_bare_storage_and_foreign_meter_without_mutation() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let other_meter = BackingMeter::with_budget(&budget);
    let mut buffer = SendBuffer::new();
    assert_eq!(buffer.retained_metadata_bytes(&meter), Some(0));
    buffer.unacked_segments.reserve(1);
    assert_eq!(buffer.retained_metadata_bytes(&meter), None);
    drop(buffer);
    let mut buffer = SendBuffer::new();
    funded(&mut buffer, &meter);
    let expected = buffer.actual_layouts_for_probe().iter()
        .filter(|(id, _)| id.is_none()).map(|(_, n)| n).sum();
    assert_eq!(buffer.retained_metadata_bytes(&other_meter), None);
    assert_eq!(buffer.retained_metadata_bytes(&meter), Some(expected));
    assert!(!budget.quarantined());
    meter.quarantine();
    assert_eq!(buffer.retained_metadata_bytes(&meter), Some(expected));
}

#[test]
fn packet_capacity_cancels_then_commits_ack_and_loss_at_full_budget() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut buffer = SendBuffer::new();
    funded(&mut buffer, &meter);
    let original = buffer.actual_layouts_for_probe();
    let charge = budget.used();
    let prepared = buffer.prepare_packet_capacity(16, 16).unwrap();
    assert!(budget.used() > charge);
    drop(prepared);
    assert_eq!(budget.used(), charge);
    assert_eq!(buffer.actual_layouts_for_probe(), original);
    let prepared = buffer.prepare_packet_capacity(16, 16).unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    buffer.install_packet_capacity(prepared).unwrap();
    for n in 0..16 {
        let range = 2*n+1..2*n+2;
        let ack = buffer.prepare_ack(range.clone()).unwrap();
        buffer.apply_ack(ack).unwrap();
        let loss = buffer.prepare_retransmit(range).unwrap();
        buffer.apply_retransmit(loss).unwrap();
    }
    assert_eq!(budget.used(), budget.limit());
    assert_eq!(buffer.unacked(), 48);
    assert_eq!(buffer.unacked_len, 64);
    drop(pressure);
    drop(buffer);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
