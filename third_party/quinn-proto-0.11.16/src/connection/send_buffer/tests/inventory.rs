use super::*;

impl Native {
    pub(in crate::connection) fn actual_layouts_for_probe(&self) -> Vec<(Option<bytes::AllocationIdentity>, usize)> {
        assert!(self.segments.layout_matches(&self.meter));
        assert!(self.acks.inventory_matches(&self.meter));
        assert!(self.retransmits.inventory_matches(&self.meter));
        let mut layouts = vec![
            (None,
             Layout::array::<Bytes>(self.segments.values.capacity()).unwrap().size()),
            (None, self.acks.actual_layout_for_probe().1), (None, self.retransmits.actual_layout_for_probe().1),
        ];
        for value in &self.segments.values {
            let allocation = value.allocation_identity().unwrap();
            assert!(value.backing_meter().unwrap().same_budget(&self.meter));
            layouts.push((Some(allocation), bytes::BytesMut::metered_copy_layout_for_len(allocation.capacity).unwrap()));
        }
        layouts
    }
}

#[test]
fn revision_last_commit_completes_then_full_cannot_mask_exhausted_authority() {
    let budget = bytes::BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut native = Native::new(meter.clone());
    native.revision = u64::MAX - 1;
    let stamp = native.stamp().unwrap();
    let storage = native.prepare(1).unwrap();
    native.install(storage, stamp, &meter).unwrap();
    assert_eq!(native.revision, u64::MAX);
    assert!(!budget.quarantined(), "last valid commit must not introduce mid-transaction Q");
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    assert!(matches!(native.prepare_ack(0..1, 1, 1), Err(MeteredCopyError::Quarantined)));
    assert!(budget.quarantined());
    drop(pressure);
    drop(native);
    assert!(budget.used() > 0);
}
