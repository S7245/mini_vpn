use super::*;

#[test]
fn fixed_host_catalog_owns_exact_layout_and_known_refusal_leaves_no_charge() {
    let size = Layout::new::<[Slot; BUDGETED_CONNECTION_LIMIT]>().size();
    let small = bytes::BackingBudget::new(size - 1).unwrap();
    assert!(Catalog::prepare(Some(&BackingMeter::with_budget(&small))).is_err());
    assert_eq!(small.used(), 0);
    assert!(!small.quarantined());
    let budget = bytes::BackingBudget::new(size).unwrap();
    let catalog = Catalog::prepare(Some(&BackingMeter::with_budget(&budget))).unwrap();
    let Catalog::Native(fixed) = &catalog else {
        panic!("hard profile must own fixed physical storage");
    };
    assert_eq!(std::mem::size_of_val(&*fixed.slots), size);
    assert_eq!(budget.used(), size);
    assert_eq!(catalog.occupied_len(), 0);
    eprintln!(
        "native-host-catalog: actual-layout={size} exact-precharge=true known-refusal-zero=true"
    );
    drop(catalog);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

impl Catalog {
    pub(in crate::endpoint) fn previous_pending_count_for_probe(&self) -> usize {
        self.entries()
            .filter(|entry| entry.previous_socket.is_some())
            .count()
    }
}
