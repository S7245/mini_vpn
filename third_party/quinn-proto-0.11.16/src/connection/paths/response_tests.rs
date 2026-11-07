use super::*;
impl PathResponses {
    pub(crate) fn snapshot_for_packet_probe(&self) -> Vec<(u64, u64, SocketAddr)> {
        self.pending
            .values
            .iter()
            .map(|r| (r.packet, r.token, r.remote))
            .collect()
    }
}
#[test]
fn response_preparation_cancels_and_full_rejects_without_old_path_mutation() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut responses = PathResponses::default();
    let remote: SocketAddr = "[::1]:4433".parse().unwrap();
    let prepared = responses.prepare_push(remote, Some(&meter)).unwrap();
    assert!(responses.is_empty());
    drop(prepared);
    assert_eq!(budget.used(), 0);
    let pressure = budget.try_reserve(budget.limit()).unwrap();
    assert!(matches!(
        responses.prepare_push(remote, Some(&meter)),
        Err(bytes::MeteredCopyError::BudgetFull)
    ));
    assert!(responses.is_empty());
    drop(pressure);
    let prepared = responses.prepare_push(remote, Some(&meter)).unwrap();
    responses.install(prepared).unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    responses.push(7, 12, remote);
    let prepared = responses.prepare_push(remote, Some(&meter)).unwrap();
    responses.install(prepared).unwrap();
    responses.push(6, 13, remote);
    responses.push(8, 14, remote);
    assert_eq!(responses.pop_on_path(remote), Some(14));
    drop((pressure, responses));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn path_response_stock_keeps_sixteen_bound_order_and_authority() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut responses = PathResponses::default();
    for n in 0..16u16 {
        let remote: SocketAddr = format!("[::1]:{}", 4433 + n).parse().unwrap();
        let prepared = responses.prepare_push(remote, Some(&meter)).unwrap();
        let before = responses.snapshot_for_packet_probe();
        let used = budget.used();
        drop(prepared);
        assert_eq!(responses.snapshot_for_packet_probe(), before);
        assert!(budget.used() <= used);
        let prepared = responses.prepare_push(remote, Some(&meter)).unwrap();
        responses.install(prepared).unwrap();
        responses.push(n as u64, 100 + n as u64, remote);
    }
    assert_eq!(responses.pending.values.len(), 16);
    assert_eq!(responses.pending.values.capacity(), 16);
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let remote: SocketAddr = "[::1]:6000".parse().unwrap();
    let prepared = responses.prepare_push(remote, Some(&meter)).unwrap();
    responses.install(prepared).unwrap();
    responses.push(99, 999, remote);
    assert_eq!(
        responses.pending.values.len(),
        16,
        "original excessive response is ignored"
    );
    for n in (0..16u16).rev() {
        let remote: SocketAddr = format!("[::1]:{}", 4433 + n).parse().unwrap();
        assert_eq!(responses.pop_on_path(remote), Some(100 + n as u64));
    }
    drop(held);
    let other_budget = bytes::BackingBudget::new(65536).unwrap();
    let other = bytes::BackingMeter::with_budget(&other_budget);
    assert!(matches!(
        responses.prepare_push(remote, Some(&other)),
        Err(bytes::MeteredCopyError::Quarantined)
    ));
    assert!(budget.quarantined() && other_budget.quarantined());
    drop(responses);
    assert_eq!(budget.used(), 1);
    assert_eq!(other_budget.used(), 1);
}
