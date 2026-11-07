use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Legacy characterization mutations remain outside the production Interface.
impl RxStock {
    pub(crate) fn test_legacy(current: Option<BytesMut>, staged: VecDeque<BytesMut>) -> Self {
        let mut stock = Self::default();
        stock.current = current.map(Into::into);
        stock.staged = staged.into_iter().map(Into::into).collect();
        stock
    }
    pub(crate) fn test_current_capacity(&self) -> usize {
        self.current.as_ref().unwrap().capacity()
    }
    pub(crate) fn test_staged_capacity(&self) -> usize {
        self.staged.capacity()
    }
    pub(crate) fn test_set_current(&mut self, packet: BytesMut) {
        assert!(self.cold.is_none());
        self.current = Some(packet.into());
    }
    pub(crate) fn test_push_staged(&mut self, packet: BytesMut) {
        assert!(self.cold.is_none());
        self.staged.push_back(packet.into());
    }
    pub(crate) fn test_staged_front(&self) -> &[u8] {
        self.staged.front().unwrap()
    }
}
fn native(slots: usize) -> (bytes::BackingBudget, BackingMeter, RxStock) {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let stock = RxStock::native(slots, &meter).unwrap();
    (budget, meter, stock)
}

mod cold;
