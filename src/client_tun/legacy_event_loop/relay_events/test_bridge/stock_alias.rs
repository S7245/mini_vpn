//! One test-only typed view from an actual selected event; no queue or permit.
//! The view is cloned/taken/dropped outside bridge locks. Receipt is a Copy
//! projection of the original Main mirror, never a replacement release proof.
use super::*;
use crate::tcp_downlink_pump::{CustodiedBytes, OwnedPayload, StockKey};
#[derive(Default)]
enum State { #[default] Empty, Armed, Capturing, Held(CustodiedBytes), Taken }
#[derive(Default)]
pub(super) struct Capture {
    target: Option<(SocketHandle, u64, StockKey)>,
    state: State,
    receipt: Option<(usize, usize, usize, usize, usize)>,
}
impl RelayEventTestBridge {
    pub(in crate::client_tun) fn arm_stock_alias(&self, handle: SocketHandle, epoch: u64, key: StockKey) {
        let mut capture = self.stock_alias.lock().unwrap();
        assert!(matches!(capture.state, State::Empty));
        capture.target = Some((handle, epoch, key));
        capture.state = State::Armed;
    }
    pub(super) fn capture_stock_alias(&self, selected: &Option<(SocketHandle, RelayEvent)>) {
        let Some((handle, RelayEvent::Data { epoch, bytes: OwnedPayload::Native(bytes), .. })) = selected else { return; };
        {
            let mut capture = self.stock_alias.lock().unwrap();
            if !matches!(capture.state, State::Armed) { return; }
            let Some((h, e, key)) = &capture.target else { return; };
            if h != handle || e != epoch { return; }
            assert!(key.matches_payload(bytes));
            capture.state = State::Capturing;
        }
        let alias = bytes.clone();
        let mut capture = self.stock_alias.lock().unwrap();
        assert!(matches!(capture.state, State::Capturing));
        capture.state = State::Held(alias);
    }
    pub(in crate::client_tun) fn take_stock_alias(&self) -> CustodiedBytes {
        let held = {
            let mut capture = self.stock_alias.lock().unwrap();
            std::mem::replace(&mut capture.state, State::Taken)
        };
        match held { State::Held(alias) => alias, _ => panic!("actual selected stock alias required") }
    }
    pub(in crate::client_tun) fn observe_stock_receipt(&self, contexts: &HashMap<SocketHandle, SocketCtx>) {
        let mut capture = self.stock_alias.lock().unwrap();
        let Some((handle, epoch, key)) = &capture.target else { return; };
        capture.receipt = contexts.get(handle).and_then(|ctx| ctx.flow_mirror.as_ref()
            .and_then(|mirror| mirror.stock_receipt_for_test(*handle, *epoch, ctx.conn_epoch, key)));
    }
    pub(in crate::client_tun) fn stock_key_for_test(&self) -> StockKey {
        self.stock_alias.lock().unwrap().target.as_ref().unwrap().2.clone()
    }
    pub(in crate::client_tun) fn stock_receipt(&self) -> Option<(usize, usize, usize, usize, usize)> {
        self.stock_alias.lock().unwrap().receipt
    }
}
