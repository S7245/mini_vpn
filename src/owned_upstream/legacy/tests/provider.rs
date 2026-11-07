//! Passive callbacks from the actual adapter and its actual Box Drop.
//! The observer cannot replace provider output, mint a receipt or settle work.
use super::*;
use std::cell::RefCell;
thread_local! {
    static OBSERVER: RefCell<Option<Box<dyn Fn(bool)>>> = const { RefCell::new(None) };
}
pub(crate) struct Registration;
impl Drop for Registration {
    fn drop(&mut self) { OBSERVER.with(|value| { value.borrow_mut().take(); }); }
}
impl<U> LegacyOwnedUpstream<U> {
    pub(crate) fn observe_provider_for_test(observer: impl Fn(bool) + 'static) -> Registration {
        OBSERVER.with(|value| {
            assert!(value.borrow().is_none());
            *value.borrow_mut() = Some(Box::new(observer));
        });
        Registration
    }
}
pub(super) fn observe(drop: bool) {
    OBSERVER.with(|value| { if let Some(observer) = value.borrow().as_ref() { observer(drop); } });
}
impl Drop for LegacyTcpOpen {
    fn drop(&mut self) { observe(true); }
}
