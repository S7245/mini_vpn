//! Original Roles' output inventory and bounded retirement work. Source owns
//! the single producer; FIFO and Main retain their existing DATA authorities.
//! Cold preparation funds the actual waiting-future layout before publication.
//! Entry Inventory; real Source/Flow lifecycle tests live in native_inventory.
use super::*;
use crate::tcp_downlink_pump::{OutputStock, StockProducer, StockKey, StockWaitError};
use std::{alloc::{alloc, Layout}, future::Future, pin::Pin};

type Waiting = Pin<Box<dyn Future<Output = (OutputStock, Result<(), StockWaitError>)> + Send>>;

pub(super) struct Inventory {
    producer: Option<StockProducer>,
    waiting: Option<Waiting>,
    returned: Option<OutputStock>,
    allowance: Option<bytes::BackingBudgetReservation>,
    key: StockKey,
    clock: FailureClock,
    issued: bool,
    done: bool,
    failed: bool,
}
impl Inventory {
    pub(super) fn prepare(mut stock: OutputStock, meter: &bytes::BackingMeter, clock: FailureClock)
        -> Result<Self, bytes::MeteredCopyError>
    {
        let key = stock.key().ok_or(bytes::MeteredCopyError::Quarantined)?;
        let producer = stock.take_producer()?;
        let work = async move {
            let result = stock.wait_returned().await;
            (stock, result)
        };
        let layout = Layout::for_value(&work);
        let allowance = match meter.reserve_cold_layout(layout.size()) {
            Ok(value) => value,
            Err(error) => { drop(producer); drop(work); return Err(error); }
        };
        // Exact cold future allocation; no late allocation under backing Full.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            drop(producer); drop(work); drop(allowance);
            return Err(bytes::MeteredCopyError::Allocation);
        }
        unsafe fn place<T>(ptr: *mut u8, value: T) -> Pin<Box<T>> {
            let ptr = ptr.cast::<T>();
            unsafe { ptr.write(value); Box::into_pin(Box::from_raw(ptr)) }
        }
        let waiting: Waiting = unsafe { place(ptr, work) };
        Ok(Self { producer: Some(producer), waiting: Some(waiting), returned: None,
            allowance, key, clock, issued: false, done: false, failed: false })
    }
    pub(super) fn key(&self) -> &StockKey { &self.key }
    pub(super) fn take_producer(&mut self) -> Option<StockProducer> {
        if self.issued { self.failed = true; self.clock.quarantine(); return None; }
        self.issued = true;
        self.producer.take()
    }
    pub(super) fn poll_retire(&mut self, shared: &super::super::super::continuous::Shared, cx: &mut Context<'_>) {
        if self.failed || self.done { return; }
        if !self.issued || self.producer.is_some() { self.failed = true; self.clock.quarantine(); return; }
        if let Some(waiting) = &mut self.waiting {
            let Poll::Ready((stock, result)) = waiting.as_mut().poll(cx) else { return; };
            self.returned = Some(stock);
            // The real wait future/proxy is destroyed before inventory retirement.
            drop(self.waiting.take());
            drop(self.allowance.take());
            if result.is_err() { self.failed = true; self.clock.quarantine(); return; }
        }
        if let Some(stock) = &mut self.returned {
            if !stock.try_retire() { self.failed = true; self.clock.quarantine(); return; }
            drop(self.returned.take());
        }
        match shared.queue.finish_stock_callbacks() {
            Ok(true) => self.done = true,
            Ok(false) => {},
            Err(_) => { self.failed = true; self.clock.quarantine(); }
        }
    }
    pub(super) fn done(&self) -> bool { self.done }
    pub(super) fn failed(&self) -> bool { self.failed }
}
impl Drop for Inventory {
    fn drop(&mut self) {
        if !self.issued {
            // Ordinary cold rollback: real writer first, then the unpolled owner.
            drop(self.producer.take());
            drop(self.waiting.take());
            drop(self.returned.take());
            drop(self.allowance.take());
        } else if !self.done {
            self.clock.quarantine();
            std::mem::forget(self.waiting.take());
            std::mem::forget(self.returned.take());
            std::mem::forget(self.allowance.take());
        }
    }
}

#[cfg(test)]
#[path = "../tests/stock_wait_observation.rs"]
mod tests;
