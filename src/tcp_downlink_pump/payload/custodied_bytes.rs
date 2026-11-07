//! Typed backing view through FIFO, held batch, segment and Main custody.
//! Bytes are destroyed before their stock loan is returned. No raw Bytes view
//! escapes an existing loan; raw ingress has no loan. Tests: Native storage and
//! output_stock/tests, then original Source/dispatcher/Main/Service acceptance.
use bytes::{AllocationIdentity, BackingMeter, Bytes};
use std::{mem::ManuallyDrop, ops::{Deref, RangeBounds}};
use super::super::output_stock::{Loan, Root};

#[derive(Debug)]
pub(crate) struct CustodiedBytes {
    bytes: ManuallyDrop<Bytes>,
    loan: ManuallyDrop<Option<Loan>>,
}
impl CustodiedBytes {
    pub(crate) fn from_ingress(bytes: Bytes) -> Self { Self::parts(bytes, None) }
    fn parts(bytes: Bytes, loan: Option<Loan>) -> Self {
        Self { bytes: ManuallyDrop::new(bytes), loan: ManuallyDrop::new(loan) }
    }
    pub(in crate::tcp_downlink_pump) fn from_stock(bytes: Bytes, loan: Loan) -> Self { Self::parts(bytes, Some(loan)) }
    pub(in crate::tcp_downlink_pump) fn quiet_scope(&self) -> Option<crate::tcp_downlink_pump::native_wait::QuietScope> {
        self.loan.as_ref().map(|loan| loan.quiet_scope())
    }
    fn view(&self, bytes: Bytes) -> Self {
        let _quiet = self.quiet_scope();
        let loan = self.loan.as_ref().and_then(|loan| loan.clone_for(&bytes));
        Self::parts(bytes, loan)
    }
    pub(crate) fn len(&self) -> usize { self.bytes.len() }
    pub(crate) fn is_empty(&self) -> bool { self.bytes.is_empty() }
    pub(crate) fn slice(&self, range: impl RangeBounds<usize>) -> Self { self.view(self.bytes.slice(range)) }
    pub(crate) fn split_to(&mut self, at: usize) -> Self {
        let _quiet = self.quiet_scope();
        self.split_to_queued(at)
    }
    /// Original FIFO's external quiet Scope covers this same-lock bookkeeping.
    /// Both actual views survive; no destructor runs under that owner lock.
    pub(in crate::tcp_downlink_pump) fn split_to_queued(&mut self, at: usize) -> Self {
        let bytes = self.bytes.split_to(at);
        let loan = self.loan.as_ref().and_then(|loan| loan.clone_for(&bytes));
        if let Some(loan) = self.loan.as_mut() { loan.truncate(self.bytes.len()); }
        Self::parts(bytes, loan)
    }
    pub(crate) fn truncate(&mut self, len: usize) {
        let _quiet = self.quiet_scope();
        self.bytes.truncate(len);
        if let Some(loan) = self.loan.as_mut() { loan.truncate(self.bytes.len()); }
    }
    pub(crate) fn backing_meter(&self) -> Option<BackingMeter> { self.bytes.backing_meter() }
    pub(crate) fn allocation_identity(&self) -> Option<AllocationIdentity> { self.bytes.allocation_identity() }
    pub(in crate::tcp_downlink_pump) fn has_stock(&self) -> bool { self.loan.is_some() }
    pub(in crate::tcp_downlink_pump) fn quarantine_stock(&self) {
        let _quiet = self.quiet_scope();
        if let Some(loan) = self.loan.as_ref() { loan.quarantine(); }
    }
    pub(in crate::tcp_downlink_pump) fn stock_matches(&self, root: *const Root) -> bool {
        self.loan.as_ref().is_some_and(|loan| loan.matches(root) && loan.valid(&self.bytes))
    }
    pub(in crate::tcp_downlink_pump) fn try_into_raw_legacy(self) -> Result<Bytes, Self> {
        if self.has_stock() { return Err(self); }
        let mut value = ManuallyDrop::new(self);
        // This branch has no loan/other resource. Transfer the original Bytes.
        Ok(unsafe { ManuallyDrop::take(&mut value.bytes) })
    }
}
impl Clone for CustodiedBytes {
    fn clone(&self) -> Self { self.view(Bytes::clone(&self.bytes)) }
}
struct DropBytes<'a> { loan: &'a ManuallyDrop<Option<Loan>>, completed: bool }
impl Drop for DropBytes<'_> {
    fn drop(&mut self) {
        if !self.completed { if let Some(loan) = self.loan.as_ref() { loan.quarantine(); } }
    }
}
impl Drop for CustodiedBytes {
    fn drop(&mut self) {
        let _quiet = self.quiet_scope();
        if std::thread::panicking() { self.quarantine_stock(); }
        let mut guard = DropBytes { loan: &self.loan, completed: false };
        // If an invalid opaque Bytes destructor unwinds, keep the loan itself
        // in ManuallyDrop, marked Q; never notify before actual Bytes disposal.
        unsafe { ManuallyDrop::drop(&mut self.bytes); }
        guard.completed = true;
        drop(guard);
        let loan = unsafe { ManuallyDrop::take(&mut self.loan) };
        drop(loan);
    }
}
impl From<Bytes> for CustodiedBytes { fn from(bytes: Bytes) -> Self { Self::from_ingress(bytes) } }
impl AsRef<[u8]> for CustodiedBytes { fn as_ref(&self) -> &[u8] { self.bytes.as_ref() } }
impl Deref for CustodiedBytes { type Target = [u8]; fn deref(&self) -> &[u8] { self.as_ref() } }
impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for CustodiedBytes { fn eq(&self, other: &T) -> bool { self.as_ref() == other.as_ref() } }

#[cfg(test)]
mod tests;
