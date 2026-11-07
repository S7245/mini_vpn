//! Static custody contract for this bounded channel. Implementations own event
//! validation and field disposition; the channel owns physical slots/callbacks.
use super::native::Fault;
pub(crate) trait Value: Send + 'static {
    type Disposition: Send + Sync + 'static;
    /// Called only after final lane quiescence. Implementations must split
    /// opaque fields before guarded destruction; an unresolved remainder stays
    /// in the original Core and quarantines it. No implicit context destructor.
    fn dispose_context(context: Self::Disposition, fault: &Fault<'_>) -> Option<Self::Disposition>;
    fn lane_bytes(&self) -> usize;
    fn valid_for(&self, meter: &bytes::BackingMeter) -> bool;
    fn dispose(self, fault: &Fault<'_>, context: &Self::Disposition) -> Option<Self>
    where
        Self: Sized;
}
