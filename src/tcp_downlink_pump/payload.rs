//! Payload ownership across Native assembly, FIFO and dispatcher. Native keeps
//! the original metered Bytes reference; Legacy keeps its existing Vec. This
//! value owns neither a quota nor a second physical refund capability.
use bytes::{BackingMeter, Bytes, BytesMut, MeteredCopyError};
mod custodied_bytes;
pub(crate) use custodied_bytes::CustodiedBytes;

#[derive(Debug)]
pub(crate) enum OwnedPayload {
    Legacy(Vec<u8>),
    Native(CustodiedBytes),
}
impl OwnedPayload {
    pub(crate) fn len(&self) -> usize {
        self.as_ref().len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn capacity(&self) -> usize {
        match self {
            Self::Legacy(v) => v.capacity(),
            Self::Native(v) => v.allocation_identity().map_or(0, |id| id.capacity),
        }
    }
    pub(crate) fn into_custodied_bytes(self) -> CustodiedBytes {
        match self {
            Self::Legacy(v) => CustodiedBytes::from_ingress(Bytes::from(v)),
            Self::Native(v) => v,
        }
    }
    /// Only the original loanless Legacy assembler adapter uses this fallible
    /// raw exit. Native FIFO/held/Main custody keeps the typed value instead.
    pub(crate) fn try_into_legacy_bytes(self) -> Result<Bytes, Self> {
        match self {
            Self::Legacy(v) => Ok(Bytes::from(v)),
            Self::Native(v) => v.try_into_raw_legacy().map_err(Self::Native),
        }
    }
    /// Raw ingress only; a downstream view must use `from_segment`.
    pub(crate) fn from_ingress_segment(bytes: &Bytes, range: std::ops::Range<usize>) -> Self {
        if bytes.backing_meter().is_some() {
            Self::Native(CustodiedBytes::from_ingress(bytes.slice(range)))
        } else {
            Self::Legacy(bytes[range].to_vec())
        }
    }
    pub(crate) fn segment(&self, range: std::ops::Range<usize>) -> Self {
        match self {
            Self::Legacy(v) => Self::Legacy(v[range].to_vec()),
            Self::Native(b) => Self::Native(b.slice(range)),
        }
    }
    pub(crate) fn from_segment(bytes: &CustodiedBytes, range: std::ops::Range<usize>) -> Self {
        if bytes.backing_meter().is_some() {
            Self::Native(bytes.slice(range))
        } else {
            Self::Legacy(bytes[range].to_vec())
        }
    }
}
impl AsRef<[u8]> for OwnedPayload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Legacy(v) => v,
            Self::Native(v) => v,
        }
    }
}
impl From<Vec<u8>> for OwnedPayload {
    fn from(v: Vec<u8>) -> Self {
        Self::Legacy(v)
    }
}

/// All allocation/copy happens before an assembler transaction can consume data.
pub(crate) enum PreparedPayload {
    Legacy(Vec<u8>),
    Native(BytesMut),
}
impl PreparedPayload {
    pub(crate) fn new(len: usize, meter: Option<&BackingMeter>) -> Result<Self, MeteredCopyError> {
        if let Some(meter) = meter {
            return BytesMut::try_zeroed_metered(len, meter).map(Self::Native);
        }
        let mut v = Vec::new();
        v.try_reserve_exact(len)
            .map_err(|_| MeteredCopyError::Allocation)?;
        v.resize(len, 0);
        Ok(Self::Legacy(v))
    }
    pub(crate) fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Legacy(v) => v,
            Self::Native(v) => v,
        }
    }
    pub(crate) fn finish(self) -> OwnedPayload {
        match self {
            Self::Legacy(v) => OwnedPayload::Legacy(v),
            Self::Native(v) => OwnedPayload::Native(v.freeze().into()),
        }
    }
}

pub(crate) trait CustodiedPayload {
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn into_custodied_bytes(self) -> CustodiedBytes;
    fn meter(&self) -> Option<BackingMeter>;
    fn has_stock(&self) -> bool;
    fn quiet_scope(&self) -> Option<super::native_wait::QuietScope>;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl CustodiedPayload for Vec<u8> {
    fn has_stock(&self) -> bool { false }
    fn quiet_scope(&self) -> Option<super::native_wait::QuietScope> { None }
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }
    fn into_custodied_bytes(self) -> CustodiedBytes {
        CustodiedBytes::from_ingress(Bytes::from(self))
    }
    fn meter(&self) -> Option<BackingMeter> {
        None
    }
}
impl CustodiedPayload for OwnedPayload {
    fn has_stock(&self) -> bool { matches!(self, Self::Native(bytes) if bytes.has_stock()) }
    fn quiet_scope(&self) -> Option<super::native_wait::QuietScope> { match self { Self::Native(bytes) => bytes.quiet_scope(), _ => None } }
    fn len(&self) -> usize {
        self.len()
    }
    fn capacity(&self) -> usize {
        self.capacity()
    }
    fn into_custodied_bytes(self) -> CustodiedBytes {
        self.into_custodied_bytes()
    }
    fn meter(&self) -> Option<BackingMeter> {
        match self {
            Self::Legacy(_) => None,
            Self::Native(b) => b.backing_meter(),
        }
    }
}

impl std::ops::Deref for OwnedPayload {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for OwnedPayload {
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

/// A same-authority mutation: unknown interruption cannot normalize to clean.
pub(crate) struct MeteredMutation(Option<BackingMeter>);
impl MeteredMutation {
    pub(crate) fn begin(meter: Option<&BackingMeter>) -> Result<Self, MeteredCopyError> {
        if let Some(m) = meter {
            m.admit_precharged()?;
        }
        Ok(Self(meter.cloned()))
    }
    pub(crate) fn dispose(meter: Option<&BackingMeter>) -> Self {
        Self(meter.cloned())
    }
    pub(crate) fn commit(mut self) {
        self.0 = None;
    }
}
impl Drop for MeteredMutation {
    fn drop(&mut self) {
        if let Some(m) = &self.0 {
            m.quarantine();
        }
    }
}

// Complete refusal shared by the two original raw FIFO entries. Collection and
// all atomic marks precede quiet publication and possible last owner Arc drops.
pub(super) fn reject_raw_stock<P: CustodiedPayload>(
    receiver: Option<super::native_wait::QuietScope>, pending: &Option<P>,
) -> super::ByteCustodyError {
    let actual = pending.as_ref().and_then(CustodiedPayload::quiet_scope);
    let meter = pending.as_ref().and_then(CustodiedPayload::meter);
    if let Some(receiver) = &receiver { receiver.mark_quarantined(); }
    if let Some(actual) = &actual { actual.mark_quarantined(); }
    if let Some(meter) = &meter { meter.quarantine(); }
    drop(actual);
    drop(receiver);
    super::ByteCustodyError::Invalid
}
