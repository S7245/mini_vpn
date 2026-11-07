use crate::shared::TargetAddr;
use crate::tuic::{FragReassembler, decode_packet_meta};
use bytes::Bytes;
use std::fmt;
use std::num::NonZeroU16;
use std::time::Instant;
use tokio::sync::mpsc;

pub(crate) mod generation;

/// The non-zero local association selected by the TUN UDP flow table.
///
/// This remains deliberately distinct from the session-scoped
/// `SessionFlowId` and from QUIC stream/packet identifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct LocalAssociationId(NonZeroU16);

impl LocalAssociationId {
    pub const fn new(value: u16) -> Option<Self> {
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// One transport-neutral UDP packet submitted by the local TUN flow owner.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct UdpUplink<'a> {
    association: LocalAssociationId,
    target: &'a TargetAddr,
    payload: &'a [u8],
}

impl fmt::Debug for UdpUplink<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UdpUplink")
            .field("association", &self.association)
            .field("target", &"[REDACTED]")
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

impl<'a> UdpUplink<'a> {
    pub const fn new(
        association: LocalAssociationId,
        target: &'a TargetAddr,
        payload: &'a [u8],
    ) -> Self {
        Self {
            association,
            target,
            payload,
        }
    }

    pub const fn association(&self) -> LocalAssociationId {
        self.association
    }

    pub const fn target(&self) -> &'a TargetAddr {
        self.target
    }

    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub const fn into_parts(self) -> (LocalAssociationId, &'a TargetAddr, &'a [u8]) {
        (self.association, self.target, self.payload)
    }
}

/// One decoded UDP packet ready for the existing association-table routing.
#[derive(Clone, PartialEq, Eq)]
pub struct UdpDownlink {
    association: LocalAssociationId,
    payload: Bytes,
}

impl fmt::Debug for UdpDownlink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UdpDownlink")
            .field("association", &self.association)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

impl UdpDownlink {
    pub fn new(association: LocalAssociationId, payload: Bytes) -> Self {
        Self {
            association,
            payload,
        }
    }

    pub const fn association(&self) -> LocalAssociationId {
        self.association
    }

    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_parts(self) -> (LocalAssociationId, Bytes) {
        (self.association, self.payload)
    }
}

/// Result of consuming exactly one downlink source item.
///
/// `Ignored` covers malformed and incomplete legacy fragments. Returning it
/// instead of looping internally preserves the old event-loop scheduling:
/// every raw TUIC item yields control back to the caller once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UdpDownlinkEvent {
    Deliver(UdpDownlink),
    Ignored,
}

enum UdpDownlinkSourceInner {
    Generation(generation::Source),
    Native(generation::native_source::Source),
    LegacyRaw {
        receiver: mpsc::Receiver<Vec<u8>>,
        reassembler: FragReassembler,
    },
    Owned {
        receiver: mpsc::Receiver<UdpDownlink>,
    },
}

/// Single-owner UDP downlink source used by both compatibility and resumable
/// adapters.
///
/// The legacy variant owns the exact raw receiver returned by
/// `TuicUpstream::start_udp` and decodes it in the caller task. It creates no
/// forwarding task. The owned variant is reserved for the future bounded UDP
/// delivery engine and likewise owns its receiver directly.
pub struct UdpDownlinkSource {
    inner: UdpDownlinkSourceInner,
}

impl UdpDownlinkSource {
    #[cfg(test)]
    pub(crate) fn generation_for_test(
        &self,
    ) -> Option<std::sync::Arc<generation::GenerationControl>> {
        match &self.inner {
            UdpDownlinkSourceInner::Generation(source) => Some(source.generation_for_test()),
            UdpDownlinkSourceInner::Native(source) => Some(source.control()),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn generation_pending(&self) -> Option<usize> {
        match &self.inner {
            UdpDownlinkSourceInner::Generation(source) => Some(source.pending()),
            UdpDownlinkSourceInner::Native(source) => Some(source.pending()),
            _ => None,
        }
    }

    pub fn legacy_raw(receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            inner: UdpDownlinkSourceInner::LegacyRaw {
                receiver,
                reassembler: FragReassembler::new(),
            },
        }
    }

    pub fn owned(receiver: mpsc::Receiver<UdpDownlink>) -> Self {
        Self {
            inner: UdpDownlinkSourceInner::Owned { receiver },
        }
    }

    /// Consume exactly one source item, or return `None` when its sole sender
    /// side is closed and drained.
    pub async fn recv(&mut self, clock: &Instant) -> Option<UdpDownlinkEvent> {
        match &mut self.inner {
            UdpDownlinkSourceInner::Generation(source) => source.recv(clock).await,
            UdpDownlinkSourceInner::Native(source) => source.recv(clock).await,
            UdpDownlinkSourceInner::LegacyRaw {
                receiver,
                reassembler,
            } => {
                let raw = receiver.recv().await?;
                // Measure after the await: time spent parked in the caller's
                // select is not the fragment's arrival time.
                let now_secs = clock.elapsed().as_secs();
                let Some(meta) = decode_packet_meta(&raw) else {
                    return Some(UdpDownlinkEvent::Ignored);
                };
                let association = LocalAssociationId::new(meta.assoc_id);
                let payload = reassembler.accept(&meta, now_secs);
                match (association, payload) {
                    (Some(association), Some(payload)) => Some(UdpDownlinkEvent::Deliver(
                        UdpDownlink::new(association, Bytes::from(payload)),
                    )),
                    _ => Some(UdpDownlinkEvent::Ignored),
                }
            }
            UdpDownlinkSourceInner::Owned { receiver } => {
                receiver.recv().await.map(UdpDownlinkEvent::Deliver)
            }
        }
    }

    /// Preserve the existing explicit legacy fragment TTL sweep. Owned UDP
    /// delivery has no TUIC fragments at this boundary.
    pub fn sweep(&mut self, now_secs: u64, ttl_secs: u64) {
        if let UdpDownlinkSourceInner::Generation(source) = &mut self.inner {
            source.sweep(now_secs, ttl_secs);
        }
        if let UdpDownlinkSourceInner::Native(source) = &mut self.inner {
            source.sweep(now_secs, ttl_secs);
        }
        if let UdpDownlinkSourceInner::LegacyRaw { reassembler, .. } = &mut self.inner {
            reassembler.sweep(now_secs, ttl_secs);
        }
    }
}

#[cfg(test)]
mod tests;
