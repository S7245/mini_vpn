use crate::shared::TargetAddr;
use crate::tuic::{FragReassembler, decode_packet_meta};
use bytes::Bytes;
use std::fmt;
use std::num::NonZeroU16;
use std::time::Instant;
use tokio::sync::mpsc;

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
        if let UdpDownlinkSourceInner::LegacyRaw { reassembler, .. } = &mut self.inner {
            reassembler.sweep(now_secs, ttl_secs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuic::encode_packet;

    fn raw_fragment(
        association: u16,
        packet: u16,
        total: u8,
        fragment: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut raw = vec![0x05, 0x02];
        raw.extend_from_slice(&association.to_be_bytes());
        raw.extend_from_slice(&packet.to_be_bytes());
        raw.push(total);
        raw.push(fragment);
        raw.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        if fragment == 0 {
            raw.push(0x01);
            raw.extend_from_slice(&[1, 2, 3, 4]);
            raw.extend_from_slice(&53u16.to_be_bytes());
        } else {
            raw.push(0xff);
        }
        raw.extend_from_slice(payload);
        raw
    }

    #[test]
    fn local_association_rejects_zero() {
        assert_eq!(LocalAssociationId::new(0), None);
        assert_eq!(LocalAssociationId::new(7).unwrap().get(), 7);
    }

    #[tokio::test]
    async fn legacy_raw_source_decodes_one_packet_and_preserves_channel_close() {
        let clock = Instant::now();
        let (sender, receiver) = mpsc::channel(1);
        let target = TargetAddr::IpPort("1.2.3.4:53".parse().unwrap());
        sender
            .send(encode_packet(7, &target, b"payload"))
            .await
            .unwrap();
        drop(sender);
        let mut source = UdpDownlinkSource::legacy_raw(receiver);

        assert_eq!(
            source.recv(&clock).await,
            Some(UdpDownlinkEvent::Deliver(UdpDownlink::new(
                LocalAssociationId::new(7).unwrap(),
                Bytes::from_static(b"payload"),
            )))
        );
        assert_eq!(source.recv(&clock).await, None);
    }

    #[tokio::test]
    async fn legacy_raw_source_reassembles_without_consuming_two_items_at_once() {
        let clock = Instant::now();
        let (sender, receiver) = mpsc::channel(2);
        sender.send(raw_fragment(9, 4, 2, 1, b"CD")).await.unwrap();
        sender.send(raw_fragment(9, 4, 2, 0, b"AB")).await.unwrap();
        let mut source = UdpDownlinkSource::legacy_raw(receiver);

        assert_eq!(source.recv(&clock).await, Some(UdpDownlinkEvent::Ignored));
        assert_eq!(
            source.recv(&clock).await,
            Some(UdpDownlinkEvent::Deliver(UdpDownlink::new(
                LocalAssociationId::new(9).unwrap(),
                Bytes::from_static(b"ABCD"),
            )))
        );
    }

    #[tokio::test]
    async fn legacy_fragment_sweep_discards_incomplete_packet() {
        let clock = Instant::now();
        let (sender, receiver) = mpsc::channel(2);
        sender.send(raw_fragment(9, 4, 2, 0, b"AB")).await.unwrap();
        sender.send(raw_fragment(9, 4, 2, 1, b"CD")).await.unwrap();
        let mut source = UdpDownlinkSource::legacy_raw(receiver);

        assert_eq!(source.recv(&clock).await, Some(UdpDownlinkEvent::Ignored));
        source.sweep(11, 10);
        assert_eq!(source.recv(&clock).await, Some(UdpDownlinkEvent::Ignored));
    }

    #[tokio::test]
    async fn owned_source_moves_neutral_downlink_without_decoding() {
        let clock = Instant::now();
        let (sender, receiver) = mpsc::channel(1);
        let downlink = UdpDownlink::new(
            LocalAssociationId::new(5).unwrap(),
            Bytes::from_static(b"owned"),
        );
        sender.send(downlink.clone()).await.unwrap();
        drop(sender);
        let mut source = UdpDownlinkSource::owned(receiver);

        assert_eq!(
            source.recv(&clock).await,
            Some(UdpDownlinkEvent::Deliver(downlink))
        );
        assert_eq!(source.recv(&clock).await, None);
    }

    #[test]
    fn udp_debug_redacts_target_and_payload_bytes() {
        let target = TargetAddr::DomainPort {
            host: "secret.example".into(),
            port: 443,
        };
        let payload = b"secret payload";
        let uplink = UdpUplink::new(LocalAssociationId::new(9).unwrap(), &target, payload);
        let downlink = UdpDownlink::new(
            LocalAssociationId::new(9).unwrap(),
            Bytes::from_static(b"secret payload"),
        );

        let uplink_debug = format!("{uplink:?}");
        assert!(uplink_debug.contains("payload_len: 14"));
        assert!(!uplink_debug.contains("secret.example"));
        assert!(!uplink_debug.contains("secret payload"));

        let downlink_debug = format!("{downlink:?}");
        assert!(downlink_debug.contains("payload_len: 14"));
        assert!(!downlink_debug.contains("secret payload"));
    }

    #[test]
    fn udp_uplink_is_a_borrowed_zero_copy_view() {
        let target = TargetAddr::IpPort("1.2.3.4:53".parse().unwrap());
        let payload = b"borrowed payload";

        let uplink = UdpUplink::new(LocalAssociationId::new(3).unwrap(), &target, payload);

        assert!(std::ptr::eq(uplink.target(), &target));
        assert_eq!(uplink.payload().as_ptr(), payload.as_ptr());
        assert_eq!(uplink.payload().len(), payload.len());
    }
}
