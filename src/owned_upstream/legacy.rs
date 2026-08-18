use super::{OpenedOwnedTcp, OwnedUpstream, UdpUplink};
use crate::shared::{ClientError, TargetAddr};
use crate::tuic::encode_packet;
use crate::upstream::{DatagramUpstream, ProxyUpstream};
use std::sync::Arc;

/// Exact compatibility adapter for the pre-Knife16 upstreams.
///
/// This wrapper owns no tasks and adds no lifecycle behavior. In particular,
/// TUIC UDP receive/reconnect ownership remains with the existing
/// `TuicUpstream::start_udp` task and its raw receiver.
pub struct LegacyOwnedUpstream<U> {
    inner: Arc<U>,
}

impl<U> LegacyOwnedUpstream<U> {
    pub fn new(inner: Arc<U>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<U> OwnedUpstream for LegacyOwnedUpstream<U>
where
    U: ProxyUpstream + DatagramUpstream + 'static,
{
    async fn open_tcp_flow(&self, target: &TargetAddr) -> Result<OpenedOwnedTcp, ClientError> {
        self.inner
            .open_tcp_relay(target)
            .await
            .map(OpenedOwnedTcp::Legacy)
    }

    async fn send_udp(&self, uplink: UdpUplink<'_>) {
        let (association, target, payload) = uplink.into_parts();
        self.inner
            .send_udp(encode_packet(association.get(), target, payload))
            .await;
    }

    fn open_is_cheap(&self) -> bool {
        self.inner.open_is_cheap()
    }

    fn failover_leg_u8(&self) -> u8 {
        self.inner.failover_leg_u8()
    }

    fn udp_drops_up(&self) -> u64 {
        self.inner.udp_drops_up()
    }

    fn udp_stream_fallbacks(&self) -> u64 {
        self.inner.udp_stream_fallbacks()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_upstream::{LocalAssociationId, OpenedOwnedTcp, OwnedUpstream};
    use crate::upstream::{
        NativeTcpChunk, NativeTcpReader, NativeTcpRelayStream, OpenedTcpRelay, RelayStream,
    };
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    struct PendingNativeReader;

    impl NativeTcpReader for PendingNativeReader {
        fn poll_read_chunk(
            &mut self,
            _cx: &mut Context<'_>,
            _max_len: usize,
        ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
            Poll::Pending
        }
    }

    #[derive(Default)]
    struct RecordingLegacyUpstream {
        open_tcp_calls: AtomicUsize,
        open_tcp_relay_calls: AtomicUsize,
        targets: Mutex<Vec<TargetAddr>>,
        datagrams: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl ProxyUpstream for RecordingLegacyUpstream {
        async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
            self.open_tcp_calls.fetch_add(1, Ordering::SeqCst);
            Err(ClientError::Io(std::io::Error::other(
                "legacy adapter must preserve open_tcp_relay",
            )))
        }

        async fn open_tcp_relay(&self, target: &TargetAddr) -> Result<OpenedTcpRelay, ClientError> {
            self.open_tcp_relay_calls.fetch_add(1, Ordering::SeqCst);
            self.targets.lock().unwrap().push(target.clone());
            Ok(OpenedTcpRelay::NativeByteOwned(NativeTcpRelayStream {
                reader: Box::new(PendingNativeReader),
                writer: Box::new(tokio::io::sink()),
            }))
        }

        fn open_is_cheap(&self) -> bool {
            false
        }

        fn failover_leg_u8(&self) -> u8 {
            7
        }
    }

    #[async_trait::async_trait]
    impl DatagramUpstream for RecordingLegacyUpstream {
        async fn send_udp(&self, datagram: Vec<u8>) {
            self.datagrams.lock().unwrap().push(datagram);
        }

        fn udp_drops_up(&self) -> u64 {
            11
        }

        fn udp_stream_fallbacks(&self) -> u64 {
            13
        }
    }

    #[tokio::test]
    async fn tcp_open_preserves_native_byte_owned_variant_without_generic_open() {
        let target = TargetAddr::IpPort("1.2.3.4:443".parse().unwrap());
        let inner = Arc::new(RecordingLegacyUpstream::default());
        let adapter: Arc<dyn OwnedUpstream> =
            Arc::new(LegacyOwnedUpstream::new(Arc::clone(&inner)));

        let opened = adapter.open_tcp_flow(&target).await.unwrap();

        assert!(matches!(
            opened,
            OpenedOwnedTcp::Legacy(OpenedTcpRelay::NativeByteOwned(_))
        ));
        assert_eq!(inner.open_tcp_calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner.open_tcp_relay_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*inner.targets.lock().unwrap(), vec![target]);
    }

    #[tokio::test]
    async fn udp_uplink_encodes_once_and_forwards_all_legacy_facts() {
        let target = TargetAddr::DomainPort {
            host: "example.com".into(),
            port: 443,
        };
        let association = LocalAssociationId::new(9).unwrap();
        let payload = b"payload";
        let expected = encode_packet(association.get(), &target, payload);
        let inner = Arc::new(RecordingLegacyUpstream::default());
        let adapter = LegacyOwnedUpstream::new(Arc::clone(&inner));

        adapter
            .send_udp(UdpUplink::new(association, &target, payload))
            .await;

        assert_eq!(*inner.datagrams.lock().unwrap(), vec![expected]);
        assert!(!adapter.open_is_cheap());
        assert_eq!(adapter.failover_leg_u8(), 7);
        assert_eq!(adapter.udp_drops_up(), 11);
        assert_eq!(adapter.udp_stream_fallbacks(), 13);
    }
}
