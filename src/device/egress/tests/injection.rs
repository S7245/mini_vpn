use super::*;

#[tokio::test]
async fn native_summary_uses_admitted_packet_framing_not_host_platform() {
    for prefix in [0, 4] {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut owner = Egress::native(1, 1200, prefix, &meter).unwrap();
        let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
            .tcp(1000, 2000, 7, 1024);
        let mut bytes = Vec::with_capacity(builder.size(5));
        builder.write(&mut bytes, b"proof").unwrap();
        owner
            .token(1200, prefix)
            .unwrap()
            .consume(bytes.len(), |out| out.copy_from_slice(&bytes));
        let (mut writer, mut peer) = tokio::io::duplex(4096);
        owner.flush(&mut writer).await.unwrap();
        let mut actual = vec![0; bytes.len() + prefix];
        peer.read_exact(&mut actual).await.unwrap();
        assert_eq!(&actual[prefix..], bytes);
        let mut batch = owner.summaries();
        let summary = batch.next();
        batch.commit();
        drop(owner);
        assert_eq!(budget.used(), 0);
        let summary = summary.expect("actual admitted frame must produce its TCP payload summary");
        assert_eq!((summary.payload_start_seq, summary.payload_bytes), (7, 5));
    }
}

// Egress interface shell only: intentionally no OS backend or Native RX claim.
fn shell(owner: Egress) -> VirtualTunDevice {
    VirtualTunDevice {
        backend: VirtualTunBackend::Stopped,
        cut: cut::CutBoundary::default(),
        stopping: false,
        shutdown_failure: None,
        shutdown_failure_transferred: false,
        shutdown_callbacks: None,
        interface_name: None,
        mtu: 1200,
        rx: crate::device::rx::RxStock::default(),
        egress: owner,
    }
}
#[test]
fn full_injection_prevents_encoder_and_rx_selection() {
    let (budget, _, owner) = native(1);
    let mut device = shell(owner);
    device
        .inject_ip_packet_with(60, |out| {
            out.fill(1);
            Ok(60)
        })
        .unwrap();
    let charged = budget.used();
    let mut called = false;
    assert_eq!(
        device
            .inject_ip_packet_with(60, |_| {
                called = true;
                Ok(60)
            })
            .unwrap_err()
            .kind(),
        ErrorKind::WouldBlock
    );
    assert!(!called);
    assert_eq!(budget.used(), charged);
    device
        .rx
        .test_push_staged(BytesMut::from(&b"original rx"[..]));
    assert!(
        device
            .receive(smoltcp::time::Instant::from_millis(0))
            .is_none()
    );
    assert_eq!(device.rx.test_staged_front(), b"original rx");
    device.egress.discard(&mut (0, 0));
    drop(device);
    assert_eq!(budget.used(), 0);
}
#[test]
fn actual_udp_typed_entry_preserves_maximum_ipv4_packet_and_wire_checksums() {
    let (budget, _, owner) = native(1);
    let mut device = shell(owner);
    let src = (Ipv4Addr::new(10, 0, 0, 1), 1000);
    let dst = (Ipv4Addr::new(10, 0, 0, 2), 2000);
    let payload = vec![0xa3; crate::udp_relay::MAX_IPV4_UDP_PAYLOAD_BYTES];
    crate::udp_relay::inject_udp_ip_packet(&mut device, src, dst, &payload).unwrap();
    let framed = device.egress.queue.front().unwrap();
    let packet = &framed[tun_packet_header_len()..];
    assert_eq!(packet.len(), 65535);
    assert_eq!(
        packet,
        crate::udp_relay::build_udp_ip_packet(src, dst, &payload).unwrap()
    );
    assert!(budget.used() > 65535);
    let parsed = crate::udp_relay::parse_inbound_udp(packet).unwrap();
    assert_eq!(parsed.payload, payload);
    device.egress.discard(&mut (0, 0));
    drop(device);
    assert_eq!(budget.used(), 0);
}
#[test]
fn injection_cancel_error_and_panic_have_distinct_settlement() {
    let (budget, _, mut owner) = native(1);
    let cold = budget.used();
    let error = owner
        .inject(100, tun_packet_header_len(), |_| {
            Err(std::io::Error::from(ErrorKind::InvalidData))
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(budget.used(), cold);
    assert!(owner.prepared.is_none());
    assert!(!budget.quarantined());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.inject(100, tun_packet_header_len(), |_| {
            panic!("original injection panic")
        })
    }))
    .unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"original injection panic")
    );
    assert!(budget.quarantined());
    assert!(owner.prepared.is_some());
    assert!(budget.used() > cold);
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
}
#[test]
fn raw_native_injection_is_closed_and_cannot_grow_packet_stock() {
    let (budget, _, owner) = native(1);
    let mut device = shell(owner);
    device.inject_ip_packet(b"must use typed preparation");
    assert!(device.egress.queue.is_empty());
    assert!(budget.quarantined());
    drop(device);
    assert_eq!(budget.used(), 1);
}
#[test]
fn external_q_during_encoding_cannot_publish_or_drop_result_during_second_unwind() {
    struct Returned;
    impl Drop for Returned {
        fn drop(&mut self) {
            panic!("original returned value Drop");
        }
    }
    let (budget, meter, mut owner) = native(1);
    let token = owner.token(1200, tun_packet_header_len()).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        token.consume(6, |_| {
            meter.quarantine();
            Returned
        })
    }))
    .err()
    .unwrap();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"original returned value Drop")
    );
    assert!(owner.queue.is_empty());
    assert!(owner.prepared.is_some());
    assert!(budget.quarantined());
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
}
#[test]
fn encoder_io_before_post_q_keeps_original_error_and_prepared_packet() {
    let (budget, meter, mut owner) = native(1);
    let error = owner
        .inject(60, tun_packet_header_len(), |_| {
            meter.quarantine();
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "original encoder IO before Q observation",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "original encoder IO before Q observation"
    );
    assert!(owner.prepared.is_some());
    assert!(owner.queue.is_empty());
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
}

struct QuarantiningWriter {
    meter: BackingMeter,
    calls: usize,
}
impl tokio::io::AsyncWrite for QuarantiningWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
        packet: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.calls += 1;
        self.meter.quarantine();
        std::task::Poll::Ready(Ok(packet.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn q_during_first_write_preserves_its_fact_but_stops_before_second_write() {
    let (budget, meter, mut owner) = native(2);
    fill(&mut owner, &tcp_packet());
    fill(&mut owner, &tcp_packet());
    let mut writer = QuarantiningWriter { meter, calls: 0 };
    assert!(owner.flush(&mut writer).await.is_err());
    assert_eq!(writer.calls, 1);
    assert_eq!(owner.summaries.len(), 1);
    assert_eq!(owner.queue.len(), 1);
    assert!(owner.selected.is_none());
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
}

impl Egress {
    // Local real AsyncWrite peer for proof tracers in Main. No summary is forged.
    pub(crate) async fn test_write_to_peer(&mut self) -> Vec<u8> {
        let (mut writer, mut peer) = tokio::io::duplex(131072);
        let bytes = self.owned_bytes();
        self.flush(&mut writer).await.unwrap();
        let mut result = vec![0; bytes];
        peer.read_exact(&mut result).await.unwrap();
        result
    }
}
impl VirtualTunDevice {
    pub(crate) fn test_native_egress_only_shell() -> (bytes::BackingBudget, BackingMeter, Self) {
        let (budget, meter, owner) = native(1);
        (budget, meter, shell(owner))
    }
}
impl Egress {
    pub(crate) async fn test_write_to_closed_peer(&mut self) -> std::io::Error {
        let (mut writer, peer) = tokio::io::duplex(4096);
        drop(peer);
        self.flush(&mut writer).await.unwrap_err()
    }
}
