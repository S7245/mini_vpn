//! Existing device characterization tests; no production behavior is owned here.
//! The device test seam preserves packet, capacity, and ingress-pump assertions.
//! All moved tests remain in this private module.

use super::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, ReadBuf};

mod lifecycle;

fn frame_tx_test_packet(packet: Vec<u8>) -> Vec<u8> {
    #[cfg(target_os = "macos")]
    {
        let mut framed = Vec::with_capacity(UTUN_IPV4_HEADER.len() + packet.len());
        framed.extend_from_slice(&UTUN_IPV4_HEADER);
        framed.extend_from_slice(&packet);
        framed
    }
    #[cfg(not(target_os = "macos"))]
    {
        packet
    }
}

#[test]
fn inject_enqueues_with_platform_header() {
    let mut q: VecDeque<BytesMut> = VecDeque::new();
    let pkt = vec![69u8, 0, 0, 28]; // IPv4 header start (version=4, ihl=5)
    push_injected(&mut q, &pkt);
    let framed = q.pop_front().expect("packet enqueued");
    #[cfg(target_os = "macos")]
    {
        assert_eq!(&framed[..4], &[0, 0, 0, 2], "macOS PI header prepended");
        assert_eq!(&framed[4..], &pkt[..]);
    }
    #[cfg(not(target_os = "macos"))]
    assert_eq!(&framed[..], &pkt[..]);
}

#[test]
fn rx_buffer_capacity_tracks_configured_mtu() {
    #[cfg(target_os = "macos")]
    assert_eq!(rx_buffer_capacity_for_mtu(1200), 1204);
    #[cfg(not(target_os = "macos"))]
    assert_eq!(rx_buffer_capacity_for_mtu(1200), 1200);
}

#[test]
fn device_capabilities_tracks_configured_mtu() {
    let caps = device_capabilities_for_mtu(1200);
    assert_eq!(caps.medium, Medium::Ip);
    assert_eq!(caps.max_transmission_unit, 1200);
    assert!(matches!(caps.checksum.tcp, smoltcp::phy::Checksum::Tx));
    assert!(matches!(caps.checksum.ipv4, smoltcp::phy::Checksum::Tx));
    assert!(matches!(caps.checksum.icmpv4, smoltcp::phy::Checksum::Tx));
}

#[test]
fn summarize_flushed_tcp_packet_reports_payload_tuple() {
    let src = [10, 0, 0, 2];
    let dst = [203, 0, 113, 9];
    let payload = b"payload-bytes";
    let builder = etherparse::PacketBuilder::ipv4(src, dst, 64).tcp(443, 53124, 1, 1024);
    let mut packet = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut packet, payload).unwrap();
    let packet = frame_tx_test_packet(packet);

    let summary = summarize_flushed_tcp_packet(&packet).expect("tcp payload summary");

    assert_eq!(summary.src_addr, Ipv4Addr::from(src));
    assert_eq!(summary.dst_addr, Ipv4Addr::from(dst));
    assert_eq!(summary.src_port, 443);
    assert_eq!(summary.dst_port, 53124);
    assert_eq!(summary.payload_bytes, payload.len());
}

#[test]
fn summarize_flushed_tcp_packet_ignores_ack_only_packets() {
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 2], [203, 0, 113, 9], 64)
        .tcp(443, 53124, 1, 1024);
    let mut packet = Vec::with_capacity(builder.size(0));
    builder.write(&mut packet, &[]).unwrap();
    let packet = frame_tx_test_packet(packet);

    assert!(summarize_flushed_tcp_packet(&packet).is_none());
}

#[tokio::test]
async fn tun_ingress_pump_preserves_fifo_and_accounts_full_wait() {
    let (mut writer, reader) = tokio::io::duplex(4096);
    let mut pump = spawn_tun_ingress_pump(reader, 1200, 1);
    let control = Arc::clone(&pump.control);
    let stats = &control.stats;
    let first = vec![0x11; 16];
    let second = vec![0x22; 16];

    writer.write_all(&first).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.packets.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first packet should enter the bounded FIFO");

    writer.write_all(&second).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.full_waits.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second packet should wait behind the full one-packet FIFO");

    assert_eq!(pump.recv().await.unwrap().unwrap().as_ref(), first);
    assert_eq!(pump.recv().await.unwrap().unwrap().as_ref(), second);
    drop(writer);
    assert_eq!(pump.observe_join().await, PumpExit::Failed(PumpFailureId));
    let failure = pump.take_failure().expect("unrequested EOF is retained");
    assert_eq!(failure.facts().io_kind, Some(ErrorKind::UnexpectedEof));
    assert_eq!(control.additional_failures.load(Ordering::Relaxed), 0);

    assert_eq!(
        stats.snapshot(1),
        TunIngressPumpSnapshot {
            enabled: true,
            packets: 2,
            bytes: 32,
            queue_high_water: 1,
            capacity_packets: 1,
            full_waits: 1,
            read_errors: 0,
            closed: 1,
        }
    );
}

struct FailingPacketReader;

impl AsyncRead for FailingPacketReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "synthetic TUN read failure",
        )))
    }
}

#[tokio::test]
async fn tun_ingress_pump_surfaces_read_error_once_then_closes() {
    let mut pump = spawn_tun_ingress_pump(FailingPacketReader, 1200, 4);
    let control = Arc::clone(&pump.control);
    let stats = &control.stats;

    let error = pump
        .recv()
        .await
        .expect("one terminal error event")
        .expect_err("event should retain the read error");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(pump.observe_join().await, PumpExit::Failed(PumpFailureId));
    let failure = pump
        .take_failure()
        .expect("read error remains in its role slot");
    assert_eq!(failure.facts().io_kind, Some(ErrorKind::InvalidData));
    assert_eq!(control.additional_failures.load(Ordering::Relaxed), 0);
    assert!(pump.recv().await.is_none());

    let snapshot = stats.snapshot(4);
    assert_eq!(snapshot.read_errors, 1);
    assert_eq!(snapshot.closed, 1);
    assert_eq!(snapshot.packets, 0);
}

#[tokio::test]
async fn tun_ingress_pump_stops_when_consumer_closes() {
    let (writer, reader) = tokio::io::duplex(4096);
    let mut pump = spawn_tun_ingress_pump(reader, 1200, 4);
    let control = Arc::clone(&pump.control);
    let stats = &control.stats;
    pump.receiver.close();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pump.observe_join())
            .await
            .expect("pump should stop after its bounded FIFO consumer closes"),
        PumpExit::ConsumerClosed
    );
    drop(writer);

    let snapshot = stats.snapshot(4);
    assert_eq!(snapshot.closed, 1);
    assert_eq!(snapshot.packets, 0);
    assert_eq!(snapshot.read_errors, 0);
}
