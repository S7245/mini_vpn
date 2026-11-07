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
