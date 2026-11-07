//! Whole-packet pressure through real encryption and original Endpoint routing.
use super::*;

struct SparsePacket {
    budget: bytes::BackingBudget,
    pair: Pair,
    client: ConnectionHandle,
    server: ConnectionHandle,
    id: StreamId,
    packet: Bytes,
    other: Option<StreamId>,
}

#[derive(Clone, Copy, Debug)]
enum PacketShape {
    Stream,
    StreamAndDatagram,
    StreamAndManyDatagrams,
    StreamAndImmediateAck,
    StreamAndMaxStreams,
    StreamAndFlowCredit,
    StreamAndStop,
    StreamAndReset,
    StreamAndStoppedReset,
    StreamAndNewCid,
    StreamAndChallenge,
    StreamAndAck,
    StreamAndRecovery,
    StreamAndRecoveryMany,
    AckOnly,
    TwoStreams,
    TwoExistingStreams,
    SameStreamFrames,
    OutgoingKeyAck,
    KeyUpdate,
    KeyUpdateAndDatagram,
}

fn sparse_packet_with_shape(shape: PacketShape) -> SparsePacket {
    sparse_packet_with_client(shape, client_config())
}

fn sparse_packet_with_client(shape: PacketShape, client_config: ClientConfig) -> SparsePacket {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut server_config = server_config();
    if matches!(shape, PacketShape::StreamAndFlowCredit) {
        let mut transport = TransportConfig::default();
        transport.receive_window(64u32.into()).stream_receive_window(64u32.into());
        server_config.transport_config(Arc::new(transport));
    } else if matches!(shape, PacketShape::SameStreamFrames) {
        // A random optimistic-ACK PN skip after `skip` would put it past the
        // 3-packet loss threshold together with `gap` (observed 75/2000).
        let mut transport = TransportConfig::default();
        transport.deterministic_packet_numbers(true);
        server_config.transport_config(Arc::new(transport));
    }
    let mut pair = Pair::new(Arc::new(config), server_config);
    let client = pair.begin_connect(client_config);
    assert!(!pair.drive_bounded());
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
    // Settle a real initial rotation so random short initial key lifetimes
    // cannot silently change the packet shape under test. Explicit key cases
    // below still exercise actual key-state effects.
    pair.client_conn_mut(client).force_key_update();
    pair.client_conn_mut(client).ping();
    assert!(!pair.drive_bounded());
    // drive_bounded stops at idle and deliberately omits KeyDiscard. Run the
    // original due timer, rather than altering key lifetimes or timeout policy.
    let retire = pair.client_conn_mut(client).packet_key_discard_for_probe().unwrap()
        .max(pair.server_conn_mut(server).packet_key_discard_for_probe().unwrap());
    pair.time = retire;
    pair.client_conn_mut(client).handle_timeout(retire);
    pair.server_conn_mut(server).handle_timeout(retire);
    assert!(!pair.drive_bounded());
    let id = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Bi)
        .unwrap();
    let request = if matches!(shape, PacketShape::StreamAndFlowCredit) { &[b'r'; 64][..] } else { b"request" };
    assert_eq!(pair.client_send(client, id).write(request).unwrap(), request.len());
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.server_conn_mut(server).streams().accept(Dir::Bi),
        Some(id)
    );
    pair.server_send(server, id).write(b"prefix").unwrap();
    assert!(!pair.drive_bounded());

    {
        let mut recv = pair.client_recv(client, id);
        let mut chunks = recv.read(false).unwrap();
        let prefix = chunks.next(6).unwrap().expect("contiguous prefix");
        assert_eq!(prefix.bytes.as_ref(), b"prefix");
        chunks.finalize();
    }
    assert!(!pair.drive_bounded());

    let other = if matches!(shape, PacketShape::TwoExistingStreams | PacketShape::StreamAndReset | PacketShape::StreamAndStoppedReset | PacketShape::StreamAndStoppedReset) {
        let other = pair.client_conn_mut(client).streams().open(Dir::Bi).unwrap();
        pair.client_send(client, other).write(b"second request").unwrap();
        assert!(!pair.drive_bounded());
        assert_eq!(pair.server_conn_mut(server).streams().accept(Dir::Bi), Some(other));
        pair.server_send(server, other).write(b"second prefix").unwrap();
        assert!(!pair.drive_bounded());
        {
            let mut recv = pair.client_recv(client, other);
            let mut chunks = recv.read(true).unwrap();
            assert_eq!(chunks.next(1024).unwrap().unwrap().bytes.as_ref(), b"second prefix");
            chunks.finalize();
        }
        assert!(!pair.drive_bounded());
        Some(other)
    } else { None };

    let lost_before_gap = pair.server_conn_mut(server).stats().path.lost_packets;
    // Lose the first transmit so the next authenticated STREAM needs sparse
    // receive stock in addition to its already admitted datagram backing.
    pair.server_send(server, id).write(b"gap").unwrap();
    pair.server.drive_outgoing(pair.time);
    assert_eq!(pair.server.outbound.drain(..).count(), 1);
    match shape {
        PacketShape::Stream => {}
        PacketShape::StreamAndChallenge => {
            pair.client.addr.set_port(pair.client.addr.port() + 1000);
            let now = pair.time;
            assert!(pair.client_conn_mut(client).try_local_address_changed(now));
            pair.drive_client();
            pair.server.drive_incoming(pair.time, pair.client.addr);
            pair.server.process_connection_events(server);
        }
        PacketShape::StreamAndStoppedReset => {
            pair.client_recv(client, other.unwrap()).stop(42u32.into()).unwrap();
            pair.server_send(server, other.unwrap()).reset(42u32.into()).unwrap();
        }
        PacketShape::StreamAndReset => { pair.server_send(server, other.unwrap()).reset(42u32.into()).unwrap(); }
        PacketShape::StreamAndStop => { pair.server_recv(server, id).stop(42u32.into()).unwrap(); }
        PacketShape::StreamAndNewCid => {
            let now = pair.time;
            pair.server_conn_mut(server).rotate_local_cid(1, now);
            let conn = pair.server.connections.get_mut(&server).unwrap();
            while let Some(event) = conn.poll_endpoint_events() {
                pair.server.endpoint.handle_connection_event(server, event, conn);
            }
        }

        PacketShape::StreamAndImmediateAck => pair.server_conn_mut(server).immediate_ack(),
        PacketShape::StreamAndFlowCredit => {
            let mut recv = pair.server_recv(server, id);
            let mut chunks = recv.read(true).unwrap();
            assert_eq!(chunks.next(1024).unwrap().unwrap().bytes.as_ref(), &[b'r'; 64]);
            chunks.finalize(); // Original application read queues both real credit frames.
        },
        PacketShape::StreamAndMaxStreams => {
            let count = pair.server_conn_mut(server).max_concurrent_streams(Dir::Bi);
            pair.server_conn_mut(server).set_max_concurrent_streams(Dir::Bi, VarInt::from_u64(count + count / 7 + 1).unwrap());
        },
        PacketShape::StreamAndDatagram | PacketShape::StreamAndManyDatagrams | PacketShape::KeyUpdateAndDatagram => {
            if matches!(shape, PacketShape::KeyUpdateAndDatagram) {
                let before = pair.server_conn_mut(server).key_phase_for_packet_probe();
                pair.server_conn_mut(server).force_key_update();
                assert_ne!(pair.server_conn_mut(server).key_phase_for_packet_probe(), before);
            }
            if matches!(shape, PacketShape::StreamAndManyDatagrams) {
                for _ in 0..12 { pair.server_datagrams(server).send(Bytes::new(), false).unwrap(); }
            }
            let meter = bytes::BackingMeter::with_budget(&budget);
            let data = BytesMut::try_copy_from_slice_metered(b"datagram", &meter)
                .unwrap()
                .freeze();
            pair.server_datagrams(server).send(data, false).unwrap();
        }
        PacketShape::OutgoingKeyAck
        | PacketShape::StreamAndAck
        | PacketShape::StreamAndRecovery
        | PacketShape::StreamAndRecoveryMany
        | PacketShape::AckOnly => {
            if matches!(shape, PacketShape::OutgoingKeyAck) {
                pair.client_conn_mut(client).force_key_update();
                assert!(pair.client_conn_mut(client).packet_local_key_update_pending_for_probe());
            }
            if matches!(
                shape,
                PacketShape::StreamAndRecovery
                    | PacketShape::StreamAndRecoveryMany
                    | PacketShape::AckOnly
            ) {
                pair.client_send(client, id).write(b"lost-uplink").unwrap();
                pair.client.drive_outgoing(pair.time);
                assert_eq!(pair.client.outbound.drain(..).count(), 1);
            }
            // Two real ack-eliciting packets cross the peer's normal delayed
            // ACK threshold without replacing its clock or injecting a frame.
            let count = match shape {
                PacketShape::StreamAndRecoveryMany => 20,
                PacketShape::StreamAndRecovery | PacketShape::AckOnly => 3,
                _ => 2,
            };
            for attempt in 0..count {
                if matches!(
                    shape,
                    PacketShape::StreamAndRecovery
                        | PacketShape::StreamAndRecoveryMany
                        | PacketShape::AckOnly
                ) {
                    pair.client_send(client, id)
                        .write(b"confirmed-uplink")
                        .unwrap();
                    if attempt + 1 == count {
                        pair.client_send(client, id).finish().unwrap();
                    }
                }
                pair.client_conn_mut(client).ping();
                pair.client.drive_outgoing(pair.time);
                assert!(!pair.client.outbound.is_empty());
                if matches!(shape, PacketShape::StreamAndRecoveryMany) && attempt % 2 == 1 {
                    pair.client.outbound.clear();
                    continue;
                }
                for (transmit, encrypted) in pair.client.outbound.drain(..) {
                    for data in encrypted.chunks(transmit.segment_size.unwrap_or(transmit.size)) {
                        pair.server
                            .inbound
                            .push_back((pair.time, None, BytesMut::from(data)));
                    }
                }
                pair.server.drive_incoming(pair.time, pair.client.addr);
                pair.server.process_connection_events(server);
            }
        }
        PacketShape::SameStreamFrames => {
            // First gap is lost by packet threshold, the immediately following
            // gap is still in flight. Retransmit + new data form two real frames
            // on this same StreamId, with a hole between them.
            pair.server_send(server, id).write(b"skip").unwrap();
            pair.server.drive_outgoing(pair.time);
            assert_eq!(pair.server.outbound.drain(..).count(), 1);
            for _ in 0..2 {
                pair.server_conn_mut(server).ping();
                pair.server.drive_outgoing(pair.time);
                for (_, encrypted) in pair.server.outbound.drain(..) {
                    pair.client.inbound.push_back((pair.time, None, BytesMut::from(encrypted.as_ref())));
                }
                pair.client.drive_incoming(pair.time, pair.server.addr);
                pair.client.process_connection_events(client);
            }
            pair.client.drive_outgoing(pair.time);
            assert!(!pair.client.outbound.is_empty());
            for (_, encrypted) in pair.client.outbound.drain(..) {
                pair.server.inbound.push_back((pair.time, None, BytesMut::from(encrypted.as_ref())));
            }
            pair.server.drive_incoming(pair.time, pair.client.addr);
            pair.server.process_connection_events(server);
            let lost_after = pair.server_conn_mut(server).stats().path.lost_packets;
            assert!(lost_after > lost_before_gap, "original loss detection must run");
        }
        PacketShape::TwoExistingStreams => {
            pair.server_send(server, other.unwrap()).write(b"second response").unwrap();
        }
        PacketShape::TwoStreams => {
            let other = pair
                .server_conn_mut(server)
                .streams()
                .open(Dir::Uni)
                .unwrap();
            pair.server_send(server, other)
                .write(b"other stream")
                .unwrap();
        }
        PacketShape::KeyUpdate => {
            let before = pair.server_conn_mut(server).key_phase_for_packet_probe();
            pair.server_conn_mut(server).force_key_update();
            assert_ne!(pair.server_conn_mut(server).key_phase_for_packet_probe(), before);
        },
    }
    if !matches!(shape, PacketShape::AckOnly) {
        pair.server_send(server, id).write(b"tail").unwrap();
    }
    pair.server.drive_outgoing(pair.time);
    let packet = if matches!(shape, PacketShape::StreamAndNewCid | PacketShape::StreamAndChallenge) {
        let mut selected = None;
        for (_, wire) in pair.server.outbound.drain(..).collect::<Vec<_>>() {
            let types = pair.client_conn_mut(client).packet_frame_types_for_probe(&wire);
            let visible: Vec<_> = types.iter().filter(|t| **t != frame::FrameType::PADDING).collect();
            eprintln!("[N1-control-mixed-wire] bytes={} {visible:?}", wire.len());
            let control = if matches!(shape, PacketShape::StreamAndNewCid) { frame::FrameType::NEW_CONNECTION_ID } else { frame::FrameType::PATH_CHALLENGE };
            if types.iter().any(|t| format!("{t}") == "STREAM") && types.contains(&control) {
                assert!(selected.is_none()); selected = Some(wire);
            }
        }
        selected.expect("real control+STREAM mixed packet")
    } else {
    let (_, packet) = pair
        .server
        .outbound
        .pop_front()
        .expect("encrypted sparse STREAM");
    assert!(pair.server.outbound.is_empty());
        packet
    };
    if matches!(shape, PacketShape::SameStreamFrames) {
        let streams: Vec<_> = pair.client_conn_mut(client).packet_frames_for_probe(&packet)
            .into_iter().filter_map(|f| match f { frame::Frame::Stream(s) => Some(s), _ => None }).collect();
        assert_eq!(streams.len(), 2, "actual retransmit and new-data frames");
        assert_eq!((streams[0].id, streams[0].offset, streams[0].data.as_ref()), (id, 6, b"gap".as_slice()));
        assert_eq!((streams[1].id, streams[1].offset, streams[1].data.as_ref()), (id, 13, b"tail".as_slice()));
    }
    SparsePacket {
        budget,
        pair,
        client,
        server,
        id,
        packet,
        other,
    }
}

#[test]
fn encrypted_sparse_stream_full_after_datagram_admission_preserves_connection() {
    recovery(true, PacketShape::Stream);
}

#[test]
fn encrypted_refused_tail_recovers_from_actual_peer_retransmission() {
    recovery(false, PacketShape::Stream);
}

#[test]
fn ack_and_stream_full_is_atomic_and_recovers_from_actual_peer() {
    recovery(false, PacketShape::StreamAndAck);
}

#[test]
fn ack_stream_send_ranges_and_loss_recover_together() {
    recovery(true, PacketShape::StreamAndRecovery);
}

#[test]
fn ack_stream_many_disjoint_ack_ranges_and_loss_recover() {
    recovery(true, PacketShape::StreamAndRecoveryMany);
}

#[test]
fn ack_only_full_preserves_send_state_and_peer_recovery() {
    recovery(false, PacketShape::AckOnly);
}

#[test]
fn multiple_existing_streams_refuse_atomically_and_recover() {
    recovery(false, PacketShape::TwoExistingStreams);
}

#[test]
fn same_stream_frames_later_full_recovers_from_actual_peer() {
    recovery(false, PacketShape::SameStreamFrames);
}

#[test]
fn outgoing_key_ack_full_preserves_keys_and_timer() {
    recovery(false, PacketShape::OutgoingKeyAck);
}

#[test]
fn immediate_ack_stream_full_refuses_without_partial_ack_and_recovers() {
    recovery(false, PacketShape::StreamAndImmediateAck);
}

fn recovery(replay_ciphertext: bool, shape: PacketShape) {
    let SparsePacket {
        budget,
        mut pair,
        client,
        server,
        id,
        packet,
        other,
    } = sparse_packet_with_shape(shape);
    let protocol_before = pair
        .client_conn_mut(client)
        .packet_admission_snapshot_for_probe();
    let refused_before = pair
        .client_conn_mut(client)
        .stats()
        .udp_rx
        .admission_refused;
    let other_before = other.map(|id| pair.client_conn_mut(client).recv_settlement_snapshot_for_probe(id));
    let before = pair
        .client_conn_mut(client)
        .recv_settlement_snapshot_for_probe(id);
    pair.client.capture_inbound_packets = matches!(
        shape,
        PacketShape::StreamAndRecoveryMany | PacketShape::AckOnly | PacketShape::TwoExistingStreams | PacketShape::SameStreamFrames | PacketShape::StreamAndImmediateAck | PacketShape::StreamAndMaxStreams | PacketShape::StreamAndFlowCredit | PacketShape::StreamAndReset | PacketShape::StreamAndStoppedReset | PacketShape::StreamAndStop | PacketShape::StreamAndNewCid | PacketShape::StreamAndChallenge
    );
    let used_before_admission = budget.used();
    pair.client
        .inbound
        .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
    pair.client.drive_incoming(pair.time, pair.server.addr);
    assert!(
        budget.used() > used_before_admission,
        "Endpoint must retain metered datagram"
    );
    if matches!(shape, PacketShape::StreamAndRecoveryMany) {
        let wire = pair
            .client
            .captured_packets
            .last()
            .expect("decrypted original wire");
        let ranges = frame::Iter::new(Bytes::copy_from_slice(wire))
            .unwrap()
            .map(Result::unwrap)
            .filter_map(|frame| match frame {
                frame::Frame::Ack(ack) => Some(ack.iter().count()),
                _ => None,
            })
            .sum::<usize>();
        assert!(
            ranges > 8,
            "must exercise charged packet workspace, actual ACK ranges={ranges}"
        );
    }
    if matches!(shape, PacketShape::TwoExistingStreams | PacketShape::SameStreamFrames) {
        let wire = pair.client.captured_packets.last().unwrap();
        let ids: Vec<_> = frame::Iter::new(Bytes::copy_from_slice(wire)).unwrap()
            .map(Result::unwrap).filter_map(|frame| match frame {
                frame::Frame::Stream(stream) => Some(stream.id), _ => None,
            }).collect();
        assert_eq!(ids.len(), 2, "actual encrypted multi STREAM packet");
        if matches!(shape, PacketShape::SameStreamFrames) { assert_eq!(ids[0], ids[1]); }
        else { assert_ne!(ids[0], ids[1]); }
    }
    if matches!(shape, PacketShape::StreamAndImmediateAck) {
        let wire = pair.client.captured_packets.last().unwrap();
        assert!(frame::Iter::new(Bytes::copy_from_slice(wire)).unwrap()
            .any(|frame| matches!(frame, Ok(frame::Frame::ImmediateAck))));
    }
    if matches!(shape, PacketShape::StreamAndMaxStreams | PacketShape::StreamAndFlowCredit) {
        controls::assert_wire(shape, pair.client.captured_packets.last().unwrap());
    }
    if matches!(shape, PacketShape::StreamAndReset | PacketShape::StreamAndStoppedReset | PacketShape::StreamAndStop | PacketShape::StreamAndNewCid | PacketShape::StreamAndChallenge) {
        remaining::assert_wire(shape, pair.client.captured_packets.last().unwrap());
    }
    if matches!(shape, PacketShape::AckOnly) {
        let wire = pair
            .client
            .captured_packets
            .last()
            .expect("actual pure ACK wire");
        let frames: Vec<_> = frame::Iter::new(Bytes::copy_from_slice(wire))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(frames.iter().any(|f| matches!(f, frame::Frame::Ack(_))));
        assert!(!frames.iter().any(|f| matches!(f, frame::Frame::Stream(_))));
    }
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert_eq!(budget.used(), budget.limit());
    assert!(pair.client.process_one_connection_event(client));

    let conn = pair.client_conn_mut(client);
    assert_eq!(conn.packet_admission_snapshot_for_probe(), protocol_before);
    assert_eq!(conn.stats().udp_rx.admission_refused, refused_before + 1);
    if let Some(other) = other {
        assert_eq!(Some(conn.recv_settlement_snapshot_for_probe(other)), other_before);
    }
    let after = conn.recv_settlement_snapshot_for_probe(id);
    let retained_error = conn.retained_error();
    assert!(!budget.quarantined());
    assert_eq!(
        (after, retained_error),
        (before, None),
        "known Full on an uncommitted packet"
    );
    drop(pressure);

    if replay_ciphertext {
        // Replay the saved original ciphertext, after pressure is physically freed.
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        pair.client.process_connection_events(client);
        assert!(pair.client_conn_mut(client).retained_error().is_none());
        let accepted = pair
            .client_conn_mut(client)
            .packet_admission_snapshot_for_probe();
        assert_ne!(accepted, protocol_before);
        let frames = pair.client_conn_mut(client).stats().frame_rx.stream;
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        pair.client.process_connection_events(client);
        assert_eq!(pair.client_conn_mut(client).stats().frame_rx.stream, frames);
    }

    let key_before_recovery = pair.client_conn_mut(client).key_phase_for_packet_probe();
    // Let the actual peer recover the lost gap through its loss/PTO machinery.
    pair.server_send(server, id).finish().unwrap();
    assert!(!pair.drive_bounded());
    if matches!(shape, PacketShape::KeyUpdate | PacketShape::KeyUpdateAndDatagram) && !replay_ciphertext {
        assert_ne!(pair.client_conn_mut(client).key_phase_for_packet_probe(), key_before_recovery);
    }
    let mut received = Vec::new();
    {
        let mut recv = pair.client_recv(client, id);
        let mut chunks = recv.read(false).unwrap();
        while let Some(chunk) = chunks.next(1024).unwrap() {
            received.push((chunk.offset, chunk.bytes.to_vec()));
        }
        let _ = chunks.finalize();
    }
    received.sort_by_key(|chunk| chunk.0);
    assert_eq!(
        received
            .iter()
            .flat_map(|chunk| chunk.1.iter().copied())
            .collect::<Vec<_>>(),
        if matches!(shape, PacketShape::AckOnly) {
            b"gap".as_slice()
        } else if matches!(shape, PacketShape::SameStreamFrames) {
            b"gapskiptail".as_slice()
        } else {
            b"gaptail".as_slice()
        }
    );
    assert!(pair.server_conn_mut(server).stats().path.lost_packets > 0);
    if matches!(
        shape,
        PacketShape::StreamAndRecovery | PacketShape::StreamAndRecoveryMany | PacketShape::AckOnly
    ) {
        assert!(pair.client_conn_mut(client).stats().path.lost_packets > 0);
        let mut recv = pair.server_recv(server, id);
        let mut chunks = recv.read(true).unwrap();
        let mut uplink = Vec::new();
        while let Some(chunk) = chunks.next(1024).unwrap() {
            uplink.extend_from_slice(&chunk.bytes);
        }
        let _ = chunks.finalize();
        let mut expected = b"requestlost-uplink".to_vec();
        let count = if matches!(shape, PacketShape::StreamAndRecoveryMany) {
            20
        } else {
            3
        };
        expected.extend_from_slice(&b"confirmed-uplink".repeat(count));
        assert_eq!(uplink, expected);
    }
    if let Some(other) = other.filter(|_| !matches!(shape, PacketShape::StreamAndReset | PacketShape::StreamAndStoppedReset)) {
        pair.server_send(server, other).finish().unwrap();
        assert!(!pair.drive_bounded());
        let mut recv = pair.client_recv(client, other);
        let mut chunks = recv.read(true).unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = chunks.next(1024).unwrap() { data.extend_from_slice(&chunk.bytes); }
        chunks.finalize();
        assert_eq!(data, b"second response");
    }
    if matches!(shape, PacketShape::StreamAndReset) {
        let mut recv = pair.client_recv(client, other.unwrap());
        let mut chunks = recv.read(true).unwrap();
        assert!(matches!(chunks.next(1024), Err(ReadError::Reset(code)) if code == 42u32.into()));
        chunks.finalize();
    }
    if matches!(shape, PacketShape::TwoStreams) {
        let remote = pair.client_conn_mut(client).streams().accept(Dir::Uni).unwrap();
        assert!(pair.client_conn_mut(client).streams().accept(Dir::Uni).is_none());
        pair.server_send(server, remote).finish().unwrap();
        assert!(!pair.drive_bounded());
        let mut recv = pair.client_recv(client, remote);
        let mut chunks = recv.read(true).unwrap();
        assert_eq!(chunks.next(1024).unwrap().unwrap().bytes.as_ref(), b"other stream");
        assert!(chunks.next(1024).unwrap().is_none());
        chunks.finalize();
    }
    if matches!(shape, PacketShape::StreamAndDatagram | PacketShape::StreamAndManyDatagrams | PacketShape::KeyUpdateAndDatagram) {
        if replay_ciphertext && matches!(shape, PacketShape::StreamAndManyDatagrams) {
            for _ in 0..12 { assert_eq!(pair.client_datagrams(client).recv().as_deref(), Some(b"".as_slice())); }
        }
        let received = pair.client_datagrams(client).recv();
        if replay_ciphertext {
            assert_eq!(received.as_deref(), Some(b"datagram".as_slice()));
        } else {
            assert!(received.is_none(), "a refused unreliable DATAGRAM is not retransmitted");
        }
        assert!(pair.client_datagrams(client).recv().is_none());
    }
    let second = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Bi)
        .unwrap();
    pair.client_send(client, second)
        .write(b"other request")
        .unwrap();
    assert!(!pair.drive_bounded());
    assert_eq!(
        pair.server_conn_mut(server).streams().accept(Dir::Bi),
        Some(second)
    );
    pair.server_send(server, second)
        .write(b"other response")
        .unwrap();
    pair.server_send(server, second).finish().unwrap();
    assert!(!pair.drive_bounded());
    {
        let mut recv = pair.client_recv(client, second);
        let mut chunks = recv.read(true).unwrap();
        assert_eq!(
            chunks.next(1024).unwrap().unwrap().bytes.as_ref(),
            b"other response"
        );
        assert!(chunks.next(1024).unwrap().is_none());
        let _ = chunks.finalize();
    }
    assert!(pair.client_conn_mut(client).retained_error().is_none());
    drop(packet);
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn repeated_authenticated_full_preserves_original_idle_deadline() {
    deadline(PacketShape::Stream);
}

#[test]
fn repeated_mixed_ack_full_preserves_original_idle_deadline() {
    deadline(PacketShape::StreamAndRecovery);
}

#[test]
fn repeated_same_stream_frames_full_keeps_original_deadline() {
    deadline(PacketShape::SameStreamFrames);
}
#[test]
fn repeated_two_streams_full_keeps_original_deadline() {
    deadline(PacketShape::TwoExistingStreams);
}
#[test]
fn repeated_outgoing_key_ack_full_keeps_original_deadline() {
    deadline(PacketShape::OutgoingKeyAck);
}

#[test]
fn repeated_immediate_ack_stream_full_keeps_original_deadline() {
    deadline(PacketShape::StreamAndImmediateAck);
}

fn deadline(shape: PacketShape) {
    let SparsePacket {
        budget,
        mut pair,
        client,
        server,
        id: _,
        packet,
        ..
    } = sparse_packet_with_shape(shape);
    let start = pair.time;
    let deadline = pair
        .client_conn_mut(client)
        .idle_deadline_for_packet_probe()
        .unwrap();
    let original = pair
        .client_conn_mut(client)
        .packet_admission_snapshot_for_probe();
    let refused = pair
        .client_conn_mut(client)
        .stats()
        .udp_rx
        .admission_refused;
    for attempt in 0..8 {
        pair.time = start + (deadline - start) * attempt / 8;
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(packet.as_ref())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(pair.client.process_one_connection_event(client));
        let conn = pair.client_conn_mut(client);
        assert_eq!(conn.packet_admission_snapshot_for_probe(), original);
        assert_eq!(
            conn.stats().udp_rx.admission_refused,
            refused + u64::from(attempt) + 1
        );
        assert_eq!(conn.idle_deadline_for_packet_probe(), Some(deadline));
        drop(pressure);
    }
    pair.time = deadline;
    pair.client_conn_mut(client).handle_timeout(deadline);
    assert_eq!(
        pair.client_conn_mut(client).retained_error(),
        Some(ConnectionError::TimedOut)
    );
    let server_deadline = pair
        .server_conn_mut(server)
        .idle_deadline_for_packet_probe()
        .unwrap();
    pair.time = deadline.max(server_deadline);
    let now = pair.time;
    pair.server_conn_mut(server).handle_timeout(now);
    assert!(!pair.drive_bounded());
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    drop(packet);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[path = "packet_pressure/datagrams.rs"]
mod datagrams;

#[path = "packet_pressure/keys.rs"]
mod keys;

#[path = "packet_pressure/materialization.rs"]
mod materialization;

#[path = "packet_pressure/controls.rs"]
mod controls;

#[path = "packet_pressure/coalesced.rs"]
mod coalesced;

#[path = "packet_pressure/crypto.rs"]
mod crypto;

#[path = "packet_pressure/remaining.rs"]
mod remaining;

mod stream_limit;
mod cid_loss;
