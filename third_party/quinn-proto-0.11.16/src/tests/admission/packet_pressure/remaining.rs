//! Real remaining control frames sharing a packet with sparse STREAM data.
use super::*;
pub(super) fn assert_wire(shape: PacketShape, wire: &[u8]) {
    let frames: Vec<_> = frame::Iter::new(Bytes::copy_from_slice(wire))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(frames.iter().any(|f| matches!(f, frame::Frame::Stream(_))));
    assert!(
        frames.iter().any(|f| match shape {
            PacketShape::StreamAndReset | PacketShape::StreamAndStoppedReset => matches!(f, frame::Frame::ResetStream(_)),
            PacketShape::StreamAndStop => matches!(f, frame::Frame::StopSending(_)),
            PacketShape::StreamAndNewCid => matches!(f, frame::Frame::NewConnectionId(_)),
            PacketShape::StreamAndChallenge => matches!(f, frame::Frame::PathChallenge(_)),
            _ => false,
        }),
        "actual wire: {:?}",
        frames.iter().map(|f| f.ty()).collect::<Vec<_>>()
    );
}
#[test]
fn stop_and_stream_full_preserves_packet_then_true_peer_recovers() {
    recovery(false, PacketShape::StreamAndStop);
}
#[test]
fn new_cid_and_stream_full_preserves_packet_then_true_peer_recovers() {
    recovery(false, PacketShape::StreamAndNewCid);
}

#[test]
fn actual_path_challenge_and_stream_full_is_atomic_then_peer_recovers() {
    recovery(false, PacketShape::StreamAndChallenge);
}

#[test]
fn remaining_controls_replay_once_and_repeated_full_keep_original_deadline() {
    for shape in [
        PacketShape::StreamAndStop,
        PacketShape::StreamAndNewCid,
        PacketShape::StreamAndChallenge,
    ] {
        recovery(true, shape);
        deadline(shape);
    }
}
#[test]
fn pure_ping_and_close_use_funded_packet_inventory_at_global_full() {
    for close in [false, true] {
        let SparsePacket {
            budget,
            mut pair,
            client,
            server,
            packet,
            ..
        } = sparse_packet_with_shape(PacketShape::Stream);
        drop(packet); // genuine STREAM loss, no injected frame
        let now = pair.time;
        if close {
            pair.server_conn_mut(server)
                .close(now, 42u32.into(), Bytes::new());
        } else {
            pair.server_conn_mut(server).ping();
        }
        pair.server.drive_outgoing(now);
        let expected_deadline = pair
            .client_conn_mut(client)
            .original_close_deadline_for_stream_probe(now);
        let before = pair.client_conn_mut(client).authenticated_packets();
        for (_, wire) in pair.server.outbound.drain(..) {
            pair.client
                .inbound
                .push_back((now, None, BytesMut::from(wire.as_ref())));
        }
        pair.client.drive_incoming(now, pair.server.addr);
        let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        pair.client.process_connection_events(client);
        assert!(pair.client_conn_mut(client).authenticated_packets() > before);
        if close {
            let conn = pair.client_conn_mut(client);
            assert!(conn.retained_error().is_some());
            assert_eq!(conn.poll_timeout(), Some(expected_deadline));
        } else {
            assert!(pair.client_conn_mut(client).retained_error().is_none());
        }
        drop(held);
        control_records::stop(&mut pair, &[client]);
        drop(pair);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn reset_and_stream_full_preserves_packet_then_true_peer_recovers() {
    recovery(false, PacketShape::StreamAndReset);
}

#[test]
fn reset_stream_replay_once_and_repeated_full_keep_original_deadline() {
    recovery(true, PacketShape::StreamAndReset);
    deadline(PacketShape::StreamAndReset);
}

#[test]
fn stopped_reset_real_peer_replay_full_and_original_deadline() {
    recovery(false, PacketShape::StreamAndStoppedReset);
    recovery(true, PacketShape::StreamAndStoppedReset);
    deadline(PacketShape::StreamAndStoppedReset);
}
