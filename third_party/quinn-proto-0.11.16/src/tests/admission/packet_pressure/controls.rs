use super::*;

pub(super) fn assert_wire(shape: PacketShape, wire: &[u8]) {
    let frames: Vec<_> = frame::Iter::new(Bytes::copy_from_slice(wire)).unwrap()
        .map(Result::unwrap).collect();
    assert!(frames.iter().any(|f| matches!(f, frame::Frame::Stream(_))));
    if matches!(shape, PacketShape::StreamAndMaxStreams) {
        assert!(frames.iter().any(|f| matches!(f, frame::Frame::MaxStreams { .. })));
    } else {
        assert!(frames.iter().any(|f| matches!(f, frame::Frame::MaxData(_))));
        assert!(frames.iter().any(|f| matches!(f, frame::Frame::MaxStreamData { .. })));
    }
}

#[test]
fn max_streams_and_stream_full_refuses_before_publishing_control_then_peer_recovers() {
    recovery(false, PacketShape::StreamAndMaxStreams);
}
#[test]
fn real_read_credit_and_stream_full_preserves_credit_then_peer_recovers() {
    recovery(false, PacketShape::StreamAndFlowCredit);
}
#[test]
fn real_read_credit_and_stream_replay_commits_once() {
    recovery(true, PacketShape::StreamAndFlowCredit);
}
#[test]
fn repeated_control_and_stream_full_preserves_original_deadline() {
    deadline(PacketShape::StreamAndFlowCredit);
    deadline(PacketShape::StreamAndMaxStreams);
}
