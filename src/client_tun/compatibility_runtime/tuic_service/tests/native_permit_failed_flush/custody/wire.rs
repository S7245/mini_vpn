//! Challenges the finite wire oracle, never a substitute for the real Service cut.
use super::*;
fn packet(bytes: &[u8], sequence: u32) -> Vec<u8> {
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
        .tcp(1000, 2000, sequence, 65535);
    let mut packet = Vec::with_capacity(builder.size(bytes.len()));
    builder.write(&mut packet, bytes).unwrap();
    packet
}
fn verify(packets: &[Vec<u8>], cut_index: usize) -> usize {
    let cut = crate::device::summarize_flushed_ip_tcp_packet(&packet(b"bc", u32::MAX)).unwrap();
    check_wire(packets.iter().map(Vec::as_slice), cut, u32::MAX - 1, cut_index, b"abcd")
}
#[test]
fn partial_wire_oracle_accepts_exact_retransmission_and_unique_suffix() {
    let packets = [packet(b"ab", u32::MAX - 1), packet(b"bc", u32::MAX)];
    assert_eq!(verify(&packets, 1), 3); // Overlap + actual sequence wrap.
    let packets = [packet(b"abc", u32::MAX - 1), packet(b"bc", u32::MAX)];
    assert_eq!(verify(&packets, 1), 3); // Cut can be entirely retransmitted.
}
#[test]
fn partial_wire_oracle_rejects_gap_corruption_false_cut_and_postcut_duplicates() {
    for fault in 0..6 {
        let mut packets = vec![packet(b"ab", u32::MAX - 1), packet(b"bc", u32::MAX)];
        let mut cut_index = 1;
        match fault {
            0 => packets[0] = packet(b"a", u32::MAX), // Gap at origin.
            1 => packets[0] = packet(b"ax", u32::MAX - 1), // Bad original byte.
            2 => packets[1] = packet(b"xc", u32::MAX), // Same summary, corrupt overlap.
            3 => packets.push(packet(b"bc", u32::MAX)), // Identical DATA after cut.
            4 => cut_index = 0, // Cut summary at wrong actual position.
            _ => cut_index = 2, // Absent actual position.
        }
        assert!(std::panic::catch_unwind(|| verify(&packets, cut_index)).is_err(), "fault {fault}");
    }
}
