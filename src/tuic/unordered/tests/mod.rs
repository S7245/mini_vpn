use super::*;

#[test]
fn ordered_quic_chunk_assembler_buffers_gap_then_drains_contiguously() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut out = [0u8; 6];
    let mut read_buf = ReadBuf::new(&mut out);

    assembler.push_chunk(3, bytes::Bytes::from_static(b"def"));
    assert_eq!(assembler.drain_into(&mut read_buf), 0);
    assert_eq!(read_buf.filled(), b"");

    assembler.push_chunk(0, bytes::Bytes::from_static(b"abc"));
    assert_eq!(assembler.drain_into(&mut read_buf), 6);
    assert_eq!(read_buf.filled(), b"abcdef");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn ordered_quic_chunk_assembler_trims_already_delivered_prefix() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut first_out = [0u8; 3];
    let mut first = ReadBuf::new(&mut first_out);
    assembler.push_chunk(0, bytes::Bytes::from_static(b"abc"));
    assert_eq!(assembler.drain_into(&mut first), 3);

    let mut second_out = [0u8; 3];
    let mut second = ReadBuf::new(&mut second_out);
    assembler.push_chunk(1, bytes::Bytes::from_static(b"bcdef"));
    assert_eq!(assembler.drain_into(&mut second), 3);
    assert_eq!(second.filled(), b"def");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn ordered_quic_chunk_assembler_keeps_longer_duplicate_at_same_offset() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut out = [0u8; 6];
    let mut read_buf = ReadBuf::new(&mut out);

    assembler.push_chunk(0, bytes::Bytes::from_static(b"abcdef"));
    assembler.push_chunk(0, bytes::Bytes::from_static(b"ab"));

    assert_eq!(assembler.drain_into(&mut read_buf), 6);
    assert_eq!(read_buf.filled(), b"abcdef");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn ordered_quic_chunk_assembler_replaces_short_duplicate_with_longer() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut out = [0u8; 6];
    let mut read_buf = ReadBuf::new(&mut out);

    assembler.push_chunk(0, bytes::Bytes::from_static(b"ab"));
    assembler.push_chunk(0, bytes::Bytes::from_static(b"abcdef"));

    assert_eq!(assembler.drain_into(&mut read_buf), 6);
    assert_eq!(read_buf.filled(), b"abcdef");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Inventory {
    pub(crate) stream: u64,
    pub(crate) next: u64,
    pub(crate) chunks: usize,
    pub(crate) stored: usize,
    pub(crate) independently_summed: usize,
    pub(crate) front: Option<(u64, usize, usize)>,
}
impl TuicChunkRelayStream {
    pub(in crate::tuic) fn inventory_for_test(&self) -> Inventory {
        Inventory {
            stream: self.recv.id().index(),
            next: self.rx.next_offset,
            chunks: self.rx.chunks.len(),
            stored: self.rx.buffered_bytes,
            independently_summed: self.rx.chunks.values().map(bytes::Bytes::len).sum(),
            front: self
                .rx
                .chunks
                .first_key_value()
                .map(|(offset, bytes)| (*offset, bytes.as_ptr() as usize, bytes.len())),
        }
    }
}

mod prefix;
