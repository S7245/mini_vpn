//! Existing ordered batch characterization outside the oversized test index.
use super::*;

#[test]
fn native_ordered_chunk_batch_combines_contiguous_bytes_and_advances_offset() {
    let mut next_offset = 41;
    let first = combine_ordered_native_chunks(
        &mut next_offset,
        &[
            bytes::Bytes::from_static(b"ab"),
            bytes::Bytes::from_static(b"cde"),
        ],
        None,
    )
    .expect("valid batch")
    .expect("nonempty chunk batch should combine");
    assert_eq!(first.offset, 41);
    assert_eq!(&first.bytes[..], b"abcde");
    assert_eq!(next_offset, 46);

    let second =
        combine_ordered_native_chunks(&mut next_offset, &[bytes::Bytes::from_static(b"fg")], None)
            .expect("valid batch")
            .expect("single chunk batch should pass through");
    assert_eq!(second.offset, 46);
    assert_eq!(&second.bytes[..], b"fg");
    assert_eq!(next_offset, 48);

    assert!(
        combine_ordered_native_chunks(&mut next_offset, &[], None)
            .unwrap()
            .is_none()
    );
    assert_eq!(next_offset, 48);
}
