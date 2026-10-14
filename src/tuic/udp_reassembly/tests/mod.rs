use crate::tuic::*;
use crate::udp_relay::MAX_IPV4_UDP_PAYLOAD_BYTES;

fn meta(assoc: u16, pkt: u16, ftot: u8, fid: u8, data: &[u8]) -> PacketMeta<'_> {
    PacketMeta {
        assoc_id: assoc,
        pkt_id: pkt,
        frag_total: ftot,
        frag_id: fid,
        data,
    }
}

#[test]
fn reassemble_single_fragment_passthrough() {
    let mut r = FragReassembler::new();
    // FRAG_TOTAL=1 立即返回整 payload，且不入表（无残留）。
    assert_eq!(
        r.accept(&meta(1, 0, 1, 0, b"hello"), 0),
        Some(b"hello".to_vec())
    );
    assert_eq!(r.pending_len(), 0);
}

#[test]
fn reassemble_two_fragments_in_order() {
    let mut r = FragReassembler::new();
    assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
    assert_eq!(
        r.accept(&meta(5, 9, 2, 1, b"CD"), 0),
        Some(b"ABCD".to_vec())
    );
    assert_eq!(r.pending_len(), 0); // 集齐后清出
}

#[test]
fn reassemble_two_fragments_out_of_order() {
    let mut r = FragReassembler::new();
    // 先到 frag_id=1，再到 0 → 仍按 frag_id 序拼接。
    assert_eq!(r.accept(&meta(5, 9, 2, 1, b"CD"), 0), None);
    assert_eq!(
        r.accept(&meta(5, 9, 2, 0, b"AB"), 0),
        Some(b"ABCD".to_vec())
    );
}

#[test]
fn reassemble_duplicate_fragment_last_writer_wins() {
    let mut r = FragReassembler::new();
    // 重复 frag_id last-writer-wins：第二个 frag_id=0 覆盖第一个（received 不重复计数），
    // 既保跨重连残片被新 frag 顶替，又不破坏完成判定。
    assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
    assert_eq!(r.accept(&meta(5, 9, 2, 0, b"XX"), 0), None); // 覆盖 slot[0]
    assert_eq!(
        r.accept(&meta(5, 9, 2, 1, b"CD"), 0),
        Some(b"XXCD".to_vec())
    ); // 用较新的
}

#[test]
fn reassemble_incomplete_swept_by_ttl() {
    let mut r = FragReassembler::new();
    assert_eq!(r.accept(&meta(5, 9, 3, 0, b"AB"), 0), None);
    assert_eq!(r.pending_len(), 1);
    r.sweep(11, 10); // first_seen=0，超 TTL=10 → 清
    assert_eq!(r.pending_len(), 0);
}

#[test]
fn reassemble_rejects_bad_frag_id() {
    let mut r = FragReassembler::new();
    // frag_id >= frag_total → 丢弃，不入表（防越界）。
    assert_eq!(r.accept(&meta(5, 9, 2, 2, b"X"), 0), None);
    assert_eq!(r.pending_len(), 0);
    // frag_total=0 也无效。
    assert_eq!(r.accept(&meta(5, 9, 0, 0, b"X"), 0), None);
    assert_eq!(r.pending_len(), 0);
}

#[test]
fn reassemble_cap_evicts_oldest() {
    let mut r = FragReassembler::with_cap(1);
    r.accept(&meta(1, 1, 2, 0, b"A"), 0); // partial #1，first_seen=0
    r.accept(&meta(2, 2, 2, 0, b"B"), 5); // 新 key 触发 evict 最老 → 仍 ≤cap
    assert_eq!(r.pending_len(), 1);
}

#[test]
fn reassemble_rejects_overlong_payload_before_retaining_or_returning_it() {
    let mut r = FragReassembler::with_cap(1);
    let too_large = vec![0x5a; MAX_IPV4_UDP_PAYLOAD_BYTES + 1];
    r.accept(&meta(8, 1, 2, 0, b"retained"), 0);
    assert!(
        r.accept(&meta(1, 1, 1, 0, &too_large), 0).is_none(),
        "single-fragment fast path must reject an invalid IPv4 UDP payload"
    );
    assert!(r.accept(&meta(1, 1, 2, 0, &too_large), 0).is_none());
    assert_eq!(
        r.accept(&meta(8, 1, 2, 1, b"tail"), 0),
        Some(b"retainedtail".to_vec()),
        "invalid new input must not evict an unrelated live packet"
    );

    let prefix = vec![0x6b; MAX_IPV4_UDP_PAYLOAD_BYTES];
    assert_eq!(
        r.accept(&meta(2, 2, 1, 0, &prefix), 0),
        Some(prefix.clone())
    );
    assert!(r.accept(&meta(3, 3, 2, 0, &prefix), 0).is_none());
    assert!(
        r.accept(&meta(3, 3, 2, 1, b"x"), 0).is_none(),
        "individually legal fragments must not form an invalid whole payload"
    );
    assert_eq!(
        r.pending_len(),
        0,
        "reject must release the affected packet"
    );
    assert!(
        r.accept(
            &meta(4, 4, 2, 0, &prefix[..MAX_IPV4_UDP_PAYLOAD_BYTES - 1]),
            0
        )
        .is_none()
    );
    let result = r.accept(&meta(4, 4, 2, 1, b"z"), 0).unwrap();
    assert_eq!(result.len(), MAX_IPV4_UDP_PAYLOAD_BYTES);
    assert_eq!(result.last(), Some(&b'z'));
    assert_eq!(r.pending_len(), 0);
}

fn assert_exact_retention(reassembler: &FragReassembler) -> usize {
    assert!(reassembler.partials.len() <= reassembler.cap);
    let mut retained = 0;
    for partial in reassembler.partials.values() {
        let actual = partial.frags.iter().flatten().map(Vec::len).sum::<usize>();
        assert_eq!(
            partial.payload_bytes, actual,
            "stored and physical bytes must agree"
        );
        assert_eq!(partial.received, partial.frags.iter().flatten().count());
        assert!(actual <= MAX_IPV4_UDP_PAYLOAD_BYTES);
        assert!(partial.frags.len() <= u8::MAX as usize);
        retained += actual;
    }
    assert!(retained <= reassembler.cap * MAX_IPV4_UDP_PAYLOAD_BYTES);
    retained
}

#[test]
fn reassemble_byte_ownership_survives_duplicate_resize_and_generation_change() {
    let mut r = FragReassembler::with_cap(2);
    r.accept(&meta(1, 1, 3, 0, b"oldlarge"), 0);
    assert_eq!(assert_exact_retention(&r), 8);
    r.accept(&meta(1, 1, 3, 0, b"a"), 0);
    assert_eq!(assert_exact_retention(&r), 1);
    r.accept(&meta(1, 1, 3, 1, b"bc"), 0);
    assert_eq!(assert_exact_retention(&r), 3);
    r.accept(&meta(1, 1, 3, 1, b"defg"), 0);
    assert_eq!(assert_exact_retention(&r), 5);
    assert_eq!(
        r.accept(&meta(1, 1, 3, 2, b"h"), 0),
        Some(b"adefgh".to_vec())
    );
    assert_eq!(assert_exact_retention(&r), 0);

    r.accept(&meta(2, 2, 4, 3, b"stale"), 0);
    assert_eq!(assert_exact_retention(&r), 5);
    r.accept(&meta(2, 2, 2, 1, b"right"), 1);
    assert_eq!(assert_exact_retention(&r), 5);
    assert_eq!(
        r.accept(&meta(2, 2, 2, 0, b"left"), 1),
        Some(b"leftright".to_vec())
    );
    assert_eq!(assert_exact_retention(&r), 0);

    let prefix = vec![0x6b; MAX_IPV4_UDP_PAYLOAD_BYTES - 1];
    r.accept(&meta(3, 3, 3, 0, &prefix), 2);
    r.accept(&meta(3, 3, 3, 1, b"x"), 2);
    r.accept(&meta(4, 4, 2, 0, b"live"), 2);
    assert_eq!(assert_exact_retention(&r), MAX_IPV4_UDP_PAYLOAD_BYTES + 4);
    assert!(r.accept(&meta(3, 3, 3, 1, b"xy"), 2).is_none());
    assert!(!r.partials.contains_key(&(3, 3)));
    assert_eq!(assert_exact_retention(&r), 4);
    assert_eq!(
        r.accept(&meta(4, 4, 2, 1, b"tail"), 2),
        Some(b"livetail".to_vec())
    );
    assert_eq!(assert_exact_retention(&r), 0);
}

#[test]
fn reassemble_lru_and_expiry_release_exact_retained_fragments() {
    let mut r = FragReassembler::with_cap(2);
    r.accept(&meta(1, 1, 2, 0, b"a"), 0);
    r.accept(&meta(2, 2, 2, 0, b"bb"), 1);
    assert_eq!(assert_exact_retention(&r), 3);
    r.accept(&meta(3, 3, 2, 0, b"ccc"), 2);
    assert!(!r.partials.contains_key(&(1, 1)));
    assert_eq!(assert_exact_retention(&r), 5);
    r.sweep(11, 10);
    assert_eq!(assert_exact_retention(&r), 5);
    r.sweep(12, 10);
    assert!(!r.partials.contains_key(&(2, 2)));
    assert_eq!(assert_exact_retention(&r), 3);
    r.sweep(13, 10);
    assert_eq!(r.pending_len(), 0);
    assert_eq!(assert_exact_retention(&r), 0);
}
