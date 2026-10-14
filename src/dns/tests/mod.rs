//! Query parsing and response wire-contract tests; transport routing belongs to client_tun.

use super::*;

#[test]
fn servfail_preserves_question_and_rd_without_answer() {
    for rd in [false, true] {
        let mut query = parse_query(&build_query(0x1234, "full.com", QTYPE_A)).unwrap();
        query.rd = rd;
        let response = build_response(&query, Answer::ServFail);
        assert_eq!(u16::from_be_bytes([response[0], response[1]]), query.id);
        assert_eq!(
            u16::from_be_bytes([response[2], response[3]]),
            0x8002 | if rd { 0x0100 } else { 0 }
        );
        assert_eq!(&response[4..12], &[0, 1, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&response[12..], query.question);
    }
}

/// 构造一个标准查询字节串（RD=1, QCLASS=IN）。
fn build_query(id: u16, qname: &str, qtype: u16) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&id.to_be_bytes());
    v.extend_from_slice(&0x0100u16.to_be_bytes()); // RD=1
    v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    for label in qname.split('.') {
        v.push(label.len() as u8);
        v.extend_from_slice(label.as_bytes());
    }
    v.push(0);
    v.extend_from_slice(&qtype.to_be_bytes());
    v.extend_from_slice(&1u16.to_be_bytes()); // QCLASS IN
    v
}

#[test]
fn parse_a_query() {
    let q = parse_query(&build_query(0x1234, "test.com", QTYPE_A)).expect("should parse");
    assert_eq!(q.id, 0x1234);
    assert_eq!(q.qname, "test.com");
    assert_eq!(q.qtype, QTYPE_A);
    assert!(q.rd);
}

#[test]
fn parse_rejects_truncated() {
    assert!(parse_query(&[0u8; 4]).is_none());
    // qname 标称长度超出缓冲 → None，不 panic。
    let mut q = build_query(1, "test.com", QTYPE_A);
    q.truncate(15);
    assert!(parse_query(&q).is_none());
}

#[test]
fn parse_rejects_multi_question() {
    let mut q = build_query(1, "test.com", QTYPE_A);
    q[4] = 0;
    q[5] = 2; // QDCOUNT = 2
    assert!(parse_query(&q).is_none());
}

#[test]
fn build_a_response_fields() {
    // question("test.com") = 10B qname + 2 qtype + 2 qclass = 14B；header 12 → answer @26。
    let q = parse_query(&build_query(0x1234, "test.com", QTYPE_A)).unwrap();
    let r = build_response(&q, Answer::A(Ipv4Addr::new(198, 18, 0, 2), 5));
    assert_eq!(u16::from_be_bytes([r[0], r[1]]), 0x1234); // id
    assert!(r[2] & 0x80 != 0); // QR=1
    assert_eq!(r[3] & 0x0F, 0); // RCODE=0
    assert_eq!(u16::from_be_bytes([r[6], r[7]]), 1); // ANCOUNT=1
    assert_eq!(&r[24..26], &[0u8, 1u8]); // echoed QCLASS=IN at end of question
    assert_eq!(&r[26..28], &[0xC0, 0x0C]); // answer NAME pointer
    assert_eq!(u32::from_be_bytes([r[32], r[33], r[34], r[35]]), 5); // TTL
    assert_eq!(&r[38..42], &[198, 18, 0, 2]); // RDATA = fake-IP
}

#[test]
fn build_nodata_response_fields() {
    let q = parse_query(&build_query(0x1234, "test.com", QTYPE_AAAA)).unwrap();
    let r = build_response(&q, Answer::NoData);
    assert!(r[2] & 0x80 != 0); // QR=1
    assert_eq!(r[3] & 0x0F, 0); // RCODE=0 (success)
    assert_eq!(u16::from_be_bytes([r[6], r[7]]), 0); // ANCOUNT=0
}
