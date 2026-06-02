use std::net::Ipv4Addr;

/// DNS QTYPE 常量（只用到 A / AAAA）。
pub const QTYPE_A: u16 = 1;
pub const QTYPE_AAAA: u16 = 28;

/// 解析出的最小 DNS 查询（单 question、无压缩指针）。
/// 中文要点：只保留 fake-IP 需要的字段；`question` 是 question 段原始字节，
/// 用于在响应里原样回显。
#[derive(Debug, Clone)]
pub struct DnsQuery {
    pub id: u16,
    pub qname: String,
    pub qtype: u16,
    /// 是否置了 RD（recursion desired），响应里原样回带。
    pub rd: bool,
    /// question 段原始字节（QNAME + QTYPE + QCLASS）。
    pub question: Vec<u8>,
}

/// 要写进响应的答案。
/// 中文要点：A → 一条 A 记录(fake-IP + ttl)；NoData → 成功但 0 答案（用于 AAAA/其它）。
pub enum Answer {
    A(Ipv4Addr, u32),
    NoData,
}

/// 解析一个标准 DNS 查询。仅接受单 question、question 段无压缩指针的查询；
/// 任何越界 / 多 question / 压缩指针 / 非法 UTF-8 一律返回 None（绝不 panic）。
pub fn parse_query(data: &[u8]) -> Option<DnsQuery> {
    if data.len() < 12 {
        return None;
    }
    let id = u16::from_be_bytes([data[0], data[1]]);
    let flags = u16::from_be_bytes([data[2], data[3]]);
    let rd = flags & 0x0100 != 0;
    let qdcount = u16::from_be_bytes([data[4], data[5]]);
    if qdcount != 1 {
        return None;
    }

    // 从 offset 12 读 QNAME（label 序列，0x00 结尾）。
    let mut pos = 12usize;
    let mut labels: Vec<String> = Vec::new();
    loop {
        let len = *data.get(pos)? as usize;
        if len == 0 {
            pos += 1;
            break;
        }
        if len & 0xC0 != 0 {
            // 压缩指针 / 保留位：question 段不应出现，拒绝。
            return None;
        }
        pos += 1;
        let label = data.get(pos..pos + len)?;
        labels.push(std::str::from_utf8(label).ok()?.to_string());
        pos += len;
    }
    let qtype = u16::from_be_bytes([*data.get(pos)?, *data.get(pos + 1)?]);
    // QCLASS 占 pos+2..pos+4；question 段到此结束。
    let question = data.get(12..pos + 4)?.to_vec();

    Some(DnsQuery {
        id,
        qname: labels.join("."),
        qtype,
        rd,
        question,
    })
}

/// 构造对查询的响应。answer=A 时带一条 A 记录，否则 NODATA（rcode=0、0 答案）。
pub fn build_response(q: &DnsQuery, answer: Answer) -> Vec<u8> {
    let mut v = Vec::with_capacity(q.question.len() + 28);
    v.extend_from_slice(&q.id.to_be_bytes());
    // flags: QR=1, Opcode=0, AA=0, TC=0, RD=回带, RA=0, RCODE=0。
    let rd_bit = if q.rd { 0x0100 } else { 0 };
    let flags: u16 = 0x8000 | rd_bit;
    v.extend_from_slice(&flags.to_be_bytes());
    v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    let ancount: u16 = match answer {
        Answer::A(..) => 1,
        Answer::NoData => 0,
    };
    v.extend_from_slice(&ancount.to_be_bytes()); // ANCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    v.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    v.extend_from_slice(&q.question); // 回显 question

    if let Answer::A(ip, ttl) = answer {
        v.extend_from_slice(&[0xC0, 0x0C]); // NAME: 指针回指 offset 12 的 qname
        v.extend_from_slice(&QTYPE_A.to_be_bytes()); // TYPE A
        v.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
        v.extend_from_slice(&ttl.to_be_bytes()); // TTL
        v.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH
        v.extend_from_slice(&ip.octets()); // RDATA
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
