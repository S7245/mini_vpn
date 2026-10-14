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
/// 中文要点：A → 一条 A 记录；NoData → 成功但 0 答案；ServFail → 可重试的服务错误。
pub enum Answer {
    A(Ipv4Addr, u32),
    NoData,
    ServFail,
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

/// 构造对查询的响应。A 带一条记录；NoData/ServFail 无答案，RCODE 分别为 0/2。
pub fn build_response(q: &DnsQuery, answer: Answer) -> Vec<u8> {
    let mut v = Vec::with_capacity(q.question.len() + 28);
    v.extend_from_slice(&q.id.to_be_bytes());
    // flags: QR=1, Opcode=0, AA=0, TC=0, RD=回带, RA=0。
    let rd_bit = if q.rd { 0x0100 } else { 0 };
    let rcode = if matches!(answer, Answer::ServFail) {
        2
    } else {
        0
    };
    let flags: u16 = 0x8000 | rd_bit | rcode;
    v.extend_from_slice(&flags.to_be_bytes());
    v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    let ancount: u16 = match answer {
        Answer::A(..) => 1,
        Answer::NoData | Answer::ServFail => 0,
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
mod tests;
