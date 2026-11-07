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
    let mut name = [0; 253];
    let query = parse_query_into(data, &mut name)?;
    Some(DnsQuery {
        id: query.id,
        qname: query.qname.to_owned(),
        qtype: query.qtype,
        rd: query.rd,
        question: query.question.to_vec(),
    })
}

/// Borrowed wire question and caller-owned bounded DNS name; no heap allocation
/// is needed before the Device has admitted reply capacity.
pub(crate) struct QueryView<'a> {
    pub id: u16,
    pub qname: &'a str,
    pub qtype: u16,
    rd: bool,
    question: &'a [u8],
}
pub(crate) fn parse_query_into<'a>(
    data: &'a [u8],
    name: &'a mut [u8; 253],
) -> Option<QueryView<'a>> {
    if data.len() < 12 || data[4..6] != [0, 1] {
        return None;
    }
    let mut pos = 12;
    let mut written = 0usize;
    loop {
        let len = *data.get(pos)? as usize;
        pos += 1;
        if len == 0 {
            break;
        }
        if len & 0xc0 != 0 {
            return None;
        }
        let label = data.get(pos..pos + len)?;
        std::str::from_utf8(label).ok()?;
        if written != 0 {
            *name.get_mut(written)? = b'.';
            written += 1;
        }
        let end = written.checked_add(len)?;
        name.get_mut(written..end)?.copy_from_slice(label);
        written = end;
        pos += len;
    }
    let qtype = u16::from_be_bytes([*data.get(pos)?, *data.get(pos + 1)?]);
    Some(QueryView {
        id: u16::from_be_bytes([data[0], data[1]]),
        qname: std::str::from_utf8(&name[..written]).ok()?,
        qtype,
        rd: u16::from_be_bytes([data[2], data[3]]) & 0x0100 != 0,
        question: data.get(12..pos + 4)?,
    })
}

/// 构造对查询的响应。A 带一条记录；NoData/ServFail 无答案，RCODE 分别为 0/2。
pub fn build_response(q: &DnsQuery, answer: Answer) -> Vec<u8> {
    let mut v = Vec::with_capacity(q.question.len() + 28);
    // Vec writes cannot refuse capacity once allocated by this legacy helper.
    let view = QueryView {
        id: q.id,
        qname: &q.qname,
        qtype: q.qtype,
        rd: q.rd,
        question: &q.question,
    };
    let _ = write_response(&view, answer, &mut v);
    v
}

/// Maximum reply size, before selecting an answer or changing fake-IP state.
pub(crate) fn response_capacity(q: &QueryView<'_>) -> Option<usize> {
    q.question.len().checked_add(28)
}
pub(crate) fn write_response_into(
    q: &QueryView<'_>,
    answer: Answer,
    out: &mut [u8],
) -> std::io::Result<usize> {
    let capacity = out.len();
    let mut cursor = out;
    write_response(q, answer, &mut cursor)?;
    Ok(capacity - cursor.len())
}
fn write_response(
    q: &QueryView<'_>,
    answer: Answer,
    mut v: impl std::io::Write,
) -> std::io::Result<()> {
    v.write_all(&q.id.to_be_bytes())?;
    // flags: QR=1, Opcode=0, AA=0, TC=0, RD=回带, RA=0。
    let rd_bit = if q.rd { 0x0100 } else { 0 };
    let rcode = if matches!(answer, Answer::ServFail) {
        2
    } else {
        0
    };
    let flags: u16 = 0x8000 | rd_bit | rcode;
    v.write_all(&flags.to_be_bytes())?;
    v.write_all(&1u16.to_be_bytes())?; // QDCOUNT
    let ancount: u16 = match answer {
        Answer::A(..) => 1,
        Answer::NoData | Answer::ServFail => 0,
    };
    v.write_all(&ancount.to_be_bytes())?; // ANCOUNT
    v.write_all(&0u16.to_be_bytes())?; // NSCOUNT
    v.write_all(&0u16.to_be_bytes())?; // ARCOUNT
    v.write_all(&q.question)?; // 回显 question

    if let Answer::A(ip, ttl) = answer {
        v.write_all(&[0xC0, 0x0C])?; // NAME: 指针回指 offset 12 的 qname
        v.write_all(&QTYPE_A.to_be_bytes())?; // TYPE A
        v.write_all(&1u16.to_be_bytes())?; // CLASS IN
        v.write_all(&ttl.to_be_bytes())?; // TTL
        v.write_all(&4u16.to_be_bytes())?; // RDLENGTH
        v.write_all(&ip.octets())?; // RDATA
    }
    Ok(())
}

#[cfg(test)]
mod tests;
