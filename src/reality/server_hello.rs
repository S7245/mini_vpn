//! ServerHello 解析（刀7，RFC 8446 §4.1.3，sans-IO）。
//!
//! 中文要点：手写字节 walk（type=0x02 + 3B len + body），提取 cipher_suite / server X25519 key_share /
//! 确认 supported_versions==0x0304；廉价拒绝路径：HRR sentinel、downgrade sentinel、compression!=0、version、
//! session_id_echo != 我方 sealed 32B。`#[cfg(test)]` 里 tls-parser 交叉验证（沿 client_hello.rs 纪律）。
//! ⚠️ **echo-match ≠ REALITY auth**：decoy 在 auth 失败时仍回显我方 session_id；真 auth 决策是刀8 的证书 HMAC。

use crate::shared::ClientError;

/// HelloRetryRequest 的 ServerHello.random 哨兵值 = SHA-256("HelloRetryRequest")（RFC 8446 §4.1.3）。
const HRR_RANDOM: [u8; 32] = [
    0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
    0xc2, 0xa2, 0x11, 0x67, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
];

/// 解析出的 ServerHello 要点（REALITY 客户端需要的）。
pub struct ParsedServerHello {
    pub cipher_suite: u16,
    pub server_key_share: [u8; 32],
}

fn err(m: &str) -> ClientError {
    ClientError::Reality(format!("ServerHello: {m}"))
}

/// 在扩展块里按类型找一个扩展的 body。
fn find_ext(exts: &[u8], want: u16) -> Option<&[u8]> {
    let mut i = 0;
    while i + 4 <= exts.len() {
        let t = u16::from_be_bytes([exts[i], exts[i + 1]]);
        let l = u16::from_be_bytes([exts[i + 2], exts[i + 3]]) as usize;
        let body = exts.get(i + 4..i + 4 + l)?;
        if t == want {
            return Some(body);
        }
        i += 4 + l;
    }
    None
}

/// supported_versions（0x002b）：ServerHello 变体是**单个** 2B selected_version（非列表）。
fn extract_selected_version(exts: &[u8]) -> Result<u16, ClientError> {
    let b = find_ext(exts, 0x002b).ok_or_else(|| err("缺 supported_versions 扩展"))?;
    if b.len() != 2 {
        return Err(err("supported_versions 长度异常"));
    }
    Ok(u16::from_be_bytes([b[0], b[1]]))
}

/// key_share（0x0033）：ServerHello 变体是**单个** KeyShareEntry = group(2)+len(2)+key；要求 X25519(0x001d)+32B。
fn extract_key_share_x25519(exts: &[u8]) -> Result<[u8; 32], ClientError> {
    let b = find_ext(exts, 0x0033).ok_or_else(|| err("缺 key_share 扩展"))?;
    if b.len() < 4 {
        return Err(err("key_share 截断"));
    }
    if u16::from_be_bytes([b[0], b[1]]) != 0x001d {
        return Err(err("key_share group != X25519"));
    }
    if u16::from_be_bytes([b[2], b[3]]) != 32 {
        return Err(err("X25519 key_share 长度 != 32"));
    }
    let key = b.get(4..36).ok_or_else(|| err("key_share 截断"))?;
    Ok(key.try_into().expect("36-4=32"))
}

/// 解析 ServerHello handshake message（RFC 8446 §4.1.3），提取 cipher_suite + server X25519 key_share，
/// 并跑廉价拒绝路径。`expected_session_id` = 我方 ClientHello 发出的 session_id（REALITY 为 32B sealed 值；
/// RFC 8448 示例为空）；echo 不一致 → Err。**echo-match 仅 RFC 一致性检查，NOT REALITY auth**（auth 决策在刀8 证书 HMAC）。
pub fn parse_server_hello(
    bytes: &[u8],
    expected_session_id: &[u8],
) -> Result<ParsedServerHello, ClientError> {
    if bytes.first() != Some(&0x02) {
        return Err(err("非 ServerHello（handshake type != 0x02）"));
    }
    let len_field = bytes.get(1..4).ok_or_else(|| err("handshake 头截断"))?;
    let body = bytes.get(4..).ok_or_else(|| err("handshake 头截断"))?;
    // 校验 handshake 长度字段（uint24）== 实际 body 长度（防截断/拼接的网络输入静默误解析）。
    let claimed = ((len_field[0] as usize) << 16) | ((len_field[1] as usize) << 8) | len_field[2] as usize;
    if claimed != body.len() {
        return Err(err("handshake 长度字段与实际 body 不符"));
    }
    let mut p = 0usize;

    if body.get(p..p + 2) != Some(&[0x03, 0x03][..]) {
        return Err(err("legacy_version != 0x0303"));
    }
    p += 2;

    let random: [u8; 32] = body.get(p..p + 32).ok_or_else(|| err("random 截断"))?.try_into().unwrap();
    p += 32;
    if random == HRR_RANDOM {
        return Err(err("HelloRetryRequest（暂不支持，刀8 处理）"));
    }
    if &random[24..32] == b"DOWNGRD\x01" || &random[24..32] == b"DOWNGRD\x00" {
        return Err(err("downgrade sentinel（服务端降级到 <1.3）"));
    }

    let sid_len = *body.get(p).ok_or_else(|| err("session_id 长度截断"))? as usize;
    p += 1;
    let sid = body.get(p..p + sid_len).ok_or_else(|| err("session_id 截断"))?;
    p += sid_len;
    if sid != expected_session_id {
        // echo≠auth：RFC 一致性检查；REALITY auth 成败由刀8 证书 HMAC 定。
        return Err(err("session_id_echo 与我方发出的 session_id 不一致"));
    }

    let cs = body.get(p..p + 2).ok_or_else(|| err("cipher_suite 截断"))?;
    let cipher_suite = u16::from_be_bytes([cs[0], cs[1]]);
    p += 2;
    // 刀7 仅支持 TLS_AES_128_GCM_SHA256（key schedule/record 硬编 SHA-256/AES-128-GCM）。
    // 在 parse 层显式拒绝其它套件，避免 0x1302(AES-256-GCM-SHA384) decoy 被静默 mis-key（见 ADR-0009 gap）。
    if cipher_suite != 0x1301 {
        return Err(err(
            "不支持的 cipher_suite（刀7 仅 TLS_AES_128_GCM_SHA256 0x1301；0x1302/0x1303 见 ADR-0009 gap）",
        ));
    }

    if *body.get(p).ok_or_else(|| err("compression 截断"))? != 0 {
        return Err(err("legacy_compression != 0"));
    }
    p += 1;

    let el = body.get(p..p + 2).ok_or_else(|| err("extensions 长度截断"))?;
    let ext_len = u16::from_be_bytes([el[0], el[1]]) as usize;
    p += 2;
    let exts = body.get(p..p + ext_len).ok_or_else(|| err("extensions 截断"))?;

    if extract_selected_version(exts)? != 0x0304 {
        return Err(err("selected_version != TLS 1.3"));
    }
    let server_key_share = extract_key_share_x25519(exts)?;

    Ok(ParsedServerHello {
        cipher_suite,
        server_key_share,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::hex;

    // RFC 8448 §3 ServerHello handshake message（90B，session_id_echo 为空）。
    const RFC8448_SH: &str = "020000560303a6af06a4121860dc5e6e60249cd34c95930c8ac5cb1434dac155772ed3e2692800130100002e00330024001d0020c9828876112095fe66762bdbf7c672e156d6cc253b833df1dd69b1b04e751f0f002b00020304";

    /// RFC 8448 §3 真 ServerHello 解析 KAT：cipher 0x1301 / key_share c982..1f0f / version 0x0304（空 echo）。
    #[test]
    fn rfc8448_serverhello_parse() {
        let sh = hex(RFC8448_SH);
        let parsed = parse_server_hello(&sh, b"").expect("应解析（RFC 示例空 session_id）");
        assert_eq!(parsed.cipher_suite, 0x1301);
        assert_eq!(
            parsed.server_key_share,
            hex("c9828876112095fe66762bdbf7c672e156d6cc253b833df1dd69b1b04e751f0f")[..]
        );
    }

    /// tls-parser 独立交叉验证 RFC 8448 SH 是合法 ServerHello（沿 client_hello.rs 纪律）。
    #[test]
    fn rfc8448_serverhello_tls_parser_crosscheck() {
        use tls_parser::{TlsMessage, TlsMessageHandshake, parse_tls_message_handshake};
        let sh = hex(RFC8448_SH);
        let (_rest, msg) = parse_tls_message_handshake(&sh).expect("tls-parser 应解析");
        match msg {
            TlsMessage::Handshake(TlsMessageHandshake::ServerHello(s)) => {
                assert_eq!(s.cipher.0, 0x1301);
            }
            _ => panic!("不是 ServerHello"),
        }
    }

    /// 各拒绝路径（在 RFC SH 克隆上做定点变异：random[6..38]、downgrade random[30..38]、compression[41]、末 2B version）。
    #[test]
    fn reject_paths() {
        let base = hex(RFC8448_SH);

        // HRR sentinel random → Err。
        let mut hrr = base.clone();
        hrr[6..38].copy_from_slice(&HRR_RANDOM);
        assert!(parse_server_hello(&hrr, b"").is_err(), "HRR sentinel");

        // downgrade sentinel（random 末 8B）→ Err。
        let mut dg = base.clone();
        dg[30..38].copy_from_slice(b"DOWNGRD\x01");
        assert!(parse_server_hello(&dg, b"").is_err(), "downgrade sentinel");

        // compression != 0 → Err（RFC SH compression 在 [41]）。
        let mut comp = base.clone();
        comp[41] = 1;
        assert!(parse_server_hello(&comp, b"").is_err(), "compression!=0");

        // selected_version != 0x0304（末 2B 0304 → 0303）→ Err。
        let mut ver = base.clone();
        let n = ver.len();
        ver[n - 2..].copy_from_slice(&[0x03, 0x03]);
        assert!(parse_server_hello(&ver, b"").is_err(), "version!=1.3");

        // 不支持的 cipher_suite（RFC SH cipher 在 [39..41] = 1301 → 改 1302）→ Err。
        let mut cs = base.clone();
        cs[39..41].copy_from_slice(&[0x13, 0x02]);
        assert!(parse_server_hello(&cs, b"").is_err(), "cipher_suite != 0x1301");

        // session_id_echo 不一致（RFC SH 空 echo，传 32B expected）→ Err。
        assert!(parse_server_hello(&base, &[0u8; 32]).is_err(), "echo mismatch");

        // 截断 → Err（不 panic）。
        assert!(parse_server_hello(&base[..10], b"").is_err(), "截断");
        assert!(parse_server_hello(&[0x02], b"").is_err(), "极短");
    }
}
