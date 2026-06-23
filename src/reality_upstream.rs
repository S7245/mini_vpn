//! REALITY 第二 Transport 上游（刀8 T9/T10，见 spec §4 + brief §3）。
//!
//! 中文要点：`RealityUpstream` impl `ProxyUpstream::open_tcp`——每条 TCP 新建一次完整 REALITY 握手、
//! 接着发 VLESS 请求 → 返回 `RealityStream`（impl AsyncRead+AsyncWrite over TLS 1.3 app record 层、含 VLESS 响应 strip）。
//! impl `DatagramUpstream::send_udp` = **no-op 静默丢**（REALITY 是 TCP-only，UDP-over-VLESS 是刀9）。
//! `RealityClientConfig::from_env`（MINI_VPN_REALITY_*，脱敏 Debug）。无连接复用（reuse 留刀9）。

use crate::shared::ClientError;

/// 解析 REALITY `public_key`（pbk）字符串 → 32B（刀8 T7，brief §1.5 / 风险 9）。
/// 中文要点：sing-box / 当前 Xray 的 `public_key` 是 Go `base64.RawURLEncoding`（URL-safe、无 `=`，32B→43 字符）。
/// 优先 base64url，回退 std（兼容历史/std 变体）；**解码后强断言恰 32B 否则 loud-fail**——错编码/错长度 →
/// AuthKey 错 → session_id 服务端解不开 → **静默回落 decoy**（看似连上 TLS 实则 REALITY auth 必败），故此处从严。
pub fn parse_pbk(s: &str) -> Result<[u8; 32], ClientError> {
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let s = s.trim();
    let decoded = URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .or_else(|_| STANDARD_NO_PAD.decode(s))
        .or_else(|_| STANDARD.decode(s))
        .map_err(|_| ClientError::Reality(format!("REALITY public_key 非合法 base64: {s:?}")))?;
    decoded.try_into().map_err(|v: Vec<u8>| {
        ClientError::Reality(format!(
            "REALITY public_key 解码后 {} 字节，须恰 32（错编码 → AuthKey 错 → 静默回落 decoy）",
            v.len()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// sing-box 风 43 字符 base64url（无 pad）→ bytes 0..31。
    #[test]
    fn pbk_base64url_singbox_style() {
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"; // base64url(bytes 0..31)
        assert_eq!(s.len(), 43, "32B base64url 无 pad = 43 字符");
        let expected: [u8; 32] = std::array::from_fn(|i| i as u8);
        assert_eq!(parse_pbk(s).unwrap(), expected);
    }

    /// std base64（含 `+`/`/`，url 字母表不接受）→ 走 std 回退。
    #[test]
    fn pbk_std_base64_fallback() {
        let s = "+//7//v/+//7//v/+//7//v/+//7//v/+//7//v/+/8"; // base64std(bytes [0xfb,0xff]×16)
        let expected: [u8; 32] = std::array::from_fn(|i| if i % 2 == 0 { 0xfb } else { 0xff });
        assert_eq!(parse_pbk(s).unwrap(), expected, "std 变体经回退解码");
    }

    /// 带 `=` padding 的 base64url 也接受。
    #[test]
    fn pbk_padded_accepted() {
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        let expected: [u8; 32] = std::array::from_fn(|i| i as u8);
        assert_eq!(parse_pbk(s).unwrap(), expected);
    }

    /// 解码后非 32B（这里 31B）→ loud-fail。
    #[test]
    fn pbk_wrong_length_rejected() {
        // base64url(bytes 0..30) = 31B，解码成功但长度错。
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGx0e"; // 30 bytes
        assert!(parse_pbk(s).is_err(), "非 32B 须拒");
    }

    /// 64-char hex（误把 short_id/其它 hex 当 pbk）→ base64 解码成 48B → 非 32B → 拒。
    #[test]
    fn pbk_hex_rejected() {
        let s = "df18652c451afa44c276c60475d9f4f6f4ae3bf9d389dd6f3215383d6d5dda0b";
        assert!(parse_pbk(s).is_err(), "64-char hex 解出 48B ≠ 32 → 拒");
    }

    /// 非 base64 垃圾 → 拒（不 panic）。
    #[test]
    fn pbk_garbage_rejected() {
        assert!(parse_pbk("!!!not base64!!!").is_err());
        assert!(parse_pbk("").is_err(), "空串解出 0B ≠ 32");
    }
}
