//! reality 模块测试共用 helper（仅 `#[cfg(test)]`）。中文要点：hex 解码 + 32B 数组，
//! 给 auth/key_schedule/record/server_hello 的 RFC 8448 / RFC 7748 KAT 单测复用，避免各处重复。

/// 把 hex 字符串解码为字节（忽略空白，便于直接粘 RFC 向量）。非法 hex 直接 panic（仅测试用）。
pub fn hex(s: &str) -> Vec<u8> {
    let clean: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    assert!(clean.len().is_multiple_of(2), "hex 位数为奇数");
    clean
        .chunks(2)
        .map(|pair| {
            let h = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(h, 16).unwrap()
        })
        .collect()
}

/// hex → `[u8; 32]`（长度必须正好 32）。
pub fn arr32(s: &str) -> [u8; 32] {
    hex(s).try_into().expect("hex 应解码为 32 字节")
}
