//! ServerHello 解析（刀7，RFC 8446 §4.1.3，sans-IO）。
//!
//! 中文要点：手写字节 walk（type=0x02 + 3B len + body），提取 cipher_suite / server X25519 key_share /
//! 确认 supported_versions==0x0304；廉价拒绝路径：HRR sentinel、downgrade sentinel、compression!=0、version、
//! session_id_echo != 我方 sealed 32B。`#[cfg(test)]` 里 tls-parser 交叉验证（沿 client_hello.rs 纪律）。
//! ⚠️ **echo-match ≠ REALITY auth**：decoy 在 auth 失败时仍回显我方 session_id；真 auth 决策是刀8 的证书 HMAC。
//!
//! T6 实现。
