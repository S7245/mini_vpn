//! TLS 1.3 key schedule（刀7，RFC 8446 §7.1/§7.3，sans-IO 纯函数；泛型-over-hash，SHA-256 先 wire）。
//!
//! 中文要点：HKDF-Expand-Label / Derive-Secret / 握手阶段密钥链（Early→derived→Handshake(from ECDHE)→
//! {c,s}_hs_traffic→key/iv）+ `compute_finished_verify_data`。KAT 用 RFC 8448 §3（见 testutil + 本刀 plan）。
//! ⚠️ 这里的 ECDHE = x25519(client 临时, **server 临时** keyshare from ServerHello)，与刀6 REALITY AuthKey 的
//! x25519(client 临时, **server 静态** pbk) 是**不同**密钥——别接错（接错不过 KAT 但破活握手）。
//! ⚠️ network 来的 keyshare 不可信：Extract 前必须拒绝全零/非贡献点 ECDHE（见 auth.rs `x25519_shared_secret` 注）。
//!
//! T1+ 实现。
