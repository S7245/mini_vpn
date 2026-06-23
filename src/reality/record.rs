//! TLS 1.3 record-layer AEAD（刀7，RFC 8446 §5.2/§5.3/§5.4，sans-IO；AES-128-GCM 先 wire，泛型-over-cipher）。
//!
//! 中文要点：per-record nonce（write_iv XOR seq，seq 右对齐进低 8B → nonce[4..12]）、AAD=5B record 头
//! `[0x17,0x03,0x03,len_hi,len_lo]`（len=**密文含 16B tag**，off-by-16 风险）、inner=content||content_type||zero-pad、
//! open 剥尾零→最后非零字节=真 content type、全零明文→Err、读/写**两个独立 seq**（密钥切换时各归零）。
//! KAT：seq=0 时 nonce==iv；round-trip；RFC 8448 §3 server-flight record open golden KAT（见本刀 plan T5）。
//!
//! T4+ 实现。
