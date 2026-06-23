//! VLESS 帧（空 flow）——请求头编码 + 响应头 strip（刀8 T1/T2，见 spec §3 不变量 8/9 + brief §1.4）。
//!
//! 中文要点：VLESS 是无状态代理协议，把一条中继请求（UUID auth + command + Target）框在已加密的流上，
//! 自身不带传输安全（靠下面的 REALITY）。**地址 = PortThenAddress**（port 2B BE 在前、atyp 在后），
//! ATYP v4=0x01/domain=0x02/v6=0x03——与 tuic.rs::encode_address（port-last + ATYP 错位）**完全不同**，
//! 故**新写专用编码器，绝不复用 tuic**。空 flow → addon_length=0。
