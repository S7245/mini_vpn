//! 刀4 — 加密 DNS(DoH/DoT/DoQ/DoH3)端点识别。
//!
//! 中文要点(已查证 + grill 对齐,见 docs/tech/2026-06-18-knife4-*):fake-IP 路由依赖**明文 DNS**——
//! 应用走加密 DNS 会拿到真实 IP、绕过 fake-IP → 真实 IP 没进隧道 → 被 GFW 墙 → 连接失败。本模块提供
//! **纯识别**:端口 853(DoT/DoQ)/ :443 的 DoH 域名 / :443 的 DoH-IP。命中由 `resolve_target` 判 `Block`
//! → TCP 发 RST、UDP 丢包,逼应用回落明文 DNS(我方伪造 fake-IP)。**只精确命中加密 DNS 端点,绝不碰
//! 普通 HTTPS/QUIC**(:443 仅按名单)。SNI 解析(任意 DoH)留后续,实测漏才上。

use std::net::Ipv4Addr;

/// 加密 DNS 的端口:853 = DoT(TCP)/ DoQ(UDP)。DoH/DoH3 走 :443,不在此判(按域名/IP)。
pub fn is_encrypted_dns_port(port: u16) -> bool {
    port == 853
}

/// 刀5:到达 `resolve_target` 的 DNS 端口都该 Block——`:53`(明文 DNS over **TCP**) + `:853`(DoT/DoQ)。
/// 中文要点(ADR-0007):**不变量**——UDP :53 已被 `classify_inbound` 截到裸包劫持路径(本地伪造 fake-IP)、
/// **不到** resolve_target,故此处 `port==53` 只命中 **TCP** :53(明文 DNS over TCP)→ RST 逼应用回落
/// UDP :53(我方应答极小、永不触发 TC 截断,标准 stub 不升级 TCP)。:853 复用 `is_encrypted_dns_port`。
pub fn is_dns_relay_port(port: u16) -> bool {
    port == 53 || is_encrypted_dns_port(port)
}

/// 内置 DoH 域名名单(公认**专用** DoH 端点;取专用名以杜绝误伤正常服务)。
/// 中文要点:`is_doh_domain` 子域规则已**自动覆盖** `*.<apex>`(如 `*.cloudflare-dns.com`),
/// 故显式列子域(mozilla./chrome.)仅为自文档;新增同 apex 子域无需再列。名单尽力而非穷尽——
/// 默认浏览器(Cloudflare/Google/Quad9)已覆盖,exotic/opt-in 端点由真出口 acceptance 实测增补 / SNI(defer)。
const DOH_DOMAINS: &[&str] = &[
    "dns.google",
    "dns.google.com", // 旧版 Google DoH 主机名（部分老客户端仍用；非 dns.google 子域，需显式列）
    "dns64.dns.google",
    "cloudflare-dns.com",
    "mozilla.cloudflare-dns.com",
    "chrome.cloudflare-dns.com",
    "one.one.one.one",
    "dns.quad9.net",
    "dns11.quad9.net",
    "dns10.quad9.net",
    "doh.opendns.com",
    "dns.adguard-dns.com",
    "dns.adguard.com",
    "doh.cleanbrowsing.org",
    "doh.dns.sb",
    "dns.nextdns.io",
];

/// 内置 DoH bootstrap IP 名单(应用硬编、不经我方 DNS 时按 IP 命中)。
const DOH_IPS: &[Ipv4Addr] = &[
    Ipv4Addr::new(1, 1, 1, 1),
    Ipv4Addr::new(1, 0, 0, 1),
    Ipv4Addr::new(8, 8, 8, 8),
    Ipv4Addr::new(8, 8, 4, 4),
    Ipv4Addr::new(9, 9, 9, 9),
    Ipv4Addr::new(149, 112, 112, 112),
    Ipv4Addr::new(208, 67, 222, 222),
    Ipv4Addr::new(208, 67, 220, 220),
    Ipv4Addr::new(94, 140, 14, 14),
    Ipv4Addr::new(94, 140, 15, 15),
];

/// 域名是否命中 DoH 名单(大小写不敏感;精确 ∨ 子域)。
/// 中文要点:子域用「精确 ∨ `.`+后缀」——`x.cloudflare-dns.com` 命中,但 `mycloudflare-dns.com`
/// 不命中(防裸后缀匹配误中,如 `notdns.google` 不应中 `dns.google`)。
pub fn is_doh_domain(domain: &str) -> bool {
    let d = domain.trim_end_matches('.').to_ascii_lowercase();
    DOH_DOMAINS.iter().any(|&doh| {
        d == doh || d.strip_suffix(doh).is_some_and(|p| p.ends_with('.'))
    })
}

/// IP 是否命中 DoH bootstrap 名单。
pub fn is_doh_ip(ip: Ipv4Addr) -> bool {
    DOH_IPS.contains(&ip)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_dns_port_only_853() {
        assert!(is_encrypted_dns_port(853));
        for p in [53, 80, 443, 8443, 0, 65535] {
            assert!(!is_encrypted_dns_port(p), "port {p} 不应判为加密 DNS");
        }
    }

    /// 刀5：到达 resolve_target 的 DNS 端口（TCP :53 明文 + DoT/DoQ :853）都应 Block。
    /// mDNS :5353 / 普通端口不命中（绝不误伤）。
    #[test]
    fn dns_relay_port_covers_53_and_853() {
        assert!(is_dns_relay_port(53));
        assert!(is_dns_relay_port(853));
        for p in [443, 80, 0, 65535, 5353, 8443] {
            assert!(!is_dns_relay_port(p), "port {p} 不应判为 DNS relay 端口");
        }
    }

    #[test]
    fn doh_domain_exact_and_case_insensitive() {
        assert!(is_doh_domain("dns.google"));
        assert!(is_doh_domain("dns.google.com")); // 旧版 Google DoH 主机名(显式列,非 dns.google 子域)
        assert!(is_doh_domain("DNS.GOOGLE")); // 大小写不敏感
        assert!(is_doh_domain("cloudflare-dns.com"));
        assert!(is_doh_domain("dns.google.")); // 末尾点(FQDN)
    }

    #[test]
    fn doh_domain_subdomain_match_but_no_naked_suffix() {
        // 子域命中(`.`+后缀)。
        assert!(is_doh_domain("foo.cloudflare-dns.com"));
        // 防误中:裸后缀拼接不算子域。
        assert!(!is_doh_domain("mycloudflare-dns.com"));
        assert!(!is_doh_domain("notdns.google"));
        // 完全无关。
        assert!(!is_doh_domain("example.com"));
        assert!(!is_doh_domain("google.com")); // 不是 dns.google
    }

    #[test]
    fn doh_ip_membership() {
        for ip in ["1.1.1.1", "8.8.8.8", "9.9.9.9", "149.112.112.112"] {
            assert!(is_doh_ip(ip.parse().unwrap()), "{ip} 应命中 DoH-IP");
        }
        for ip in ["93.184.216.34", "10.0.0.1", "198.18.0.2"] {
            assert!(!is_doh_ip(ip.parse().unwrap()), "{ip} 不应命中");
        }
    }
}
