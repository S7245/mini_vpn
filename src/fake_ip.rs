use std::collections::HashMap;
use std::net::Ipv4Addr;

/// fake-IP 池与双向映射表（见 CONTEXT.md / ADR-0002）。
/// 中文要点：从 `198.18.0.0/15` 给域名发占位 IP，记 `domain ↔ fake-IP`；TCP 时凭
/// fake-IP 查回域名。同域名稳定复用同一 IP。主循环独占持有，无锁。
pub struct FakeIpPool {
    range_start: u32,
    range_end: u32,
    /// 下一个待分配地址（环形游标）。
    next: u32,
    domain_to_ip: HashMap<String, Ipv4Addr>,
    ip_to_domain: HashMap<Ipv4Addr, String>,
}

impl Default for FakeIpPool {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeIpPool {
    /// 构造：`198.18.0.0/15`，从 `.2` 起分配（`.0` 不用、`.1` 预留给 DNS resolver）。
    pub fn new() -> Self {
        let range_start = u32::from(Ipv4Addr::new(198, 18, 0, 0));
        let range_end = u32::from(Ipv4Addr::new(198, 19, 255, 255));
        Self {
            range_start,
            range_end,
            next: range_start + 2,
            domain_to_ip: HashMap::new(),
            ip_to_domain: HashMap::new(),
        }
    }

    /// 为域名分配 fake-IP；已分配过则稳定返回同一个。
    /// 中文要点：稳定复用是硬要求——否则 DNS 给的 IP 与 TCP 时查的表会不一致。
    pub fn alloc(&mut self, domain: &str) -> Ipv4Addr {
        if let Some(&ip) = self.domain_to_ip.get(domain) {
            return ip;
        }
        let ip = Ipv4Addr::from(self.next);
        self.next += 1;
        if self.next > self.range_end {
            // 环形回绕，跳过 .0 / .1。
            self.next = self.range_start + 2;
        }
        self.domain_to_ip.insert(domain.to_string(), ip);
        self.ip_to_domain.insert(ip, domain.to_string());
        ip
    }

    /// 由 fake-IP 查回域名（TCP target 改写用）。未分配返回 None。
    pub fn resolve(&self, ip: Ipv4Addr) -> Option<String> {
        self.ip_to_domain.get(&ip).cloned()
    }

    /// 该 IP 是否落在 fake-IP 段内。
    pub fn is_fake(&self, ip: Ipv4Addr) -> bool {
        let v = u32::from(ip);
        v >= self.range_start && v <= self.range_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_stable_per_domain() {
        let mut p = FakeIpPool::new();
        let a = p.alloc("facebook.com");
        let b = p.alloc("facebook.com");
        assert_eq!(a, b, "same domain must reuse the same fake-IP");
        let c = p.alloc("google.com");
        assert_ne!(a, c, "different domains get different fake-IPs");
    }

    #[test]
    fn alloc_starts_at_dot_two_skipping_resolver() {
        let mut p = FakeIpPool::new();
        assert_eq!(p.alloc("a.com"), Ipv4Addr::new(198, 18, 0, 2));
        assert_eq!(p.alloc("b.com"), Ipv4Addr::new(198, 18, 0, 3));
    }

    #[test]
    fn resolve_round_trips_and_misses() {
        let mut p = FakeIpPool::new();
        let ip = p.alloc("x.com");
        assert_eq!(p.resolve(ip).as_deref(), Some("x.com"));
        assert_eq!(p.resolve(Ipv4Addr::new(198, 18, 0, 250)), None);
    }

    #[test]
    fn is_fake_range_boundaries() {
        let p = FakeIpPool::new();
        assert!(p.is_fake(Ipv4Addr::new(198, 18, 0, 0)));
        assert!(p.is_fake(Ipv4Addr::new(198, 19, 255, 255)));
        assert!(!p.is_fake(Ipv4Addr::new(198, 17, 255, 255)));
        assert!(!p.is_fake(Ipv4Addr::new(198, 20, 0, 0)));
        assert!(!p.is_fake(Ipv4Addr::new(1, 1, 1, 1)));
    }
}
