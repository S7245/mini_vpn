use std::collections::HashMap;
use std::net::Ipv4Addr;

/// 一个 fake-IP 映射的运行时状态（刀2：引用计数回收）。
/// 中文要点：`refcount` = 当前仍在用这条映射的活跃 flow 数（TCP listener 槽 + UDP assoc）；
/// `last_used` = 最近一次 DNS 查询/flow 边界的秒级时间戳。回收条件 = `refcount==0 且 idle>TTL`，
/// 保证**绝不回收仍有活跃连接的映射**（回收会让 resolve 失败 → Refuse → 断连）。
#[derive(Debug)]
struct Mapping {
    domain: String,
    refcount: u32,
    last_used: u64,
}

/// fake-IP 池与双向映射表（见 CONTEXT.md / ADR-0002）。
/// 中文要点：从 `198.18.0.0/15` 给域名发占位 IP，记 `domain ↔ fake-IP`；TCP 时凭
/// fake-IP 查回域名。同域名稳定复用同一 IP。主循环独占持有，无锁。
/// 刀2 起每条映射带引用计数 + last_used，支持 `acquire`/`release`/`sweep` 安全回收。
pub struct FakeIpPool {
    range_start: u32,
    range_end: u32,
    /// 下一个待分配地址（环形游标）。
    next: u32,
    domain_to_ip: HashMap<String, Ipv4Addr>,
    ip_to_mapping: HashMap<Ipv4Addr, Mapping>,
}

impl Default for FakeIpPool {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeIpPool {
    /// 构造：`198.18.0.0/15`，从 `.2` 起分配（`.0` 不用、`.1` 预留为可对外广告的 resolver 地址）。
    pub fn new() -> Self {
        let range_start = u32::from(Ipv4Addr::new(198, 18, 0, 0));
        let range_end = u32::from(Ipv4Addr::new(198, 19, 255, 255));
        Self {
            range_start,
            range_end,
            next: range_start + 2,
            domain_to_ip: HashMap::new(),
            ip_to_mapping: HashMap::new(),
        }
    }

    /// 为域名分配 fake-IP；已分配过则稳定返回同一个，并 touch `last_used`。
    /// 中文要点：稳定复用是硬要求——否则 DNS 给的 IP 与 TCP 时查的表会不一致。
    /// `alloc` 是 DNS 查询触发（不是 flow），**不改 refcount**；flow 引用由 `acquire`/`release` 管。
    pub fn alloc(&mut self, domain: &str, now: u64) -> Ipv4Addr {
        if let Some(&ip) = self.domain_to_ip.get(domain) {
            if let Some(m) = self.ip_to_mapping.get_mut(&ip) {
                m.last_used = now;
            }
            return ip;
        }
        let ip = self.next_free_ip();
        self.domain_to_ip.insert(domain.to_string(), ip);
        self.ip_to_mapping.insert(
            ip,
            Mapping {
                domain: domain.to_string(),
                refcount: 0,
                last_used: now,
            },
        );
        ip
    }

    /// 取下一个未被在册映射占用的 fake-IP（环形探测，回绕跳过占用地址，绝不覆盖在册映射）。
    /// 中文要点：在册映射靠 `sweep` 回收 idle 的来腾空间；正常情况第一格即空闲，O(1)。
    fn next_free_ip(&mut self) -> Ipv4Addr {
        let span = self.range_end - (self.range_start + 2) + 1;
        for _ in 0..span {
            let cand = Ipv4Addr::from(self.next);
            self.advance_next();
            if !self.ip_to_mapping.contains_key(&cand) {
                return cand;
            }
        }
        // 一圈全占用（极端：~13 万映射全在册）：覆盖当前 next（罕见，调用方应已 sweep）。
        // review #3：覆盖前清掉 victim 的 domain_to_ip，否则旧域名残留一条悬挂别名指向这个 IP，
        // 而 ip_to_mapping[ip] 即将被改写成新域名 → resolve(旧域名查到的 IP) 串到新域名。
        let cand = Ipv4Addr::from(self.next);
        self.advance_next();
        if let Some(victim) = self.ip_to_mapping.remove(&cand) {
            self.domain_to_ip.remove(&victim.domain);
        }
        cand
    }

    fn advance_next(&mut self) {
        self.next += 1;
        if self.next > self.range_end {
            self.next = self.range_start + 2;
        }
    }

    /// flow 开始：该 fake-IP 引用计数 +1，touch `last_used`。
    /// 中文要点：TCP listener 槽首开远端 / UDP 新 assoc 时调，确保 flow 存活期间不被 sweep 回收。
    pub fn acquire(&mut self, ip: Ipv4Addr, now: u64) {
        if let Some(m) = self.ip_to_mapping.get_mut(&ip) {
            m.refcount += 1;
            m.last_used = now;
        }
    }

    /// flow 结束：该 fake-IP 引用计数 -1（饱和减，不下溢），touch `last_used`。
    /// 中文要点：TCP 槽 rearm / UDP assoc 被 sweep 回收时调。归零后该映射进入「可回收」候选。
    pub fn release(&mut self, ip: Ipv4Addr, now: u64) {
        if let Some(m) = self.ip_to_mapping.get_mut(&ip) {
            m.refcount = m.refcount.saturating_sub(1);
            m.last_used = now;
        }
    }

    /// 回收所有 `refcount==0 且 now-last_used > ttl` 的映射，返回回收数。
    /// 中文要点：**只回收无活跃 flow 且已 idle 超 TTL 的映射**——活跃 flow（refcount>0）永不回收。
    pub fn sweep(&mut self, now: u64, ttl: u64) -> usize {
        let stale: Vec<Ipv4Addr> = self
            .ip_to_mapping
            .iter()
            .filter(|(_, m)| m.refcount == 0 && now.saturating_sub(m.last_used) > ttl)
            .map(|(&ip, _)| ip)
            .collect();
        for ip in &stale {
            if let Some(m) = self.ip_to_mapping.remove(ip) {
                self.domain_to_ip.remove(&m.domain);
            }
        }
        stale.len()
    }

    /// 由 fake-IP 查回域名（TCP/UDP target 改写用）。未分配返回 None。
    /// 中文要点：只读查询，不 touch `last_used`（flow 边界的 touch 由 acquire/release 负责，
    /// 热路径每包 resolve 不应改状态）。
    pub fn resolve(&self, ip: Ipv4Addr) -> Option<String> {
        self.ip_to_mapping.get(&ip).map(|m| m.domain.clone())
    }

    /// 该 IP 是否落在 fake-IP 段内。
    pub fn is_fake(&self, ip: Ipv4Addr) -> bool {
        let v = u32::from(ip);
        v >= self.range_start && v <= self.range_end
    }

    /// 池用量快照：`(total, active)` —— total=在册映射数、active=refcount>0（有活跃 flow）的映射数。
    /// 中文要点（刀11 可观测性）：只读 `&self`、不 touch `last_used`、不分配（不像 `resolve` clone domain）。
    /// `Mapping.refcount` 私有 → 必须是 `FakeIpPool` 的 inherent 方法。total=O(1)、active=O(n) 一遍 values()，
    /// **只在 30s tick 采样**（最坏 ~131k 映射，O(n) 进每包热路径不可接受）。`alloc` 不改 refcount，故
    /// active 追踪「有活跃 flow」而非「已分配」——刚 alloc 未连的域名计入 total 不计 active。
    pub fn usage(&self) -> (usize, usize) {
        let total = self.ip_to_mapping.len();
        let active = self
            .ip_to_mapping
            .values()
            .filter(|m| m.refcount > 0)
            .count();
        (total, active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_stable_per_domain() {
        let mut p = FakeIpPool::new();
        let a = p.alloc("facebook.com", 0);
        let b = p.alloc("facebook.com", 1);
        assert_eq!(a, b, "same domain must reuse the same fake-IP");
        let c = p.alloc("google.com", 0);
        assert_ne!(a, c, "different domains get different fake-IPs");
    }

    #[test]
    fn alloc_starts_at_dot_two_skipping_resolver() {
        let mut p = FakeIpPool::new();
        assert_eq!(p.alloc("a.com", 0), Ipv4Addr::new(198, 18, 0, 2));
        assert_eq!(p.alloc("b.com", 0), Ipv4Addr::new(198, 18, 0, 3));
    }

    #[test]
    fn resolve_round_trips_and_misses() {
        let mut p = FakeIpPool::new();
        let ip = p.alloc("x.com", 0);
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

    /// 引用计数回收：活跃 flow（refcount>0）即使 idle 超 TTL 也**绝不回收**。
    #[test]
    fn sweep_never_reclaims_active_flow() {
        let mut p = FakeIpPool::new();
        let ip = p.alloc("live.com", 0);
        p.acquire(ip, 0); // 一条活跃 flow
        // 远超 TTL，但 refcount>0 → 不回收。
        assert_eq!(p.sweep(10_000, 300), 0);
        assert_eq!(p.resolve(ip).as_deref(), Some("live.com"));
    }

    /// 引用计数回收：refcount 归零 + idle 超 TTL → 回收；回收后 resolve miss。
    #[test]
    fn sweep_reclaims_idle_zero_refcount() {
        let mut p = FakeIpPool::new();
        let ip = p.alloc("idle.com", 0);
        p.acquire(ip, 0);
        p.release(ip, 5); // flow 结束，last_used=5，refcount=0
        // idle 未到 TTL（now=100, ttl=300）→ 不回收。
        assert_eq!(p.sweep(100, 300), 0);
        assert!(p.resolve(ip).is_some());
        // idle 超 TTL（now=5+301）→ 回收。
        assert_eq!(p.sweep(306, 300), 1);
        assert_eq!(p.resolve(ip), None);
    }

    /// release 饱和减不下溢（多 release 不 panic、不回绕成巨值）。
    #[test]
    fn release_saturates_at_zero() {
        let mut p = FakeIpPool::new();
        let ip = p.alloc("x.com", 0);
        p.release(ip, 1); // refcount 本就是 0
        p.release(ip, 2);
        // 归零状态，idle 超 TTL 即可回收（证明没被 release 弄成巨值卡住）。
        assert_eq!(p.sweep(1000, 300), 1);
    }

    /// 池用量 usage()（刀11）：alloc 计 total 不计 active；acquire/release 动 active；sweep 降 total。
    #[test]
    fn usage_tracks_total_and_active() {
        let mut p = FakeIpPool::new();
        assert_eq!(p.usage(), (0, 0), "空池");
        let ip = p.alloc("a.com", 0);
        assert_eq!(p.usage(), (1, 0), "alloc 计 total、不改 refcount → active=0");
        p.acquire(ip, 0);
        assert_eq!(p.usage(), (1, 1), "acquire → active=1");
        let _ = p.alloc("b.com", 0);
        assert_eq!(p.usage(), (2, 1), "第二域名进 total、未 acquire");
        p.release(ip, 5);
        assert_eq!(p.usage(), (2, 0), "release 归零 → active=0、total 不变");
        // a.com、b.com 均 refcount==0 且 idle 超 TTL → 回收。
        assert_eq!(p.sweep(1000, 300), 2);
        assert_eq!(p.usage(), (0, 0), "sweep 后池空");
    }

    /// 回收腾出的 IP 可被新域名复用（next_free_ip 跳过在册、回收后空出）。
    #[test]
    fn reclaimed_ip_is_reusable() {
        let mut p = FakeIpPool::new();
        let ip1 = p.alloc("a.com", 0);
        let _ip2 = p.alloc("b.com", 0);
        // a.com idle 回收。
        assert_eq!(p.sweep(1000, 300), 2); // a 和 b 都 idle 无 flow
        assert_eq!(p.resolve(ip1), None);
        // 新分配应能再拿到地址（池未泄漏）。
        let ip3 = p.alloc("c.com", 2000);
        assert!(p.is_fake(ip3));
        assert_eq!(p.resolve(ip3).as_deref(), Some("c.com"));
    }
}
