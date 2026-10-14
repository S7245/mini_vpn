use std::collections::HashMap;
use std::net::Ipv4Addr;

/// 所有 fake-IP 都仍在册；调用方应返回 DNS SERVFAIL，等待安全回收后重试。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FakeIpPoolExhausted;

impl std::fmt::Display for FakeIpPoolExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fake-IP pool exhausted")
    }
}

impl std::error::Error for FakeIpPoolExhausted {}

/// 一个 fake-IP 映射的运行时状态（刀2：引用计数回收）。
/// 中文要点：`refcount` = 当前仍在用这条映射的活跃 flow 数（TCP listener 槽 + UDP assoc）；
/// `last_used` = 最近一次 DNS 查询/flow 边界的秒级时间戳。回收条件 = `refcount==0 且 idle>TTL`，
/// 保证**绝不回收仍有活跃连接的映射**（回收会让 resolve 失败 → Refuse → 断连）。
#[derive(Debug)]
struct Mapping {
    domain: String,
    refcount: u32,
    incarnation: u64,
    checked_refs: u32,
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
    checked_owner: std::sync::Arc<()>,
    next_mapping: u64,
    next_acquisition: u64,
    checked_leases: HashMap<u64, (Ipv4Addr, u64)>,
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
            checked_owner: std::sync::Arc::new(()),
            next_mapping: 0,
            next_acquisition: 0,
            checked_leases: HashMap::new(),
            domain_to_ip: HashMap::new(),
            ip_to_mapping: HashMap::new(),
        }
    }

    /// 为域名分配 fake-IP；已分配过则稳定返回同一个，并 touch `last_used`。
    /// 所有地址仍在册时返回容量错误，绝不覆盖映射；只有 `sweep` 能释放地址。
    /// 中文要点：稳定复用是硬要求——否则 DNS 给的 IP 与 TCP 时查的表会不一致。
    /// `alloc` 是 DNS 查询触发（不是 flow），**不改 refcount**；flow 引用由 `acquire`/`release` 管。
    pub fn alloc(&mut self, domain: &str, now: u64) -> Result<Ipv4Addr, FakeIpPoolExhausted> {
        if let Some(&ip) = self.domain_to_ip.get(domain) {
            if let Some(m) = self.ip_to_mapping.get_mut(&ip) {
                m.last_used = now;
            }
            return Ok(ip);
        }
        let incarnation = self
            .next_mapping
            .checked_add(1)
            .ok_or(FakeIpPoolExhausted)?;
        let ip = self.next_free_ip()?;
        self.next_mapping = incarnation;
        self.domain_to_ip.insert(domain.to_string(), ip);
        self.ip_to_mapping.insert(
            ip,
            Mapping {
                domain: domain.to_string(),
                refcount: 0,
                incarnation,
                checked_refs: 0,
                last_used: now,
            },
        );
        Ok(ip)
    }

    /// 取下一个未被在册映射占用的 fake-IP（环形探测，回绕跳过占用地址，绝不覆盖在册映射）。
    /// 中文要点：在册映射靠 `sweep` 回收 idle 的来腾空间；正常情况第一格即空闲，O(1)。
    fn next_free_ip(&mut self) -> Result<Ipv4Addr, FakeIpPoolExhausted> {
        let span = self.range_end - (self.range_start + 2) + 1;
        // 满池重试不扫描约 13 万地址，避免 DNS 压力放大主循环停顿。
        if self.ip_to_mapping.len() >= span as usize {
            return Err(FakeIpPoolExhausted);
        }
        for _ in 0..span {
            let cand = Ipv4Addr::from(self.next);
            self.advance_next();
            if !self.ip_to_mapping.contains_key(&cand) {
                return Ok(cand);
            }
        }
        Err(FakeIpPoolExhausted)
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
            .filter(|(_, m)| {
                m.refcount == 0 && m.checked_refs == 0 && now.saturating_sub(m.last_used) > ttl
            })
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

const MAX_CHECKED_LEASES: usize = 4224;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckedFakeIpError {
    WrongPool,
    MissingMapping,
    ReverseMappingMismatch,
    WrongIncarnation,
    UnknownAcquisition,
    CountMismatch,
    CapacityReached,
    AcquisitionIdExhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FakeIpLeaseReceipt {
    pub(crate) ip: Ipv4Addr,
    pub(crate) incarnation: u64,
    pub(crate) acquisition: u64,
    pub(crate) before: u32,
    pub(crate) after: u32,
}

/// A pool-bound, non-clone capability. Dropping it is not a release.
#[derive(Debug)]
pub(crate) struct CheckedFakeIpLease {
    owner: std::sync::Arc<()>,
    receipt: FakeIpLeaseReceipt,
}

impl CheckedFakeIpLease {
    pub(crate) fn receipt(&self) -> FakeIpLeaseReceipt {
        self.receipt
    }
}

/// One stop-only census over the original registry and physical capabilities.
/// Invalid acquisition and mapping verdicts are local; missing observations do
/// not invalidate a different capability, even when both share an IP.
pub(crate) struct CheckedLeaseAudit<'a> {
    owner: &'a std::sync::Arc<()>,
    observed: HashMap<u64, (FakeIpLeaseReceipt, bool)>,
    complete: bool,
}

impl CheckedLeaseAudit<'_> {
    pub(crate) fn complete(&self) -> bool {
        self.complete
    }

    pub(crate) fn accepts(&self, lease: &CheckedFakeIpLease) -> bool {
        std::sync::Arc::ptr_eq(self.owner, &lease.owner)
            && self
                .observed
                .get(&lease.receipt.acquisition)
                .is_some_and(|(receipt, valid)| *valid && *receipt == lease.receipt)
    }
}

/// All validation precedes a caller's fallible listener step. This borrow keeps
/// the exact registry entry and mapping stable until the final infallible commit.
pub(crate) struct PreparedFakeIpRelease<'a> {
    mapping: &'a mut Mapping,
    entry: std::collections::hash_map::OccupiedEntry<'a, u64, (Ipv4Addr, u64)>,
    receipt: FakeIpLeaseReceipt,
    checked_after: u32,
    now: u64,
}

impl PreparedFakeIpRelease<'_> {
    pub(crate) fn commit(self) -> FakeIpLeaseReceipt {
        self.mapping.refcount = self.receipt.after;
        self.mapping.checked_refs = self.checked_after;
        self.mapping.last_used = self.now;
        self.entry.remove();
        self.receipt
    }
}

impl FakeIpPool {
    fn checked_mapping(&self, ip: Ipv4Addr) -> Result<&Mapping, CheckedFakeIpError> {
        if self.checked_leases.len() > MAX_CHECKED_LEASES {
            return Err(CheckedFakeIpError::CountMismatch);
        }
        let mapping = self
            .ip_to_mapping
            .get(&ip)
            .ok_or(CheckedFakeIpError::MissingMapping)?;
        if self.domain_to_ip.get(&mapping.domain) != Some(&ip) {
            return Err(CheckedFakeIpError::ReverseMappingMismatch);
        }
        let mut checked = 0u32;
        for (other_ip, incarnation) in self.checked_leases.values() {
            if *other_ip == ip {
                if *incarnation != mapping.incarnation {
                    return Err(CheckedFakeIpError::WrongIncarnation);
                }
                checked = checked
                    .checked_add(1)
                    .ok_or(CheckedFakeIpError::CountMismatch)?;
            }
        }
        if checked != mapping.checked_refs || mapping.refcount < checked {
            return Err(CheckedFakeIpError::CountMismatch);
        }
        Ok(mapping)
    }

    pub(crate) fn audit_checked_leases<'pool, 'lease>(
        &'pool self,
        leases: impl Iterator<Item = &'lease CheckedFakeIpLease>,
    ) -> CheckedLeaseAudit<'pool> {
        let mut audit = CheckedLeaseAudit {
            owner: &self.checked_owner,
            observed: HashMap::new(),
            complete: true,
        };
        if self.checked_leases.len() > MAX_CHECKED_LEASES {
            audit.complete = false;
            return audit;
        }
        // At most 4224 registry acquisitions and therefore at most 4224 IPs.
        // Counts come from this physical registry, not the observed capabilities.
        let mut counts = HashMap::<Ipv4Addr, (u32, bool)>::new();
        for (ip, incarnation) in self.checked_leases.values() {
            let entry = counts.entry(*ip).or_insert((0, true));
            entry.0 += 1;
            entry.1 &= self.ip_to_mapping.get(ip).is_some_and(|mapping| {
                mapping.incarnation == *incarnation
                    && self.domain_to_ip.get(&mapping.domain) == Some(ip)
            });
        }
        // Keep the original full mapping census, including orphan checked_refs.
        // Such an orphan fails completeness without inventing a flow or a lease.
        for (ip, mapping) in &self.ip_to_mapping {
            let count = counts.get(ip).map_or(0, |entry| entry.0);
            let valid = mapping.checked_refs == count && mapping.refcount >= count;
            audit.complete &= valid;
            if let Some(entry) = counts.get_mut(ip) {
                entry.1 &= valid;
            }
        }
        for (index, lease) in leases.enumerate() {
            if index == MAX_CHECKED_LEASES {
                audit.observed.clear();
                audit.complete = false;
                return audit;
            }
            let receipt = lease.receipt;
            let valid = std::sync::Arc::ptr_eq(&self.checked_owner, &lease.owner)
                && self.checked_leases.get(&receipt.acquisition)
                    == Some(&(receipt.ip, receipt.incarnation))
                && counts.get(&receipt.ip).is_some_and(|entry| entry.1);
            match audit.observed.entry(receipt.acquisition) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert((receipt, valid));
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    // Both observations of this acquisition are uncertain.
                    entry.get_mut().1 = false;
                    audit.complete = false;
                }
            }
        }
        audit.complete &= audit.observed.len() == self.checked_leases.len()
            && audit.observed.values().all(|(_, valid)| *valid);
        audit
    }

    pub(crate) fn validate_checked_leases<'a>(
        &self,
        leases: impl Iterator<Item = &'a CheckedFakeIpLease>,
    ) -> bool {
        self.audit_checked_leases(leases).complete()
    }

    pub(crate) fn acquire_checked(
        &mut self,
        ip: Ipv4Addr,
        now: u64,
    ) -> Result<CheckedFakeIpLease, CheckedFakeIpError> {
        let mapping = self.checked_mapping(ip)?;
        if self.checked_leases.len() == MAX_CHECKED_LEASES {
            return Err(CheckedFakeIpError::CapacityReached);
        }
        let after = mapping
            .refcount
            .checked_add(1)
            .ok_or(CheckedFakeIpError::CountMismatch)?;
        let checked_after = mapping
            .checked_refs
            .checked_add(1)
            .ok_or(CheckedFakeIpError::CountMismatch)?;
        let acquisition = self
            .next_acquisition
            .checked_add(1)
            .ok_or(CheckedFakeIpError::AcquisitionIdExhausted)?;
        let receipt = FakeIpLeaseReceipt {
            ip,
            incarnation: mapping.incarnation,
            acquisition,
            before: mapping.refcount,
            after,
        };
        let owner = std::sync::Arc::clone(&self.checked_owner);
        if self.checked_leases.contains_key(&acquisition) {
            return Err(CheckedFakeIpError::UnknownAcquisition);
        }
        let mapping = self
            .ip_to_mapping
            .get_mut(&ip)
            .ok_or(CheckedFakeIpError::MissingMapping)?;
        // The ID is monotonic and checked before any physical state changes.
        self.checked_leases
            .insert(acquisition, (ip, receipt.incarnation));
        self.next_acquisition = acquisition;
        mapping.refcount = after;
        mapping.checked_refs = checked_after;
        mapping.last_used = now;
        Ok(CheckedFakeIpLease { owner, receipt })
    }

    pub(crate) fn prepare_checked_release<'a>(
        &'a mut self,
        lease: &CheckedFakeIpLease,
        now: u64,
    ) -> Result<PreparedFakeIpRelease<'a>, CheckedFakeIpError> {
        if !std::sync::Arc::ptr_eq(&self.checked_owner, &lease.owner) {
            return Err(CheckedFakeIpError::WrongPool);
        }
        let receipt = lease.receipt;
        let mapping = self.checked_mapping(receipt.ip)?;
        if mapping.incarnation != receipt.incarnation {
            return Err(CheckedFakeIpError::WrongIncarnation);
        }
        let after = mapping
            .refcount
            .checked_sub(1)
            .ok_or(CheckedFakeIpError::CountMismatch)?;
        let checked_after = mapping
            .checked_refs
            .checked_sub(1)
            .ok_or(CheckedFakeIpError::CountMismatch)?;
        let before = mapping.refcount;
        let entry = match self.checked_leases.entry(receipt.acquisition) {
            std::collections::hash_map::Entry::Occupied(entry)
                if *entry.get() == (receipt.ip, receipt.incarnation) =>
            {
                entry
            }
            _ => return Err(CheckedFakeIpError::UnknownAcquisition),
        };
        let mapping = self
            .ip_to_mapping
            .get_mut(&receipt.ip)
            .ok_or(CheckedFakeIpError::MissingMapping)?;
        Ok(PreparedFakeIpRelease {
            mapping,
            entry,
            receipt: FakeIpLeaseReceipt {
                before,
                after,
                ..receipt
            },
            checked_after,
            now,
        })
    }
}

#[cfg(test)]
mod tests;
