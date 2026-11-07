//! Owns the original bounded UDP tuple/association maps and fake-IP release list.
//! Does not own transport, timers or the fake-IP pool. Existing intern/sweep
//! behavior is unchanged; entry methods remain at tuic::AssocTable. Tests live
//! under assoc/tests; physical resource retirement belongs to its caller.
use super::{FlowEntry, FourTuple, MAX_UDP_FLOWS};
use std::collections::HashMap;
use std::net::Ipv4Addr;

/// TUIC UDP 关联表:每条 UDP flow(4 元组)分配一个 **u16 assoc-id**(≈ Stage 12 flow-id),
/// 双向 demux 用。主循环独占、无锁;`now` 注入便于单测。结构同 udp_relay::FlowTable,只是 id 宽 16 位。
/// 中文要点:复用 FourTuple/FlowEntry,不动 Stage 12 的 FlowTable(系统稳定优先,接受少量重复)。
#[derive(Debug)]
pub struct AssocTable {
    tuple_to_id: HashMap<FourTuple, u16>,
    id_to_entry: HashMap<u16, FlowEntry>,
    /// 刀2：assoc-id → 本 UDP flow 占用的 fake-IP（若 target 经 fake-IP 改写）。
    /// 中文要点：用于回收（evict/sweep）该 assoc 时知道要 `release` 哪个 fake-IP（引用计数）。
    id_to_fake_ip: HashMap<u16, Ipv4Addr>,
    /// 刀2：本轮被回收（evict/sweep）且有 fake-IP 的 assoc 的 fake-IP，待主循环 drain 后 `release`。
    reclaimed_fake_ips: Vec<Ipv4Addr>,
    next_id: u16,
    cap: usize,
}

impl Default for AssocTable {
    fn default() -> Self {
        Self::new()
    }
}

impl AssocTable {
    pub fn new() -> Self {
        Self::with_cap(MAX_UDP_FLOWS)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            tuple_to_id: HashMap::new(),
            id_to_entry: HashMap::new(),
            id_to_fake_ip: HashMap::new(),
            reclaimed_fake_ips: Vec::new(),
            next_id: 1,
            cap: cap.max(1).min(u16::MAX as usize),
        }
    }

    /// 刀2：登记该 assoc 占用的 fake-IP（UDP 新 flow 时调，调用方随后 `fake_pool.acquire`）。
    pub fn set_fake_ip(&mut self, assoc_id: u16, ip: Ipv4Addr) {
        self.id_to_fake_ip.insert(assoc_id, ip);
    }

    /// 刀2：取走本轮被回收（evict/sweep）的 fake-IP 列表，主循环对每个 `fake_pool.release`。
    /// 中文要点：assoc 回收与 fake-IP release 解耦——AssocTable 不持有 FakeIpPool，
    /// 只累积「该 release 谁」，由独占两者的主循环执行，避免交叉借用/循环依赖。
    pub fn take_reclaimed_fake_ips(&mut self) -> Vec<Ipv4Addr> {
        std::mem::take(&mut self.reclaimed_fake_ips)
    }

    /// assoc 被回收时，若它占用了 fake-IP，移出映射并记入待 release 队列。
    fn note_reclaimed(&mut self, assoc_id: u16) {
        if let Some(ip) = self.id_to_fake_ip.remove(&assoc_id) {
            self.reclaimed_fake_ips.push(ip);
        }
    }

    /// 查/铸 assoc-id（已在册稳定返回;否则铸新,到顶 LRU 驱逐）。
    pub fn intern(&mut self, tuple: FourTuple) -> u16 {
        if let Some(&id) = self.tuple_to_id.get(&tuple) {
            return id;
        }
        if self.id_to_entry.len() >= self.cap {
            self.evict_lru();
        }
        let id = self.alloc_id();
        self.id_to_entry.insert(
            id,
            FlowEntry {
                tuple,
                last_activity: 0,
            },
        );
        self.tuple_to_id.insert(tuple, id);
        id
    }

    /// 分配一个**空闲且非 0** 的 assoc-id（单调推进，跳过 0 与仍在册的 id）。
    /// 中文要点：u16 空间仅 65535,回绕后 `next_id` 可能落到仍在册的 flow 上——不跳过就会
    /// `insert` 覆盖活跃 flow(回程串到错误端点)并泄漏其 tuple_to_id 映射。活跃集(≤cap≤1024)
    /// 远小于 id 空间,故必有空闲 id,扫描代价极小。调用前已 evict 保证 len<cap,循环必然终止。
    fn alloc_id(&mut self) -> u16 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                self.next_id = 1; // 跳过 0,保持非零、单调
            }
            if !self.id_to_entry.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn resolve(&self, assoc_id: u16) -> Option<&FlowEntry> {
        self.id_to_entry.get(&assoc_id)
    }

    /// 该 4 元组是否已在册（用于"每流一次"日志，避免热路径刷屏）。
    pub fn contains(&self, tuple: &FourTuple) -> bool {
        self.tuple_to_id.contains_key(tuple)
    }

    pub fn touch(&mut self, assoc_id: u16, now: u64) {
        if let Some(e) = self.id_to_entry.get_mut(&assoc_id) {
            e.last_activity = now;
        }
    }

    /// 回收 idle 超 `idle_secs` 的 assoc，**直接返回**这些 assoc 占用的 fake-IP（供调用方 release）。
    /// 中文要点：review #8——sweep 自包含返回，不再依赖 stash-and-drain（调用方无需记得随后 take）；
    /// `take_reclaimed_fake_ips` 仅服务 intern 内部的 LRU 驱逐（那里无法返回值）。
    pub fn sweep(&mut self, now: u64, idle_secs: u64) -> Vec<Ipv4Addr> {
        let expired: Vec<(u16, FourTuple)> = self
            .id_to_entry
            .iter()
            .filter(|(_, e)| now.saturating_sub(e.last_activity) > idle_secs)
            .map(|(id, e)| (*id, e.tuple))
            .collect();
        let mut reclaimed = Vec::new();
        for (id, tuple) in expired {
            self.id_to_entry.remove(&id);
            self.tuple_to_id.remove(&tuple);
            if let Some(ip) = self.id_to_fake_ip.remove(&id) {
                reclaimed.push(ip); // 刀2：该 assoc 占用的 fake-IP → 调用方 release
            }
        }
        reclaimed
    }

    /// Cold session retirement. Each entry represents one original acquire;
    /// duplicate IP values must remain duplicates. No synthetic clock/sweep.
    pub(crate) fn retire_all(&mut self) -> Vec<Ipv4Addr> {
        let mut reclaimed = std::mem::take(&mut self.reclaimed_fake_ips);
        reclaimed.extend(self.id_to_fake_ip.drain().map(|(_, ip)| ip));
        self.tuple_to_id.clear();
        self.id_to_entry.clear();
        reclaimed
    }

    fn evict_lru(&mut self) {
        let victim = self
            .id_to_entry
            .iter()
            .min_by_key(|(id, e)| (e.last_activity, **id))
            .map(|(id, _)| *id);
        if let Some(id) = victim
            && let Some(e) = self.id_to_entry.remove(&id)
        {
            self.tuple_to_id.remove(&e.tuple);
            self.note_reclaimed(id); // 刀2：LRU 驱逐时同样回收 fake-IP
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.id_to_entry.len()
    }
}

#[cfg(test)]
mod tests;
