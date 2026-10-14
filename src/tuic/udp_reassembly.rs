//! Owns the complete TUIC fragment collection, replacement, expiry, and completion transaction.
//! Public storage and API identity remain at `tuic::FragReassembler`; this child owns its
//! implementation, not association routing or TUN packet construction. Every partial retains at
//! most one IPv4 UDP payload, so the default 256-packet cap bounds retained payload to 16,769,792
//! bytes, plus at most 256 * 255 fragment slots. There is no second global byte ledger. Replacing
//! a slot adjusts its owner's byte sum before mutation; completion/expiry/eviction release the
//! whole owner. Entry points are `new`, `with_cap`, `accept`, and `sweep`; tests live below.
//! Completion additionally allocates at most 65,507 bytes for the joined output; its ownership
//! transfers to the caller. The incoming encoded Packet and downstream queues remain caller-owned.

use super::{FRAG_REASSEMBLY_CAP, FragPartial, FragReassembler, PacketMeta};
use crate::udp_relay::MAX_IPV4_UDP_PAYLOAD_BYTES;
use std::collections::HashMap;

impl Default for FragReassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl FragReassembler {
    pub fn new() -> Self {
        Self::with_cap(FRAG_REASSEMBLY_CAP)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            partials: HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// 喂入一个（分片）Packet。集齐返回完整 payload；未齐/无效返回 None。
    /// 中文要点：`FRAG_TOTAL==1` 直通不入表；`frag_id>=frag_total` 或 `frag_total==0` 视为无效丢弃；
    /// 重复 frag_id **last-writer-wins**（保留较新；见下「跨重连」）；新 key 到 cap 触发 LRU（按 first_seen）驱逐最老。
    /// 跨重连：连接断/重连后 server 可能复用同 `(assoc_id, pkt_id)`。frag_total 变 → 整体重建；frag_total 同
    /// → last-writer-wins 让新连接的 frag 覆盖残片（减少串味），未被新包重发的残留 slot 仍可能混入，由
    /// `FRAG_REASSEMBLY_TTL_SECS` sweep 兜底（窄窗、可容忍：分片仅大包尾部，重组失败应用自愈）。
    pub fn accept(&mut self, m: &PacketMeta, now: u64) -> Option<Vec<u8>> {
        if m.frag_total == 0 || m.frag_id >= m.frag_total {
            return None; // 无效分片头，丢弃（防越界/除零）
        }
        let key = (m.assoc_id, m.pkt_id);
        if m.data.len() > MAX_IPV4_UDP_PAYLOAD_BYTES {
            self.partials.remove(&key);
            return None;
        }
        if m.frag_total == 1 {
            return Some(m.data.to_vec()); // 快路径：单帧直通，不入表
        }
        let frag_total = m.frag_total as usize;
        let payload_bytes = match self
            .partials
            .get(&key)
            .filter(|partial| partial.frags.len() == frag_total)
        {
            Some(partial) => partial
                .payload_bytes
                .checked_sub(
                    partial.frags[m.frag_id as usize]
                        .as_ref()
                        .map_or(0, Vec::len),
                )
                .and_then(|bytes| bytes.checked_add(m.data.len())),
            None => Some(m.data.len()),
        };
        let Some(payload_bytes) =
            payload_bytes.filter(|bytes| *bytes <= MAX_IPV4_UDP_PAYLOAD_BYTES)
        else {
            // An invalid whole packet cannot displace an unrelated key or retain its own prefix.
            self.partials.remove(&key);
            return None;
        };
        // pkt_id 复用但 frag_total 变化 → 旧残留作废，按新分片重建。
        if self
            .partials
            .get(&key)
            .is_some_and(|p| p.frags.len() != frag_total)
        {
            self.partials.remove(&key);
        }
        // 新 key 到 cap → 先驱逐最老未完成（在借用 entry 前做，避免交叉借用）。
        if !self.partials.contains_key(&key) {
            self.evict_if_full();
        }
        let p = self.partials.entry(key).or_insert_with(|| FragPartial {
            frags: (0..frag_total).map(|_| None).collect(),
            received: 0,
            payload_bytes: 0,
            first_seen: now,
        });
        let slot = &mut p.frags[m.frag_id as usize];
        if slot.is_none() {
            p.received += 1;
        }
        *slot = Some(m.data.to_vec()); // last-writer-wins（覆盖；跨重连残片被新 frag 顶替）
        p.payload_bytes = payload_bytes;
        if p.received < p.frags.len() {
            return None;
        }
        // 集齐：按 frag_id 序拼接，清出该项。
        let done = self.partials.remove(&key)?;
        let mut whole = Vec::with_capacity(done.payload_bytes);
        for frag in done.frags.into_iter().flatten() {
            whole.extend_from_slice(&frag);
        }
        Some(whole)
    }

    /// Control-only physical reset, after the original producer has joined.
    pub(crate) fn retire_generation(&mut self) -> bool {
        let valid = self.partials.values().all(|p| {
            p.frags.iter().flatten().map(Vec::len).sum::<usize>() == p.payload_bytes
                && p.frags.iter().filter(|f| f.is_some()).count() == p.received
        });
        self.partials.clear();
        valid
    }

    /// 回收未集齐且超 `ttl` 秒的分片包（丢片自愈，防内存泄漏）。
    pub fn sweep(&mut self, now: u64, ttl: u64) {
        self.partials
            .retain(|_, p| now.saturating_sub(p.first_seen) <= ttl);
    }

    /// 新 key 到 cap 前驱逐最老（first_seen 最小）未完成包。
    fn evict_if_full(&mut self) {
        if self.partials.len() < self.cap {
            return;
        }
        if let Some(victim) = self
            .partials
            .iter()
            .min_by_key(|(k, p)| (p.first_seen, **k))
            .map(|(k, _)| *k)
        {
            self.partials.remove(&victim);
        }
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.partials.len()
    }
}

#[cfg(test)]
mod tests;
