//! Prepublication proof and held lock for one Native reply splice.
//! Owns validation of the actual pending/source chains and physical free-list
//! count, then an infallible link transfer. It does not own Root routes, CID
//! sequence policy or budgets. Entry: Pool::prepare_reply_splice; tests: admission/registration_reply.
use super::*;

pub(crate) struct ReplySplice<'a, 'b> {
    pool: &'a Pool,
    state: MutexGuard<'a, State>,
    target: &'b mut Chain,
    source: &'b mut Chain,
    indices: [u32; LOC_CID_COUNT as usize],
    count: usize,
}
impl Pool {
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn prepare_reply_splice<'a, 'b>(
        &'a self,
        target: &'b mut Frames,
        source: &'b mut Frames,
        expected: &[IssuedCid],
    ) -> Option<ReplySplice<'a, 'b>> {
        let (Frames::Cold(target), Frames::Cold(source)) = (target, source) else {
            self.quarantine();
            return None;
        };
        if !self.same_owner(&target.pool)
            || !self.same_owner(&source.pool)
            || expected.len() > LOC_CID_COUNT as usize
            || self.0.meter.admit_precharged().is_err()
        {
            self.quarantine();
            return None;
        }
        let state = match self.0.state.lock() {
            Ok(state) => state,
            Err(_) => {
                self.quarantine();
                return None;
            }
        };
        let checked = (|| {
            let mut seen = free_inventory(&state, self.0.free.load(Ordering::Relaxed))?;
            audit_chain(&state, target, &mut seen, None)?;
            if source.len as usize != expected.len() || source.encoded != 0 {
                return None;
            }
            let indices = audit_chain(&state, source, &mut seen, Some(expected))?;
            if target.len.checked_add(source.len)? as usize > state.nodes.len() {
                return None;
            }
            Some(indices)
        })();
        let Some(indices) = checked else {
            self.quarantine();
            return None;
        };
        Some(ReplySplice {
            pool: self,
            state,
            target,
            source,
            indices,
            count: expected.len(),
        })
    }
}

pub(super) fn free_inventory(state: &State, stored: usize) -> Option<u128> {
    if state.nodes.len() > 128 {
        return None;
    }
    let mut seen = 0u128;
    let mut current = state.free_head;
    let mut count = 0;
    while current != NONE {
        let node = state.nodes.get(current as usize)?;
        let bit = 1u128.checked_shl(current)?;
        if seen & bit != 0 || node.value.is_some() || node.encoded {
            return None;
        }
        seen |= bit;
        count += 1;
        current = node.next;
    }
    (count == stored).then_some(seen)
}
fn audit_chain(
    state: &State,
    chain: &Chain,
    seen: &mut u128,
    expected: Option<&[IssuedCid]>,
) -> Option<[u32; LOC_CID_COUNT as usize]> {
    let mut indices = [NONE; LOC_CID_COUNT as usize];
    if chain.len == 0 {
        return (chain.head == NONE && chain.tail == NONE && chain.encoded == 0).then_some(indices);
    }
    if chain.len as usize > state.nodes.len() {
        return None;
    }
    let mut current = chain.head;
    let mut last = NONE;
    let mut count = 0;
    let mut encoded = 0;
    while current != NONE {
        if count >= chain.len as usize {
            return None;
        }
        let node = state.nodes.get(current as usize)?;
        let value = node.value.as_ref()?;
        let bit = 1u128.checked_shl(current)?;
        if *seen & bit != 0 {
            return None;
        }
        *seen |= bit;
        if let Some(expected) = expected {
            // Prepared::finish preserves the existing reverse response order.
            let wanted = expected.get(expected.len().checked_sub(count + 1)?)?;
            if value.sequence != wanted.sequence
                || value.id != wanted.id
                || value.reset_token != wanted.reset_token
            {
                return None;
            }
            *indices.get_mut(count)? = current;
        }
        encoded += u32::from(node.encoded);
        count += 1;
        last = current;
        current = node.next;
    }
    (count == chain.len as usize && last == chain.tail && encoded == chain.encoded)
        .then_some(indices)
}
impl ReplySplice<'_, '_> {
    /// Target/source remain mutably borrowed since validation. The Root
    /// publication closure cannot replace either chain or its owner.
    pub(crate) fn commit(mut self) {
        if self.count == 0 {
            return;
        }
        let was_empty = self.target.len == 0;
        let mut next = self.target.head;
        for index in &self.indices[..self.count] {
            self.state.nodes[*index as usize].next = next;
            next = *index;
        }
        self.target.head = next;
        if was_empty {
            self.target.tail = self.indices[0];
        }
        self.target.len += self.source.len;
        self.target.encoded += self.source.encoded;
        self.source.head = NONE;
        self.source.tail = NONE;
        self.source.len = 0;
        self.source.encoded = 0;
    }
}
impl Drop for ReplySplice<'_, '_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.pool.quarantine();
        }
    }
}
