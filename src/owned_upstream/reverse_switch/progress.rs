//! Original reducer-derived reverse progress and bounded stall scanning.
//!
//! Owns the complete progress mutation/retirement/reset/scan Implementation.
//! Capacity/state and issuer capability Interfaces remain in the parent.
//! Preserves revision, generation, error ordering and bounded scan work.
//! Concrete slots allocate only at construction; their AVL/free partition stays local.
//! Entries are OwnerReverseProgress methods; tests cover native storage and real C pages.

use super::*;
use crate::resumable::effect_storage::{EffectStorageError, allocate_exact_vec};
use std::alloc::{Layout, handle_alloc_error};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
struct ProgressNode {
    key: DirectionalFlowKey,
    value: OwnerDirectionalProgress,
    left: Option<usize>,
    right: Option<usize>,
    parent: Option<usize>,
    height: usize,
}

#[derive(Clone, Copy)]
enum ProgressSlot {
    Vacant { next: Option<usize> },
    Occupied(ProgressNode),
}

pub(super) struct OwnerProgressStorage {
    slots: Box<[ProgressSlot]>,
    root: Option<usize>,
    free: Option<usize>,
    count: usize,
}

impl OwnerProgressStorage {
    fn cold_layout(capacity: usize) -> Result<(Layout, Layout), OwnerProgressError> {
        let slots = Layout::array::<ProgressSlot>(capacity)
            .map_err(|_| OwnerProgressError::StorageLayout)?;
        let combined = Layout::new::<OwnerReverseProgress>()
            .extend(slots)
            .map_err(|_| OwnerProgressError::StorageLayout)?
            .0
            .pad_to_align();
        Ok((slots, combined))
    }

    fn cold(capacity: usize) -> Result<Self, OwnerProgressError> {
        // Reject the complete original owner geometry before the backing allocator.
        // A later Root aggregate must count this embedded header only once.
        let (layout, _) = Self::cold_layout(capacity)?;
        let mut slots = match allocate_exact_vec(capacity) {
            Ok(slots) => slots,
            Err(EffectStorageError::Reserve) => handle_alloc_error(layout),
            Err(_) => return Err(OwnerProgressError::StorageLayout),
        };
        for index in 0..capacity {
            slots.push(ProgressSlot::Vacant {
                next: (index + 1 < capacity).then_some(index + 1),
            });
        }
        Ok(Self {
            slots: slots.into_boxed_slice(),
            root: None,
            free: (capacity != 0).then_some(0),
            count: 0,
        })
    }

    fn node(&self, index: usize) -> &ProgressNode {
        match &self.slots[index] {
            ProgressSlot::Occupied(node) => node,
            ProgressSlot::Vacant { .. } => unreachable!("private AVL edge refers to a vacant slot"),
        }
    }

    fn node_mut(&mut self, index: usize) -> &mut ProgressNode {
        match &mut self.slots[index] {
            ProgressSlot::Occupied(node) => node,
            ProgressSlot::Vacant { .. } => unreachable!("private AVL edge refers to a vacant slot"),
        }
    }

    fn find(&self, key: DirectionalFlowKey) -> Option<usize> {
        let mut cursor = self.root;
        while let Some(index) = cursor {
            let node = self.node(index);
            cursor = match key.cmp(&node.key) {
                std::cmp::Ordering::Less => node.left,
                std::cmp::Ordering::Greater => node.right,
                std::cmp::Ordering::Equal => return Some(index),
            };
        }
        None
    }

    fn get(&self, key: &DirectionalFlowKey) -> Option<&OwnerDirectionalProgress> {
        self.find(*key).map(|index| &self.node(index).value)
    }

    fn get_mut(&mut self, key: &DirectionalFlowKey) -> Option<&mut OwnerDirectionalProgress> {
        self.find(*key).map(|index| &mut self.node_mut(index).value)
    }

    fn len(&self) -> usize {
        self.count
    }

    fn height(&self, index: Option<usize>) -> usize {
        index.map_or(0, |index| self.node(index).height)
    }

    fn update_height(&mut self, index: usize) {
        let node = *self.node(index);
        self.node_mut(index).height = 1 + self.height(node.left).max(self.height(node.right));
    }

    fn set_child(&mut self, parent: usize, child: Option<usize>, left: bool) {
        if left {
            self.node_mut(parent).left = child;
        } else {
            self.node_mut(parent).right = child;
        }
        if let Some(child) = child {
            self.node_mut(child).parent = Some(parent);
        }
    }

    fn rotate(&mut self, root: usize, left: bool) -> usize {
        let old = *self.node(root);
        let Some(pivot) = (if left { old.right } else { old.left }) else {
            return root;
        };
        let inner = if left {
            self.node(pivot).left
        } else {
            self.node(pivot).right
        };
        self.set_child(root, inner, !left);
        self.set_child(pivot, Some(root), left);
        self.node_mut(pivot).parent = old.parent;
        self.update_height(root);
        self.update_height(pivot);
        pivot
    }

    fn balance(&mut self, root: usize) -> usize {
        self.update_height(root);
        let node = *self.node(root);
        let left = self.height(node.left);
        let right = self.height(node.right);
        if left > right + 1 {
            if let Some(child) = node.left {
                let inner = *self.node(child);
                if self.height(inner.right) > self.height(inner.left) {
                    let rotated = self.rotate(child, true);
                    self.set_child(root, Some(rotated), true);
                }
            }
            self.rotate(root, false)
        } else if right > left + 1 {
            if let Some(child) = node.right {
                let inner = *self.node(child);
                if self.height(inner.left) > self.height(inner.right) {
                    let rotated = self.rotate(child, false);
                    self.set_child(root, Some(rotated), false);
                }
            }
            self.rotate(root, true)
        } else {
            root
        }
    }

    fn insert_at(&mut self, root: Option<usize>, index: usize) -> usize {
        let Some(root) = root else { return index };
        let left = self.node(index).key < self.node(root).key;
        let child = if left {
            self.node(root).left
        } else {
            self.node(root).right
        };
        let inserted = self.insert_at(child, index);
        self.set_child(root, Some(inserted), left);
        self.balance(root)
    }

    // Parent scope preserves the original test's exact corrupt-entry fixture;
    // production calls only after duplicate/capacity/revision preflight.
    pub(super) fn insert(
        &mut self,
        key: DirectionalFlowKey,
        value: OwnerDirectionalProgress,
    ) -> Result<(), OwnerProgressError> {
        let index = self.free.ok_or(OwnerProgressError::StorageCorrupt)?;
        let Some(ProgressSlot::Vacant { next }) = self.slots.get(index).copied() else {
            return Err(OwnerProgressError::StorageCorrupt);
        };
        self.slots[index] = ProgressSlot::Occupied(ProgressNode {
            key,
            value,
            left: None,
            right: None,
            parent: None,
            height: 1,
        });
        self.free = next;
        let root = self.insert_at(self.root, index);
        self.node_mut(root).parent = None;
        self.root = Some(root);
        self.count += 1;
        Ok(())
    }

    fn minimum(&self, mut index: usize) -> usize {
        while let Some(left) = self.node(index).left {
            index = left;
        }
        index
    }

    fn after(&self, key: Option<DirectionalFlowKey>) -> Option<usize> {
        let mut cursor = self.root;
        let mut found = None;
        while let Some(index) = cursor {
            let node = self.node(index);
            if key.is_none_or(|key| key < node.key) {
                found = Some(index);
                cursor = node.left;
            } else {
                cursor = node.right;
            }
        }
        found
    }

    fn successor(&self, mut index: usize) -> Option<usize> {
        if let Some(right) = self.node(index).right {
            return Some(self.minimum(right));
        }
        while let Some(parent) = self.node(index).parent {
            if self.node(parent).left == Some(index) {
                return Some(parent);
            }
            index = parent;
        }
        None
    }

    fn release(&mut self, index: usize) {
        self.slots[index] = ProgressSlot::Vacant { next: self.free };
        self.free = Some(index);
        self.count -= 1;
    }

    fn remove_at(&mut self, root: usize, key: DirectionalFlowKey) -> Option<usize> {
        let node = *self.node(root);
        if key == node.key {
            match (node.left, node.right) {
                (Some(_), Some(right)) => {
                    let replacement = *self.node(self.minimum(right));
                    self.node_mut(root).key = replacement.key;
                    self.node_mut(root).value = replacement.value;
                    let remaining = self.remove_at(right, replacement.key);
                    self.set_child(root, remaining, false);
                }
                (left, right) => {
                    let child = left.or(right);
                    if let Some(child) = child {
                        self.node_mut(child).parent = node.parent;
                    }
                    self.release(root);
                    return child;
                }
            }
        } else {
            let left = key < node.key;
            let child = if left { node.left } else { node.right };
            if let Some(child) = child {
                let remaining = self.remove_at(child, key);
                self.set_child(root, remaining, left);
            }
        }
        Some(self.balance(root))
    }

    fn retire_direction(&mut self, key: DirectionalFlowKey) {
        if let Some(root) = self.root {
            self.root = self.remove_at(root, key);
        }
        if let Some(root) = self.root {
            self.node_mut(root).parent = None;
        }
    }

    fn clear_subtree(&mut self, index: usize) {
        let node = *self.node(index);
        if let Some(left) = node.left {
            self.clear_subtree(left);
        }
        if let Some(right) = node.right {
            self.clear_subtree(right);
        }
        self.release(index);
    }

    fn clear(&mut self) {
        if let Some(root) = self.root.take() {
            self.clear_subtree(root);
        }
    }

    // The independent depth/visit bounds do NOT trust any stored height. Strict
    // ancestor bounds plus exact parents prohibit shared nodes; enum states keep
    // the tree disjoint from the bounded free chain. Exact totals prove coverage.
    fn audit_tree(
        &self,
        index: Option<usize>,
        parent: Option<usize>,
        bounds: (Option<DirectionalFlowKey>, Option<DirectionalFlowKey>),
        depth: usize,
        remaining: &mut usize,
    ) -> Result<usize, OwnerProgressError> {
        let Some(index) = index else { return Ok(0) };
        if depth >= 2 * usize::BITS as usize || *remaining == 0 {
            return Err(OwnerProgressError::StorageCorrupt);
        }
        *remaining -= 1;
        let Some(ProgressSlot::Occupied(node)) = self.slots.get(index) else {
            return Err(OwnerProgressError::StorageCorrupt);
        };
        if node.parent != parent
            || bounds.0.is_some_and(|key| node.key <= key)
            || bounds.1.is_some_and(|key| node.key >= key)
        {
            return Err(OwnerProgressError::StorageCorrupt);
        }
        let left = self.audit_tree(
            node.left,
            Some(index),
            (bounds.0, Some(node.key)),
            depth + 1,
            remaining,
        )?;
        let right = self.audit_tree(
            node.right,
            Some(index),
            (Some(node.key), bounds.1),
            depth + 1,
            remaining,
        )?;
        let height = 1 + left.max(right);
        if left.abs_diff(right) > 1 || node.height != height {
            return Err(OwnerProgressError::StorageCorrupt);
        }
        Ok(height)
    }

    fn audit(&self, capacity: usize) -> Result<(), OwnerProgressError> {
        if capacity != self.slots.len() || self.count > capacity {
            return Err(OwnerProgressError::StorageCorrupt);
        }
        let mut remaining = capacity;
        self.audit_tree(self.root, None, (None, None), 0, &mut remaining)?;
        if capacity - remaining != self.count {
            return Err(OwnerProgressError::StorageCorrupt);
        }
        let mut cursor = self.free;
        while let Some(index) = cursor {
            if remaining == 0 {
                return Err(OwnerProgressError::StorageCorrupt);
            }
            let Some(ProgressSlot::Vacant { next }) = self.slots.get(index) else {
                return Err(OwnerProgressError::StorageCorrupt);
            };
            remaining -= 1;
            cursor = *next;
        }
        if remaining != 0 {
            return Err(OwnerProgressError::StorageCorrupt);
        }
        Ok(())
    }
}

impl OwnerProgressCapacity {
    pub(crate) fn checked(
        max_live_flows: usize,
        max_terminal_tombstones: usize,
    ) -> Result<Self, OwnerProgressCapacityError> {
        let max_tracked_flows = max_live_flows
            .checked_add(max_terminal_tombstones)
            .ok_or(OwnerProgressCapacityError::TrackedFlowCapacityOverflow)?;
        let max_entries = max_tracked_flows
            .checked_mul(2)
            .ok_or(OwnerProgressCapacityError::DirectionalEntryCapacityOverflow)?;
        Ok(Self { max_entries })
    }

    pub(crate) const fn max_entries(self) -> usize {
        self.max_entries
    }
}

impl OwnerReverseProgress {
    #[cfg(test)]
    pub(crate) fn new(max_flows: usize) -> Result<Self, OwnerProgressError> {
        let capacity = OwnerProgressCapacity::checked(max_flows, 0)
            .map_err(|_| OwnerProgressError::CapacityLimitOverflow)?;
        Self::from_capacity(capacity)
    }

    pub(crate) fn from_capacity(
        capacity: OwnerProgressCapacity,
    ) -> Result<Self, OwnerProgressError> {
        Ok(Self {
            capacity,
            entries: OwnerProgressStorage::cold(capacity.max_entries())?,
            revision: 0,
        })
    }

    pub(crate) fn cold_audit(&self) -> Result<(), OwnerProgressError> {
        self.entries.audit(self.capacity.max_entries())
    }

    #[cfg(test)]
    pub(crate) fn storage_measurement_for_test(
        &self,
    ) -> Result<(usize, usize, usize, usize), OwnerProgressError> {
        self.cold_audit()?;
        Ok((
            self.entries.slots.as_ptr() as usize,
            self.max_entries(),
            self.len(),
            self.max_entries() - self.len(),
        ))
    }

    /// Clears all session-owned progress after the reducer proves whole-session
    /// expiry. The original typed envelope remains installed; reset cannot
    /// accidentally fall back to a live-flow-only formula.
    pub(crate) fn reset_for_session_expiry(&mut self) {
        self.entries.clear();
        self.revision = 0;
    }

    pub(crate) fn register_live(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<(), OwnerProgressError> {
        let key = DirectionalFlowKey::new(flow_id, direction);
        if let Some(existing) = self.entries.get(&key) {
            return if existing.generation == generation && !existing.terminal {
                Ok(())
            } else {
                Err(OwnerProgressError::FlowAlreadyRegistered)
            };
        }
        if self.entries.len() == self.capacity.max_entries() {
            return Err(OwnerProgressError::CapacityExceeded {
                max_entries: self.capacity.max_entries(),
            });
        }
        let next_revision = self.next_revision()?;
        self.entries.insert(
            key,
            OwnerDirectionalProgress {
                generation,
                outstanding_episode: 0,
                oldest_unacknowledged: None,
                outstanding_since: None,
                latest_positive_ack: None,
                terminal: false,
            },
        )?;
        self.revision = next_revision;
        Ok(())
    }

    pub(crate) fn observe_outstanding(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
        oldest_unacknowledged: ByteOffset,
        observed_at: SimTime,
    ) -> Result<(), OwnerProgressError> {
        let next_revision = self.next_revision()?;
        {
            let entry = self.live_entry_mut(generation, flow_id, direction)?;
            if let Some(previous) = entry.oldest_unacknowledged
                && oldest_unacknowledged < previous
            {
                return Err(OwnerProgressError::OldestOutstandingRegressed {
                    previous,
                    observed: oldest_unacknowledged,
                });
            }
            let previous_observation = entry
                .latest_positive_ack
                .map(|ack| ack.arrived_at)
                .into_iter()
                .chain(entry.outstanding_since)
                .max();
            if let Some(previous) = previous_observation {
                observed_at
                    .checked_duration_since(previous)
                    .map_err(OwnerProgressError::Clock)?;
            }
            if entry.outstanding_since.is_none() {
                entry.outstanding_episode = entry
                    .outstanding_episode
                    .checked_add(1)
                    .ok_or(OwnerProgressError::OutstandingEpisodeExhausted)?;
                entry.outstanding_since = Some(observed_at);
            }
            entry.oldest_unacknowledged = Some(oldest_unacknowledged);
        }
        self.revision = next_revision;
        Ok(())
    }

    pub(crate) fn observe_positive_ack(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
        acknowledged_to: ByteOffset,
        arrived_at: SimTime,
    ) -> Result<(), OwnerProgressError> {
        let next_revision = self.next_revision()?;
        {
            let entry = self.live_entry_mut(generation, flow_id, direction)?;
            let oldest = entry
                .oldest_unacknowledged
                .ok_or(OwnerProgressError::NoOutstandingReplay)?;
            let previous_offset = entry
                .latest_positive_ack
                .map_or(oldest, |ack| std::cmp::max(oldest, ack.offset));
            if acknowledged_to <= previous_offset {
                return Err(OwnerProgressError::ApplicationAckDidNotAdvance {
                    previous: previous_offset,
                    observed: acknowledged_to,
                });
            }
            let previous_time = entry
                .latest_positive_ack
                .map(|ack| ack.arrived_at)
                .or(entry.outstanding_since)
                .ok_or(OwnerProgressError::NoOutstandingReplay)?;
            arrived_at
                .checked_duration_since(previous_time)
                .map_err(OwnerProgressError::Clock)?;
            entry.latest_positive_ack = Some(PositiveApplicationAck {
                offset: acknowledged_to,
                arrived_at,
            });
        }
        self.revision = next_revision;
        Ok(())
    }

    pub(crate) fn clear_outstanding(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<(), OwnerProgressError> {
        let next_revision = self.next_revision()?;
        {
            let entry = self.live_entry_mut(generation, flow_id, direction)?;
            entry.oldest_unacknowledged = None;
            entry.outstanding_since = None;
        }
        self.revision = next_revision;
        Ok(())
    }

    pub(crate) fn mark_terminal(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<(), OwnerProgressError> {
        let next_revision = self.next_revision()?;
        self.exact_entry_mut(generation, flow_id, direction)?
            .terminal = true;
        self.revision = next_revision;
        Ok(())
    }

    /// Rebinds one exact retained direction to a strictly newer generation.
    /// The reducer-owned outstanding and ACK timestamps survive replacement;
    /// callers must separately prove that `committed_generation` is the
    /// executor's current committed generation.
    pub(crate) fn rebase_direction(
        &mut self,
        committed_generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<(), OwnerProgressError> {
        let next_revision = self.next_revision()?;
        {
            let entry = self
                .entries
                .get_mut(&DirectionalFlowKey::new(flow_id, direction))
                .ok_or(OwnerProgressError::UnknownFlowDirection)?;
            if committed_generation <= entry.generation {
                return Err(OwnerProgressError::GenerationDidNotAdvance {
                    previous: entry.generation,
                    observed: committed_generation,
                });
            }
            entry.generation = committed_generation;
        }
        self.revision = next_revision;
        Ok(())
    }

    pub(crate) fn retire_flow(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
    ) -> Result<usize, OwnerProgressError> {
        let keys = Self::flow_keys(flow_id);
        if keys.iter().any(|key| {
            self.entries
                .get(key)
                .is_some_and(|entry| entry.generation != generation)
        }) {
            return Err(OwnerProgressError::GenerationMismatch);
        }
        let next_revision = self.next_revision()?;
        let before = self.entries.len();
        for key in keys {
            self.entries.retire_direction(key);
        }
        let removed = before - self.entries.len();
        if removed == 0 {
            return Err(OwnerProgressError::UnknownFlowDirection);
        }
        self.revision = next_revision;
        Ok(removed)
    }

    /// Releases all retained directions only after the caller proves that a
    /// flow-wide terminal-grace retirement committed at `generation`.
    /// Directions retained from an older replacement generation are safe to
    /// release; a direction from a newer generation fails closed. An
    /// untracked flow is a successful no-op because progress is admitted only
    /// when reducer replay or a directional terminal is observed.
    pub(crate) fn retire_flow_after_committed_generation(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
    ) -> Result<usize, OwnerProgressError> {
        let mut tracked = false;
        let keys = Self::flow_keys(flow_id);
        for entry in keys.iter().filter_map(|key| self.entries.get(key)) {
            tracked = true;
            if entry.generation > generation {
                return Err(OwnerProgressError::GenerationDidNotAdvance {
                    previous: entry.generation,
                    observed: generation,
                });
            }
        }
        if !tracked {
            return Ok(0);
        }
        let next_revision = self.next_revision()?;
        let before = self.entries.len();
        for key in keys {
            self.entries.retire_direction(key);
        }
        let removed = before - self.entries.len();
        debug_assert_ne!(removed, 0, "tracked flow must retire at least one entry");
        self.revision = next_revision;
        Ok(removed)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    fn flow_keys(flow_id: SessionFlowId) -> [DirectionalFlowKey; 2] {
        [
            DirectionalFlowKey::new(flow_id, Direction::ClientToTarget),
            DirectionalFlowKey::new(flow_id, Direction::TargetToClient),
        ]
    }

    pub(crate) const fn max_entries(&self) -> usize {
        self.capacity.max_entries()
    }

    pub(super) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn direction_snapshot(
        &self,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<OwnerReverseDirectionSnapshot, OwnerProgressError> {
        let entry = self
            .entries
            .get(&DirectionalFlowKey::new(flow_id, direction))
            .ok_or(OwnerProgressError::UnknownFlowDirection)?;
        Ok(OwnerReverseDirectionSnapshot {
            oldest_unacknowledged: entry.oldest_unacknowledged,
            outstanding_since: entry.outstanding_since,
            latest_positive_ack: entry
                .latest_positive_ack
                .map(|ack| (ack.offset, ack.arrived_at)),
            terminal: entry.terminal,
        })
    }

    /// Advances one fixed-work pass over the reducer-derived registry. The
    /// caller-owned cursor is invalidated by any successful progress mutation,
    /// so a cross-turn scan never combines facts from different revisions.
    pub(super) fn scan_reverse_stall(
        &self,
        generation: LegGeneration,
        stall_after: Duration,
        scan: &mut Option<OwnerReverseStallScanCursor>,
    ) -> Result<OwnerReverseStallScan, OwnerProgressError> {
        if scan
            .as_ref()
            .is_none_or(|cursor| cursor.revision != self.revision)
        {
            *scan = Some(OwnerReverseStallScanCursor::new(self.revision));
        }
        let cursor = scan.get_or_insert_with(|| OwnerReverseStallScanCursor::new(self.revision));
        let mut next = self.entries.after(cursor.last_key);
        for _ in 0..OWNER_REVERSE_SCAN_BUDGET_PER_TURN {
            let Some(index) = next else { break };
            let node = self.entries.node(index);
            let candidate =
                Self::reverse_stall_candidate(node.key, &node.value, generation, stall_after)?;
            let scanned_entries = cursor
                .scanned_entries
                .checked_add(1)
                .ok_or(OwnerProgressError::ScanCountOverflow)?;
            // Commit the scan cursor only after this exact entry has been
            // classified successfully.  A fallible entry therefore remains
            // the next retry target instead of being silently skipped.
            cursor.last_key = Some(node.key);
            cursor.scanned_entries = scanned_entries;
            next = self.entries.successor(index);
            let Some(candidate) = candidate else {
                continue;
            };
            if cursor.earliest.as_ref().is_none_or(|selected| {
                (candidate.deadline, candidate.flow_id, candidate.direction)
                    < (selected.deadline, selected.flow_id, selected.direction)
            }) {
                cursor.earliest = Some(candidate);
            }
        }
        if cursor.scanned_entries < self.entries.len() {
            return Ok(OwnerReverseStallScan::Scanning);
        }
        let Some(completed) = scan.take() else {
            return Err(OwnerProgressError::ScanCursorInvariant);
        };
        Ok(OwnerReverseStallScan::Complete(completed.earliest))
    }

    fn reverse_stall_candidate(
        key: DirectionalFlowKey,
        entry: &OwnerDirectionalProgress,
        generation: LegGeneration,
        stall_after: Duration,
    ) -> Result<Option<OwnerReverseStallCandidate>, OwnerProgressError> {
        if key.direction != Direction::TargetToClient
            || entry.generation != generation
            || entry.terminal
        {
            return Ok(None);
        }
        let Some(oldest_unacknowledged) = entry.oldest_unacknowledged else {
            return Ok(None);
        };
        let outstanding_since = entry
            .outstanding_since
            .ok_or(OwnerProgressError::MissingOutstandingAdmissionTime)?;
        let (baseline_offset, baseline_at) = match entry.latest_positive_ack {
            Some(ack) if ack.arrived_at >= outstanding_since => (Some(ack.offset), ack.arrived_at),
            _ => (None, outstanding_since),
        };
        let deadline = baseline_at
            .checked_add(stall_after)
            .map_err(OwnerProgressError::Clock)?;
        Ok(Some(OwnerReverseStallCandidate {
            generation,
            flow_id: key.flow_id,
            direction: key.direction,
            oldest_unacknowledged,
            baseline_offset,
            baseline_at,
            outstanding_episode: entry.outstanding_episode,
            deadline,
        }))
    }

    pub(super) fn exact_reverse_stall_candidate(
        &self,
        fingerprint: StallFingerprint,
        stall_after: Duration,
    ) -> Result<Option<OwnerReverseStallCandidate>, OwnerProgressError> {
        let key = DirectionalFlowKey::new(fingerprint.flow_id, fingerprint.direction);
        let entry = self
            .entries
            .get(&key)
            .ok_or(OwnerProgressError::UnknownFlowDirection)?;
        Self::reverse_stall_candidate(key, entry, fingerprint.generation, stall_after)
    }

    fn next_revision(&self) -> Result<u64, OwnerProgressError> {
        self.revision
            .checked_add(1)
            .ok_or(OwnerProgressError::RevisionExhausted)
    }

    pub(super) fn exact_entry(
        &self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<&OwnerDirectionalProgress, OwnerProgressError> {
        let entry = self
            .entries
            .get(&DirectionalFlowKey::new(flow_id, direction))
            .ok_or(OwnerProgressError::UnknownFlowDirection)?;
        if entry.generation != generation {
            return Err(OwnerProgressError::GenerationMismatch);
        }
        Ok(entry)
    }

    fn exact_entry_mut(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<&mut OwnerDirectionalProgress, OwnerProgressError> {
        let entry = self
            .entries
            .get_mut(&DirectionalFlowKey::new(flow_id, direction))
            .ok_or(OwnerProgressError::UnknownFlowDirection)?;
        if entry.generation != generation {
            return Err(OwnerProgressError::GenerationMismatch);
        }
        Ok(entry)
    }

    fn live_entry_mut(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<&mut OwnerDirectionalProgress, OwnerProgressError> {
        let entry = self.exact_entry_mut(generation, flow_id, direction)?;
        if entry.terminal {
            return Err(OwnerProgressError::TerminalFlowDirection);
        }
        Ok(entry)
    }
}
