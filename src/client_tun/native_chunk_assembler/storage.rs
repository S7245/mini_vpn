//! The assembler's ordered index and exact physical layout. One slab owns AVL
//! nodes, free slots and Bytes; no stream/queue authority or independent refund.
//! Growth prepares old and new layouts together, then moves without allocation.
//! Entry: Inventory::prepare/insert/remove_first; tests are in ../tests/storage.rs.
use bytes::{BackingBudgetReservation, BackingMeter, Bytes, MeteredCopyError};
use std::alloc::{Layout, alloc};

const NONE: usize = usize::MAX;
#[derive(Debug)]
struct Node {
    offset: u64,
    bytes: Bytes,
    left: usize,
    right: usize,
    height: u8,
}
#[derive(Debug, Default)]
pub(super) struct Slab {
    values: Vec<Node>,
    // Actual Vec storage drops before its original allowance.
    _allowance: Option<BackingBudgetReservation>,
    allocated_bytes: usize,
}
#[derive(Debug)]
pub(super) struct Inventory {
    slab: Slab,
    root: usize,
    free: usize,
    len: usize,
}
impl Default for Inventory {
    fn default() -> Self {
        Self {
            slab: Slab::default(),
            root: NONE,
            free: NONE,
            len: 0,
        }
    }
}
impl Inventory {
    pub(super) fn prepare(
        &self,
        offset: u64,
        meter: Option<&BackingMeter>,
    ) -> Result<Option<Slab>, MeteredCopyError> {
        if self.get(&offset).is_some() {
            return Ok(None);
        }
        self.prepare_next(meter)
    }
    pub(super) fn prepare_next(
        &self,
        meter: Option<&BackingMeter>,
    ) -> Result<Option<Slab>, MeteredCopyError> {
        if self.free != NONE || self.slab.values.len() < self.slab.values.capacity() {
            return Ok(None);
        }
        let capacity = self
            .slab
            .values
            .capacity()
            .checked_mul(2)
            .ok_or(MeteredCopyError::TooLarge)?
            .max(4);
        let layout = Layout::array::<Node>(capacity).map_err(|_| MeteredCopyError::TooLarge)?;
        let allowance = meter
            .map(|m| m.reserve_cold_layout(layout.size()))
            .transpose()?
            .flatten();
        // SAFETY: nonzero Node layout; Vec uses this exact capacity and layout.
        let ptr = unsafe { alloc(layout).cast::<Node>() };
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        Ok(Some(Slab {
            values: unsafe { Vec::from_raw_parts(ptr, 0, capacity) },
            _allowance: allowance,
            allocated_bytes: layout.size(),
        }))
    }
    pub(super) fn install(&mut self, new: Option<Slab>) {
        if let Some(mut new) = new {
            new.values.append(&mut self.slab.values);
            self.slab = new;
        }
    }
    /// Cold terminal audit, independent of the live markers used by DATA lookup.
    /// Strict ancestor bounds reject cycles/shared subtrees without a heap bitmap.
    pub(super) fn valid_inventory(&self, meter: Option<&BackingMeter>) -> bool {
        let Some(layout) = Layout::array::<Node>(self.slab.values.capacity()).ok() else {
            return false;
        };
        if layout.size() != self.slab.allocated_bytes
            || !match meter {
                Some(meter) => {
                    meter.cold_layout_matches(self.slab._allowance.as_ref(), layout.size())
                }
                None => self.slab._allowance.is_none(),
            }
            || (self.root == NONE) != (self.len == 0)
        {
            return false;
        }
        let mut stack = [(NONE, None::<u64>, None::<u64>); 128];
        let mut depth = 0usize;
        if self.root != NONE {
            stack[0] = (self.root, None, None);
            depth = 1;
        }
        let mut reachable = 0usize;
        while depth != 0 {
            depth -= 1;
            let (id, low, high) = stack[depth];
            let Some(node) = self.slab.values.get(id) else {
                return false;
            };
            if node.height == 0
                || node.bytes.is_empty()
                || low.is_some_and(|low| node.offset <= low)
                || high.is_some_and(|high| node.offset >= high)
                || reachable == self.slab.values.len()
            {
                return false;
            }
            reachable += 1;
            let child_height = |child: usize| {
                if child == NONE {
                    Some(0u8)
                } else {
                    self.slab
                        .values
                        .get(child)
                        .filter(|n| n.height != 0)
                        .map(|n| n.height)
                }
            };
            let (Some(left), Some(right)) = (child_height(node.left), child_height(node.right))
            else {
                return false;
            };
            if left.abs_diff(right) > 1 || left.max(right).checked_add(1) != Some(node.height) {
                return false;
            }
            for item in [
                (node.left, low, Some(node.offset)),
                (node.right, Some(node.offset), high),
            ] {
                if item.0 != NONE {
                    if depth == stack.len() {
                        return false;
                    }
                    stack[depth] = item;
                    depth += 1;
                }
            }
        }
        let physical_live = self.slab.values.iter().filter(|n| n.height != 0).count();
        if reachable != self.len || reachable != physical_live {
            return false;
        }
        let (mut free, mut free_count) = (self.free, 0usize);
        while free != NONE {
            let Some(node) = self.slab.values.get(free) else {
                return false;
            };
            if free_count == self.slab.values.len()
                || node.height != 0
                || !node.bytes.is_empty()
                || node.bytes.allocation_identity().is_some()
                || node.bytes.backing_meter().is_some()
            {
                return false;
            }
            free_count += 1;
            free = node.left;
        }
        free_count.checked_add(reachable) == Some(self.slab.values.len())
    }

    pub(super) fn has_storage(&self) -> bool {
        self.slab.values.capacity() != 0
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub(super) fn get(&self, offset: &u64) -> Option<&Bytes> {
        let mut id = self.root;
        while id != NONE {
            let n = &self.slab.values[id];
            match offset.cmp(&n.offset) {
                std::cmp::Ordering::Less => id = n.left,
                std::cmp::Ordering::Greater => id = n.right,
                std::cmp::Ordering::Equal => return Some(&n.bytes),
            }
        }
        None
    }
    pub(super) fn first(&self) -> Option<(u64, &Bytes)> {
        self.at_or_after(0)
    }
    pub(super) fn at_or_after(&self, offset: u64) -> Option<(u64, &Bytes)> {
        let (mut id, mut found) = (self.root, NONE);
        while id != NONE {
            let n = &self.slab.values[id];
            if n.offset >= offset {
                found = id;
                id = n.left;
            } else {
                id = n.right;
            }
        }
        (found != NONE).then(|| {
            let n = &self.slab.values[found];
            (n.offset, &n.bytes)
        })
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &Bytes> {
        self.slab
            .values
            .iter()
            .filter(|n| n.height != 0)
            .map(|n| &n.bytes)
    }
    fn height(&self, id: usize) -> i16 {
        if id == NONE {
            0
        } else {
            self.slab.values[id].height as i16
        }
    }
    fn update(&mut self, id: usize) {
        let n = &self.slab.values[id];
        let h = 1 + self.height(n.left).max(self.height(n.right));
        self.slab.values[id].height = h as u8;
    }
    fn left(&mut self, id: usize) -> usize {
        let next = self.slab.values[id].right;
        self.slab.values[id].right = self.slab.values[next].left;
        self.slab.values[next].left = id;
        self.update(id);
        self.update(next);
        next
    }
    fn right(&mut self, id: usize) -> usize {
        let next = self.slab.values[id].left;
        self.slab.values[id].left = self.slab.values[next].right;
        self.slab.values[next].right = id;
        self.update(id);
        self.update(next);
        next
    }
    fn balance(&mut self, id: usize) -> usize {
        self.update(id);
        let (left, right) = (self.slab.values[id].left, self.slab.values[id].right);
        if self.height(left) - self.height(right) > 1 {
            let n = &self.slab.values[left];
            if self.height(n.right) > self.height(n.left) {
                self.slab.values[id].left = self.left(left);
            }
            self.right(id)
        } else if self.height(right) - self.height(left) > 1 {
            let n = &self.slab.values[right];
            if self.height(n.left) > self.height(n.right) {
                self.slab.values[id].right = self.right(right);
            }
            self.left(id)
        } else {
            id
        }
    }
    fn insert_node(&mut self, root: usize, id: usize) -> usize {
        if root == NONE {
            return id;
        }
        if self.slab.values[id].offset < self.slab.values[root].offset {
            self.slab.values[root].left = self.insert_node(self.slab.values[root].left, id);
        } else {
            self.slab.values[root].right = self.insert_node(self.slab.values[root].right, id);
        }
        self.balance(root)
    }
    /// Requires prepare/install, or the free node just returned by remove_first.
    pub(super) fn insert(&mut self, offset: u64, bytes: Bytes) -> Option<Bytes> {
        let mut found = self.root;
        while found != NONE {
            let n = &self.slab.values[found];
            if offset == n.offset {
                return Some(std::mem::replace(&mut self.slab.values[found].bytes, bytes));
            }
            found = if offset < n.offset { n.left } else { n.right };
        }
        let node = Node {
            offset,
            bytes,
            left: NONE,
            right: NONE,
            height: 1,
        };
        let id = if self.free != NONE {
            let id = self.free;
            self.free = self.slab.values[id].left;
            self.slab.values[id] = node;
            id
        } else {
            assert!(
                self.slab.values.len() < self.slab.values.capacity(),
                "missing assembler admission"
            );
            let id = self.slab.values.len();
            self.slab.values.push(node);
            id
        };
        self.root = self.insert_node(self.root, id);
        self.len += 1;
        None
    }
    fn take_first(&mut self, root: usize) -> (usize, u64, Bytes) {
        let left = self.slab.values[root].left;
        if left == NONE {
            let n = &mut self.slab.values[root];
            let (right, offset, bytes) = (n.right, n.offset, std::mem::take(&mut n.bytes));
            n.height = 0;
            n.left = self.free;
            n.right = NONE;
            self.free = root;
            self.len -= 1;
            return (right, offset, bytes);
        }
        let (left, offset, bytes) = self.take_first(left);
        self.slab.values[root].left = left;
        (self.balance(root), offset, bytes)
    }
    pub(super) fn remove_first(&mut self) -> Option<(u64, Bytes)> {
        if self.root == NONE {
            return None;
        }
        let (root, offset, bytes) = self.take_first(self.root);
        self.root = root;
        Some((offset, bytes))
    }
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod tests;
