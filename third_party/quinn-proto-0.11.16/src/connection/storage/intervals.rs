//! Assembler's received-offset inventory: a slab AVL tree, not a second owner.
//! Nodes and their free list share one pre-admitted layout. Read-only overlap
//! queries precede mutation; insert/remove never allocate. Height bounds keep
//! peer-selected sparse offsets from turning admission into a linear scan.
use super::Slab;
use bytes::{BackingMeter, MeteredCopyError};
use std::ops::Range;
const NONE: usize = usize::MAX;
#[derive(Debug)]
pub(in crate::connection) struct Node {
    range: Range<u64>,
    left: usize,
    right: usize,
    height: u8,
}
#[derive(Debug)]
pub(in crate::connection) struct Intervals {
    nodes: Slab<Node>,
    root: usize,
    free: usize,
    bytes: u64,
}
impl Default for Intervals {
    fn default() -> Self {
        Self {
            nodes: Slab::default(),
            root: NONE,
            free: NONE,
            bytes: 0,
        }
    }
}
impl Intervals {
    pub(in crate::connection) fn prepare(
        &self,
        range: Range<u64>,
        meter: Option<&BackingMeter>,
    ) -> Result<Option<Slab<Node>>, MeteredCopyError> {
        if range.is_empty() || self.free != NONE || self.touching(range).is_some() {
            return Ok(None);
        }
        self.nodes.growth(1, meter)
    }
    pub(in crate::connection) fn insertion_funded(&self, range: Range<u64>) -> bool {
        self.free != NONE || self.nodes.values.len() < self.nodes.values.capacity() || !self.needs_node(range)
    }
    pub(in crate::connection) fn needs_node(&self, range: Range<u64>) -> bool {
        !range.is_empty() && self.touching(range).is_none()
    }
    /// A packet may insert several ranges before any are retired. Reserve a
    /// physical upper bound without changing the tree or its free list.
    pub(in crate::connection) fn prepare_insertions(
        &self, additional: usize, meter: &BackingMeter,
    ) -> Result<Option<Slab<Node>>, MeteredCopyError> {
        self.nodes.growth(additional, Some(meter))
    }
    pub(in crate::connection) fn install(&mut self, replacement: Option<Slab<Node>>) {
        self.nodes.install(replacement);
    }
    fn height(&self, id: usize) -> i16 {
        if id == NONE {
            0
        } else {
            self.nodes.values[id].height as i16
        }
    }
    fn update(&mut self, id: usize) {
        let n = &self.nodes.values[id];
        let h = 1 + self.height(n.left).max(self.height(n.right));
        self.nodes.values[id].height = h as u8;
    }
    fn rotate_left(&mut self, id: usize) -> usize {
        let next = self.nodes.values[id].right;
        self.nodes.values[id].right = self.nodes.values[next].left;
        self.nodes.values[next].left = id;
        self.update(id);
        self.update(next);
        next
    }
    fn rotate_right(&mut self, id: usize) -> usize {
        let next = self.nodes.values[id].left;
        self.nodes.values[id].left = self.nodes.values[next].right;
        self.nodes.values[next].right = id;
        self.update(id);
        self.update(next);
        next
    }
    fn balance(&mut self, id: usize) -> usize {
        if id == NONE {
            return id;
        }
        self.update(id);
        let (left, right) = (self.nodes.values[id].left, self.nodes.values[id].right);
        if self.height(left) - self.height(right) > 1 {
            let n = &self.nodes.values[left];
            if self.height(n.right) > self.height(n.left) {
                self.nodes.values[id].left = self.rotate_left(left);
            }
            self.rotate_right(id)
        } else if self.height(right) - self.height(left) > 1 {
            let n = &self.nodes.values[right];
            if self.height(n.left) > self.height(n.right) {
                self.nodes.values[id].right = self.rotate_right(right);
            }
            self.rotate_left(id)
        } else {
            id
        }
    }
    fn insert_node(&mut self, root: usize, id: usize) -> usize {
        if root == NONE {
            return id;
        }
        if self.nodes.values[id].range.start < self.nodes.values[root].range.start {
            self.nodes.values[root].left = self.insert_node(self.nodes.values[root].left, id);
        } else {
            self.nodes.values[root].right = self.insert_node(self.nodes.values[root].right, id);
        }
        self.balance(root)
    }
    fn release(&mut self, id: usize) {
        self.nodes.values[id].height = 0;
        self.nodes.values[id].left = self.free;
        self.free = id;
    }
    fn remove(&mut self, root: usize, start: u64) -> usize {
        if root == NONE {
            return root;
        }
        let range_start = self.nodes.values[root].range.start;
        if start < range_start {
            self.nodes.values[root].left = self.remove(self.nodes.values[root].left, start);
        } else if start > range_start {
            self.nodes.values[root].right = self.remove(self.nodes.values[root].right, start);
        } else {
            let (left, right) = (self.nodes.values[root].left, self.nodes.values[root].right);
            if left == NONE || right == NONE {
                self.release(root);
                return if left == NONE { right } else { left };
            }
            let mut successor = right;
            while self.nodes.values[successor].left != NONE {
                successor = self.nodes.values[successor].left;
            }
            let range = self.nodes.values[successor].range.clone();
            self.nodes.values[root].range = range.clone();
            self.nodes.values[root].right = self.remove(right, range.start);
        }
        self.balance(root)
    }
    fn at_or_before(&self, start: u64) -> Option<Range<u64>> {
        let (mut id, mut found) = (self.root, None);
        while id != NONE {
            let node = &self.nodes.values[id];
            if node.range.start <= start {
                found = Some(node.range.clone());
                id = node.right;
            } else {
                id = node.left;
            }
        }
        found
    }
    fn at_or_after(&self, start: u64) -> Option<Range<u64>> {
        let (mut id, mut found) = (self.root, None);
        while id != NONE {
            let node = &self.nodes.values[id];
            if node.range.start >= start {
                found = Some(node.range.clone());
                id = node.left;
            } else {
                id = node.right;
            }
        }
        found
    }
    fn touching(&self, range: Range<u64>) -> Option<Range<u64>> {
        self.at_or_before(range.start)
            .filter(|r| r.end >= range.start)
            .or_else(|| {
                self.at_or_after(range.start)
                    .filter(|r| r.start <= range.end)
            })
    }
    /// Caller has admitted one possible new node. Coalescing frees its own slot.
    pub(in crate::connection) fn insert(&mut self, mut range: Range<u64>) {
        if range.is_empty() {
            return;
        }
        while let Some(old) = self.touching(range.clone()) {
            range.start = range.start.min(old.start);
            range.end = range.end.max(old.end);
            self.bytes -= old.end - old.start;
            self.root = self.remove(self.root, old.start);
        }
        self.bytes += range.end - range.start;
        let node = Node {
            range,
            left: NONE,
            right: NONE,
            height: 1,
        };
        let id = if self.free != NONE {
            let id = self.free;
            self.free = self.nodes.values[id].left;
            self.nodes.values[id] = node;
            id
        } else {
            debug_assert!(self.nodes.values.len() < self.nodes.values.capacity());
            let id = self.nodes.values.len();
            self.nodes.values.push(node);
            id
        };
        self.root = self.insert_node(self.root, id);
    }
    pub(in crate::connection) fn min(&self) -> Option<u64> {
        self.at_or_after(0).map(|range| range.start)
    }
    pub(in crate::connection) fn pop_min(&mut self) -> Option<Range<u64>> {
        let range = self.at_or_after(0)?;
        self.root = self.remove(self.root, range.start);
        self.bytes -= range.end - range.start;
        Some(range)
    }
    pub(in crate::connection) fn bytes(&self) -> u64 { self.bytes }
    pub(in crate::connection) fn metadata_bytes(&self, meter: &BackingMeter) -> Option<usize> {
        self.inventory_matches(meter).then_some(())?;
        std::alloc::Layout::array::<Node>(self.nodes.values.capacity()).ok().map(|l| l.size())
    }
    pub(in crate::connection) fn has_native_layout(&self) -> bool { self.nodes.allowance.is_some() }
    pub(in crate::connection) fn layout_matches(&self, meter: &BackingMeter) -> bool {
        self.nodes.layout_matches(meter)
    }
    /// Cold/retirement verification: actual live tree plus actual free chain
    /// partition the initialized slab; independently sum ranges and heights.
    pub(in crate::connection) fn inventory_matches(&self, meter: &BackingMeter) -> bool {
        self.inventory_within(meter, u64::MAX)
    }
    pub(in crate::connection) fn inventory_within(&self, meter: &BackingMeter, end: u64) -> bool {
        fn walk(tree: &Intervals, id: usize, bounds: Range<u64>, depth: usize)
            -> Option<(usize, u64, i16)> {
            if id == NONE { return Some((0, 0, 0)); }
            if depth >= 128 { return None; }
            let n = tree.nodes.values.get(id)?;
            if n.range.is_empty() || n.range.start < bounds.start || n.range.end > bounds.end { return None; }
            let (lc, lb, lh) = walk(tree, n.left, bounds.start..n.range.start, depth + 1)?;
            let (rc, rb, rh) = walk(tree, n.right, n.range.end..bounds.end, depth + 1)?;
            let height = 1 + lh.max(rh);
            if (lh - rh).abs() > 1 || n.height as i16 != height { return None; }
            Some((lc.checked_add(rc)?.checked_add(1)?, lb.checked_add(rb)?.checked_add(n.range.end - n.range.start)?, height))
        }
        if !self.layout_matches(meter) { return false; }
        let Some((live, bytes, _)) = walk(self, self.root, 0..end, 0) else { return false; };
        let (mut free, mut count) = (self.free, 0usize);
        while free != NONE {
            let Some(n) = self.nodes.values.get(free) else { return false; };
            if n.height != 0 || count >= self.nodes.values.len() { return false; }
            count += 1; free = n.left;
        }
        live.checked_add(count) == Some(self.nodes.values.len()) && bytes == self.bytes
    }
    pub(in crate::connection) fn overlaps(&self, range: Range<u64>) -> Overlaps<'_> {
        Overlaps { set: self, range }
    }
}
pub(in crate::connection) struct Overlaps<'a> {
    set: &'a Intervals,
    range: Range<u64>,
}
impl Iterator for Overlaps<'_> {
    type Item = Range<u64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.range.is_empty() {
            return None;
        }
        let range = self
            .set
            .at_or_before(self.range.start)
            .filter(|r| r.end > self.range.start)
            .or_else(|| self.set.at_or_after(self.range.start))?;
        if range.start >= self.range.end {
            return None;
        }
        let overlap = range.start.max(self.range.start)..range.end.min(self.range.end);
        self.range.start = overlap.end;
        Some(overlap)
    }
}

#[cfg(test)]
#[path = "tests/intervals.rs"]
mod tests;
