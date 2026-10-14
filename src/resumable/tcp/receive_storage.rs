//! Original receiver's nonmoving AVL nodes and recycled vacant storage.
//! Owns requested Node Layout, preparation, ordering and iterative free-chain Drop.
//! No Session policy, generic comparator, payload allocation or secondary owner.
//! Entry: ReceiveRanges; private tests exercise rotations, rollback and reuse.

#[cfg(test)]
use super::TcpReceiveWindow;
use super::{OwnedPayload, TcpOwnershipError};
use crate::resumable::effect_storage::{EffectStorageError, allocate_exact_vec};
use std::alloc::Layout;

#[cfg(test)]
mod tests;

type Link = Option<Box<[Node; 1]>>;

struct Node {
    key: u64,
    payload: OwnedPayload,
    left: Link,
    right: Link,
    height: u8,
}

#[cfg(test)]
impl Drop for Node {
    fn drop(&mut self) {
        LIVE_NODES.set(LIVE_NODES.get() - 1);
    }
}

#[derive(Default)]
pub(super) struct ReceiveRanges {
    root: Link,
    free: FreeNodes,
    len: usize,
}

impl std::fmt::Debug for ReceiveRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveRanges")
            .field("live", &self.len)
            .field("free", &self.free.len)
            .finish()
    }
}

#[derive(Default)]
pub(super) struct FreeNodes {
    head: Link,
    len: usize,
}

impl FreeNodes {
    fn push(&mut self, mut node: Box<[Node; 1]>) {
        debug_assert!(node[0].left.is_none() && node[0].right.is_none());
        node[0].left = self.head.take();
        self.head = Some(node);
        self.len += 1;
    }

    fn pop(&mut self) -> Link {
        let mut node = self.head.take()?;
        self.head = node[0].left.take();
        self.len -= 1;
        Some(node)
    }
}

impl Drop for FreeNodes {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

fn height(link: &Link) -> u8 {
    link.as_ref().map_or(0, |n| n[0].height)
}
fn refresh(node: &mut Node) {
    node.height = 1 + height(&node.left).max(height(&node.right));
}

fn rotate_left(mut root: Box<[Node; 1]>) -> Box<[Node; 1]> {
    let Some(mut next) = root[0].right.take() else {
        return root;
    };
    root[0].right = next[0].left.take();
    refresh(&mut root[0]);
    next[0].left = Some(root);
    refresh(&mut next[0]);
    next
}

fn rotate_right(mut root: Box<[Node; 1]>) -> Box<[Node; 1]> {
    let Some(mut next) = root[0].left.take() else {
        return root;
    };
    root[0].left = next[0].right.take();
    refresh(&mut root[0]);
    next[0].right = Some(root);
    refresh(&mut next[0]);
    next
}

fn balanced(mut root: Box<[Node; 1]>) -> Box<[Node; 1]> {
    refresh(&mut root[0]);
    if height(&root[0].left) > height(&root[0].right) + 1 {
        if let Some(left) = root[0].left.as_ref()
            && height(&left[0].right) > height(&left[0].left)
        {
            root[0].left = root[0].left.take().map(rotate_left);
        }
        rotate_right(root)
    } else if height(&root[0].right) > height(&root[0].left) + 1 {
        if let Some(right) = root[0].right.as_ref()
            && height(&right[0].left) > height(&right[0].right)
        {
            root[0].right = root[0].right.take().map(rotate_right);
        }
        rotate_left(root)
    } else {
        root
    }
}

fn insert(root: Link, node: Box<[Node; 1]>) -> Box<[Node; 1]> {
    let Some(mut root) = root else {
        return node;
    };
    if node[0].key < root[0].key {
        root[0].left = Some(insert(root[0].left.take(), node));
    } else {
        debug_assert_ne!(node[0].key, root[0].key);
        root[0].right = Some(insert(root[0].right.take(), node));
    }
    balanced(root)
}

fn pop_first(root: &mut Link) -> Link {
    let mut node = root.take()?;
    #[cfg(test)]
    TREE_VISITS.set(TREE_VISITS.get() + 1);
    if node[0].left.is_none() {
        *root = node[0].right.take();
        node[0].height = 1;
        Some(node)
    } else {
        let first = pop_first(&mut node[0].left);
        *root = Some(balanced(node));
        first
    }
}

fn recycle_tree(root: Link, free: &mut FreeNodes) {
    let Some(mut node) = root else {
        return;
    };
    #[cfg(test)]
    TREE_VISITS.set(TREE_VISITS.get() + 1);
    // One destructive traversal; input is an AVL, so recursive depth is bounded
    // by its height. Isolate children before placing this node on the free chain.
    recycle_tree(node[0].left.take(), free);
    recycle_tree(node[0].right.take(), free);
    node[0].payload = OwnedPayload {
        bytes: bytes::Bytes::new(),
        backing_bytes: 0,
    };
    free.push(node);
}

impl ReceiveRanges {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn get(&self, key: &u64) -> Option<&OwnedPayload> {
        let mut link = self.root.as_ref();
        while let Some(node) = link {
            match key.cmp(&node[0].key) {
                std::cmp::Ordering::Equal => return Some(&node[0].payload),
                std::cmp::Ordering::Less => link = node[0].left.as_ref(),
                std::cmp::Ordering::Greater => link = node[0].right.as_ref(),
            }
        }
        None
    }
    pub(super) fn contains_key(&self, key: &u64) -> bool {
        self.get(key).is_some()
    }
    pub(super) fn first_key_value(&self) -> Option<(&u64, &OwnedPayload)> {
        let mut node = self.root.as_ref()?;
        while let Some(left) = node[0].left.as_ref() {
            node = left;
        }
        Some((&node[0].key, &node[0].payload))
    }
    pub(super) fn last_key_value(&self) -> Option<(&u64, &OwnedPayload)> {
        let mut node = self.root.as_ref()?;
        while let Some(right) = node[0].right.as_ref() {
            node = right;
        }
        Some((&node[0].key, &node[0].payload))
    }
    pub(super) fn range(
        &self,
        end: std::ops::RangeTo<u64>,
    ) -> impl Iterator<Item = (&u64, &OwnedPayload)> {
        self.iter().take_while(move |(key, _)| **key < end.end)
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &OwnedPayload> {
        self.iter().map(|(_, p)| p)
    }
    pub(super) fn iter(&self) -> Ranges<'_> {
        let mut iter = Ranges {
            stack: [None; MAX_HEIGHT],
            depth: 0,
        };
        iter.descend(self.root.as_ref());
        iter
    }

    /// Preparation owns only new vacancies: old tree/free nodes are untouched.
    pub(super) fn prepare(
        &self,
        added: usize,
        bound: usize,
    ) -> Result<FreeNodes, TcpOwnershipError> {
        let after = self
            .len
            .checked_add(added)
            .ok_or(TcpOwnershipError::ReceiveStorageLayout)?;
        let resident = self
            .len
            .checked_add(self.free.len)
            .ok_or(TcpOwnershipError::ReceiveStorageLayout)?;
        let peak = resident.max(after);
        if peak > bound {
            return Err(TcpOwnershipError::SegmentCapacityExceeded {
                attempted: peak,
                max: bound,
            });
        }
        Layout::array::<Node>(peak).map_err(|_| TcpOwnershipError::ReceiveStorageLayout)?;
        let mut prepared = FreeNodes::default();
        for _ in 0..added.saturating_sub(self.free.len) {
            #[cfg(test)]
            if FAIL_NODE.get() == Some(0) {
                FAIL_NODE.set(None);
                return Err(TcpOwnershipError::ReceiveStorageReserve);
            }
            #[cfg(test)]
            if let Some(remaining) = FAIL_NODE.get() {
                FAIL_NODE.set(Some(remaining - 1));
            }
            let mut single = allocate_exact_vec(1).map_err(storage_error)?;
            #[cfg(test)]
            LIVE_NODES.set(LIVE_NODES.get() + 1);
            single.push(Node {
                key: 0,
                payload: OwnedPayload {
                    bytes: bytes::Bytes::new(),
                    backing_bytes: 0,
                },
                left: None,
                right: None,
                height: 1,
            });
            // Equal len/cap Vec -> Box slice and checked array conversion are
            // in-place; no second allocation or new unsafe implementation.
            let boxed: Box<[Node]> = single.into_boxed_slice();
            let node = <Box<[Node; 1]>>::try_from(boxed).map_err(|_| {
                TcpOwnershipError::InvariantViolation("receive node length changed")
            })?;
            prepared.push(node);
        }
        Ok(prepared)
    }

    pub(super) fn insert_prepared(
        &mut self,
        prepared: &mut FreeNodes,
        key: u64,
        payload: OwnedPayload,
    ) {
        // The exclusive prepared transaction proved U <= old free + new.
        let mut node = self
            .free
            .pop()
            .or_else(|| prepared.pop())
            .expect("prepared receiver vacancy count");
        node[0].key = key;
        node[0].payload = payload;
        node[0].height = 1;
        self.root = Some(insert(self.root.take(), node));
        self.len += 1;
    }

    pub(super) fn accept_prefix(&mut self, accepted: u64, mut partial: Option<OwnedPayload>) {
        while self
            .first_key_value()
            .is_some_and(|(key, _)| *key < accepted)
        {
            let Some(mut node) = pop_first(&mut self.root) else {
                break;
            };
            self.len -= 1;
            if node[0].key + node[0].payload.len() as u64 > accepted {
                let suffix = partial.take().expect("prepared receiver partial suffix");
                node[0].key = accepted;
                node[0].payload = suffix;
                self.root = Some(insert(self.root.take(), node));
                self.len += 1;
                break;
            }
            node[0].payload = OwnedPayload {
                bytes: bytes::Bytes::new(),
                backing_bytes: 0,
            };
            self.free.push(node);
        }
        debug_assert!(partial.is_none());
    }

    pub(super) fn clear(&mut self) {
        recycle_tree(self.root.take(), &mut self.free);
        self.len = 0;
    }
}

// AVL min nodes(h) >= 2^(h/2)-1; a representable usize node count cannot
// reach height 2*usize::BITS. This is ancestor storage, never a heap stack.
const MAX_HEIGHT: usize = 2 * usize::BITS as usize;
pub(super) struct Ranges<'a> {
    stack: [Option<&'a Node>; MAX_HEIGHT],
    depth: usize,
}
impl<'a> Ranges<'a> {
    fn descend(&mut self, mut link: Option<&'a Box<[Node; 1]>>) {
        while let Some(node) = link {
            self.stack[self.depth] = Some(&node[0]);
            self.depth += 1;
            link = node[0].left.as_ref();
        }
    }
}
impl<'a> Iterator for Ranges<'a> {
    type Item = (&'a u64, &'a OwnedPayload);
    fn next(&mut self) -> Option<Self::Item> {
        if self.depth == 0 {
            return None;
        }
        self.depth -= 1;
        let node = self.stack[self.depth].take()?;
        self.descend(node.right.as_ref());
        Some((&node.key, &node.payload))
    }
}

pub(super) fn storage_error(error: EffectStorageError) -> TcpOwnershipError {
    match error {
        EffectStorageError::Reserve => TcpOwnershipError::ReceiveStorageReserve,
        EffectStorageError::Layout | EffectStorageError::Bound | EffectStorageError::Overflow => {
            TcpOwnershipError::ReceiveStorageLayout
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static FAIL_NODE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static LIVE_NODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
std::thread_local! { static TREE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
impl TcpReceiveWindow {
    pub(in crate::resumable) fn fail_next_receive_node_allocation_for_test() {
        FAIL_NODE.set(Some(0));
    }
    pub(in crate::resumable) fn take_receive_node_failure_for_test() -> bool {
        FAIL_NODE.take().is_some()
    }
}
