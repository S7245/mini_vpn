//! Original Supervisor DATA admission, replay nodes and receipt settlement.
//! Owns actual staging/replay permits, quarantine, node vacancies and prepared
//! scalar candidate backing. Not Model/Target or a second lifecycle ledger.
//! Pre-admission failure returns original DATA; admitted Owner/Legacy policies
//! remain distinct. ACK/terminal/expiry borrow root-held nodes through callbacks.
//! Entry: apply_driver_input_observed / apply_owner_data_observed; real tests
//! live under Supervisor and OwnerTarget, structural tests in driver_input/tests.

use super::*;
use crate::owned_upstream::tcp::UplinkData;

use crate::resumable::effect_storage::{EffectStorageError, allocate_exact_vec, next_capacity};
use std::alloc::Layout;

#[cfg(test)]
mod tests;

type Link = Option<Box<[Node; 1]>>;
type Candidate = (ReplayExtentKey, bool);

// The original payload/permits move with this one allocation in every phase.
enum Value {
    Vacant,
    Staging(UplinkOwnership),
    Replay {
        key: ReplayExtentKey,
        ownership: UplinkReplayOwnership,
    },
}
struct Node {
    value: Value,
    left: Link,
    right: Link,
    height: u8,
}
impl Node {
    fn key(&self) -> ReplayExtentKey {
        match self.value {
            Value::Replay { key, .. } => key,
            _ => unreachable!("only replay nodes enter the ordered tree"),
        }
    }
    fn bytes(&self) -> usize {
        match &self.value {
            Value::Vacant => 0,
            Value::Staging(value) => value.bytes(),
            Value::Replay { ownership, .. } => ownership.bytes(),
        }
    }
}
#[derive(Default)]
struct Chain {
    head: Link,
    len: usize,
}
impl Chain {
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
    fn current(&self, position: usize) -> Option<&Node> {
        (position.checked_add(1) == Some(self.len)).then_some(())?;
        self.head.as_ref().map(|node| &node[0])
    }
    fn current_mut(&mut self, position: usize) -> Option<&mut Node> {
        (position.checked_add(1) == Some(self.len)).then_some(())?;
        self.head.as_mut().map(|node| &mut node[0])
    }
    fn nodes(&self) -> impl Iterator<Item = &Node> {
        std::iter::successors(self.head.as_ref().map(|n| &n[0]), |n| {
            n.left.as_ref().map(|n| &n[0])
        })
    }
}
impl Drop for Chain {
    fn drop(&mut self) {
        // If one real permit wake unwinds, drain the remaining detached nodes
        // iteratively too. A second destructor panic retains Rust's abort policy.
        struct Drain<'a>(&'a mut Chain);
        impl Drop for Drain<'_> {
            fn drop(&mut self) {
                while self.0.pop().is_some() {}
            }
        }
        let drain = Drain(self);
        while drain.0.pop().is_some() {}
    }
}
#[derive(Default)]
pub(super) struct UplinkStorage {
    replay: Link,
    replay_len: usize,
    quarantine: Chain,
    vacant: Chain,
    candidates: Vec<Candidate>,
    #[cfg(test)]
    fail_scratch: bool,
    #[cfg(test)]
    fail_node: bool,
}
struct PreparedUplink {
    node: Link,
    scratch: Option<Vec<Candidate>>,
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
    let Some(mut root) = root else { return node };
    if node[0].key() < root[0].key() {
        root[0].left = Some(insert(root[0].left.take(), node));
    } else {
        debug_assert_ne!(node[0].key(), root[0].key());
        root[0].right = Some(insert(root[0].right.take(), node));
    }
    balanced(root)
}
fn pop_first(root: &mut Link) -> Link {
    let mut node = root.take()?;
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
fn take_node(root: &mut Link, key: ReplayExtentKey) -> Link {
    let mut node = root.take()?;
    let removed = match key.cmp(&node[0].key()) {
        std::cmp::Ordering::Less => take_node(&mut node[0].left, key),
        std::cmp::Ordering::Greater => take_node(&mut node[0].right, key),
        std::cmp::Ordering::Equal => {
            *root = match pop_first(&mut node[0].right) {
                Some(mut next) => {
                    next[0].left = node[0].left.take();
                    next[0].right = node[0].right.take();
                    Some(balanced(next))
                }
                None => node[0].left.take(),
            };
            node[0].height = 1;
            return Some(node);
        }
    };
    *root = Some(balanced(node));
    removed
}
// Native AVL height is bounded by twice the address width; never a heap stack.
struct Nodes<'a> {
    stack: [Option<&'a Node>; 2 * usize::BITS as usize],
    depth: usize,
}
impl<'a> Nodes<'a> {
    fn new(root: &'a Link) -> Self {
        let mut iter = Self {
            stack: [None; 2 * usize::BITS as usize],
            depth: 0,
        };
        iter.descend(root);
        iter
    }
    fn descend(&mut self, mut root: &'a Link) {
        while let Some(node) = root {
            self.stack[self.depth] = Some(&node[0]);
            self.depth += 1;
            root = &node[0].left;
        }
    }
}
impl<'a> Iterator for Nodes<'a> {
    type Item = &'a Node;
    fn next(&mut self) -> Option<Self::Item> {
        if self.depth == 0 {
            return None;
        }
        self.depth -= 1;
        let node = self.stack[self.depth].take()?;
        self.descend(&node.right);
        Some(node)
    }
}

impl UplinkStorage {
    #[cfg(test)]
    pub(super) fn get(&self, key: &ReplayExtentKey) -> Option<&UplinkReplayOwnership> {
        Nodes::new(&self.replay)
            .find(|n| n.key() == *key)
            .and_then(|n| match &n.value {
                Value::Replay { ownership, .. } => Some(ownership),
                _ => None,
            })
    }

    pub(super) fn len(&self) -> usize {
        self.replay_len
    }
    pub(super) fn is_empty(&self) -> bool {
        self.replay.is_none()
    }
    fn keys(&self) -> impl Iterator<Item = ReplayExtentKey> + '_ {
        Nodes::new(&self.replay).map(Node::key)
    }
    fn contains_key(&self, key: &ReplayExtentKey) -> bool {
        let mut at = self.replay.as_ref();
        while let Some(node) = at {
            match key.cmp(&node[0].key()) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => at = node[0].left.as_ref(),
                std::cmp::Ordering::Greater => at = node[0].right.as_ref(),
            }
        }
        false
    }
    fn get_mut(&mut self, key: ReplayExtentKey) -> Option<&mut UplinkReplayOwnership> {
        let mut at = self.replay.as_mut();
        while let Some(node) = at {
            match key.cmp(&node[0].key()) {
                std::cmp::Ordering::Equal => {
                    return match &mut node[0].value {
                        Value::Replay { ownership, .. } => Some(ownership),
                        _ => None,
                    };
                }
                std::cmp::Ordering::Less => at = node[0].left.as_mut(),
                std::cmp::Ordering::Greater => at = node[0].right.as_mut(),
            }
        }
        None
    }
    fn prepare(&mut self, replay_limit: usize) -> Result<PreparedUplink, EffectStorageError> {
        use EffectStorageError::{Bound, Layout as BadLayout};
        let maximum = replay_limit.checked_add(1).ok_or(Bound)?;
        let held = self
            .replay_len
            .checked_add(self.quarantine.len)
            .ok_or(Bound)?;
        let allocated = held.checked_add(self.vacant.len).ok_or(Bound)?;
        let need = held.checked_add(1).ok_or(Bound)?;
        let peak = allocated.max(need);
        if peak > maximum {
            return Err(Bound);
        }
        let old = self.candidates.capacity();
        let next = next_capacity(old, need, 1, maximum)?;
        let scratch_peak = if next == old {
            old
        } else {
            old.checked_add(next).ok_or(BadLayout)?
        };
        let node_bytes = Layout::array::<Node>(peak).map_err(|_| BadLayout)?.size();
        let scratch_bytes = Layout::array::<Candidate>(scratch_peak)
            .map_err(|_| BadLayout)?
            .size();
        node_bytes.checked_add(scratch_bytes).ok_or(BadLayout)?;
        let node = if self.vacant.head.is_none() {
            #[cfg(test)]
            if std::mem::take(&mut self.fail_node) {
                return Err(EffectStorageError::Reserve);
            }
            let mut prepared = allocate_exact_vec(1)?;
            prepared.push(Node {
                value: Value::Vacant,
                left: None,
                right: None,
                height: 1,
            });
            Some(
                prepared
                    .into_boxed_slice()
                    .try_into()
                    .map_err(|_| BadLayout)?,
            )
        } else {
            None
        };
        let scratch = if next != old {
            #[cfg(test)]
            if std::mem::take(&mut self.fail_scratch) {
                return Err(EffectStorageError::Reserve);
            }
            Some(allocate_exact_vec(next)?)
        } else {
            None
        };
        Ok(PreparedUplink { node, scratch })
    }
    fn admit(
        &mut self,
        mut prepared: PreparedUplink,
        data: UplinkData,
    ) -> Result<(Box<[Node; 1]>, SessionEvent), UplinkData> {
        let Some(mut node) = prepared.node.take().or_else(|| self.vacant.pop()) else {
            return Err(data);
        };
        if let Some(mut replacement) = prepared.scratch.take() {
            // Scalar-only moves into proven backing. Preserve any poisoned prefix.
            for candidate in self.candidates.drain(..) {
                replacement.push(candidate);
            }
            self.candidates = replacement;
        }
        let (event, staging) = data.into_event_and_ownership();
        node[0].value = Value::Staging(staging);
        Ok((node, event))
    }
    fn publish(&mut self, node: Box<[Node; 1]>) {
        self.replay = Some(insert(self.replay.take(), node));
        self.replay_len += 1;
    }
    fn receipt_entry(&self) -> Result<(), SessionSupervisorError> {
        if !self.candidates.is_empty() || self.candidates.capacity() < self.replay_len {
            return Err(SessionSupervisorError::DataCustodyInvariant);
        }
        Ok(())
    }
    fn recycle(&mut self, key: ReplayExtentKey) -> Result<(), SessionSupervisorError> {
        let mut node = take_node(&mut self.replay, key)
            .ok_or(SessionSupervisorError::ReplayExtentDisappeared)?;
        self.replay_len -= 1;
        node[0].value = Value::Vacant;
        self.vacant.push(node);
        Ok(())
    }
    fn acknowledge(
        &mut self,
        receipt: &crate::resumable::ReplayAcknowledged,
    ) -> Result<(), SessionSupervisorError> {
        self.receipt_entry()?;
        // O(R) in-order scan, no lookup per candidate until removal. Nodes and
        // the completed prefix stay at the original root through every callback.
        fn visit(
            root: &mut Link,
            scratch: &mut Vec<Candidate>,
            receipt: &crate::resumable::ReplayAcknowledged,
        ) -> Result<(), SessionSupervisorError> {
            if let Some(node) = root {
                visit(&mut node[0].left, scratch, receipt)?;
                let key = node[0].key();
                if key.flow_id == receipt.flow_id() && key.direction == receipt.direction() {
                    if scratch.len() == scratch.capacity() {
                        return Err(SessionSupervisorError::DataCustodyInvariant);
                    }
                    let Value::Replay { ownership, .. } = &mut node[0].value else {
                        return Err(SessionSupervisorError::DataCustodyInvariant);
                    };
                    let released = ownership.release_after_reducer_ack(receipt)?;
                    scratch.push((key, released));
                }
                visit(&mut node[0].right, scratch, receipt)?;
            }
            Ok(())
        }
        visit(&mut self.replay, &mut self.candidates, receipt)?;
        for index in 0..self.candidates.len() {
            let (key, released) = self.candidates[index];
            if released {
                self.recycle(key)?;
            }
        }
        self.candidates.clear();
        Ok(())
    }
    fn terminal(
        &mut self,
        flow_id: SessionFlowId,
        effect: &SessionEffect,
    ) -> Result<(), SessionSupervisorError> {
        self.receipt_entry()?;
        for node in Nodes::new(&self.replay) {
            let key = node.key();
            if key.flow_id == flow_id {
                if self.candidates.len() == self.candidates.capacity() {
                    return Err(SessionSupervisorError::DataCustodyInvariant);
                }
                self.candidates.push((key, false));
            }
        }
        for index in 0..self.candidates.len() {
            let (key, _) = self.candidates[index];
            self.get_mut(key)
                .ok_or(SessionSupervisorError::ReplayExtentDisappeared)?
                .release_after_reducer_terminal(effect)?;
            self.recycle(key)?;
        }
        self.candidates.clear();
        Ok(())
    }
    fn expire(&mut self, effect: &SessionEffect) -> Result<(), SessionSupervisorError> {
        self.receipt_entry()?;
        fn release(root: &mut Link, effect: &SessionEffect) -> Result<(), SessionSupervisorError> {
            if let Some(node) = root {
                release(&mut node[0].left, effect)?;
                let Value::Replay { ownership, .. } = &mut node[0].value else {
                    return Err(SessionSupervisorError::DataCustodyInvariant);
                };
                ownership.release_after_reducer_session_expiry(effect)?;
                release(&mut node[0].right, effect)?;
            }
            Ok(())
        }
        fn recycle(root: Link, vacant: &mut Chain) {
            if let Some(mut node) = root {
                recycle(node[0].left.take(), vacant);
                recycle(node[0].right.take(), vacant);
                node[0].value = Value::Vacant;
                node[0].height = 1;
                vacant.push(node);
            }
        }
        release(&mut self.replay, effect)?;
        // No permit callbacks remain; recycle each original Box exactly once.
        recycle(self.replay.take(), &mut self.vacant);
        self.replay_len = 0;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::owned_upstream) struct UplinkStorageColdInventory {
    pub(in crate::owned_upstream) replay_nodes: usize,
    pub(in crate::owned_upstream) quarantine_nodes: usize,
    pub(in crate::owned_upstream) vacant_nodes: usize,
    pub(in crate::owned_upstream) node_bytes: usize,
    pub(in crate::owned_upstream) candidate_len: usize,
    pub(in crate::owned_upstream) candidate_capacity: usize,
    pub(in crate::owned_upstream) candidate_bytes: usize,
    pub(in crate::owned_upstream) uplink:
        crate::owned_upstream::tcp::cold_inventory::UplinkColdInventory,
}
impl SessionSupervisor {
    /// Cold, borrowed actual allocation/permit audit. Not a hot-path snapshot,
    /// shared Bytes backing sum, or proof of whole-session release.
    pub(in crate::owned_upstream) fn uplink_storage_cold_inventory(
        &self,
    ) -> Result<UplinkStorageColdInventory, SessionSupervisorError> {
        use crate::owned_upstream::tcp::cold_inventory::UplinkColdInventory;
        let bad = || SessionSupervisorError::DataCustodyInvariant;
        let storage = &self.uplink_replay;
        let mut uplink = UplinkColdInventory::empty();
        let (mut replay_nodes, mut quarantine_nodes, mut vacant_nodes) = (0usize, 0usize, 0usize);
        let mut previous = None;
        for node in Nodes::new(&storage.replay) {
            let Value::Replay { key, ownership } = &node.value else {
                return Err(bad());
            };
            if *key != ReplayExtentKey::of(ownership)
                || previous.is_some_and(|p| p >= *key)
                || node.height != 1 + height(&node.left).max(height(&node.right))
                || height(&node.left).abs_diff(height(&node.right)) > 1
            {
                return Err(bad());
            }
            previous = Some(*key);
            replay_nodes = replay_nodes.checked_add(1).ok_or_else(bad)?;
            uplink = uplink
                .checked_add(ownership.cold_inventory())
                .ok_or_else(bad)?;
        }
        for node in storage.quarantine.nodes() {
            if node.right.is_some() || node.height != 1 {
                return Err(bad());
            }
            let inventory = match &node.value {
                Value::Staging(ownership) => ownership.cold_inventory(),
                Value::Replay { key, ownership } if *key == ReplayExtentKey::of(ownership) => {
                    ownership.cold_inventory()
                }
                _ => return Err(bad()),
            };
            quarantine_nodes = quarantine_nodes.checked_add(1).ok_or_else(bad)?;
            uplink = uplink.checked_add(inventory).ok_or_else(bad)?;
        }
        for node in storage.vacant.nodes() {
            if !matches!(node.value, Value::Vacant) || node.right.is_some() || node.height != 1 {
                return Err(bad());
            }
            vacant_nodes = vacant_nodes.checked_add(1).ok_or_else(bad)?;
        }
        if replay_nodes != storage.replay_len
            || quarantine_nodes != storage.quarantine.len
            || vacant_nodes != storage.vacant.len
        {
            return Err(bad());
        }
        let allocated = replay_nodes
            .checked_add(quarantine_nodes)
            .and_then(|n| n.checked_add(vacant_nodes))
            .ok_or_else(bad)?;
        let maximum = self
            .model
            .config()
            .replay_limit(self.model.role().local_send_direction())
            .max_segments()
            .checked_add(1)
            .ok_or_else(bad)?;
        if allocated > maximum
            || storage.candidates.capacity() > maximum
            || storage.candidates.capacity()
                < replay_nodes.checked_add(quarantine_nodes).ok_or_else(bad)?
        {
            return Err(bad());
        }
        Ok(UplinkStorageColdInventory {
            replay_nodes,
            quarantine_nodes,
            vacant_nodes,
            node_bytes: Layout::array::<Node>(allocated).map_err(|_| bad())?.size(),
            candidate_len: storage.candidates.len(),
            candidate_capacity: storage.candidates.capacity(),
            candidate_bytes: Layout::array::<Candidate>(storage.candidates.capacity())
                .map_err(|_| bad())?
                .size(),
            uplink,
        })
    }
}

enum DataPath {
    Legacy,
    OwnerObserved,
}

enum DataCustody {
    Local(Box<[Node; 1]>),
    OwnerSlot(usize),
    Published,
}

/// Locator only; all resources stay in the exclusively borrowed original root.
struct DataTransaction<'a> {
    supervisor: &'a mut SessionSupervisor,
    custody: DataCustody,
    owner_observed: bool,
    complete: bool,
}
impl<'a> DataTransaction<'a> {
    #[cfg(test)]
    fn unwind_if_armed(&mut self, point: OwnerDataUnwindPoint) {
        if self.owner_observed && self.supervisor.owner_data_unwind_once == Some(point) {
            self.supervisor.owner_data_unwind_once = None;
            panic!("single-use Owner DATA custody unwind");
        }
    }
    fn new(
        supervisor: &'a mut SessionSupervisor,
        data: UplinkData,
        path: DataPath,
    ) -> Result<(Self, SessionEvent), SessionSupervisorError> {
        let limit = supervisor
            .model
            .config()
            .replay_limit(supervisor.model.role().local_send_direction())
            .max_segments();
        let prepared = match supervisor.uplink_replay.prepare(limit) {
            Ok(prepared) => prepared,
            Err(cause) => {
                let input = DriverInput::Data(data);
                return Err(match cause {
                    EffectStorageError::Bound | EffectStorageError::Overflow => {
                        SessionSupervisorError::UplinkStorageCapacity { input }
                    }
                    EffectStorageError::Layout => {
                        SessionSupervisorError::UplinkStorageLayout { input }
                    }
                    EffectStorageError::Reserve => {
                        SessionSupervisorError::UplinkStorageReserve { input }
                    }
                });
            }
        };
        let (node, event) = supervisor
            .uplink_replay
            .admit(prepared, data)
            .map_err(|data| SessionSupervisorError::UplinkStorageCapacity {
                input: DriverInput::Data(data),
            })?;
        let owner_observed = matches!(path, DataPath::OwnerObserved);
        let custody = if owner_observed {
            let position = supervisor.uplink_replay.quarantine.len;
            supervisor.uplink_replay.quarantine.push(node);
            DataCustody::OwnerSlot(position)
        } else {
            DataCustody::Local(node)
        };
        Ok((
            Self {
                supervisor,
                custody,
                owner_observed,
                complete: false,
            },
            event,
        ))
    }
    fn corrupt(&mut self) -> SessionSupervisorError {
        self.supervisor.poisoned = true;
        SessionSupervisorError::DataCustodyInvariant
    }
    fn node(&self) -> Option<&Node> {
        match &self.custody {
            DataCustody::Local(node) => Some(&node[0]),
            DataCustody::OwnerSlot(position) => {
                self.supervisor.uplink_replay.quarantine.current(*position)
            }
            DataCustody::Published => None,
        }
    }
    fn node_mut(&mut self) -> Option<&mut Node> {
        match &mut self.custody {
            DataCustody::Local(node) => Some(&mut node[0]),
            DataCustody::OwnerSlot(position) => self
                .supervisor
                .uplink_replay
                .quarantine
                .current_mut(*position),
            DataCustody::Published => None,
        }
    }
    fn origin_matches(&mut self) -> Result<bool, SessionSupervisorError> {
        let Some(expected) = self.supervisor.tcp_factory_origin.as_ref() else {
            return Ok(false);
        };
        match self.node().map(|n| &n.value) {
            Some(Value::Staging(staging)) => Ok(staging.matches_factory_origin(expected)),
            Some(Value::Replay { ownership, .. }) => Ok(ownership.matches_factory_origin(expected)),
            _ => Err(self.corrupt()),
        }
    }
    fn quarantine(&mut self) {
        self.supervisor.poisoned = true;
        if matches!(self.custody, DataCustody::Local(_)) {
            let DataCustody::Local(node) =
                std::mem::replace(&mut self.custody, DataCustody::Published)
            else {
                return;
            };
            self.supervisor.uplink_replay.quarantine.push(node);
        }
    }
    fn bind_replay(
        &mut self,
        stored: crate::resumable::ReplayStored,
    ) -> Result<(), SessionSupervisorError> {
        let Some(node) = self.node_mut() else {
            return Err(self.corrupt());
        };
        let Value::Staging(staging) = &mut node.value else {
            return Err(self.corrupt());
        };
        let ownership = staging
            .bind_replay(stored)
            .map_err(SessionSupervisorError::FlowPort)?;
        node.value = Value::Replay {
            key: ReplayExtentKey::of(&ownership),
            ownership,
        };
        Ok(())
    }
    fn replay_key(&mut self) -> Result<ReplayExtentKey, SessionSupervisorError> {
        match self.node().map(|n| &n.value) {
            Some(Value::Replay { ownership, .. }) => Ok(ReplayExtentKey::of(ownership)),
            _ => Err(self.corrupt()),
        }
    }
    fn publish(&mut self, key: ReplayExtentKey) -> Result<(), SessionSupervisorError> {
        if !matches!(self.node().map(|n| &n.value), Some(Value::Replay { key: actual, .. }) if *actual == key)
        {
            return Err(self.corrupt());
        }
        // All validation precedes the sole owning move; native ordering has no
        // user callbacks and insertion consumes this already allocated Box.
        let node = match &self.custody {
            DataCustody::OwnerSlot(_) => self.supervisor.uplink_replay.quarantine.pop(),
            DataCustody::Local(_) => {
                match std::mem::replace(&mut self.custody, DataCustody::Published) {
                    DataCustody::Local(node) => Some(node),
                    _ => None,
                }
            }
            DataCustody::Published => None,
        };
        let Some(node) = node else {
            return Err(self.corrupt());
        };
        self.custody = DataCustody::Published;
        self.supervisor.uplink_replay.publish(node);
        Ok(())
    }

    fn finish_owner_committed_data(
        mut self,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        // No second visible Vec: bind while borrowing the original receipt,
        // then remove only that successfully settled item from original backing.
        self.bind_owner_journal_receipt()?;
        #[cfg(test)]
        self.unwind_if_armed(OwnerDataUnwindPoint::ReplayBound);
        if !self.origin_matches()? {
            return Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch);
        }
        let key = self.replay_key()?;
        if self.supervisor.uplink_replay.contains_key(&key) {
            return Err(SessionSupervisorError::DuplicateReplayExtent);
        }
        self.publish(key)?;
        #[cfg(test)]
        self.unwind_if_armed(OwnerDataUnwindPoint::ReplayPublished);
        let observed = self.supervisor.finish_receipt_journal()?;
        Ok(self.finish(observed))
    }

    fn bind_owner_journal_receipt(&mut self) -> Result<(), SessionSupervisorError> {
        let DataCustody::OwnerSlot(position) = self.custody else {
            return Err(self.corrupt());
        };
        let supervisor = &mut *self.supervisor;
        let journal = supervisor
            .receipt_journal
            .as_mut()
            .ok_or(SessionSupervisorError::DataCustodyInvariant)?;
        let mut stored_position = None;
        for (position, effect) in journal.effects.iter().enumerate() {
            if matches!(effect, SessionEffect::ReplayStored { .. }) {
                if stored_position.is_some() {
                    return Err(SessionSupervisorError::DuplicateReplayStored);
                }
                stored_position = Some(position);
            }
        }
        let stored_position = stored_position.ok_or(SessionSupervisorError::MissingReplayStored)?;
        let Some(SessionEffect::ReplayStored { receipt }) = journal.effects.get(stored_position)
        else {
            return Err(SessionSupervisorError::DataCustodyInvariant);
        };
        let already_outstanding = supervisor
            .uplink_replay
            .keys()
            .any(|key| key.flow_id == receipt.flow_id() && key.direction == receipt.direction());
        if !already_outstanding {
            if journal.observations.replay_became_outstanding.is_some() {
                return Err(SessionSupervisorError::DuplicateReplayOutstandingObservation);
            }
            journal.observations.replay_became_outstanding = Some(ReducerReplayBecameOutstanding {
                session_id: receipt.session_id(),
                flow_id: receipt.flow_id(),
                generation: supervisor.model.snapshot().generation(),
                direction: receipt.direction(),
                start: receipt.start(),
                end: receipt.end(),
                session_became_nonempty: supervisor.uplink_replay.is_empty(),
            });
        }
        let slot = supervisor
            .uplink_replay
            .quarantine
            .current_mut(position)
            .ok_or(SessionSupervisorError::DataCustodyInvariant)?;
        let Value::Staging(staging) = &mut slot.value else {
            return Err(SessionSupervisorError::DataCustodyInvariant);
        };
        let replay = staging
            .bind_owner_replay(receipt)
            .map_err(SessionSupervisorError::FlowPort)?;
        // No callback/fallible operation between the successful transfer and
        // same-slot publication; all rejecting paths above leave receipt held.
        slot.value = Value::Replay {
            key: ReplayExtentKey::of(&replay),
            ownership: replay,
        };
        drop(journal.effects.remove(stored_position));
        Ok(())
    }

    fn finish(mut self, observed: ReducerObservedApply) -> ReducerObservedApply {
        self.complete = true;
        observed
    }
}

impl Drop for DataTransaction<'_> {
    fn drop(&mut self) {
        if self.owner_observed && !self.complete {
            self.supervisor.poisoned = true;
        }
    }
}

impl SessionSupervisor {
    #[cfg(test)]
    pub(in crate::owned_upstream) fn arm_owner_data_unwind(&mut self, point: OwnerDataUnwindPoint) {
        assert!(self.owner_data_unwind_once.replace(point).is_none());
    }

    pub(super) fn apply_driver_input_observed(
        &mut self,
        input: DriverInput,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        match input {
            DriverInput::Control(event) => self.apply_event_observed_inner(event),
            DriverInput::Data(data) => self.apply_data_observed(data, DataPath::Legacy),
        }
    }

    pub(super) fn apply_owner_data_observed(
        &mut self,
        data: UplinkData,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        self.apply_data_observed(data, DataPath::OwnerObserved)
    }

    fn apply_data_observed(
        &mut self,
        data: UplinkData,
        path: DataPath,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        let (mut turn, event) = DataTransaction::new(self, data, path)?;
        #[cfg(test)]
        turn.unwind_if_armed(OwnerDataUnwindPoint::Staging);
        if turn.owner_observed {
            // An already-poisoned Owner still owns this exact incoming DATA.
            turn.supervisor.require_healthy()?;
        }
        if !turn.origin_matches()? {
            turn.quarantine();
            return Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch);
        }
        let reduced = if turn.owner_observed {
            turn.supervisor
                .reduce_into_receipt_journal(event)?
                .map(|()| Vec::new())
        } else {
            turn.supervisor.model.reduce(event)
        };
        let effects = match reduced {
            Ok(effects) => effects,
            Err(
                source @ (SessionError::ReplayByteBudgetExceeded { .. }
                | SessionError::ReplaySegmentBudgetExceeded { .. }),
            ) => {
                turn.quarantine();
                return Err(SessionSupervisorError::ReducerReplayCapacityInvariant { source });
            }
            Err(error) => return Err(SessionSupervisorError::Reducer(error)),
        };
        if turn.owner_observed {
            return turn.finish_owner_committed_data();
        }
        let mut visible = effects;
        let mut stored_position = None;
        for (position, effect) in visible.iter().enumerate() {
            if matches!(effect, SessionEffect::ReplayStored { .. }) {
                if stored_position.replace(position).is_some() {
                    turn.quarantine();
                    return Err(SessionSupervisorError::DuplicateReplayStored);
                }
            }
        }
        let Some(position) = stored_position else {
            turn.quarantine();
            return Err(SessionSupervisorError::MissingReplayStored);
        };
        let SessionEffect::ReplayStored { receipt: stored } = visible.remove(position) else {
            turn.quarantine();
            return Err(SessionSupervisorError::DataCustodyInvariant);
        };
        let session_became_nonempty = turn.supervisor.uplink_replay.is_empty();
        let already_outstanding = turn
            .supervisor
            .uplink_replay
            .keys()
            .any(|key| key.flow_id == stored.flow_id() && key.direction == stored.direction());
        let replay_became_outstanding =
            (!already_outstanding).then_some(ReducerReplayBecameOutstanding {
                session_id: stored.session_id(),
                flow_id: stored.flow_id(),
                generation: turn.supervisor.model.snapshot().generation(),
                direction: stored.direction(),
                start: stored.start(),
                end: stored.end(),
                session_became_nonempty,
            });
        if let Err(error) = turn.bind_replay(stored) {
            turn.quarantine();
            return Err(error);
        }
        #[cfg(test)]
        turn.unwind_if_armed(OwnerDataUnwindPoint::ReplayBound);
        if !turn.origin_matches()? {
            turn.quarantine();
            return Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch);
        }
        let key = turn.replay_key()?;
        if turn.supervisor.uplink_replay.contains_key(&key) {
            turn.quarantine();
            return Err(SessionSupervisorError::DuplicateReplayExtent);
        }
        turn.publish(key)?;
        #[cfg(test)]
        turn.unwind_if_armed(OwnerDataUnwindPoint::ReplayPublished);
        turn.supervisor.begin_receipt_journal(visible)?;
        if let Some(fact) = replay_became_outstanding {
            let journal = turn
                .supervisor
                .receipt_journal
                .as_mut()
                .ok_or(SessionSupervisorError::DataCustodyInvariant)?;
            if journal.observations.replay_became_outstanding.is_some() {
                return Err(SessionSupervisorError::DuplicateReplayOutstandingObservation);
            }
            journal.observations.replay_became_outstanding = Some(fact);
        }
        let observed = turn.supervisor.finish_receipt_journal()?;
        Ok(turn.finish(observed))
    }

    pub(crate) fn snapshot(&self) -> SessionSupervisorSnapshot {
        SessionSupervisorSnapshot {
            session: self.model.snapshot(),
            uplink_replay_extents: self.uplink_replay.len(),
            uplink_replay_owned_bytes: Nodes::new(&self.uplink_replay.replay)
                .map(Node::bytes)
                .sum(),
            poisoned: self.poisoned,
            retained_receipt_effects: self.receipt_journal.as_ref().map_or(0, |j| j.effects.len()),
            retained_receipt_observations: self.receipt_journal.as_ref().map_or(0, |j| {
                usize::from(j.observations.replay_became_outstanding.is_some())
                    + usize::from(j.observations.application_ack.is_some())
                    + usize::from(j.observations.flow_terminal.is_some())
                    + usize::from(j.observations.session_expired.is_some())
                    + usize::from(j.sink_accepted_frontier.is_some())
            }),
            quarantined_uplink_extents: self.uplink_replay.quarantine.len,
            quarantined_uplink_owned_bytes: self
                .uplink_replay
                .quarantine
                .nodes()
                .map(Node::bytes)
                .sum(),
            owner_catch_up_permit: self.owner_catch_up_permit.is_some(),
            owner_catch_up_permit_armed: self
                .owner_catch_up_permit
                .as_ref()
                .is_some_and(|permit| permit.armed),
        }
    }

    /// Test-only fault injection for post-model receipt correlation. The
    /// reducer state, replay keys, permit counts, and byte capacities stay
    /// unchanged; only the two exact adapter ownership values are exchanged.
    #[cfg(test)]
    pub(crate) fn swap_uplink_replay_ownership_for_test(
        &mut self,
        first_flow_id: SessionFlowId,
        second_flow_id: SessionFlowId,
        direction: Direction,
    ) -> bool {
        let mut first = self
            .uplink_replay
            .keys()
            .filter(|k| k.flow_id == first_flow_id && k.direction == direction);
        let mut second = self
            .uplink_replay
            .keys()
            .filter(|k| k.flow_id == second_flow_id && k.direction == direction);
        let (Some(first_key), Some(second_key)) = (first.next(), second.next()) else {
            return false;
        };
        if first.next().is_some() || second.next().is_some() || first_key == second_key {
            return false;
        }
        drop((first, second));
        let Some(mut first) = take_node(&mut self.uplink_replay.replay, first_key) else {
            return false;
        };
        let Some(second) = self.uplink_replay.get_mut(second_key) else {
            self.uplink_replay.replay = Some(insert(self.uplink_replay.replay.take(), first));
            return false;
        };
        if let Value::Replay { ownership, .. } = &mut first[0].value {
            std::mem::swap(ownership, second);
        }
        self.uplink_replay.replay = Some(insert(self.uplink_replay.replay.take(), first));
        true
    }

    #[cfg(test)]
    pub(in crate::owned_upstream) fn fail_next_uplink_scratch_reserve_for_test(&mut self) {
        assert!(!std::mem::replace(
            &mut self.uplink_replay.fail_scratch,
            true
        ));
    }
    /// Reindexes the same concrete extent without modifying its receipt/permits.
    #[cfg(test)]
    pub(super) fn rekey_uplink_replay_for_test(
        &mut self,
        old: ReplayExtentKey,
        new: ReplayExtentKey,
    ) -> bool {
        if self.uplink_replay.contains_key(&new) {
            return false;
        }
        let Some(mut node) = take_node(&mut self.uplink_replay.replay, old) else {
            return false;
        };
        if let Value::Replay { key, .. } = &mut node[0].value {
            *key = new;
        }
        self.uplink_replay.replay = Some(insert(self.uplink_replay.replay.take(), node));
        true
    }
    pub(super) fn consume_internal_receipts(&mut self) -> Result<(), SessionSupervisorError> {
        let snapshot = self.model.snapshot();
        let session_id = self.model.session_id();
        let journal = self
            .receipt_journal
            .as_mut()
            .ok_or(SessionSupervisorError::PoisonedOwnershipInvariant)?;
        let observations = &mut journal.observations;
        let replay = &mut self.uplink_replay;
        #[cfg(test)]
        let unwind = &mut self.owner_data_unwind_once;
        let mut error = None;
        // retain_mut keeps the original backing/current item and restores its
        // unfinished suffix on unwind. Once a receipt fails, keep the remainder
        // untouched: some matching replay extents may already be released.
        journal.effects.retain_mut(|effect| {
            if error.is_some() {
                return true;
            }
            let settled = (|| -> Result<bool, SessionSupervisorError> {
                match &*effect {
                    SessionEffect::ReplayStored { .. } => {
                        Err(SessionSupervisorError::UnownedReplayStored)
                    }
                    SessionEffect::ReplayAcknowledged { receipt } => {
                        replay.acknowledge(receipt)?;
                        if receipt.next_accepted() > receipt.previous_accepted() {
                            let released_bytes = NonZeroUsize::new(receipt.released_bytes())
                                .ok_or(SessionSupervisorError::InvalidReplayAcknowledgedDelta)?;
                            if observations.application_ack.is_some() {
                                return Err(
                                    SessionSupervisorError::DuplicateReplayAcknowledgedObservation,
                                );
                            }
                            observations.application_ack = Some(ReducerApplicationAckAdvanced {
                                session_id: receipt.session_id(),
                                flow_id: receipt.flow_id(),
                                generation: receipt.generation(),
                                direction: receipt.direction(),
                                previous_accepted: receipt.previous_accepted(),
                                next_accepted: receipt.next_accepted(),
                                released_bytes,
                                final_accepted: receipt.final_accepted(),
                                remaining_oldest: None,
                                session_has_outstanding_replay: false,
                            });
                        }
                        Ok(false)
                    }
                    SessionEffect::SessionExpired => {
                        replay.expire(effect)?;
                        if observations.session_expired.is_some() {
                            return Err(SessionSupervisorError::DuplicateSessionExpiredObservation);
                        }
                        observations.session_expired = Some(ReducerSessionExpired {
                            session_id,
                            generation: snapshot.generation(),
                        });
                        Ok(true)
                    }
                    SessionEffect::FlowFinished {
                        flow_id, terminal, ..
                    } => {
                        #[cfg(test)]
                        if *unwind == Some(OwnerDataUnwindPoint::ReceiptTerminal) {
                            *unwind = None;
                            panic!("single-use Owner terminal receipt unwind");
                        }
                        replay.terminal(*flow_id, effect)?;
                        if observations.flow_terminal.is_some() {
                            return Err(SessionSupervisorError::DuplicateFlowTerminalObservation);
                        }
                        observations.flow_terminal = Some(ReducerFlowDirectionTerminal {
                            terminal: *terminal,
                            generation: snapshot.generation(),
                        });
                        Ok(true)
                    }
                    _ => Ok(true),
                }
            })();
            match settled {
                Ok(keep) => keep,
                Err(cause) => {
                    error = Some(cause);
                    true
                }
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        if let Some(application_ack) = observations.application_ack.as_mut() {
            let still_outstanding = replay.keys().any(|key| {
                key.flow_id == application_ack.flow_id && key.direction == application_ack.direction
            });
            application_ack.remaining_oldest =
                still_outstanding.then_some(application_ack.next_accepted);
            application_ack.session_has_outstanding_replay = !replay.is_empty();
        }
        Ok(())
    }
}
