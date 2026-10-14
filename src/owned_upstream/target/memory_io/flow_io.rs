//! Complete original TargetIo transaction on the parent's MemoryTarget.
//! Owns native nodes, requested-backing admission and complete TargetIo ordering.
//! Uses ancestor preparations; storage/configuration remain Target-private.
//! No forwarding or second owner; entry is the existing TargetIo Interface.
//! Real operation and ownership tests remain in target/tests/.

use super::*;

fn height(link: &TargetNodeLink) -> u8 {
    link.as_ref().map_or(0, |n| n[0].height)
}
fn refresh(node: &mut TargetNode) {
    node.height = 1 + height(&node.left).max(height(&node.right));
}
fn rotate_left(mut root: Box<[TargetNode; 1]>) -> Box<[TargetNode; 1]> {
    let Some(mut next) = root[0].right.take() else {
        return root;
    };
    root[0].right = next[0].left.take();
    refresh(&mut root[0]);
    next[0].left = Some(root);
    refresh(&mut next[0]);
    next
}
fn rotate_right(mut root: Box<[TargetNode; 1]>) -> Box<[TargetNode; 1]> {
    let Some(mut next) = root[0].left.take() else {
        return root;
    };
    root[0].left = next[0].right.take();
    refresh(&mut root[0]);
    next[0].right = Some(root);
    refresh(&mut next[0]);
    next
}
fn balanced(mut root: Box<[TargetNode; 1]>) -> Box<[TargetNode; 1]> {
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
fn insert(root: TargetNodeLink, node: Box<[TargetNode; 1]>) -> Box<[TargetNode; 1]> {
    let Some(mut root) = root else { return node };
    if node[0].key < root[0].key {
        root[0].left = Some(insert(root[0].left.take(), node));
    } else {
        debug_assert_ne!(node[0].key, root[0].key);
        root[0].right = Some(insert(root[0].right.take(), node));
    }
    balanced(root)
}
fn pop_first(root: &mut TargetNodeLink) -> TargetNodeLink {
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
fn take_node(root: &mut TargetNodeLink, key: SessionFlowId) -> TargetNodeLink {
    let mut node = root.take()?;
    let removed = match key.cmp(&node[0].key) {
        std::cmp::Ordering::Less => take_node(&mut node[0].left, key),
        std::cmp::Ordering::Greater => take_node(&mut node[0].right, key),
        std::cmp::Ordering::Equal => {
            // Relink successor's whole Box, retaining this exact key's Box.
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
fn find(root: &TargetNodeLink, key: SessionFlowId) -> Option<&TargetNode> {
    let mut at = root.as_ref();
    while let Some(node) = at {
        match key.cmp(&node[0].key) {
            std::cmp::Ordering::Equal => return Some(&node[0]),
            std::cmp::Ordering::Less => at = node[0].left.as_ref(),
            std::cmp::Ordering::Greater => at = node[0].right.as_ref(),
        }
    }
    None
}

// An AVL with height h needs Fibonacci(h+2)-1 nodes; 2*pointer bits is a
// conservative bound for any addressable native-node tree, without a heap stack.
struct Nodes<'a> {
    stack: [Option<&'a TargetNode>; 2 * usize::BITS as usize],
    depth: usize,
}
impl<'a> Nodes<'a> {
    fn new(root: &'a TargetNodeLink) -> Self {
        let mut iter = Self {
            stack: [None; 2 * usize::BITS as usize],
            depth: 0,
        };
        iter.descend(root);
        iter
    }
    fn descend(&mut self, mut root: &'a TargetNodeLink) {
        while let Some(node) = root {
            self.stack[self.depth] = Some(&node[0]);
            self.depth += 1;
            root = &node[0].left;
        }
    }
}
impl<'a> Iterator for Nodes<'a> {
    type Item = &'a TargetNode;
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

#[cfg(test)]
std::thread_local! {
    static FAIL_NODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
#[cfg(test)]
impl MemoryTarget {
    pub(in crate::owned_upstream::target) fn fail_next_node_allocation_for_test() {
        assert!(!FAIL_NODE.replace(true));
    }
}

// This candidate owns only a new allocation. Existing free custody is untouched
// throughout address preparation and every modeled rejection.
struct PreparedNode {
    new: TargetNodeLink,
}

fn address_backing(target: &TargetAddr) -> usize {
    match target {
        TargetAddr::IpPort(_) => 0,
        TargetAddr::DomainPort { host, .. } => host.capacity(),
    }
}

impl MemoryTarget {
    fn check_address_overlap(&self, owned: usize, borrowed: usize) -> Result<(), TargetIoError> {
        let bytes = owned
            .checked_add(borrowed)
            .ok_or(TargetIoError::StorageLayout)?;
        let addresses =
            std::alloc::Layout::array::<u8>(bytes).map_err(|_| TargetIoError::StorageLayout)?;
        self.config
            .requested_storage_peak
            .extend(addresses)
            .map_err(|_| TargetIoError::StorageLayout)?;
        Ok(())
    }
}

impl MemoryTargetStorage {
    fn cold_requested_backing(
        &self,
        config: MemoryTargetConfig,
    ) -> Result<usize, TargetIoColdInventoryError> {
        let m = config.per_flow_buffer_bytes.min(config.global_buffer_bytes);
        let check = |actual, max| {
            if actual > max {
                Err(TargetIoColdInventoryError::StoredMismatch {
                    domain: "Target requested backing",
                    summed: actual,
                    stored: max,
                })
            } else {
                Ok(())
            }
        };
        let mut addresses = 0usize;
        // cold_check already validated both trees/phases before this traversal.
        for node in Nodes::new(&self.live).chain(Nodes::new(&self.joined)) {
            let target = match &node.phase {
                TargetNodePhase::Live(flow) => {
                    check(flow.readable.capacity(), m)?;
                    check(flow.written.capacity(), m)?;
                    check(
                        flow.write_directives.capacity(),
                        config.max_write_directives_per_flow,
                    )?;
                    check(flow.output.len(), m)?;
                    &flow.target
                }
                TargetNodePhase::Joined(flow) => &flow.target,
                TargetNodePhase::Vacant => unreachable!("cold-validated occupied roots"),
            };
            addresses = addresses.checked_add(address_backing(target)).ok_or(
                TargetIoColdInventoryError::CounterOverflow {
                    domain: "Target address backing",
                },
            )?;
        }
        Ok(addresses)
    }

    pub(in crate::owned_upstream::target) fn checked_peak(
        config: &MemoryTargetConfig,
    ) -> Result<std::alloc::Layout, TargetIoConfigError> {
        use std::alloc::Layout;
        // Keep the original individual checks and their error precedence.
        let readiness = MemoryReadiness::check_layout(config.max_flows)?;
        Self::check_layout(config.max_flows, config.max_terminal_tombstones)?;
        let bad = || TargetIoConfigError::StorageLayoutOverflow;
        let nodes = config
            .max_flows
            .checked_add(config.max_terminal_tombstones)
            .ok_or_else(bad)?;
        let m = config.per_flow_buffer_bytes.min(config.global_buffer_bytes);
        let per_direction = config.max_flows.checked_mul(m).ok_or_else(bad)?;
        let byte_backing = per_direction.checked_mul(2).ok_or_else(bad)?;
        let output = config.global_buffer_bytes.min(per_direction);
        let mut layout = Layout::new::<MemoryTarget>();
        for part in [
            Layout::array::<TargetNode>(nodes).map_err(|_| bad())?,
            readiness,
            Layout::array::<u8>(byte_backing).map_err(|_| bad())?,
            Layout::array::<MemoryWriteDirective>(config.max_write_directives)
                .map_err(|_| bad())?,
            Layout::array::<u8>(output).map_err(|_| bad())?,
        ] {
            layout = layout.extend(part).map_err(|_| bad())?.0;
        }
        // Only one original operation owns the mutable Target. Pending output
        // on other flows may coexist with either replacement branch. Compare
        // complete aligned compositions, not the unaligned candidate sizes.
        let bytes = layout
            .extend(Layout::array::<u8>(m).map_err(|_| bad())?)
            .map_err(|_| bad())?
            .0;
        let directives = layout
            .extend(
                Layout::array::<MemoryWriteDirective>(config.max_write_directives_per_flow)
                    .map_err(|_| bad())?,
            )
            .map_err(|_| bad())?
            .0;
        Ok(if bytes.size() >= directives.size() {
            bytes
        } else {
            directives
        })
    }

    pub(in crate::owned_upstream::target) fn check_layout(
        f: usize,
        z: usize,
    ) -> Result<(), TargetIoConfigError> {
        let count = f
            .checked_add(z)
            .ok_or(TargetIoConfigError::StorageLayoutOverflow)?;
        std::alloc::Layout::array::<TargetNode>(count)
            .map_err(|_| TargetIoConfigError::StorageLayoutOverflow)?;
        Ok(())
    }

    pub(in crate::owned_upstream::target) fn len(&self) -> usize {
        self.live_len
    }
    pub(in crate::owned_upstream::target) fn get(
        &self,
        key: &SessionFlowId,
    ) -> Option<&MemoryTargetFlow> {
        match &find(&self.live, *key)?.phase {
            TargetNodePhase::Live(flow) => Some(flow),
            _ => None,
        }
    }
    pub(in crate::owned_upstream::target) fn get_mut(
        &mut self,
        key: &SessionFlowId,
    ) -> Option<&mut MemoryTargetFlow> {
        let mut at = self.live.as_mut();
        while let Some(node) = at {
            match key.cmp(&node[0].key) {
                std::cmp::Ordering::Equal => {
                    return match &mut node[0].phase {
                        TargetNodePhase::Live(flow) => Some(flow),
                        _ => None,
                    };
                }
                std::cmp::Ordering::Less => at = node[0].left.as_mut(),
                std::cmp::Ordering::Greater => at = node[0].right.as_mut(),
            }
        }
        None
    }
    pub(in crate::owned_upstream::target) fn get_joined(
        &self,
        key: &SessionFlowId,
    ) -> Option<&JoinedTargetFlow> {
        match &find(&self.joined, *key)?.phase {
            TargetNodePhase::Joined(flow) => Some(flow),
            _ => None,
        }
    }
    pub(in crate::owned_upstream::target) fn iter(
        &self,
    ) -> impl Iterator<Item = (&SessionFlowId, &MemoryTargetFlow)> {
        Nodes::new(&self.live).map(|node| match &node.phase {
            TargetNodePhase::Live(flow) => (&node.key, flow),
            _ => unreachable!("private live root owns only Live"),
        })
    }
    pub(in crate::owned_upstream::target) fn values(
        &self,
    ) -> impl Iterator<Item = &MemoryTargetFlow> {
        self.iter().map(|(_, flow)| flow)
    }

    fn prepare(
        &self,
        key: SessionFlowId,
        config: MemoryTargetConfig,
    ) -> Result<PreparedNode, TargetIoError> {
        let limit = config
            .max_flows
            .checked_add(config.max_terminal_tombstones)
            .ok_or(TargetIoError::StorageLayout)?;
        let occupied = self
            .live_len
            .checked_add(self.joined_len)
            .ok_or(TargetIoError::StorageLayout)?;
        let resident = occupied
            .checked_add(self.free_len)
            .ok_or(TargetIoError::StorageLayout)?;
        let needed = occupied
            .checked_add(1)
            .ok_or(TargetIoError::StorageLayout)?;
        let peak = resident.max(needed);
        if peak > limit || self.free.is_some() != (self.free_len != 0) {
            return Err(TargetIoError::StorageLayout);
        }
        std::alloc::Layout::array::<TargetNode>(peak).map_err(|_| TargetIoError::StorageLayout)?;
        if let Some(node) = &self.free {
            if !matches!(node[0].phase, TargetNodePhase::Vacant)
                || node[0].right.is_some()
                || node[0].height != 1
                || node[0].left.is_some() != (self.free_len > 1)
            {
                return Err(TargetIoError::StorageLayout);
            }
            return Ok(PreparedNode { new: None });
        }
        #[cfg(test)]
        if FAIL_NODE.replace(false) {
            return Err(TargetIoError::StorageReserve);
        }
        let mut allocation = allocate_exact_vec::<TargetNode>(1).map_err(|error| match error {
            EffectStorageError::Reserve => TargetIoError::StorageReserve,
            _ => TargetIoError::StorageLayout,
        })?;
        allocation.push(TargetNode {
            key,
            phase: TargetNodePhase::Vacant,
            left: None,
            right: None,
            height: 1,
        });
        let node = allocation
            .into_boxed_slice()
            .try_into()
            .map_err(|_| TargetIoError::StorageLayout)?;
        Ok(PreparedNode { new: Some(node) })
    }

    fn install(&mut self, key: SessionFlowId, flow: MemoryTargetFlow, prepared: PreparedNode) {
        // Exclusive OPEN borrow: prepare proved this unchanged candidate. No
        // allocation, user callback or fallible operation occurs during commit.
        let mut node = prepared
            .new
            .or_else(|| self.pop_free())
            .expect("prepared OPEN retains one exact new or free node");
        node[0].key = key;
        node[0].phase = TargetNodePhase::Live(flow);
        self.live = Some(insert(self.live.take(), node));
        self.live_len += 1;
    }
    fn pop_free(&mut self) -> TargetNodeLink {
        let mut node = self.free.take()?;
        self.free = node[0].left.take();
        self.free_len -= 1;
        Some(node)
    }
    fn join(&mut self, key: SessionFlowId, terminal: TargetTerminalKind) {
        let mut node =
            take_node(&mut self.live, key).expect("exclusive join preflight proved live node");
        let TargetNodePhase::Live(flow) =
            std::mem::replace(&mut node[0].phase, TargetNodePhase::Vacant)
        else {
            unreachable!("private live root owns only Live")
        };
        node[0].phase = TargetNodePhase::Joined(JoinedTargetFlow {
            target: flow.target,
            terminal,
        });
        self.live_len -= 1;
        self.joined = Some(insert(self.joined.take(), node));
        self.joined_len += 1;
        // Only normalized empty buffers are disposed here. Cached user Wakers
        // remain in readiness until the entire owning-tree move has committed.
    }
    fn retire(&mut self, key: SessionFlowId) -> TargetTerminalKind {
        let mut node = take_node(&mut self.joined, key)
            .expect("exclusive retire preflight proved joined node");
        let TargetNodePhase::Joined(flow) =
            std::mem::replace(&mut node[0].phase, TargetNodePhase::Vacant)
        else {
            unreachable!("private joined root owns only Joined")
        };
        self.joined_len -= 1;
        node[0].left = self.free.take();
        self.free = Some(node);
        self.free_len += 1;
        let terminal = flow.terminal;
        drop(flow);
        terminal
    }

    fn cold_check(&self, config: MemoryTargetConfig) -> Result<(), TargetIoColdInventoryError> {
        let mismatch = || TargetIoColdInventoryError::StoredMismatch {
            domain: "Target native nodes",
            summed: self.live_len,
            stored: self.joined_len,
        };
        // Audit before iteration so even defensive malformed phase/height data
        // fail closed. Depth is bounded independently of the stored height.
        fn tree(
            root: &TargetNodeLink,
            joined: bool,
            previous: &mut Option<SessionFlowId>,
            depth: usize,
        ) -> Option<(usize, u8)> {
            let Some(node) = root else {
                return Some((0, 0));
            };
            if depth >= 2 * usize::BITS as usize {
                return None;
            }
            let node = &node[0];
            let (left_count, left_height) = tree(&node.left, joined, previous, depth + 1)?;
            if previous.is_some_and(|old| old >= node.key) {
                return None;
            }
            *previous = Some(node.key);
            if !matches!(
                (&node.phase, joined),
                (TargetNodePhase::Live(_), false) | (TargetNodePhase::Joined(_), true)
            ) {
                return None;
            }
            let (right_count, right_height) = tree(&node.right, joined, previous, depth + 1)?;
            let height = left_height.max(right_height).checked_add(1)?;
            if node.height != height || left_height.abs_diff(right_height) > 1 {
                return None;
            }
            Some((left_count.checked_add(right_count)?.checked_add(1)?, height))
        }
        let live = tree(&self.live, false, &mut None, 0)
            .ok_or_else(mismatch)?
            .0;
        let joined = tree(&self.joined, true, &mut None, 0)
            .ok_or_else(mismatch)?
            .0;
        if live != self.live_len || joined != self.joined_len {
            return Err(mismatch());
        }
        // Merge two ordered walks, O(F+Z), not a lookup for every node.
        let mut a = Nodes::new(&self.live).peekable();
        let mut b = Nodes::new(&self.joined).peekable();
        while let (Some(x), Some(y)) = (a.peek(), b.peek()) {
            match x.key.cmp(&y.key) {
                std::cmp::Ordering::Less => {
                    a.next();
                }
                std::cmp::Ordering::Greater => {
                    b.next();
                }
                std::cmp::Ordering::Equal => return Err(mismatch()),
            }
        }
        let mut free = &self.free;
        let mut seen = 0usize;
        while let Some(node) = free {
            if seen >= self.free_len
                || !matches!(node[0].phase, TargetNodePhase::Vacant)
                || node[0].right.is_some()
                || node[0].height != 1
            {
                return Err(mismatch());
            }
            seen += 1;
            free = &node[0].left;
        }
        let resident = live
            .checked_add(joined)
            .and_then(|n| n.checked_add(seen))
            .ok_or_else(mismatch)?;
        let limit = config
            .max_flows
            .checked_add(config.max_terminal_tombstones)
            .ok_or_else(mismatch)?;
        if seen != self.free_len
            || live > config.max_flows
            || joined > config.max_terminal_tombstones
            || resident > limit
        {
            return Err(mismatch());
        }
        std::alloc::Layout::array::<TargetNode>(resident).map_err(|_| mismatch())?;
        Ok(())
    }
}

impl Drop for MemoryTargetStorage {
    fn drop(&mut self) {
        // Retained vacancies form a potentially long chain, unlike AVL roots.
        while let Some(mut node) = self.free.take() {
            self.free = node[0].left.take();
        }
    }
}

impl TargetIo for MemoryTarget {
    fn capacity_witness(&self) -> TargetIoCapacityWitness {
        self.config.capacity_witness()
    }

    fn cold_inventory(&self) -> Result<TargetIoColdInventory, TargetIoColdInventoryError> {
        self.storage.cold_check(self.config)?;
        let (summed_buffered_bytes, summed_write_directives) = self.storage.values().try_fold(
            (0usize, 0usize),
            |(buffered_total, directive_total), flow| {
                let flow_buffered = flow
                    .written
                    .len()
                    .checked_add(flow.readable.len())
                    .and_then(|count| count.checked_add(flow.output.len()))
                    .ok_or(TargetIoColdInventoryError::CounterOverflow {
                        domain: "flow_buffered_bytes",
                    })?;
                let buffered_total = buffered_total.checked_add(flow_buffered).ok_or(
                    TargetIoColdInventoryError::CounterOverflow {
                        domain: "buffered_bytes",
                    },
                )?;
                let directive_total = directive_total
                    .checked_add(flow.write_directives.len())
                    .ok_or(TargetIoColdInventoryError::CounterOverflow {
                        domain: "write_directives",
                    })?;
                Ok((buffered_total, directive_total))
            },
        )?;
        let inventory = TargetIoColdInventory::checked(
            self.capacity_witness(),
            self.storage.len(),
            self.storage.joined_len,
            summed_buffered_bytes,
            self.buffered_bytes,
            summed_write_directives,
            self.queued_write_directives,
        )?;
        self.ready.cold_check(&self.storage)?;
        let addresses = self.storage.cold_requested_backing(self.config)?;
        if addresses != self.owned_address_capacity {
            return Err(TargetIoColdInventoryError::StoredMismatch {
                domain: "Target address backing",
                summed: addresses,
                stored: self.owned_address_capacity,
            });
        }
        self.check_address_overlap(addresses, 0).map_err(|_| {
            TargetIoColdInventoryError::CounterOverflow {
                domain: "Target requested backing",
            }
        })?;
        Ok(inventory)
    }

    fn open(
        &mut self,
        flow_id: SessionFlowId,
        target: &TargetAddr,
    ) -> Result<TargetOpenCompletion, TargetIoError> {
        bump(&mut self.snapshot.open_attempts, "open_attempts")?;
        if let Some(flow) = self.storage.get_joined(&flow_id) {
            if flow.target == *target {
                bump(&mut self.snapshot.duplicate_opens, "duplicate_opens")?;
                return Ok(TargetOpenCompletion::AlreadyTerminal(flow.terminal));
            }
            bump(&mut self.snapshot.conflicting_opens, "conflicting_opens")?;
            return Err(TargetIoError::ConflictingTarget { flow_id });
        }
        if let Some(flow) = self.storage.get(&flow_id) {
            if flow.target == *target {
                flow.require_output_idle(flow_id)?;
                bump(&mut self.snapshot.duplicate_opens, "duplicate_opens")?;
                return Ok(match flow.terminal {
                    Some(kind) => TargetOpenCompletion::AlreadyTerminal(kind),
                    None => TargetOpenCompletion::AlreadyOpen,
                });
            }
            bump(&mut self.snapshot.conflicting_opens, "conflicting_opens")?;
            return Err(TargetIoError::ConflictingTarget { flow_id });
        }
        if let Some(highest_seen) = self
            .highest_flow_id
            .filter(|highest_seen| flow_id <= *highest_seen)
        {
            return Err(TargetIoError::StaleFlowId {
                flow_id,
                highest_seen,
            });
        }
        if self.storage.len() >= self.config.max_flows {
            // Consuming a new flow ID is monotonic even when live-handle
            // admission fails. A later stale copy of that OPEN must never gain
            // a socket merely because capacity became available.
            self.highest_flow_id = Some(flow_id);
            return Ok(TargetOpenCompletion::Failed(
                TargetOpenFailure::ResourceExhausted,
            ));
        }
        let opened_flows =
            self.snapshot
                .opened_flows
                .checked_add(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "opened_flows",
                })?;
        let ready = self.ready.prepare_open()?;
        let copied = match target {
            TargetAddr::IpPort(_) => 0,
            TargetAddr::DomainPort { host, .. } => host.len(),
        };
        let addresses = self
            .owned_address_capacity
            .checked_add(copied)
            .ok_or(TargetIoError::StorageLayout)?;
        // Check the complete measurable overlap before even the fresh Node
        // allocation. The caller's original String remains borrowed custody.
        self.check_address_overlap(addresses, address_backing(target))?;
        let node = self.storage.prepare(flow_id, self.config)?;
        let target = prepare_address(target)?;
        self.highest_flow_id = Some(flow_id);
        self.storage.install(
            flow_id,
            MemoryTargetFlow {
                ready_position: ready.position,
                target,
                written: VecDeque::new(),
                readable: VecDeque::new(),
                write_directives: VecDeque::new(),
                output: MemoryTargetOutput::Idle,
                read_eof_queued: false,
                read_eof_delivered: false,
                write_half_closed: false,
                terminal: None,
            },
            node,
        );
        self.snapshot.opened_flows = opened_flows;
        self.owned_address_capacity = addresses;
        self.ready.bind(flow_id, ready);
        Ok(TargetOpenCompletion::Opened)
    }

    fn write(
        &mut self,
        flow_id: SessionFlowId,
        bytes: &[u8],
    ) -> Result<TargetWriteCompletion, TargetIoError> {
        if bytes.is_empty() {
            return Err(TargetIoError::EmptyWrite);
        }
        bump(&mut self.snapshot.write_calls, "write_calls")?;

        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        if let Some(kind) = flow.terminal {
            return Err(TargetIoError::FlowTerminal { flow_id, kind });
        }
        if flow.write_half_closed {
            return Err(TargetIoError::WriteAfterHalfClose { flow_id });
        }
        match flow.write_directives.front().copied() {
            Some(MemoryWriteDirective::WouldBlock) => {
                let queued_write_directives = self.queued_write_directives.checked_sub(1).ok_or(
                    TargetIoError::CounterOverflow {
                        counter: "queued_write_directives",
                    },
                )?;
                let write_would_block = self.snapshot.write_would_block.checked_add(1).ok_or(
                    TargetIoError::CounterOverflow {
                        counter: "write_would_block",
                    },
                )?;
                let ready = self
                    .ready
                    .prepare(flow_id, flow, ReadyChange::WriteDirective(true))?;
                flow.write_directives.pop_front();
                self.queued_write_directives = queued_write_directives;
                self.snapshot.write_would_block = write_would_block;
                self.ready.publish(ready);
                return Ok(TargetWriteCompletion::WouldBlock);
            }
            Some(MemoryWriteDirective::Zero) => {
                let queued_write_directives = self.queued_write_directives.checked_sub(1).ok_or(
                    TargetIoError::CounterOverflow {
                        counter: "queued_write_directives",
                    },
                )?;
                let write_zero = self.snapshot.write_zero.checked_add(1).ok_or(
                    TargetIoError::CounterOverflow {
                        counter: "write_zero",
                    },
                )?;
                let ready = self
                    .ready
                    .prepare(flow_id, flow, ReadyChange::WriteDirective(true))?;
                flow.write_directives.pop_front();
                self.queued_write_directives = queued_write_directives;
                self.snapshot.write_zero = write_zero;
                self.ready.publish(ready);
                return Ok(TargetWriteCompletion::Zero);
            }
            Some(MemoryWriteDirective::AcceptAtMost(_)) | None => {}
        }

        let flow_available = self
            .config
            .per_flow_buffer_bytes
            .saturating_sub(flow.buffered_bytes());
        let global_available = self
            .config
            .global_buffer_bytes
            .saturating_sub(self.buffered_bytes);
        let directive_limit = match flow.write_directives.front().copied() {
            Some(MemoryWriteDirective::AcceptAtMost(limit)) => limit.get(),
            Some(MemoryWriteDirective::WouldBlock | MemoryWriteDirective::Zero) => {
                return Err(TargetIoError::CounterOverflow {
                    counter: "write_directive_order",
                });
            }
            None => self.config.max_write_chunk,
        };
        let accepted = bytes
            .len()
            .min(self.config.max_write_chunk)
            .min(directive_limit)
            .min(flow_available)
            .min(global_available);
        if accepted == 0 {
            bump(&mut self.snapshot.write_would_block, "write_would_block")?;
            return Ok(TargetWriteCompletion::WouldBlock);
        }
        let consumes_directive = matches!(
            flow.write_directives.front(),
            Some(MemoryWriteDirective::AcceptAtMost(_))
        );
        let queued_write_directives = if consumes_directive {
            self.queued_write_directives
                .checked_sub(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "queued_write_directives",
                })?
        } else {
            self.queued_write_directives
        };
        let buffered_bytes =
            self.buffered_bytes
                .checked_add(accepted)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "buffered_bytes",
                })?;
        let accepted_u64 = u64::try_from(accepted).map_err(|_| TargetIoError::CounterOverflow {
            counter: "write_accepted_bytes",
        })?;
        let write_accepted_bytes = self
            .snapshot
            .write_accepted_bytes
            .checked_add(accepted_u64)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "write_accepted_bytes",
            })?;
        let completion = TargetWriteCompletion::accepted(accepted)?;
        let ready = self.ready.prepare(
            flow_id,
            flow,
            ReadyChange::WriteDirective(consumes_directive),
        )?;
        let need = flow
            .written
            .len()
            .checked_add(accepted)
            .ok_or(TargetIoError::StorageLayout)?;
        let prepared = prepare_buffer(
            &flow.written,
            need,
            self.config
                .per_flow_buffer_bytes
                .min(self.config.global_buffer_bytes),
        )?;
        if let Some(prepared) = prepared {
            flow.written = prepared;
        }
        if consumes_directive {
            flow.write_directives.pop_front();
        }
        for byte in bytes[..accepted].iter().copied() {
            flow.written.push_back(byte);
        }
        self.queued_write_directives = queued_write_directives;
        self.buffered_bytes = buffered_bytes;
        self.buffer_high_water = self.buffer_high_water.max(self.buffered_bytes);
        self.snapshot.write_accepted_bytes = write_accepted_bytes;
        self.ready.publish(ready);
        Ok(completion)
    }

    fn read(
        &mut self,
        flow_id: SessionFlowId,
        max_bytes: usize,
    ) -> Result<TargetReadCompletion, TargetIoError> {
        if max_bytes == 0 {
            return Err(TargetIoError::ZeroReadLimit);
        }
        bump(&mut self.snapshot.read_calls, "read_calls")?;
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        if let Some(kind @ (TargetTerminalKind::Reset | TargetTerminalKind::Cancelled)) =
            flow.terminal
        {
            return Err(TargetIoError::FlowTerminal { flow_id, kind });
        }
        if flow.read_eof_delivered {
            return Err(TargetIoError::ReadAfterEof { flow_id });
        }
        if !flow.readable.is_empty() {
            let count = max_bytes
                .min(self.config.max_read_chunk)
                .min(flow.readable.len());
            let buffered_bytes =
                self.buffered_bytes
                    .checked_sub(count)
                    .ok_or(TargetIoError::CounterOverflow {
                        counter: "buffered_bytes",
                    })?;
            let read_delivered_bytes = checked_add_usize_to_u64(
                self.snapshot.read_delivered_bytes,
                count,
                "read_delivered_bytes",
            )?;
            let ready = self
                .ready
                .prepare(flow_id, flow, ReadyChange::BytesReleased(count))?;
            let bytes = prepare_output(&flow.readable, count)?;
            flow.output = MemoryTargetOutput::Publishing {
                direction: MemoryTargetOutputDirection::Read,
                bytes,
            };
            drop(flow.readable.drain(..count));
            self.ready.publish(ready);
            self.buffered_bytes = buffered_bytes;
            self.snapshot.read_delivered_bytes = read_delivered_bytes;
            return Ok(TargetReadCompletion::Data(flow.output.finish()));
        }
        if flow.read_eof_queued {
            let read_eof =
                self.snapshot
                    .read_eof
                    .checked_add(1)
                    .ok_or(TargetIoError::CounterOverflow {
                        counter: "read_eof",
                    })?;
            let graceful = flow.write_half_closed;
            let released_directives = if graceful {
                flow.write_directives.len()
            } else {
                0
            };
            let queued_write_directives = self
                .queued_write_directives
                .checked_sub(released_directives)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "queued_write_directives",
                })?;
            let graceful_flows = if graceful {
                self.snapshot.graceful_flows.checked_add(1).ok_or(
                    TargetIoError::CounterOverflow {
                        counter: "graceful_flows",
                    },
                )?
            } else {
                self.snapshot.graceful_flows
            };
            let ready = self.ready.prepare(flow_id, flow, ReadyChange::Eof)?;
            flow.read_eof_delivered = true;
            self.snapshot.read_eof = read_eof;
            if graceful {
                flow.write_directives.clear();
                flow.terminal = Some(TargetTerminalKind::Graceful);
                self.queued_write_directives = queued_write_directives;
                self.snapshot.graceful_flows = graceful_flows;
            }
            self.ready.publish(ready);
            return Ok(TargetReadCompletion::Eof);
        }
        bump(&mut self.snapshot.read_would_block, "read_would_block")?;
        Ok(TargetReadCompletion::WouldBlock)
    }

    fn half_close_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetHalfCloseCompletion, TargetIoError> {
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        if let Some(kind @ (TargetTerminalKind::Reset | TargetTerminalKind::Cancelled)) =
            flow.terminal
        {
            return Err(TargetIoError::FlowTerminal { flow_id, kind });
        }
        if flow.write_half_closed {
            return Ok(TargetHalfCloseCompletion::AlreadyClosed);
        }
        let write_half_closes = self.snapshot.write_half_closes.checked_add(1).ok_or(
            TargetIoError::CounterOverflow {
                counter: "write_half_closes",
            },
        )?;
        let graceful = flow.read_eof_delivered;
        let released_directives = if graceful {
            flow.write_directives.len()
        } else {
            0
        };
        let queued_write_directives = self
            .queued_write_directives
            .checked_sub(released_directives)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "queued_write_directives",
            })?;
        let graceful_flows = if graceful {
            self.snapshot
                .graceful_flows
                .checked_add(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "graceful_flows",
                })?
        } else {
            self.snapshot.graceful_flows
        };
        let ready = self.ready.prepare(flow_id, flow, ReadyChange::HalfClose)?;
        flow.write_half_closed = true;
        self.snapshot.write_half_closes = write_half_closes;
        if graceful {
            flow.write_directives.clear();
            flow.terminal = Some(TargetTerminalKind::Graceful);
            self.queued_write_directives = queued_write_directives;
            self.snapshot.graceful_flows = graceful_flows;
        }
        self.ready.publish(ready);
        Ok(TargetHalfCloseCompletion::Closed)
    }

    fn reset(&mut self, flow_id: SessionFlowId) -> Result<TargetResetCompletion, TargetIoError> {
        let needs_increment = self
            .storage
            .get(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?
            .terminal
            .is_none();
        let reset_flows = if needs_increment {
            self.snapshot
                .reset_flows
                .checked_add(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "reset_flows",
                })?
        } else {
            self.snapshot.reset_flows
        };
        match self.terminalize(flow_id, TargetTerminalKind::Reset)? {
            TerminalizeOutcome::Changed(released, ready) => {
                self.snapshot.reset_flows = reset_flows;
                self.ready.publish(ready);
                Ok(TargetResetCompletion::Reset {
                    released_read_bytes: released.read_bytes,
                    released_written_bytes: released.written_bytes,
                })
            }
            TerminalizeOutcome::Existing(TargetTerminalKind::Reset) => {
                Ok(TargetResetCompletion::AlreadyReset)
            }
            TerminalizeOutcome::Existing(kind) => Ok(TargetResetCompletion::AlreadyTerminal(kind)),
        }
    }

    fn cancel(&mut self, flow_id: SessionFlowId) -> Result<TargetCancelCompletion, TargetIoError> {
        let needs_increment = self
            .storage
            .get(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?
            .terminal
            .is_none();
        let cancelled_flows = if needs_increment {
            self.snapshot
                .cancelled_flows
                .checked_add(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "cancelled_flows",
                })?
        } else {
            self.snapshot.cancelled_flows
        };
        match self.terminalize(flow_id, TargetTerminalKind::Cancelled)? {
            TerminalizeOutcome::Changed(released, ready) => {
                self.snapshot.cancelled_flows = cancelled_flows;
                self.ready.publish(ready);
                Ok(TargetCancelCompletion::Cancelled {
                    released_read_bytes: released.read_bytes,
                    released_written_bytes: released.written_bytes,
                })
            }
            TerminalizeOutcome::Existing(TargetTerminalKind::Cancelled) => {
                Ok(TargetCancelCompletion::AlreadyCancelled)
            }
            TerminalizeOutcome::Existing(kind) => Ok(TargetCancelCompletion::AlreadyTerminal(kind)),
        }
    }

    fn expire_session_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetExpireCompletion, TargetIoError> {
        let flow = self
            .storage
            .get(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        let released = flow.released_buffers()?;
        let released_read_bytes = released.read_bytes;
        let released_written_bytes = released.written_bytes;
        let released_total = released_read_bytes
            .checked_add(released_written_bytes)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "released_buffered_bytes",
            })?;
        let released_directives = flow.write_directives.len();
        let buffered_bytes = self.buffered_bytes.checked_sub(released_total).ok_or(
            TargetIoError::CounterOverflow {
                counter: "buffered_bytes",
            },
        )?;
        let queued_write_directives = self
            .queued_write_directives
            .checked_sub(released_directives)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "queued_write_directives",
            })?;
        let terminal = flow.terminal.unwrap_or(TargetTerminalKind::Cancelled);
        let newly_cancelled = flow.terminal.is_none();
        let cancelled_flows = if newly_cancelled {
            self.snapshot
                .cancelled_flows
                .checked_add(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "cancelled_flows",
                })?
        } else {
            self.snapshot.cancelled_flows
        };

        let ready = self.ready.prepare(flow_id, flow, ReadyChange::Terminal)?;
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.readable.clear();
        flow.written.clear();
        flow.write_directives.clear();
        flow.output = MemoryTargetOutput::Idle;
        flow.terminal = Some(terminal);
        self.buffered_bytes = buffered_bytes;
        self.queued_write_directives = queued_write_directives;
        self.snapshot.cancelled_flows = cancelled_flows;
        self.ready.publish(ready);
        Ok(TargetExpireCompletion {
            terminal,
            released_read_bytes,
            released_written_bytes,
        })
    }

    fn readiness(&self, flow_id: SessionFlowId) -> Result<TargetReadiness, TargetIoError> {
        if let Some(flow) = self.storage.get_joined(&flow_id) {
            return Err(TargetIoError::FlowJoined {
                flow_id,
                kind: flow.terminal,
            });
        }
        let flow = self
            .storage
            .get(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        let flow_available = self
            .config
            .per_flow_buffer_bytes
            .saturating_sub(flow.buffered_bytes());
        let global_available = self
            .config
            .global_buffer_bytes
            .saturating_sub(self.buffered_bytes);
        let directive_ready = !matches!(
            flow.write_directives.front(),
            Some(MemoryWriteDirective::WouldBlock)
        );
        Ok(TargetReadiness {
            readable: !flow.readable.is_empty()
                || (flow.read_eof_queued && !flow.read_eof_delivered),
            writable: flow.terminal.is_none()
                && !flow.write_half_closed
                && directive_ready
                && flow_available > 0
                && global_available > 0,
            joinable: flow.terminal.is_some()
                && flow.buffered_bytes() == 0
                && flow.write_directives.is_empty(),
            terminal: flow.terminal,
        })
    }

    fn poll_readiness(
        &mut self,
        flow_id: SessionFlowId,
        interest: TargetReadyInterest,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TargetReadyOutcome, TargetIoError>> {
        if let Some(flow) = self.storage.get_joined(&flow_id) {
            return Poll::Ready(if interest == TargetReadyInterest::Join {
                Ok(TargetReadyOutcome::Attempt)
            } else {
                Err(TargetIoError::FlowJoined {
                    flow_id,
                    kind: flow.terminal,
                })
            });
        }
        let Some(flow) = self.storage.get(&flow_id) else {
            return Poll::Ready(Err(TargetIoError::UnknownFlow { flow_id }));
        };
        if let Err(error) = flow.require_output_idle(flow_id) {
            return Poll::Ready(Err(error));
        }
        let ready = match interest {
            TargetReadyInterest::Read => {
                if let Some(kind @ (TargetTerminalKind::Reset | TargetTerminalKind::Cancelled)) =
                    flow.terminal
                {
                    return Poll::Ready(Err(TargetIoError::FlowTerminal { flow_id, kind }));
                }
                if flow.read_eof_delivered {
                    return Poll::Ready(Err(TargetIoError::ReadAfterEof { flow_id }));
                }
                !flow.readable.is_empty() || flow.read_eof_queued
            }
            TargetReadyInterest::Write => {
                if let Some(kind) = flow.terminal {
                    return Poll::Ready(Err(TargetIoError::FlowTerminal { flow_id, kind }));
                }
                if flow.write_half_closed {
                    return Poll::Ready(Err(TargetIoError::WriteAfterHalfClose { flow_id }));
                }
                match flow.write_directives.front() {
                    Some(MemoryWriteDirective::WouldBlock | MemoryWriteDirective::Zero) => true,
                    _ => {
                        let Some(own) = flow.readable.len().checked_add(flow.written.len()) else {
                            return Poll::Ready(Err(TargetIoError::ReadinessInvariant(
                                "buffer sum overflow",
                            )));
                        };
                        if own > self.config.per_flow_buffer_bytes
                            || self.buffered_bytes > self.config.global_buffer_bytes
                        {
                            return Poll::Ready(Err(TargetIoError::ReadinessInvariant(
                                "buffer capacity",
                            )));
                        }
                        own < self.config.per_flow_buffer_bytes
                            && self.buffered_bytes < self.config.global_buffer_bytes
                    }
                }
            }
            TargetReadyInterest::Join => {
                flow.terminal.is_some()
                    && flow.readable.is_empty()
                    && flow.written.is_empty()
                    && flow.write_directives.is_empty()
                    && self.storage.joined_len < self.config.max_terminal_tombstones
            }
        };
        if let Err(error) =
            self.ready
                .register(flow_id, flow.ready_position, interest, !ready, cx.waker())
        {
            return Poll::Ready(Err(error));
        }
        if ready {
            Poll::Ready(Ok(TargetReadyOutcome::Attempt))
        } else {
            Poll::Pending
        }
    }

    fn poll_ready_notifications(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TargetIoError>> {
        self.ready.poll_common(&self.storage, cx)
    }

    fn join(&mut self, flow_id: SessionFlowId) -> Result<TargetJoinCompletion, TargetIoError> {
        if let Some(flow) = self.storage.get_joined(&flow_id) {
            return Ok(TargetJoinCompletion::AlreadyJoined(flow.terminal));
        }
        let flow = self
            .storage
            .get(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        let Some(terminal) = flow.terminal else {
            return Err(TargetIoError::JoinBeforeTerminal { flow_id });
        };
        let bytes = flow.buffered_bytes();
        let directives = flow.write_directives.len();
        if bytes != 0 || directives != 0 {
            return Ok(TargetJoinCompletion::PendingOwnedState { bytes, directives });
        }
        if self.storage.joined_len >= self.config.max_terminal_tombstones {
            return Ok(TargetJoinCompletion::PendingTerminalCapacity {
                max: self.config.max_terminal_tombstones,
            });
        }
        let ready = self.ready.prepare_remove(flow_id, flow.ready_position)?;
        self.storage.join(flow_id, terminal);
        self.ready.remove(ready);
        Ok(TargetJoinCompletion::Joined(terminal))
    }

    fn retire_terminal_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetTombstoneRetireCompletion, TargetIoError> {
        let Some(joined) = self.storage.get_joined(&flow_id) else {
            return Ok(TargetTombstoneRetireCompletion::Absent);
        };
        let released = address_backing(&joined.target);
        let revision = self.ready.prepare_shared_release()?;
        let addresses = self
            .owned_address_capacity
            .checked_sub(released)
            .ok_or(TargetIoError::StorageLayout)?;
        let terminal = self.storage.retire(flow_id);
        self.owned_address_capacity = addresses;
        self.ready.publish_shared(revision);
        Ok(TargetTombstoneRetireCompletion::Retired(terminal))
    }
}
