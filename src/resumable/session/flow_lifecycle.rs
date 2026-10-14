//! Original Session OPEN, terminal transition and expiry transactions.
//! Owns precommit validation, identity publication and terminal effect ordering.
//! Not TCP/Target/transport authority; recovery remains in the Session parent.
//! Entries are existing SessionModel methods; tests use real model events.

use super::*;
use crate::resumable::effect_storage::allocate_exact_vec;
use std::alloc::Layout;

#[cfg(test)]
mod tests;

type Link = Option<Box<[Node; 1]>>;

// One physical Layout across all phases permits an owning-node move, with no
// second allocation at terminal commit. Even vacant/terminal nodes pay this size.
enum Value {
    Vacant,
    Live(FlowState),
    Terminal(TerminalTombstone),
}

struct Node {
    key: SessionFlowId,
    value: Value,
    left: Link,
    right: Link,
    height: u8,
}

#[derive(Default)]
struct Vacancies {
    head: Link,
    len: usize,
}

impl Vacancies {
    fn push(&mut self, mut node: Box<[Node; 1]>) {
        debug_assert!(node[0].left.is_none() && node[0].right.is_none());
        debug_assert!(matches!(node[0].value, Value::Vacant));
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

impl Drop for Vacancies {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[derive(Default)]
pub(super) struct FlowStorage {
    live: Link,
    terminal: Link,
    free: Vacancies,
    live_len: usize,
    terminal_len: usize,
}

// The original PreparedLocalOpen already owns the exclusive Model borrow.
// This guard owns only a new missing node; it never extracts an old vacancy.
pub(super) struct PreparedFlowNode {
    new: Link,
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
fn take_node(root: &mut Link, key: SessionFlowId) -> Link {
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
fn find(root: &Link, key: SessionFlowId) -> Option<&Node> {
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

impl FlowStorage {
    pub(super) fn len(&self) -> usize {
        self.live_len
    }
    pub(super) fn terminal_len(&self) -> usize {
        self.terminal_len
    }
    pub(super) fn get(&self, key: &SessionFlowId) -> Option<&FlowState> {
        match &find(&self.live, *key)?.value {
            Value::Live(flow) => Some(flow),
            _ => None,
        }
    }
    pub(super) fn get_mut(&mut self, key: &SessionFlowId) -> Option<&mut FlowState> {
        let mut at = self.live.as_mut();
        while let Some(node) = at {
            match key.cmp(&node[0].key) {
                std::cmp::Ordering::Equal => {
                    return match &mut node[0].value {
                        Value::Live(flow) => Some(flow),
                        _ => None,
                    };
                }
                std::cmp::Ordering::Less => at = node[0].left.as_mut(),
                std::cmp::Ordering::Greater => at = node[0].right.as_mut(),
            }
        }
        None
    }
    pub(super) fn get_terminal(&self, key: &SessionFlowId) -> Option<&TerminalTombstone> {
        match &find(&self.terminal, *key)?.value {
            Value::Terminal(tombstone) => Some(tombstone),
            _ => None,
        }
    }
    pub(super) fn contains_key(&self, key: &SessionFlowId) -> bool {
        self.get(key).is_some()
    }
    pub(super) fn contains_terminal(&self, key: &SessionFlowId) -> bool {
        self.get_terminal(key).is_some()
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = (&SessionFlowId, &FlowState)> {
        Nodes::new(&self.live).map(|node| match &node.value {
            Value::Live(flow) => (&node.key, flow),
            _ => unreachable!("private live root contains only live nodes"),
        })
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &FlowState> {
        self.iter().map(|(_, flow)| flow)
    }
    pub(super) fn terminal_values(&self) -> impl Iterator<Item = &TerminalTombstone> {
        Nodes::new(&self.terminal).map(|node| match &node.value {
            Value::Terminal(tombstone) => tombstone,
            _ => unreachable!("private terminal root contains only terminal nodes"),
        })
    }

    fn prepare(
        &self,
        key: SessionFlowId,
        config: SessionConfig,
    ) -> Result<PreparedFlowNode, SessionError> {
        let limit = config
            .max_flows()
            .checked_add(config.max_terminal_tombstones())
            .ok_or(SessionError::FlowStorageLayout)?;
        let occupied = self
            .live_len
            .checked_add(self.terminal_len)
            .ok_or(SessionError::FlowStorageLayout)?;
        let resident = occupied
            .checked_add(self.free.len)
            .ok_or(SessionError::FlowStorageLayout)?;
        let needed = occupied
            .checked_add(1)
            .ok_or(SessionError::FlowStorageLayout)?;
        let peak = resident.max(needed);
        if peak > limit {
            return Err(SessionError::FlowStorageLayout);
        }
        Layout::array::<Node>(peak).map_err(|_| SessionError::FlowStorageLayout)?;
        if self.free.len != 0 {
            return Ok(PreparedFlowNode { new: None });
        }
        #[cfg(test)]
        if FAIL_NODE.replace(false) {
            return Err(SessionError::FlowStorageReserve);
        }
        let mut allocation = allocate_exact_vec::<Node>(1).map_err(|error| match error {
            EffectStorageError::Reserve => SessionError::FlowStorageReserve,
            _ => SessionError::FlowStorageLayout,
        })?;
        allocation.push(Node {
            key,
            value: Value::Vacant,
            left: None,
            right: None,
            height: 1,
        });
        let node = allocation
            .into_boxed_slice()
            .try_into()
            .map_err(|_| SessionError::FlowStorageLayout)?;
        Ok(PreparedFlowNode { new: Some(node) })
    }
    fn insert_prepared(&mut self, key: SessionFlowId, flow: FlowState, prepared: PreparedFlowNode) {
        // Exclusive Model borrow spans preparation and any Adapter call. No
        // callback or fallible operation remains before final owning insert.
        let mut node = prepared
            .new
            .or_else(|| self.free.pop())
            .expect("exclusive prepared OPEN owns a new or unchanged vacant node");
        node[0].key = key;
        node[0].value = Value::Live(flow);
        self.live = Some(insert(self.live.take(), node));
        self.live_len += 1;
    }
    fn retire(&mut self, key: SessionFlowId, terminal: TerminalTombstone) -> FlowState {
        let mut node = take_node(&mut self.live, key)
            .expect("exclusive terminal preflight proved original live node");
        let Value::Live(flow) = std::mem::replace(&mut node[0].value, Value::Terminal(terminal))
        else {
            unreachable!("private live root contains only live nodes")
        };
        self.live_len -= 1;
        self.terminal = Some(insert(self.terminal.take(), node));
        self.terminal_len += 1;
        flow
    }
    fn expire(&mut self, key: SessionFlowId) -> TerminalTombstone {
        let mut node = take_node(&mut self.terminal, key)
            .expect("exclusive expiry matched the complete terminal capability");
        let Value::Terminal(terminal) = std::mem::replace(&mut node[0].value, Value::Vacant) else {
            unreachable!("private terminal root contains only terminal nodes")
        };
        self.terminal_len -= 1;
        self.free.push(node);
        terminal
    }
}

#[cfg(test)]
std::thread_local! {
    static FAIL_NODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_FLOW_DROP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
// A deterministic test-only destructor, not a claim that normalized payload
// disposal executes arbitrary user callbacks in production.
#[cfg(test)]
impl Drop for FlowState {
    fn drop(&mut self) {
        assert!(
            !FAIL_FLOW_DROP.replace(false),
            "test-only FlowState disposal"
        );
    }
}
#[cfg(test)]
impl SessionModel {
    pub(in crate::resumable) fn fail_next_flow_disposal_for_test() {
        FAIL_FLOW_DROP.set(true);
    }
    pub(in crate::resumable) fn fail_next_flow_node_allocation_for_test() {
        FAIL_NODE.set(true);
    }
}

impl PreparedLocalOpen<'_> {
    pub(super) fn install(self) -> Vec<SessionEffect> {
        let Self {
            model,
            flow_id,
            flow,
            prepared_node,
            local_flow,
            next_local_flow_id,
            next_local_authority_id,
            open_frame,
            transmit_open,
            retry_target: _,
            mut effects,
        } = self;
        if transmit_open {
            effects.push(SessionEffect::Transmit(open_frame));
        }
        model
            .flow_storage
            .insert_prepared(flow_id, flow, prepared_node);
        model.next_local_flow_id = next_local_flow_id;
        model.next_local_authority_id = next_local_authority_id;
        debug_assert!(matches!(
            effects.first(),
            Some(SessionEffect::LocalFlowOpened { flow }) if *flow == local_flow
        ));
        effects
    }

    #[allow(
        clippy::result_large_err,
        reason = "adapter rejection returns the exact prepared OPEN input inline"
    )]
    pub(super) fn commit_with<T, E>(
        self,
        prepare: impl FnOnce(PreparedLocalOpenInput) -> Result<T, PreparedLocalOpenAdapterRejection<E>>,
    ) -> Result<ReducerCommittedLocalOpen<T>, PreparedLocalOpenAdapterFailure<E>> {
        let Self {
            model,
            flow_id,
            flow,
            prepared_node,
            local_flow,
            next_local_flow_id,
            next_local_authority_id,
            open_frame,
            transmit_open: _,
            retry_target,
            effects: _,
        } = self;
        let prepared_adapter =
            match prepare(PreparedLocalOpenInput::from_parts(local_flow, open_frame)) {
                Ok(prepared_adapter) => prepared_adapter,
                Err(rejection) => {
                    let (input, error) = rejection.into_parts();
                    return Err(PreparedLocalOpenAdapterFailure {
                        target: retry_target,
                        input,
                        error,
                    });
                }
            };
        model
            .flow_storage
            .insert_prepared(flow_id, flow, prepared_node);
        model.next_local_flow_id = next_local_flow_id;
        model.next_local_authority_id = next_local_authority_id;
        Ok(ReducerCommittedLocalOpen {
            prepared_adapter,
            local_flow,
        })
    }
}

impl SessionModel {
    pub fn snapshot(&self) -> SessionSnapshot {
        let mut outstanding_sink_offers = 0usize;
        for flow in self.flow_storage.values() {
            outstanding_sink_offers += usize::from(flow.pending_sink_offer.is_some());
        }
        SessionSnapshot {
            phase: self.phase,
            generation: self.current_leg.generation(),
            flow_count: self.flow_storage.len(),
            client_to_target_replay: self.client_to_target_replay,
            target_to_client_replay: self.target_to_client_replay,
            receive_owned_bytes: self.receive_usage.bytes,
            receive_ranges: self.receive_usage.segments,
            outstanding_sink_offers,
            terminal_tombstones: self.flow_storage.terminal_len(),
            next_local_flow_id: self.next_local_flow_id,
            highest_peer_flow_id: self.highest_peer_flow_id,
        }
    }

    pub fn flow_snapshot(&self, flow_id: SessionFlowId) -> Option<SessionFlowSnapshot> {
        self.flow_storage
            .get(&flow_id)
            .map(|flow| SessionFlowSnapshot {
                send: flow.send.snapshot(),
                receive: flow.receive.snapshot(),
                has_outstanding_sink_offer: flow.pending_sink_offer.is_some(),
                has_pending_half_close: flow.pending_half_close.is_some(),
            })
    }

    pub(super) fn local_open(
        &mut self,
        leg: CommittedLeg,
        target: TargetAddr,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        let prepared = self
            .preflight_local_open(leg, target, false)
            .map_err(|failure| failure.into_parts().1)?;
        effects.mark_mutation();
        effects.install_prepared(prepared.install());
        Ok(())
    }

    pub(super) fn preflight_local_open(
        &mut self,
        leg: CommittedLeg,
        target: TargetAddr,
        require_active: bool,
    ) -> Result<PreparedLocalOpen<'_>, PreparedLocalOpenReducerFailure> {
        if let Err(error) = self.require_effects_live() {
            return Err(PreparedLocalOpenReducerFailure::new(target, error));
        }
        if let Err(error) = self.validate_current_capability(&leg) {
            return Err(PreparedLocalOpenReducerFailure::new(target, error));
        }
        if self.role != SessionRole::Client {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::RoleCannotInitiateOpen(self.role),
            ));
        }
        if require_active && self.phase != SessionPhase::Active {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::PreparedLocalOpenRequiresActive { actual: self.phase },
            ));
        }
        if let Err(source) = validate_target(&target) {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::InvalidProtocolRecord { source },
            ));
        }
        if let Err(error) = self.precheck_flow_capacity() {
            return Err(PreparedLocalOpenReducerFailure::new(target, error));
        }
        let Some(raw) = self.next_local_flow_id else {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::FlowIdExhausted,
            ));
        };
        let flow_id = match SessionFlowId::new(raw) {
            Ok(flow_id) => flow_id,
            Err(_) => {
                return Err(PreparedLocalOpenReducerFailure::new(
                    target,
                    SessionError::FlowIdExhausted,
                ));
            }
        };
        let Some(authority_id) = self.next_local_authority_id else {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::LocalFlowAuthorityExhausted,
            ));
        };
        if self.flow_storage.contains_key(&flow_id) || self.flow_storage.contains_terminal(&flow_id)
        {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::InvariantViolation("fresh local flow id already existed"),
            ));
        }
        let open_frame = match Frame::try_new(
            self.current_leg.generation(),
            Record::Open {
                flow_id,
                target: target.clone(),
            },
        ) {
            Ok(frame) => frame,
            Err(source) => {
                return Err(PreparedLocalOpenReducerFailure::new(
                    target,
                    SessionError::InvalidProtocolRecord { source },
                ));
            }
        };
        let normalized_target = match open_frame.record() {
            Record::Open { target, .. } => target.clone(),
            _ => {
                return Err(PreparedLocalOpenReducerFailure::new(
                    target,
                    SessionError::InvariantViolation(
                        "local OPEN record construction changed variant",
                    ),
                ));
            }
        };
        let next = raw.checked_add(1);
        let local_flow = LocalFlow {
            session_id: self.session_id,
            flow_id,
            direction: self.role.local_send_direction(),
            authority_id,
        };
        let mut flow = FlowState::new(normalized_target, FlowOrigin::Local, self.config);
        flow.local_flow = Some(local_flow);
        let mut effects = Vec::new();
        if effects
            .try_reserve_exact(self.config.client_incremental_effect_bound())
            .is_err()
        {
            return Err(PreparedLocalOpenReducerFailure::new(
                target,
                SessionError::LocalOpenEffectAllocationFailed,
            ));
        }
        effects.push(SessionEffect::LocalFlowOpened { flow: local_flow });
        let transmit_open = self.phase == SessionPhase::Active;
        let prepared_node = match self.flow_storage.prepare(flow_id, self.config) {
            Ok(prepared) => prepared,
            Err(error) => return Err(PreparedLocalOpenReducerFailure::new(target, error)),
        };
        Ok(PreparedLocalOpen {
            model: self,
            flow_id,
            flow,
            prepared_node,
            local_flow,
            next_local_flow_id: next,
            next_local_authority_id: authority_id.checked_add(1),
            open_frame,
            transmit_open,
            retry_target: target,
            effects,
        })
    }

    pub(super) fn expire_resume_grace(
        &mut self,
        leg: CommittedLeg,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.validate_current_capability(&leg)?;
        if self.phase != SessionPhase::Legless {
            return Err(SessionError::ResumeGraceNotActive);
        }
        effects.prepare_ordinary()?;
        effects.mark_mutation();
        let retired = std::mem::take(&mut self.flow_storage);
        self.client_to_target_replay = ReplayBudgetUsage::default();
        self.target_to_client_replay = ReplayBudgetUsage::default();
        self.receive_usage = ReplayBudgetUsage::default();
        self.phase = SessionPhase::Expired;
        let publication = effects.push(SessionEffect::SessionExpired);
        drop(retired);
        publication.map_err(SessionError::from)
    }

    pub(super) fn receive_open(
        &mut self,
        flow_id: SessionFlowId,
        target: TargetAddr,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        if self.role != SessionRole::Owner {
            return Err(SessionError::UnexpectedPeerOpen(self.role));
        }
        if let Some(tombstone) = self.flow_storage.get_terminal(&flow_id) {
            if tombstone.target != target {
                return Err(SessionError::FlowTargetConflict { flow_id });
            }
            return self.replay_terminal_control(tombstone, effects);
        }
        if let Some(flow) = self.flow_storage.get(&flow_id) {
            if flow.target != target {
                return Err(SessionError::FlowTargetConflict { flow_id });
            }
            return match flow.peer_open_result {
                Some(result) => {
                    self.transmit_if_active(Record::OpenResult { flow_id, result }, effects)
                }
                None => Ok(()),
            };
        }
        if flow_id.get() <= self.highest_peer_flow_id {
            return Err(SessionError::PeerFlowIdNotMonotonic {
                highest: self.highest_peer_flow_id,
                attempted: flow_id.get(),
            });
        }
        self.precheck_flow_capacity()?;
        let request_id = self
            .next_open_request_id
            .ok_or(SessionError::OpenRequestIdExhausted)?;
        let authority_id = self
            .next_local_authority_id
            .ok_or(SessionError::LocalFlowAuthorityExhausted)?;
        effects.prepare_ordinary()?;
        let request = PeerOpenRequest {
            session_id: self.session_id,
            flow_id,
            request_id,
        };
        let local_flow = LocalFlow {
            session_id: self.session_id,
            flow_id,
            direction: self.role.local_send_direction(),
            authority_id,
        };
        let mut flow = FlowState::new(target.clone(), FlowOrigin::Peer, self.config);
        flow.peer_open_request = Some(request);
        flow.local_flow = Some(local_flow);
        let prepared_node = self.flow_storage.prepare(flow_id, self.config)?;
        effects.mark_mutation();
        self.flow_storage
            .insert_prepared(flow_id, flow, prepared_node);
        self.highest_peer_flow_id = flow_id.get();
        self.next_open_request_id = request_id.checked_add(1);
        self.next_local_authority_id = authority_id.checked_add(1);
        {
            effects.push(SessionEffect::PeerOpenRequested {
                request,
                flow: local_flow,
                target,
            })?;
            Ok(())
        }
    }

    pub(super) fn expire_terminal_grace(
        &mut self,
        terminal: TerminalGrace,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        if terminal.session_id() != self.session_id {
            return Err(SessionError::CrossSessionTerminalGrace);
        }
        let flow_id = terminal.flow_id();
        let tombstone = self
            .flow_storage
            .get_terminal(&flow_id)
            .ok_or(SessionError::UnknownTerminalGrace)?;
        if tombstone.terminal != terminal {
            return Err(SessionError::UnknownTerminalGrace);
        }
        effects.mark_mutation();
        drop(self.flow_storage.expire(flow_id));
        Ok(())
    }

    pub(super) fn precheck_flow_finish(&self, flow_id: SessionFlowId) -> Result<(), SessionError> {
        self.precheck_terminal_capacity()?;
        let flow = self
            .flow_storage
            .get(&flow_id)
            .ok_or(SessionError::UnknownFlow(flow_id))?;
        if flow.local_flow.is_none() {
            return Err(SessionError::InvariantViolation(
                "terminal flow has no local authority",
            ));
        }
        let send = flow.send.snapshot();
        let receive = flow.receive.snapshot();
        let usage = self.replay_usage(self.role.local_send_direction());
        if usage.bytes < send.retained_bytes() || usage.segments < send.segment_count() {
            return Err(SessionError::InvariantViolation(
                "terminal flow release exceeds global replay ownership",
            ));
        }
        if self.receive_usage.bytes < receive.buffered_bytes()
            || self.receive_usage.segments < receive.range_count()
        {
            return Err(SessionError::InvariantViolation(
                "terminal flow release exceeds global receive ownership",
            ));
        }
        if self.flow_storage.contains_terminal(&flow_id) {
            return Err(SessionError::InvariantViolation(
                "live flow already has terminal tombstone",
            ));
        }
        Ok(())
    }

    pub(super) fn finish_flow(
        &mut self,
        flow_id: SessionFlowId,
        reason: FlowFinishReason,
        replay_control: Option<Record>,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        self.precheck_flow_finish(flow_id)?;
        let direction = self.role.local_send_direction();
        let (local_flow, target, send_bytes, send_segments, receive_bytes, receive_ranges) = {
            let flow = self
                .flow_storage
                .get(&flow_id)
                .ok_or(SessionError::UnknownFlow(flow_id))?;
            let send = flow.send.snapshot();
            let receive = flow.receive.snapshot();
            (
                flow.local_flow.ok_or(SessionError::InvariantViolation(
                    "terminal flow has no local authority",
                ))?,
                flow.target.clone(),
                send.retained_bytes(),
                send.segment_count(),
                receive.buffered_bytes(),
                receive.range_count(),
            )
        };
        let tombstone_id = self
            .next_tombstone_id
            .ok_or(SessionError::TerminalTombstoneIdExhausted)?;
        let terminal = TerminalGrace {
            local_flow,
            tombstone_id,
        };

        effects.prepare_ordinary()?;
        effects.mark_mutation();
        let retired = self.flow_storage.retire(
            flow_id,
            TerminalTombstone {
                terminal,
                target,
                replay_control,
            },
        );
        let usage = self.replay_usage_mut(direction);
        usage.bytes -= send_bytes;
        usage.segments -= send_segments;
        self.receive_usage.bytes -= receive_bytes;
        self.receive_usage.segments -= receive_ranges;
        self.next_tombstone_id = tombstone_id.checked_add(1);
        let publication = effects.push(SessionEffect::FlowFinished {
            flow_id,
            reason,
            terminal,
        });
        drop(retired);
        publication.map_err(SessionError::from)
    }

    pub(super) fn replay_terminal_control(
        &self,
        tombstone: &TerminalTombstone,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        match tombstone.replay_control.clone() {
            Some(control) => self.transmit_if_active(control, effects),
            None => Ok(()),
        }
    }

    pub(super) fn transmit_if_active(
        &self,
        record: Record,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionError> {
        if self.phase == SessionPhase::Active {
            effects.prepare_ordinary()?;
            effects.push(SessionEffect::Transmit(Frame::new(
                self.current_leg.generation(),
                record,
            )))?;
        }
        Ok(())
    }
}
