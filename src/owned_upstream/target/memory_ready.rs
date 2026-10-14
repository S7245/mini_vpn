//! Fixed MemoryTarget registrations and bounded shared-release passes.
//!
//! Owns only positions, cached Wakers and two waiting hierarchies. Actual
//! bytes/directives/terminal/tombstones stay in MemoryTarget. Mutation plans
//! preflight before business commit; publication afterward is infallible apart
//! from the controlled outer notification's unwind contract. No poll allocates
//! or scans all Flows. Entries are prepare/publish/register/poll_common;
//! real Adapter tests live in tests/owner_readiness.rs.

use super::*;
use std::alloc::Layout;
use std::task::Waker;

const WIDTH: usize = usize::BITS as usize;

pub(super) struct MemoryReadiness {
    slots: Box<[MemoryReadySlot]>,
    free: Option<usize>,
    waiting: [WaitingTree; 2],
    common: Option<Waker>,
    requested: u64,
    completed: u64,
    pass: ReadyPass,
}

enum MemoryReadySlot {
    Free {
        next: Option<usize>,
    },
    Bound {
        flow_id: SessionFlowId,
        wakers: [Option<Waker>; 3],
    },
}

#[derive(Clone, Copy)]
enum ReadyPass {
    Idle,
    Scanning { revision: u64, cursor: usize },
}

pub(super) enum ReadyChange {
    Read,
    WriteDirective(bool),
    BytesReleased(usize),
    Terminal,
    Eof,
    HalfClose,
}

pub(super) struct ReadyMutation {
    position: usize,
    notify: u8,
    clear: u8,
    revision: Option<u64>,
}

pub(super) struct ReadyOpen {
    pub(super) position: usize,
    next: Option<usize>,
}

pub(super) struct ReadyRemoval {
    position: usize,
    next: Option<usize>,
}

struct WaitingTree {
    levels: Box<[Box<[usize]>]>,
}

fn words(bits: usize) -> usize {
    bits / WIDTH + usize::from(bits % WIDTH != 0)
}

fn exact_backing<T>(capacity: usize) -> Vec<T> {
    // Only concrete, already checked construction geometries call this. Keep
    // the original unrecoverable-allocation policy, not the Effect fault domain.
    crate::resumable::effect_storage::allocate_exact_vec(capacity).unwrap_or_else(|_| {
        let layout = Layout::array::<T>(capacity).expect("checked readiness geometry");
        std::alloc::handle_alloc_error(layout)
    })
}

impl MemoryReadiness {
    pub(super) fn check_layout(capacity: usize) -> Result<Layout, TargetIoConfigError> {
        let bad = || TargetIoConfigError::ReadinessLayoutOverflow;
        if capacity == 0 {
            return Err(bad());
        }
        let mut layout = Layout::new::<()>();
        layout = layout
            .extend(Layout::array::<MemoryReadySlot>(capacity).map_err(|_| bad())?)
            .map_err(|_| bad())?
            .0;
        let (mut remaining, mut count, mut total) = (capacity, 0usize, 0usize);
        loop {
            remaining = words(remaining);
            total = total.checked_add(remaining).ok_or_else(bad)?;
            count += 1;
            if remaining == 1 {
                break;
            }
        }
        for _ in 0..2 {
            layout = layout
                .extend(Layout::array::<Box<[usize]>>(count).map_err(|_| bad())?)
                .map_err(|_| bad())?
                .0;
            layout = layout
                .extend(Layout::array::<usize>(total).map_err(|_| bad())?)
                .map_err(|_| bad())?
                .0;
        }
        // Preserve the former readiness-only rejection before returning just
        // backing: the complete Target already contains this inline header.
        Layout::new::<Self>().extend(layout).map_err(|_| bad())?;
        Ok(layout)
    }

    pub(super) fn new(capacity: usize) -> Self {
        // Config's private constructor already checked this exact geometry.
        let mut slots = exact_backing(capacity);
        for position in 0..capacity {
            slots.push(MemoryReadySlot::Free {
                next: (position + 1 < capacity).then_some(position + 1),
            });
        }
        Self {
            slots: slots.into_boxed_slice(),
            free: (capacity != 0).then_some(0),
            waiting: [WaitingTree::new(capacity), WaitingTree::new(capacity)],
            common: None,
            requested: 0,
            completed: 0,
            pass: ReadyPass::Idle,
        }
    }

    fn check_pass(&self) -> Result<(), TargetIoError> {
        if self.completed > self.requested {
            return Err(TargetIoError::ReadinessInvariant("release revision order"));
        }
        if let ReadyPass::Scanning { revision, cursor } = self.pass {
            if revision <= self.completed || revision > self.requested || cursor > self.slots.len()
            {
                return Err(TargetIoError::ReadinessInvariant("shared pass phase"));
            }
        }
        Ok(())
    }

    fn check_position(&self, flow_id: SessionFlowId, position: usize) -> Result<(), TargetIoError> {
        self.check_pass()?;
        match self.slots.get(position) {
            Some(MemoryReadySlot::Bound {
                flow_id: actual, ..
            }) if *actual == flow_id => Ok(()),
            _ => Err(TargetIoError::ReadinessInvariant("Flow position identity")),
        }
    }

    pub(super) fn prepare_open(&self) -> Result<ReadyOpen, TargetIoError> {
        self.check_pass()?;
        let Some(position) = self.free else {
            return Err(TargetIoError::ReadinessInvariant(
                "live capacity without free position",
            ));
        };
        match self.slots.get(position) {
            Some(MemoryReadySlot::Free { next }) => Ok(ReadyOpen {
                position,
                next: *next,
            }),
            _ => Err(TargetIoError::ReadinessInvariant("free position chain")),
        }
    }

    pub(super) fn bind(&mut self, flow_id: SessionFlowId, prepared: ReadyOpen) {
        self.slots[prepared.position] = MemoryReadySlot::Bound {
            flow_id,
            wakers: [None, None, None],
        };
        self.free = prepared.next;
    }

    pub(super) fn prepare_remove(
        &self,
        flow_id: SessionFlowId,
        position: usize,
    ) -> Result<ReadyRemoval, TargetIoError> {
        self.check_position(flow_id, position)?;
        if let Some(free) = self.free {
            if !matches!(self.slots.get(free), Some(MemoryReadySlot::Free { .. })) {
                return Err(TargetIoError::ReadinessInvariant("free position head"));
            }
        }
        Ok(ReadyRemoval {
            position,
            next: self.free,
        })
    }

    pub(super) fn remove(&mut self, prepared: ReadyRemoval) {
        let old = std::mem::replace(
            &mut self.slots[prepared.position],
            MemoryReadySlot::Free {
                next: prepared.next,
            },
        );
        self.free = Some(prepared.position);
        for tree in &mut self.waiting {
            tree.set(prepared.position, false);
        }
        drop(old);
    }

    pub(super) fn prepare(
        &self,
        flow_id: SessionFlowId,
        flow: &MemoryTargetFlow,
        change: ReadyChange,
    ) -> Result<ReadyMutation, TargetIoError> {
        self.check_position(flow_id, flow.ready_position)?;
        let (notify, clear, shared) = match change {
            ReadyChange::Read => (1, 0, false),
            ReadyChange::WriteDirective(changed) => (if changed { 2 } else { 0 }, 0, false),
            ReadyChange::BytesReleased(bytes) => (if bytes != 0 { 4 } else { 0 }, 0, bytes != 0),
            ReadyChange::Terminal => {
                let changed = flow.terminal.is_none()
                    || flow.buffered_bytes() != 0
                    || !flow.write_directives.is_empty();
                (if changed { 7 } else { 0 }, 3, flow.buffered_bytes() != 0)
            }
            ReadyChange::Eof => (4, if flow.write_half_closed { 3 } else { 1 }, false),
            ReadyChange::HalfClose => (4, if flow.read_eof_delivered { 3 } else { 2 }, false),
        };
        let revision = if shared {
            Some(self.prepare_shared_release()?)
        } else {
            None
        };
        Ok(ReadyMutation {
            position: flow.ready_position,
            notify,
            clear,
            revision,
        })
    }

    pub(super) fn prepare_shared_release(&self) -> Result<u64, TargetIoError> {
        self.check_pass()?;
        self.requested
            .checked_add(1)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "Target readiness release revision",
            })
    }

    pub(super) fn publish_shared(&mut self, revision: u64) {
        self.requested = revision;
        let common = self.common.take();
        if let Some(waker) = common {
            waker.wake();
        }
    }

    pub(super) fn publish(&mut self, prepared: ReadyMutation) {
        let mut notify = [None, None, None];
        let mut discard = [None, None, None];
        if let MemoryReadySlot::Bound { wakers, .. } = &mut self.slots[prepared.position] {
            for interest in 0..3 {
                if prepared.notify & (1 << interest) != 0 {
                    notify[interest] = wakers[interest].take();
                } else if prepared.clear & (1 << interest) != 0 {
                    discard[interest] = wakers[interest].take();
                }
            }
        }
        for interest in 1..3 {
            if (prepared.notify | prepared.clear) & (1 << interest) != 0 {
                self.waiting[interest - 1].set(prepared.position, false);
            }
        }
        let common = prepared.revision.and_then(|revision| {
            self.requested = revision;
            self.common.take()
        });
        // Commit all registration/pass disposition before a controlled call.
        drop(discard);
        for waker in notify.into_iter().flatten() {
            waker.wake();
        }
        if let Some(waker) = common {
            waker.wake();
        }
    }

    pub(super) fn register(
        &mut self,
        flow_id: SessionFlowId,
        position: usize,
        interest: TargetReadyInterest,
        pending: bool,
        waker: &Waker,
    ) -> Result<(), TargetIoError> {
        self.check_position(flow_id, position)?;
        let prepared = waker.clone();
        let index = interest as usize;
        let MemoryReadySlot::Bound { wakers, .. } = &mut self.slots[position] else {
            return Err(TargetIoError::ReadinessInvariant("registration position"));
        };
        let old = wakers[index].replace(prepared);
        if index != 0 {
            self.waiting[index - 1].set(position, pending);
        }
        drop(old);
        Ok(())
    }

    pub(super) fn poll_common(
        &mut self,
        flows: &MemoryTargetStorage,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TargetIoError>> {
        if let Err(error) = self.check_pass() {
            return Poll::Ready(Err(error));
        }
        let (revision, cursor) = match self.pass {
            ReadyPass::Idle if self.requested == self.completed => {
                let old = self.common.replace(cx.waker().clone());
                drop(old);
                return Poll::Pending;
            }
            ReadyPass::Idle => (self.requested, 0),
            ReadyPass::Scanning { revision, cursor } => (revision, cursor),
        };
        let selected = (|| {
            let a = self.waiting[0].select_from(cursor)?;
            let b = self.waiting[1].select_from(cursor)?;
            let candidate = match (a, b) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if let Some(position) = candidate {
                let Some(MemoryReadySlot::Bound { flow_id, .. }) = self.slots.get(position) else {
                    return Err(TargetIoError::ReadinessInvariant(
                        "waiting position is not live",
                    ));
                };
                if !matches!(flows.get(flow_id), Some(flow) if flow.ready_position == position) {
                    return Err(TargetIoError::ReadinessInvariant(
                        "waiting Flow back-reference",
                    ));
                }
            }
            Ok(candidate)
        })();
        let candidate = match selected {
            Ok(candidate) => candidate,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let old = self.common.replace(cx.waker().clone());
        let mut notify = [None, None, None];
        if let Some(position) = candidate {
            self.pass = ReadyPass::Scanning {
                revision,
                cursor: position + 1,
            };
            if let MemoryReadySlot::Bound { wakers, .. } = &mut self.slots[position] {
                for (returned, stored) in notify.iter_mut().zip(wakers) {
                    *returned = stored.take();
                }
            }
            for tree in &mut self.waiting {
                tree.set(position, false);
            }
        } else {
            self.completed = revision;
            self.pass = ReadyPass::Idle;
        }
        drop(old);
        for waker in notify.into_iter().flatten() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }

    pub(super) fn cold_check(
        &self,
        flows: &MemoryTargetStorage,
    ) -> Result<(), TargetIoColdInventoryError> {
        let mismatch = || TargetIoColdInventoryError::StoredMismatch {
            domain: "Target readiness positions",
            summed: flows.len(),
            stored: self.slots.len(),
        };
        self.check_pass().map_err(|_| mismatch())?;
        for tree in &self.waiting {
            tree.check(self.slots.len()).map_err(|_| mismatch())?;
        }
        for (&flow_id, flow) in flows.iter() {
            self.check_position(flow_id, flow.ready_position)
                .map_err(|_| mismatch())?;
        }
        let mut free_count = 0;
        for (position, slot) in self.slots.iter().enumerate() {
            match slot {
                MemoryReadySlot::Free { .. } => {
                    free_count += 1;
                    if self.waiting.iter().any(|tree| tree.contains(position)) {
                        return Err(mismatch());
                    }
                }
                MemoryReadySlot::Bound { flow_id, wakers } => {
                    if !matches!(flows.get(flow_id), Some(flow) if flow.ready_position == position)
                        || (1..3)
                            .any(|i| self.waiting[i - 1].contains(position) && wakers[i].is_none())
                    {
                        return Err(mismatch());
                    }
                }
            }
        }
        let (mut free, mut seen) = (self.free, 0);
        while let Some(position) = free {
            if seen >= free_count {
                return Err(mismatch());
            }
            match self.slots.get(position) {
                Some(MemoryReadySlot::Free { next }) => free = *next,
                _ => return Err(mismatch()),
            }
            seen += 1;
        }
        if seen != free_count {
            return Err(mismatch());
        }
        Ok(())
    }
}

impl WaitingTree {
    fn new(capacity: usize) -> Self {
        let (mut remaining, mut count) = (capacity, 0);
        loop {
            remaining = words(remaining);
            count += 1;
            if remaining <= 1 {
                break;
            }
        }
        let mut levels = exact_backing(count);
        remaining = capacity;
        loop {
            remaining = words(remaining);
            let mut bits = exact_backing(remaining);
            for _ in 0..remaining {
                bits.push(0usize);
            }
            levels.push(bits.into_boxed_slice());
            if remaining <= 1 {
                break;
            }
        }
        Self {
            levels: levels.into_boxed_slice(),
        }
    }

    fn contains(&self, position: usize) -> bool {
        self.levels[0][position / WIDTH] & (1 << (position % WIDTH)) != 0
    }

    fn set(&mut self, mut position: usize, present: bool) {
        for level in &mut self.levels {
            let word = &mut level[position / WIDTH];
            let old = *word;
            let bit = 1 << (position % WIDTH);
            if present {
                *word |= bit;
            } else {
                *word &= !bit;
            }
            if (old == 0) == (*word == 0) {
                break;
            }
            position /= WIDTH;
        }
    }

    fn select_from(&self, start: usize) -> Result<Option<usize>, TargetIoError> {
        self.at_or_after(0, start)
    }

    fn at_or_after(&self, level: usize, start: usize) -> Result<Option<usize>, TargetIoError> {
        let index = start / WIDTH;
        let Some(word) = self.levels[level].get(index) else {
            return Ok(None);
        };
        let masked = *word & (usize::MAX << (start % WIDTH));
        if masked != 0 {
            return Ok(Some(index * WIDTH + masked.trailing_zeros() as usize));
        }
        if level + 1 == self.levels.len() {
            return Ok(None);
        }
        let Some(next) = self.at_or_after(level + 1, index + 1)? else {
            return Ok(None);
        };
        let Some(word) = self.levels[level].get(next).filter(|word| **word != 0) else {
            return Err(TargetIoError::ReadinessInvariant(
                "waiting hierarchy ancestor",
            ));
        };
        Ok(Some(next * WIDTH + word.trailing_zeros() as usize))
    }

    fn check(&self, capacity: usize) -> Result<(), TargetIoError> {
        let mut bits = capacity;
        for (index, level) in self.levels.iter().enumerate() {
            let expected = words(bits);
            if expected == 0
                || level.len() != expected
                || (expected == 1) != (index + 1 == self.levels.len())
            {
                return Err(TargetIoError::ReadinessInvariant("waiting hierarchy shape"));
            }
            let padding = bits % WIDTH;
            if padding != 0 && level[expected - 1] & (usize::MAX << padding) != 0 {
                return Err(TargetIoError::ReadinessInvariant(
                    "waiting hierarchy padding",
                ));
            }
            bits = expected;
        }
        if self.levels.is_empty() {
            return Err(TargetIoError::ReadinessInvariant("empty waiting hierarchy"));
        }
        for level in 1..self.levels.len() {
            for (index, &word) in self.levels[level - 1].iter().enumerate() {
                let bit = self.levels[level][index / WIDTH] & (1 << (index % WIDTH)) != 0;
                if bit != (word != 0) {
                    return Err(TargetIoError::ReadinessInvariant("waiting hierarchy"));
                }
            }
        }
        Ok(())
    }
}
