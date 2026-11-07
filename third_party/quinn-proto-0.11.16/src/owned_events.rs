//! Owns exact precharged event slots, linear permits and ordered terminal/control delivery.
//! This module owns no Endpoint, Connection, protocol mutation or Root receipt.
//! Slot capacity is admitted before allocation; handles retain its reservation
//! until the last owner drops. Tests live in owned_events/tests/mod.rs.

use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout, LayoutError},
    fmt, io, ptr,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// Exact allocation layout shared by the cold planner and slot owner.
pub fn slot_layout<T>(count: usize) -> Result<Layout, LayoutError> {
    Layout::array::<Slot<T>>(count)
}

struct State<T> {
    // The slot allocation drops before its budget reservation.
    slots: Box<[Slot<T>]>,
    _reservation: BackingBudgetReservation,
    ordinary_capacity: usize,
    control_indices: Option<[usize; 2]>,
    control_reserved: [Option<u64>; 2],
    terminal_index: Option<usize>,
    terminal_sent: bool,
    next_seq: u64,
    head: usize,
    len: usize,
    reserved_seq: Option<u64>,
    failed: bool,
    sender_alive: bool,
    receiver_alive: bool,
    receiver_waker: Option<Waker>,
    producer_waker: Option<Waker>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    meter: BackingMeter,
}

mod callbacks;
use callbacks::Callbacks;
mod control;
pub use control::{ControlWake, HeldControl, PublishedControl};
mod inventory;
pub use inventory::ChannelIdentity;

struct Slot<T> {
    seq: u64,
    value: Option<T>,
}

pub struct Sender<T>(Arc<Inner<T>>);
pub struct Receiver<T>(Arc<Inner<T>>);

/// One slot held across the caller's Proto operation. Not Clone or Copy.
pub struct Permit<T>(Option<Arc<Inner<T>>>);

/// One operation's pre-admitted ordinary burst. Each send consumes one slot;
/// Drop returns only unused capacity. The reservation fence advances with sends.
pub struct BatchPermit<T> {
    inner: Option<Arc<Inner<T>>>,
    remaining: usize,
}

impl<T> fmt::Debug for BatchPermit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchPermit")
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

/// Capacity for one synchronous Endpoint control event, reserved before rebind
/// changes the socket. Not Clone or Copy.
pub struct ControlPermit<T> {
    inner: Arc<Inner<T>>,
    kind: ControlKind,
    seq: u64,
    armed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlKind {
    Rebind,
    Close,
}

impl ControlKind {
    fn index(self) -> usize {
        match self {
            Self::Rebind => 0,
            Self::Close => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveError {
    Full,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Closed,
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BudgetedEventSender")
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BudgetedEventReceiver")
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Permit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BudgetedEventPermit")
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for ControlPermit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BudgetedControlPermit")
            .finish_non_exhaustive()
    }
}

/// Structured cold-budget rejection retained through the io-facing constructor.
#[derive(Debug)]
pub struct AdmissionError(pub bytes::MeteredCopyError);
impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event slot admission {:?}", self.0)
    }
}
impl std::error::Error for AdmissionError {}
impl From<AdmissionError> for io::Error {
    fn from(error: AdmissionError) -> Self {
        let kind = match error.0 {
            bytes::MeteredCopyError::BudgetFull | bytes::MeteredCopyError::TooLarge => {
                io::ErrorKind::WouldBlock
            }
            bytes::MeteredCopyError::Allocation => io::ErrorKind::OutOfMemory,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(kind, error)
    }
}

/// Allocate exactly `capacity` event slots after admission to the shared hard budget.
/// The Arc control object is connection-instance state; the owned queue's slot layout
/// is the exact `Layout::array::<Slot<T>>` charged here.
pub fn channel<T>(capacity: usize, meter: &BackingMeter) -> io::Result<(Sender<T>, Receiver<T>)> {
    make_channel(capacity, false, false, meter)
}

/// Reserve one additional slot that ordinary events cannot consume. Sending the
/// terminal event is infallible while the receiving Endpoint still owns its end.
pub fn terminal_channel<T>(
    ordinary_capacity: usize,
    meter: &BackingMeter,
) -> io::Result<(Sender<T>, Receiver<T>)> {
    make_channel(ordinary_capacity, true, false, meter)
}

/// Ordinary ring plus one independent Rebind and Close slot. Control permits
/// preserve the original Endpoint lock's event order without waiting on a full
/// datagram lane.
pub fn control_channel<T>(
    ordinary_capacity: usize,
    meter: &BackingMeter,
) -> io::Result<(Sender<T>, Receiver<T>)> {
    make_channel(ordinary_capacity, false, true, meter)
}

fn make_channel<T>(
    capacity: usize,
    terminal: bool,
    controls: bool,
    meter: &BackingMeter,
) -> io::Result<(Sender<T>, Receiver<T>)> {
    if capacity == 0 && !terminal {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "zero event capacity",
        ));
    }
    let slot_count = capacity
        .checked_add(2 * usize::from(controls))
        .and_then(|count| count.checked_add(usize::from(terminal)))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event slot count"))?;
    let layout = slot_layout::<T>(slot_count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "event slot layout"))?;
    if layout.size() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "zero-sized event slot layout",
        ));
    }
    let reservation = meter
        .reserve_cold_layout(layout.size())
        .map_err(|error| io::Error::from(AdmissionError(error)))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "event budget required"))?;
    let raw = unsafe { alloc(layout) }.cast::<Slot<T>>();
    if raw.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "event slot allocation",
        ));
    }
    for index in 0..slot_count {
        unsafe {
            ptr::write(
                raw.add(index),
                Slot {
                    seq: 0,
                    value: None,
                },
            )
        };
    }
    // SAFETY: allocation has the exact slice layout and every slot is initialized.
    let slots = unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(raw, slot_count)) };
    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            slots,
            _reservation: reservation,
            ordinary_capacity: capacity,
            control_indices: controls.then_some([capacity, capacity + 1]),
            control_reserved: [None; 2],
            terminal_index: terminal.then_some(capacity + 2 * usize::from(controls)),
            terminal_sent: false,
            next_seq: 0,
            head: 0,
            len: 0,
            reserved_seq: None,
            failed: false,
            sender_alive: true,
            receiver_alive: true,
            receiver_waker: None,
            producer_waker: None,
        }),
        meter: meter.clone(),
    });
    drop(inner.lock());
    Ok((Sender(inner.clone()), Receiver(inner)))
}

impl<T> State<T> {
    fn take_seq(&mut self) -> Option<u64> {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.checked_add(1)?;
        Some(seq)
    }

    fn push(&mut self, seq: u64, value: T) {
        debug_assert!(self.len < self.ordinary_capacity);
        let index = (self.head + self.len) % self.ordinary_capacity;
        debug_assert!(self.slots[index].value.is_none());
        self.slots[index].seq = seq;
        self.slots[index].value = Some(value);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<T> {
        self.pop_internal(false)
    }

    fn pop_internal(&mut self, ignore_pending: bool) -> Option<T> {
        let ordinary = (self.len > 0).then_some(self.head);
        let mut earliest = ordinary;
        if let Some(indices) = self.control_indices {
            for index in indices {
                if self.slots[index].value.is_some()
                    && earliest.is_none_or(|old| self.slots[index].seq < self.slots[old].seq)
                {
                    earliest = Some(index);
                }
            }
        }
        if !ignore_pending && self.sender_alive {
            let pending = self
                .reserved_seq
                .into_iter()
                .chain(self.control_reserved.into_iter().flatten())
                .min();
            if pending.is_some_and(|seq| earliest.is_none_or(|index| seq < self.slots[index].seq)) {
                return None;
            }
        }
        if let Some(index) = earliest {
            let value = self.slots[index].value.take();
            if ordinary == Some(index) {
                self.head = (self.head + 1) % self.ordinary_capacity;
                self.len -= 1;
            }
            return value;
        }
        if ignore_pending
            || !self.sender_alive
            || (self.reserved_seq.is_none() && self.control_reserved.iter().all(Option::is_none))
        {
            return self
                .terminal_index
                .and_then(|index| self.slots[index].value.take());
        }
        None
    }

    fn pop_for_disposal(&mut self) -> Option<T> {
        self.pop_internal(true)
    }

    fn commit_wakers(&mut self) -> (Option<Waker>, Option<Waker>) {
        let producer = if self.reserved_seq.is_none() && self.len < self.ordinary_capacity {
            self.producer_waker.take()
        } else {
            None
        };
        (self.receiver_waker.take(), producer)
    }
}

impl<T> Sender<T> {
    /// Physical ordinary values only; does not count unpublished reservations.
    pub fn ordinary_len(&self) -> usize {
        self.0.lock().len
    }

    /// The sole producer may wait for closure while reading, then wait for a
    /// reservation. These waits must not run concurrently on different tasks:
    /// both use the original single producer waiter.
    pub fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut callbacks = Callbacks::new(&self.0.meter);
        let mut prepared = false;
        loop {
            let mut state = self.0.lock();
            if state.failed || !state.receiver_alive {
                return Poll::Ready(());
            }
            if prepared {
                std::mem::swap(&mut state.producer_waker, &mut callbacks.slots[0]);
                return Poll::Pending;
            }
            drop(state);
            callbacks.clone_waiter(cx.waker());
            prepared = true;
        }
    }
    /// Reserve the complete burst before the caller mutates protocol state.
    /// Full rejection changes neither sequence nor stored/reserved inventory.
    pub fn try_reserve_batch(&self, count: usize) -> Result<BatchPermit<T>, ReserveError> {
        let mut state = self.0.lock();
        if !state.sender_alive
            || !state.receiver_alive
            || state.terminal_sent
            || state.next_seq == u64::MAX
        {
            return Err(ReserveError::Closed);
        }
        if count == 0 {
            return Ok(BatchPermit {
                inner: None,
                remaining: 0,
            });
        }
        if state.reserved_seq.is_some() || count > state.ordinary_capacity - state.len {
            return Err(ReserveError::Full);
        }
        let count_u64 = u64::try_from(count).map_err(|_| ReserveError::Closed)?;
        let next = state
            .next_seq
            .checked_add(count_u64)
            .ok_or(ReserveError::Closed)?;
        state.reserved_seq = Some(state.next_seq);
        state.next_seq = next;
        Ok(BatchPermit {
            inner: Some(self.0.clone()),
            remaining: count,
        })
    }
    pub fn try_reserve(&self) -> Result<Permit<T>, ReserveError> {
        let mut state = self.0.lock();
        if !state.sender_alive
            || !state.receiver_alive
            || state.terminal_sent
            || state.next_seq == u64::MAX
        {
            return Err(ReserveError::Closed);
        }
        if state.len == state.ordinary_capacity || state.reserved_seq.is_some() {
            return Err(ReserveError::Full);
        }
        state.reserved_seq = state.take_seq();
        Ok(Permit(Some(self.0.clone())))
    }

    /// Register the sole producer driver before checking capacity, avoiding a lost wake.
    pub fn poll_reserve(&self, cx: &mut Context<'_>) -> Poll<Result<Permit<T>, ReserveError>> {
        let mut callbacks = Callbacks::new(&self.0.meter);
        let mut prepared = false;
        loop {
            let mut state = self.0.lock();
            if !state.sender_alive
                || !state.receiver_alive
                || state.terminal_sent
                || state.next_seq == u64::MAX
            {
                return Poll::Ready(Err(ReserveError::Closed));
            }
            if state.len < state.ordinary_capacity && state.reserved_seq.is_none() {
                state.reserved_seq = state.take_seq();
                return Poll::Ready(Ok(Permit(Some(self.0.clone()))));
            }
            if prepared {
                std::mem::swap(&mut state.producer_waker, &mut callbacks.slots[0]);
                return Poll::Pending;
            }
            drop(state);
            callbacks.clone_waiter(cx.waker());
            prepared = true;
        }
    }

    /// Reserve a control slot before changing Endpoint socket or close state.
    /// A second event of the same kind must wait for the first to be consumed.
    pub fn try_reserve_control(&self, kind: ControlKind) -> Result<ControlPermit<T>, ReserveError> {
        let mut state = self.0.lock();
        if !state.sender_alive || !state.receiver_alive || state.next_seq == u64::MAX {
            return Err(ReserveError::Closed);
        }
        let Some(indices) = state.control_indices else {
            return Err(ReserveError::Closed);
        };
        let control = kind.index();
        if state.control_reserved[control].is_some()
            || state.slots[indices[control]].value.is_some()
        {
            return Err(ReserveError::Full);
        }
        let seq = state.take_seq().ok_or(ReserveError::Closed)?;
        state.control_reserved[control] = Some(seq);
        Ok(ControlPermit {
            inner: self.0.clone(),
            kind,
            seq,
            armed: true,
        })
    }

    /// Commit the single terminal witness without consuming ordinary capacity.
    /// The caller retains `value` if the receiver is gone or the slot was used.
    pub fn send_terminal(&self, value: T) -> Result<(), T> {
        let (receiver_wake, producer_wake) = {
            let mut state = self.0.lock();
            let Some(index) = state.terminal_index else {
                return Err(value);
            };
            if !state.sender_alive || !state.receiver_alive || state.terminal_sent {
                return Err(value);
            }
            state.terminal_sent = true;
            state.slots[index].value = Some(value);
            (state.receiver_waker.take(), state.producer_waker.take())
        };
        Callbacks::wake(&self.0.meter, (receiver_wake, producer_wake));
        Ok(())
    }
}

impl<T> BatchPermit<T> {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Retain the value on closed receiver or exhausted operation credit.
    pub fn send(&mut self, value: T) -> Result<(), T> {
        let Some(inner) = self.inner.as_ref() else {
            return Err(value);
        };
        let (receiver_wake, producer_wake) = {
            let mut state = inner.lock();
            if !state.sender_alive || !state.receiver_alive {
                return Err(value);
            }
            let seq = state.reserved_seq.expect("live batch reservation");
            state.push(seq, value);
            self.remaining -= 1;
            state.reserved_seq = (self.remaining != 0).then_some(seq + 1);
            state.commit_wakers()
        };
        let completed = if self.remaining == 0 {
            self.inner.take()
        } else {
            None
        };
        let inner = completed
            .as_ref()
            .or(self.inner.as_ref())
            .expect("live batch owner");
        Callbacks::wake(&inner.meter, (receiver_wake, producer_wake));
        Ok(())
    }
}

impl<T> Drop for BatchPermit<T> {
    fn drop(&mut self) {
        if self.remaining == 0 {
            return;
        }
        let Some(inner) = self.inner.take() else {
            return;
        };
        let (producer, receiver) = {
            let mut state = inner.lock();
            state.reserved_seq = None;
            (state.producer_waker.take(), state.receiver_waker.take())
        };
        Callbacks::wake(&inner.meter, (producer, receiver));
    }
}

impl<T> Permit<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
        let inner = self.0.take().expect("permit consumed once");
        let (receiver_wake, producer_wake) = {
            let mut state = inner.lock();
            let seq = state
                .reserved_seq
                .take()
                .expect("reserved ordinary sequence");
            if !state.sender_alive || !state.receiver_alive {
                return Err(value);
            }
            state.push(seq, value);
            state.commit_wakers()
        };
        Callbacks::wake(&inner.meter, (receiver_wake, producer_wake));
        Ok(())
    }
}

impl<T> Drop for Permit<T> {
    fn drop(&mut self) {
        let Some(inner) = self.0.take() else {
            return;
        };
        let (producer_wake, receiver_wake) = {
            let mut state = inner.lock();
            state.reserved_seq = None;
            (state.producer_waker.take(), state.receiver_waker.take())
        };
        Callbacks::wake(&inner.meter, (producer_wake, receiver_wake));
    }
}

impl<T> Receiver<T> {
    /// Nonblocking dispatch never replaces a registered consumer waiter.
    /// Quarantine closes dispatch, while len/close_and_drain retain access to
    /// the actual physical prefix for the caller's terminal disposition.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut state = self.0.lock();
        if state.failed {
            return Err(TryRecvError::Closed);
        }
        if let Some(value) = state.pop() {
            let wake = state.producer_waker.take();
            drop(state);
            Callbacks::wake(&self.0.meter, (wake, None));
            return Ok(value);
        }
        Err(if state.sender_alive && state.receiver_alive {
            TryRecvError::Empty
        } else {
            TryRecvError::Closed
        })
    }

    /// Close admission without discarding the physical prefix.
    pub fn close(&mut self) {
        let wake = {
            let mut state = self.0.lock();
            state.receiver_alive = false;
            state.producer_waker.take()
        };
        Callbacks::wake(&self.0.meter, (wake, None));
    }
    /// Control-path observation of all stored values, including control/terminal
    /// slots. Q closes dispatch but never hides physical inventory from this view.
    pub fn len(&self) -> usize {
        let state = self.0.lock();
        state.len
            + state.control_indices.map_or(0, |indices| {
                indices
                    .into_iter()
                    .filter(|&index| state.slots[index].value.is_some())
                    .count()
            })
            + usize::from(
                state
                    .terminal_index
                    .is_some_and(|index| state.slots[index].value.is_some()),
            )
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut callbacks = Callbacks::new(&self.0.meter);
        let mut prepared = false;
        loop {
            let mut state = self.0.lock();
            if state.failed {
                return Poll::Ready(None);
            }
            if let Some(value) = state.pop() {
                let wake = state.producer_waker.take();
                drop(state);
                // Keep an unused prepared callback protected during selected wake.
                Callbacks::wake(&self.0.meter, (wake, None));
                return Poll::Ready(Some(value));
            }
            if !state.sender_alive || !state.receiver_alive {
                return Poll::Ready(None);
            }
            if prepared {
                std::mem::swap(&mut state.receiver_waker, &mut callbacks.slots[0]);
                return Poll::Pending;
            }
            drop(state);
            callbacks.clone_waiter(cx.waker());
            prepared = true;
        }
    }

    /// Stop admission and hand every queued value, including a terminal value,
    /// to the caller. Each disposition runs outside the lane lock. The caller
    /// must establish any Root receipt or charged Q required by that value.
    pub fn close_and_drain(&mut self, mut dispose: impl FnMut(T)) {
        self.close();
        loop {
            let value = self.0.lock().pop_for_disposal();
            let Some(value) = value else { break };
            dispose(value);
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.0.lock();
            state.sender_alive = false;
            state.receiver_waker.take()
        };
        if let Some(waker) = wake {
            self.0.wake(waker);
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close_and_drain(drop);
    }
}

#[cfg(test)]
#[path = "owned_events/tests/mod.rs"]
mod tests;
