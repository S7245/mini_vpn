//! One original capability FIFO and TCP payload proof transaction per SocketCtx.
//! Owns no DATA, actor, timer or replacement policy. Real socket tails bind the
//! epoch; flush and ACK share one refunded prefix, never a byte-count delta.
//! Exactly 32 inline gap proofs plus one first-overflow diagnostic are retained.
//! Unknown/interrupted work is sticky: even an empty FIFO is not settled zero.
//! Entry: owner's prepare/commit/prove/settle; tests: downlink_custody/tests.
use super::*;
use crate::tcp_downlink_pump::PermitQueueIdentity;

pub(super) const PROOF_SLOTS: usize = 32;
#[derive(Debug)]
pub(super) enum EndFailure {
    Identity,
    IncompleteIo,
}
#[cfg(all(test, feature = "harness"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) struct EgressLedgerSnapshot {
    pub epoch: u64,
    pub origin: u32,
    pub admitted: u64,
    pub proved: u64,
    pub refunded: u64,
    pub first_cap_remaining: usize,
    pub first_cap_ordinal: u64,
    pub cap_count: usize,
    pub quarantined: bool,
    pub events: [Option<EgressLedgerEvent>; 32],
    pub event_count: usize,
    pub event_overflow: bool,
}
#[cfg(all(test, feature = "harness"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) enum EgressLedgerEventKind {
    Admit,
    Refund,
}
#[cfg(all(test, feature = "harness"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) enum EgressRefundSource {
    PacketFlush,
    AckDrain,
}
#[cfg(all(test, feature = "harness"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun) struct EgressLedgerEvent {
    pub kind: EgressLedgerEventKind,
    pub source: Option<EgressRefundSource>,
    pub epoch: u64,
    pub start: u64,
    pub end: u64,
    pub cap_ordinal: u64,
    pub cap_before: usize,
    pub cap_after: usize,
}
const HALF: usize = 1usize << 31;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Binding {
    pub(super) epoch: u64,
    pub(super) local: IpEndpoint,
    pub(super) remote: IpEndpoint,
}
impl Binding {
    pub(super) fn read(epoch: u64, socket: &TcpSocket<'_>) -> Option<Self> {
        Some(Self {
            epoch,
            local: socket.local_endpoint()?,
            remote: socket.remote_endpoint()?,
        })
    }
}
#[derive(Clone, Copy, Debug)]
struct Range {
    start: u64,
    end: u64,
}

#[derive(Debug, Default)]
pub(in crate::client_tun) struct EgressPermitLedger {
    // The original FIFO is the sole owner of the original capabilities.
    pub(super) permits: PermitStock,
    pub(super) capture: capture::Slot,
    binding: Option<Binding>,
    queue: Option<PermitQueueIdentity>,
    origin: u32,
    admitted: u64,
    proved: u64,
    refunded: u64,
    discarded: u64,
    fragments: [Option<Range>; PROOF_SLOTS],
    overflow: Option<Range>,
    pub(super) busy: bool,
    observing: bool,
    terminal: bool,
    fault: Option<&'static str>,
    #[cfg(all(test, feature = "harness"))]
    events: [Option<EgressLedgerEvent>; 32],
    #[cfg(all(test, feature = "harness"))]
    event_count: usize,
    #[cfg(all(test, feature = "harness"))]
    event_overflow: bool,
    #[cfg(all(test, feature = "harness"))]
    front_ordinal: u64,
    #[cfg(all(test, feature = "harness"))]
    next_cap_ordinal: u64,
}
impl EgressPermitLedger {
    pub(super) fn capture_queue_matches(&self, expected: &PermitQueueIdentity) -> bool {
        self.queue
            .as_ref()
            .is_some_and(|q| q.same_identity(expected))
    }
    pub(super) fn requalify_capture(&mut self, owner: capture::Owner) -> bool {
        if !self.is_empty() || !self.capture.set_idle_owner(owner) {
            return false;
        }
        *self = Self {
            permits: std::mem::take(&mut self.permits),
            capture: std::mem::take(&mut self.capture),
            ..Self::default()
        };
        true
    }

    pub(super) fn capture_source_matches(&self, source: Option<&AsyncLeasedByteFlowQueue>) -> bool {
        source.is_none_or(|source| self.queue.as_ref().is_some_and(|q| q.matches_queue(source)))
    }

    // Read-only physical custody check at capture boundaries, including after
    // cancellable IO. A binding/marker cannot substitute for the actual caps.
    pub(super) fn capture_inventory_matches(
        &self,
        pending: &PermitStock,
        bytes: usize,
        epoch: u64,
    ) -> bool {
        let sum = |stock: &PermitStock| {
            stock.iter().try_fold(0usize, |sum, cap| {
                if cap.remaining_bytes() == 0
                    || !self
                        .queue
                        .as_ref()
                        .is_some_and(|q| q.matches(cap, DownstreamPermitReleaseMode::OnEgressDrain))
                {
                    return None;
                }
                sum.checked_add(cap.remaining_bytes())
            })
        };
        sum(pending) == Some(bytes)
            && sum(&self.permits).is_some_and(|n| {
                self.binding.is_some_and(|b| b.epoch == epoch)
                    && self.admitted.checked_sub(self.refunded) == Some(n as u64)
            })
    }

    pub(super) fn capture_pristine(&self) -> bool {
        !self.quarantined()
            && self.permits.capacity() == 0
            && self.binding.is_none()
            && self.queue.is_none()
            && self.origin == 0
            && self.admitted == 0
            && self.proved == 0
            && self.refunded == 0
            && self.discarded == 0
            && !self.terminal
            && self.fragments.iter().all(Option::is_none)
            && self.capture.pristine()
    }
    // Only the original capture owner calls this after every planned binding
    // was validated under the same exclusive contexts borrow.
    pub(super) fn publish_capture_observation(&mut self) {
        self.observing = true;
    }

    #[cfg(all(test, feature = "harness"))]
    fn record_event(&mut self, event: EgressLedgerEvent) {
        // Compress only a validated continuous run. Creation stays separate;
        // every invalid delta, source change, gap and cap boundary stays visible.
        if let Some(Some(last)) = self.event_count.checked_sub(1).filter(|_| self.permits.native()).and_then(|i| self.events.get_mut(i)) {
            let valid = |e: &EgressLedgerEvent| {
                let delta = e.end.checked_sub(e.start).and_then(|n| usize::try_from(n).ok()).filter(|n| *n > 0);
                match (e.kind, e.source, delta) {
                    (EgressLedgerEventKind::Admit, None, Some(n)) => e.cap_before > 0 && e.cap_before.checked_add(n) == Some(e.cap_after),
                    (EgressLedgerEventKind::Refund, Some(_), Some(n)) => e.cap_before.checked_sub(n) == Some(e.cap_after),
                    _ => false,
                }
            };
            if last.kind == event.kind && last.source == event.source
                && last.epoch == event.epoch && last.cap_ordinal == event.cap_ordinal
                && last.end == event.start && last.cap_after == event.cap_before
                && last.cap_after > 0 && valid(last) && valid(&event)
            {
                last.end = event.end;
                last.cap_after = event.cap_after;
                return;
            }
        }
        if self.event_count < self.events.len() {
            self.events[self.event_count] = Some(event);
            self.event_count += 1;
        } else {
            self.event_overflow = true;
        }
    }
    #[cfg(all(test, feature = "harness"))]
    pub(super) fn record_admitted_caps(
        &mut self,
        prior_count: usize,
        prior_bytes: usize,
        moved: usize,
        epoch: u64,
    ) {
        let Some(mut start) = self.admitted.checked_sub(moved as u64) else {
            self.event_overflow = true;
            return;
        };
        if self.permits.native() && prior_count != 0 {
            self.record_event(EgressLedgerEvent {
                kind: EgressLedgerEventKind::Admit,
                source: None,
                epoch,
                start,
                end: self.admitted,
                cap_ordinal: self.front_ordinal,
                cap_before: prior_bytes,
                cap_after: prior_bytes + moved,
            });
            return;
        }
        for index in prior_count..self.permits.len() {
            let remaining = self.permits[index].remaining_bytes();
            let Some(end) = start.checked_add(remaining as u64) else {
                self.event_overflow = true;
                return;
            };
            self.record_event(EgressLedgerEvent {
                kind: EgressLedgerEventKind::Admit,
                source: None,
                epoch,
                start,
                end,
                cap_ordinal: self.next_cap_ordinal,
                cap_before: remaining,
                cap_after: remaining,
            });
            self.next_cap_ordinal += 1;
            start = end;
        }
        if start != self.admitted {
            self.event_overflow = true;
        }
    }
    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn snapshot_for_queue(
        &self,
        queue: &AsyncLeasedByteFlowQueue,
    ) -> Option<EgressLedgerSnapshot> {
        let binding = self.binding?;
        if !self.queue.as_ref()?.matches_queue(queue) {
            return None;
        }
        Some(EgressLedgerSnapshot {
            epoch: binding.epoch,
            origin: self.origin,
            admitted: self.admitted,
            proved: self.proved,
            refunded: self.refunded,
            first_cap_remaining: self
                .permits
                .front()
                .map_or(0, DownstreamBytePermit::remaining_bytes),
            first_cap_ordinal: self.front_ordinal,
            cap_count: self.permits.len(),
            quarantined: self.quarantined(),
            events: self.events,
            event_count: self.event_count,
            event_overflow: self.event_overflow,
        })
    }
    pub(in crate::client_tun) fn is_empty(&self) -> bool {
        !self.quarantined()
            && !self.capture.active()
            && self.permits.is_empty()
            && (self.binding.is_none() || self.refunded == self.admitted)
            && self.fragments.iter().all(Option::is_none)
    }
    pub(in crate::client_tun) fn quarantined(&self) -> bool {
        self.busy || self.observing || self.fault.is_some() || self.overflow.is_some()
    }
    pub(super) fn fault(&self) -> Option<&'static str> {
        self.fault
    }
    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn test_install_foreign_inflight(
        &mut self,
        foreign: DownstreamBytePermit,
    ) {
        self.queue = Some(foreign.queue_identity());
        assert!(self.permits.insert(&mut Some(foreign)));
    }
    pub(super) fn fail(&mut self, why: &'static str) -> bool {
        self.fault.get_or_insert(why);
        false
    }
    pub(super) fn binding(&self) -> Option<Binding> {
        self.binding
    }
    fn tail(&self) -> u32 {
        self.origin.wrapping_add(self.admitted as u32)
    }
    pub(super) fn matches_socket(&self, epoch: u64, socket: &TcpSocket<'_>) -> bool {
        self.binding.is_some() && self.binding == Binding::read(epoch, socket)
    }
    pub(super) fn validate_caps(&mut self, caps: &PermitStock, expected: usize) -> bool {
        let mut sum = 0usize;
        for cap in caps.iter() {
            let n = cap.remaining_bytes();
            if n == 0 || cap.release_mode() != DownstreamPermitReleaseMode::OnEgressDrain {
                return self.fail("invalid egress capability mode/remaining");
            }
            if self.queue.is_none() {
                self.queue = Some(cap.queue_identity());
            }
            if !self
                .queue
                .as_ref()
                .is_some_and(|q| q.matches(cap, DownstreamPermitReleaseMode::OnEgressDrain))
            {
                return self.fail("foreign egress capability queue");
            }
            let Some(next) = sum.checked_add(n) else {
                return self.fail("capability byte overflow");
            };
            sum = next;
        }
        if sum != expected {
            return self.fail("payload/capability mismatch");
        }
        true
    }
    pub(in crate::client_tun) fn validate_source(
        &mut self,
        queue: Option<&AsyncLeasedByteFlowQueue>,
    ) -> bool {
        if let Some(queue) = queue {
            // Queued-only terminal work may not have issued a capability yet.
            let identity = self
                .queue
                .get_or_insert_with(|| PermitQueueIdentity::from_queue(queue));
            if !identity.matches_queue(queue) {
                return self.fail("capability does not belong to original flow queue");
            }
        }
        true
    }
    pub(super) fn validate_inflight(&mut self, epoch: u64) -> bool {
        if self.binding.is_some_and(|b| b.epoch != epoch) {
            return self.fail("terminal epoch mismatch");
        }
        if self.queue.is_none() {
            self.queue = self
                .permits
                .front()
                .map(DownstreamBytePermit::queue_identity);
        }
        let mut sum = 0usize;
        for cap in self.permits.iter() {
            if !self
                .queue
                .as_ref()
                .is_some_and(|q| q.matches(cap, DownstreamPermitReleaseMode::OnEgressDrain))
                || cap.remaining_bytes() == 0
            {
                return self.fail("terminal inflight capability mismatch");
            }
            let Some(next) = sum.checked_add(cap.remaining_bytes()) else {
                return self.fail("inflight byte overflow");
            };
            sum = next;
        }
        if self.binding.is_some() && self.admitted.checked_sub(self.refunded) != Some(sum as u64) {
            return self.fail("inflight range/capability mismatch");
        }
        true
    }
    pub(super) fn prepare(
        &mut self,
        epoch: u64,
        socket: &TcpSocket<'_>,
        max: usize,
        pieces: usize,
    ) -> bool {
        if self.quarantined() || self.capture.active() {
            return false;
        }
        let Some(binding) = Binding::read(epoch, socket) else {
            return self.fail("missing socket identity");
        };
        let Some((head, len)) = socket.send_payload_range() else {
            return self.fail("unknown socket payload range");
        };
        if !socket.may_send()
            || len >= HALF
            || max >= HALF
            || len.checked_add(max).is_none_or(|n| n >= HALF)
        {
            return self.fail("invalid socket admission span/state");
        }
        if self.binding.is_some_and(|old| old.epoch != epoch) {
            if !self.is_empty() || len != 0 {
                return self.fail("unsettled epoch replacement");
            }
            // The original rearm gate rejects nonempty/observing/failed ledgers.
            *self = Self {
                permits: std::mem::take(&mut self.permits),
                capture: std::mem::take(&mut self.capture),
                ..Self::default()
            };
        }
        if self.binding.is_none() {
            if len != 0 || !self.permits.is_empty() {
                return self.fail("unbound socket payload/capability");
            }
            self.binding = Some(binding);
            self.origin = head;
        }
        if self.binding != Some(binding)
            || self.terminal
            || head.wrapping_add(len as u32) != self.tail()
            || self.admitted.checked_add(max as u64).is_none()
        {
            return self.fail("socket tail/epoch transaction mismatch");
        }
        if !self.permits.reserve(pieces) {
            return self.fail("capability storage reservation refused");
        }
        self.busy = true;
        true
    }
    pub(super) fn accepted(&mut self, epoch: u64, socket: &TcpSocket<'_>, n: usize) -> bool {
        let Some(next) = self.admitted.checked_add(n as u64) else {
            return self.fail("admitted overflow");
        };
        if !self.busy
            || !self.matches_socket(epoch, socket)
            || socket.send_payload_range().is_none_or(|(head, len)| {
                head.wrapping_add(len as u32) != self.origin.wrapping_add(next as u32)
            })
        {
            return self.fail("interrupted socket acceptance");
        }
        self.admitted = next;
        true
    }
    pub(super) fn begin_batch(&mut self, epoch: u64, socket: &TcpSocket<'_>) -> Option<Binding> {
        let Some(binding) = Binding::read(epoch, socket) else {
            self.fail("missing flush socket identity");
            return None;
        };
        self.begin_bound_batch(binding)
    }
    pub(super) fn begin_bound_batch(&mut self, binding: Binding) -> Option<Binding> {
        if self.quarantined() || self.binding != Some(binding) {
            self.fail("flush identity/transaction mismatch");
            return None;
        }
        self.observing = true;
        self.binding
    }
    pub(super) fn end_batch(&mut self, binding: Binding, complete: bool) -> Result<(), EndFailure> {
        if self.binding != Some(binding) {
            self.fail("flush crossed epoch");
            return Err(EndFailure::Identity);
        }
        if complete {
            self.observing = false;
            Ok(())
        } else {
            self.fail("unknown partial device flush");
            Err(EndFailure::IncompleteIo)
        }
    }
    pub(super) fn prove_packet(&mut self, seq: u32, len: usize) -> bool {
        if self.fault.is_some() || self.busy || self.binding.is_none() {
            return false;
        }
        if len == 0 {
            return true;
        }
        if len >= HALF {
            return self.fail("unknown payload proof span");
        }
        let behind = self.tail().wrapping_sub(seq) as usize;
        if behind >= HALF {
            return self.fail("ambiguous/future payload proof");
        }
        let Some(start) = self.admitted.checked_sub(behind as u64) else {
            return true;
        }; // Before this origin.
        let Some(end) = start.checked_add(len as u64) else {
            return self.fail("payload proof overflow");
        };
        if end > self.admitted {
            return self.fail("proof beyond actual socket admission");
        }
        self.prove(Range { start, end })
    }
    pub(super) fn prove_ack(&mut self, epoch: u64, socket: &TcpSocket<'_>) -> bool {
        if self.quarantined() || !self.matches_socket(epoch, socket) {
            return self.fail("ACK identity/transaction mismatch");
        }
        let Some((head, len)) = socket.send_payload_range() else {
            return true;
        }; // Closed/FIN is not payload ACK.
        if len >= HALF || head.wrapping_add(len as u32) != self.tail() {
            return self.fail("ACK payload tail mismatch");
        }
        let Some(end) = self.admitted.checked_sub(len as u64) else {
            return self.fail("ACK outside owned payload");
        };
        self.prove(Range { start: 0, end })
    }
    fn prove(&mut self, mut input: Range) -> bool {
        if input.end <= self.proved {
            return true;
        }
        input.start = input.start.max(self.proved);
        // Merge all touching intervals without allocating or exceeding the inline slots.
        loop {
            let mut merged = false;
            for slot in &mut self.fragments {
                if let Some(old) = *slot {
                    if old.start <= input.end && input.start <= old.end {
                        input.start = input.start.min(old.start);
                        input.end = input.end.max(old.end);
                        *slot = None;
                        merged = true;
                    }
                }
            }
            if !merged {
                break;
            }
        }
        if input.start <= self.proved {
            self.proved = self.proved.max(input.end);
            return true;
        }
        if let Some(slot) = self.fragments.iter_mut().find(|s| s.is_none()) {
            *slot = Some(input);
            return true;
        }
        self.overflow.get_or_insert(input);
        self.fail("proof fragment capacity exceeded")
    }
    pub(super) fn refund_proved(
        &mut self,
        #[cfg(all(test, feature = "harness"))] source: EgressRefundSource,
    ) -> usize {
        if self.fault.is_some() || self.busy {
            return 0;
        }
        let Some(mut due) = self.proved.checked_sub(self.refunded) else {
            self.fail("refund prefix mismatch");
            return 0;
        };
        let mut released = 0usize;
        self.busy = true; // Before the original release/Notify, including a zero remaining cap after panic.
        while due > 0 {
            let Some(front) = self.permits.front_mut() else {
                self.fail("missing original inflight capability");
                return released;
            };
            let before = front.remaining_bytes();
            #[cfg(all(test, feature = "harness"))]
            let n = front.release_proved_for_test(
                due.min(usize::MAX as u64) as usize,
                match source {
                    EgressRefundSource::PacketFlush => {
                        crate::tcp_downlink_pump::PermitProvedReleaseSource::PacketFlush
                    }
                    EgressRefundSource::AckDrain => {
                        crate::tcp_downlink_pump::PermitProvedReleaseSource::AckDrain
                    }
                },
            );
            #[cfg(not(all(test, feature = "harness")))]
            let n = front.release(due.min(usize::MAX as u64) as usize);
            if n == 0 || before.checked_sub(n) != Some(front.remaining_bytes()) {
                self.fail("capability refund mismatch");
                return released;
            }
            let after = front.remaining_bytes();
            if after == 0 {
                self.permits.pop_front();
            }
            #[cfg(all(test, feature = "harness"))]
            {
                self.record_event(EgressLedgerEvent {
                    kind: EgressLedgerEventKind::Refund,
                    source: Some(source),
                    epoch: self.binding.map_or(0, |binding| binding.epoch),
                    start: self.refunded,
                    end: self.refunded + n as u64,
                    cap_ordinal: self.front_ordinal,
                    cap_before: before,
                    cap_after: after,
                });
                if after == 0 {
                    self.front_ordinal += 1;
                }
            }
            self.refunded += n as u64;
            released += n;
            due -= n as u64;
        }
        self.busy = false;
        released
    }
    pub(super) fn clear_committed(&mut self) {
        self.discarded = self
            .discarded
            .saturating_add(self.admitted.saturating_sub(self.refunded));
        self.refunded = self.admitted;
        self.fragments = [None; PROOF_SLOTS];
        self.terminal = true;
        self.busy = false;
    }
}

#[cfg(test)]
mod tests;

