//! Fixed per-ledger capture storage and one stack batch index. The ledger still
//! owns all proof/refund authority; this retains identities across cancellable
//! IO. No DATA allocation, flow registry, ACK owner or retry policy is added.
//! Entry: Batch::begin/prepare_flush/finish/ack. Real-wire tests live under
//! client_tun/tests/local_egress_service/ordered_flush/native_storage.
use super::*;
use bytes::{BackingMeter, MeteredCopyError};
type Link = Option<(SocketHandle, u64)>;
#[derive(Debug, Default)]
pub(in crate::client_tun) struct Slot {
    records: Vec<Record>, // Exact one-element allocation, before its allowance.
    cold: Option<storage::Cold>,
}
#[derive(Debug)]
struct Record {
    owner: Owner,
    state: State,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun::downlink_custody) enum Owner {
    Flow((usize, SocketHandle, u64, u64)),
    #[cfg(test)]
    Standalone,
}
#[derive(Debug)]
enum State {
    Idle,
    Active {
        ack: AckCapture,
        source: SourceCapture,
        next: Link,
        flush: Option<FlushCapture>,
        flushed: bool,
        phase: Phase,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Poll,
    Prepared,
    Flushed,
}
impl Slot {
    pub(in crate::client_tun) fn layout_bytes() -> usize {
        std::mem::size_of::<Record>()
    }
    pub(in crate::client_tun::downlink_custody) fn prepare(
        meter: &BackingMeter,
        owner: Owner,
    ) -> Result<Self, MeteredCopyError> {
        let (mut records, cold) = storage::exact(1, meter)?;
        records.push(Record {
            owner,
            state: State::Idle,
        });
        Ok(Self {
            records,
            cold: Some(cold),
        })
    }
    pub(in crate::client_tun::downlink_custody) fn native(&self) -> bool {
        self.cold.is_some()
    }
    pub(in crate::client_tun::downlink_custody) fn pristine(&self) -> bool {
        self.cold.is_none() && self.records.is_empty() && self.records.capacity() == 0
    }
    pub(in crate::client_tun::downlink_custody) fn active(&self) -> bool {
        self.records
            .iter()
            .any(|r| matches!(r.state, State::Active { .. }))
    }
    pub(in crate::client_tun::downlink_custody) fn idle_for(&self, meter: &BackingMeter) -> bool {
        self.valid(meter) && matches!(self.records.first().map(|r| &r.state), Some(State::Idle))
    }
    fn valid(&self, meter: &BackingMeter) -> bool {
        self.records.len() == 1
            && self.records.capacity() == 1
            && self.cold.as_ref().is_some_and(|c| {
                c.meter.same_authority(meter) && c.matches_layout(Self::layout_bytes())
            })
    }
    pub(super) fn quarantine(&self) {
        if let Some(c) = &self.cold {
            c.meter.quarantine();
        }
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if self.active() || std::thread::panicking() {
            self.quarantine();
        }
    }
}
pub(in crate::client_tun) struct Batch {
    meter: BackingMeter,
    head: Link,
    count: usize,
    armed: bool,
    phase: Phase,
    refusal: Option<CaptureRefusal>,
}
impl Drop for Batch {
    fn drop(&mut self) {
        if self.armed {
            self.meter.quarantine();
        }
    }
}
impl Batch {
    pub(super) fn refusal(&self) -> Option<CaptureRefusal> {
        self.refusal
    }
    fn admit(&mut self) -> bool {
        if self.refusal.is_some() {
            return false;
        }
        if let Err(error) = self.meter.admit_precharged() {
            self.refusal = Some(CaptureRefusal::Admission(error));
            return false;
        }
        true
    }
    fn fail(&mut self) {
        #[cfg(test)]
        tests::rejected_batch(self);
        self.refusal.get_or_insert(CaptureRefusal::Inventory);
        self.meter.quarantine();
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn begin(
        meter: &BackingMeter,
        sockets: &SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<Self, CaptureRefusal> {
        // A refused operation has not inspected inventory and cannot invent a
        // second physical fault from the original owner's sticky Q.
        meter
            .admit_precharged()
            .map_err(CaptureRefusal::Admission)?;
        // All representations, including zero-byte Active slots, are checked
        // before installing any prefix. Native mode never falls back to Vec.
        let valid = contexts.iter().all(|(handle, ctx)| {
            if !physical_socket(sockets, *handle, ctx) {
                return false;
            }
            if storage::any_native(ctx) {
                storage::capture_inventory(ctx, meter)
                    && ctx.downlink_inflight_permits.capture.idle_for(meter)
                    && ctx
                        .downlink_inflight_permits
                        .capture
                        .authorizes(*handle, ctx)
                    && !ctx.downlink_inflight_permits.quarantined()
            } else {
                storage::pristine(ctx)
            }
        });
        let custody = valid
            && contexts.iter_mut().all(|(handle, ctx)| {
                if !storage::any_native(ctx) {
                    return true;
                }
                // Original cold qualification remains installed while an
                // empty context waits for the next Root Ready. Historical
                // epoch/source bindings carry no capture debt in this state.
                if ctx.downlink_pending.is_empty()
                    && ctx.downlink_pending_permits.is_empty()
                    && ctx.downlink_inflight_permits.is_empty()
                {
                    return true;
                }
                if !pending_mode_matches(ctx) {
                    return false;
                }
                let ledger = &mut ctx.downlink_inflight_permits;
                (!ctx.downlink_delay_permit_release_until_egress
                    || ledger
                        .validate_caps(&ctx.downlink_pending_permits, ctx.downlink_pending.len()))
                    && ledger.validate_inflight(ctx.conn_epoch)
                    && ledger.validate_source(ctx.d16_downlink_queue.as_ref())
                    && ctx.flow_mirror.as_ref().is_none_or(|mirror| {
                        ledger.validate_source(mirror.d16_queue())
                            && ledger.validate_source(mirror.bound_egress_queue())
                    })
                    && source_inventory_matches(*handle, ctx)
            });
        if !custody {
            #[cfg(test)]
            tests::rejected_begin(meter, sockets, contexts, valid);
            meter.quarantine();
            for ctx in contexts.values() {
                ctx.downlink_inflight_permits.capture.quarantine();
            }
            return Err(CaptureRefusal::Inventory);
        }
        meter
            .admit_precharged()
            .map_err(CaptureRefusal::Admission)?;
        let mut batch = Self {
            meter: meter.clone(),
            head: None,
            count: 0,
            armed: contexts
                .values()
                .any(|ctx| ctx.downlink_inflight_permit_bytes() > 0),
            phase: Phase::Poll,
            refusal: None,
        };
        for (handle, ctx) in contexts.iter_mut() {
            let Some(cap) = ctx.downlink_inflight_permits.permits.front() else {
                continue;
            };
            let source = SourceCapture::capture(cap, ctx);
            let socket = sockets.get::<TcpSocket>(*handle);
            let ack = AckCapture {
                handle: *handle,
                epoch: ctx.conn_epoch,
                binding: Binding::read(ctx.conn_epoch, socket),
                range: socket.send_payload_range(),
            };
            // Preflight proved the one real slot. No fallible work follows.
            ctx.downlink_inflight_permits.capture.records[0].state = State::Active {
                ack,
                source,
                next: batch.head,
                flush: None,
                flushed: false,
                phase: Phase::Poll,
            };
            batch.head = Some((*handle, ctx.conn_epoch));
            batch.count += 1;
        }
        batch.armed = batch.count != 0;
        Ok(batch)
    }
    fn record<'a>(
        &self,
        contexts: &'a HashMap<SocketHandle, SocketCtx>,
        link: Link,
    ) -> Option<&'a State> {
        let (handle, epoch) = link?;
        let ctx = contexts.get(&handle)?;
        let slot = &ctx.downlink_inflight_permits.capture;
        if ctx.conn_epoch != epoch || !slot.valid(&self.meter) {
            return None;
        }
        let record = &slot.records.first()?.state;
        if let State::Active { source, .. } = record {
            if !source.matches(ctx) {
                return None;
            }
        }
        match record {
            State::Active { ack, phase, .. }
                if ack.handle == handle && ack.epoch == epoch && *phase == self.phase =>
            {
                Some(record)
            }
            _ => None,
        }
    }
    fn valid(
        &self,
        contexts: &HashMap<SocketHandle, SocketCtx>,
        sockets: Option<&SocketSet<'_>>,
    ) -> bool {
        // One bounded reverse inventory at transaction boundaries, never per
        // summary. Exact finite chain + equal physical count proves coverage.
        let mut actual = 0usize;
        for (handle, ctx) in contexts {
            // Check every physical obligation, including bytes in an inactive
            // slot. Unique map keys and checked actual slots prove both sides
            // without a second full SocketSet/hash lookup pass.
            if sockets.is_some_and(|s| !physical_socket(s, *handle, ctx)) {
                return false;
            }
            let slot = &ctx.downlink_inflight_permits.capture;
            if !slot.active() {
                continue;
            }
            if self
                .record(contexts, Some((*handle, ctx.conn_epoch)))
                .is_none()
                || !storage::capture_inventory(ctx, &self.meter)
                || !pending_mode_matches(ctx)
                || !source_inventory_matches(*handle, ctx)
                || !ctx.downlink_inflight_permits.capture_inventory_matches(
                    &ctx.downlink_pending_permits,
                    ctx.downlink_pending.len(),
                    ctx.conn_epoch,
                )
            {
                return false;
            }
            actual += 1;
        }
        if actual != self.count {
            return false;
        }
        let mut link = self.head;
        for _ in 0..self.count {
            let Some(State::Active { next, .. }) = self.record(contexts, link) else {
                return false;
            };
            link = *next;
        }
        link.is_none() && (self.count != 0) == self.armed
    }
    pub(super) fn prepare_flush(
        &mut self,
        sockets: &SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> bool {
        if !self.admit() {
            return false;
        }
        #[cfg(test)]
        tests::after_admission(&self.meter);
        if self.phase != Phase::Poll
            || !self.valid(contexts, Some(sockets))
        {
            self.fail();
            return false;
        }
        if !self.admit() {
            return false;
        }
        let mut link = self.head;
        for _ in 0..self.count {
            let Some((handle, _)) = link else {
                self.fail();
                return false;
            };
            let Some(ctx) = contexts.get_mut(&handle) else {
                self.fail();
                return false;
            };
            // Plan into the existing cold record. No ledger observation is
            // published until every participant has a valid original identity.
            let plan = plan_flush(handle, sockets, ctx);
            let Ok(plan) = plan else {
                self.fail();
                return false;
            };
            let State::Active { flush, next, .. } =
                &mut ctx.downlink_inflight_permits.capture.records[0].state
            else {
                self.fail();
                return false;
            };
            *flush = plan;
            link = *next;
        }
        link = self.head;
        for _ in 0..self.count {
            let Some((handle, _)) = link else {
                self.fail();
                return false;
            };
            let Some(ctx) = contexts.get_mut(&handle) else {
                self.fail();
                return false;
            };
            let State::Active {
                flush, next, phase, ..
            } = &mut ctx.downlink_inflight_permits.capture.records[0].state
            else {
                self.fail();
                return false;
            };
            link = *next;
            *phase = Phase::Prepared;
            if flush.is_some() {
                ctx.downlink_inflight_permits.publish_capture_observation();
            }
        }
        self.phase = Phase::Prepared;
        true
    }
    pub(super) fn finish(
        &mut self,
        mut packets: crate::device::FlushedTcpBatch<'_>,
        sockets: &SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
        complete: bool,
    ) -> TunFlushPermitRelease {
        let mut release = TunFlushPermitRelease::default();
        if !self.admit() {
            packets.quarantine();
            release.refusal = self.refusal;
            return release;
        }
        if self.phase != Phase::Prepared
            || !self.valid(contexts, Some(sockets))
        {
            self.fail();
            packets.quarantine();
            release.refusal = self.refusal;
            return release;
        }
        if !self.admit() {
            packets.quarantine();
            release.refusal = self.refusal;
            return release;
        }
        // Dead-retained ACK records can be nonempty without any published
        // flush observation. Known IO then creates no unknown proof debt.
        let mut link = self.head;
        let mut observed = false;
        for _ in 0..self.count {
            if let Some(State::Active { next, flush, .. }) = self.record(contexts, link) {
                observed |= flush.is_some();
                link = *next;
            }
        }
        let incomplete_io = !complete && observed;
        let mut okay = true;
        while let Some(packet) = packets.next() {
            release.flushed_tcp_payload_bytes = release
                .flushed_tcp_payload_bytes
                .saturating_add(packet.payload_bytes);
            let mut chosen = None;
            let mut ambiguous = false;
            let mut link = self.head;
            // Only C links of this batch, never all F contexts per packet.
            for _ in 0..self.count {
                let Some(State::Active {
                    ack, next, flush, ..
                }) = self.record(contexts, link)
                else {
                    okay = false;
                    break;
                };
                let handle = ack.handle;
                link = *next;
                let matches = flush.as_ref().is_some_and(|c| {
                    flushed_tcp_packet_matches_endpoint_pair(
                        packet,
                        Some(c.binding.local),
                        Some(c.binding.remote),
                    )
                });
                if !matches {
                    continue;
                }
                if packet.payload_bytes > 0 {
                    if let Some(ctx) = contexts.get_mut(&handle) {
                        if let State::Active { flushed, .. } =
                            &mut ctx.downlink_inflight_permits.capture.records[0].state
                        {
                            *flushed = true;
                        }
                    }
                }
                if let Some(first) = chosen {
                    ambiguous = true;
                    for h in [first, handle] {
                        if let Some(ctx) = contexts.get_mut(&h) {
                            ctx.downlink_inflight_permits
                                .fail("ambiguous flush endpoint pair");
                        }
                    }
                } else {
                    chosen = Some(handle);
                }
            }
            if ambiguous {
                okay = false;
                packets.quarantine();
                continue;
            }
            let Some(handle) = chosen else { continue };
            let Some(ctx) = contexts.get_mut(&handle) else {
                okay = false;
                continue;
            };
            let same = match &ctx.downlink_inflight_permits.capture.records[0].state {
                State::Active { flush: Some(c), .. } => {
                    c.matches(ctx, sockets.get::<TcpSocket>(handle))
                }
                _ => false,
            };
            if !same {
                ctx.downlink_inflight_permits
                    .fail("actual flush crossed physical epoch");
                okay = false;
                packets.quarantine();
                continue;
            }
            if ctx
                .downlink_inflight_permits
                .prove_packet(packet.payload_start_seq, packet.payload_bytes)
            {
                release.released_inflight_permit_bytes +=
                    ctx.downlink_inflight_permits.refund_proved(
                        #[cfg(all(test, feature = "harness"))]
                        ledger::EgressRefundSource::PacketFlush,
                    );
            }
            if ctx.downlink_inflight_permits.fault().is_some() {
                okay = false;
                packets.quarantine();
            }
        }
        let mut link = self.head;
        for _ in 0..self.count {
            let Some(State::Active {
                ack, next, flush, ..
            }) = self.record(contexts, link)
            else {
                okay = false;
                break;
            };
            let handle = ack.handle;
            link = *next;
            let end = flush.as_ref().map(|c| {
                (
                    c.binding,
                    c.matches(&contexts[&handle], sockets.get::<TcpSocket>(handle)),
                )
            });
            if let Some(ctx) = contexts.get_mut(&handle) {
                if ctx.downlink_inflight_permits.fault().is_some() {
                    okay = false;
                }
                if let Some((binding, same)) = end {
                    if !same {
                        okay = false;
                    }
                    if matches!(
                        ctx.downlink_inflight_permits
                            .end_batch(binding, complete && same),
                        Err(ledger::EndFailure::Identity)
                    ) {
                        okay = false;
                    }
                }
                if ctx.downlink_inflight_permits.quarantined() {
                    if complete {
                        okay = false;
                    }
                    packets.quarantine();
                }
            }
        }
        if !okay {
            self.fail();
            packets.quarantine();
        } else if incomplete_io {
            self.refusal = Some(CaptureRefusal::IncompleteIo);
            self.meter.quarantine();
            packets.quarantine();
        }
        packets.commit();
        if okay && !incomplete_io {
            let mut link = self.head;
            for _ in 0..self.count {
                let Some((handle, _)) = link else { break };
                if let Some(ctx) = contexts.get_mut(&handle) {
                    if let State::Active { next, phase, .. } =
                        &mut ctx.downlink_inflight_permits.capture.records[0].state
                    {
                        link = *next;
                        *phase = Phase::Flushed;
                    }
                }
            }
            self.phase = Phase::Flushed;
        }
        release.refusal = self.refusal;
        release
    }
    pub(super) fn ack(
        &mut self,
        sockets: &SocketSet<'_>,
        contexts: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> usize {
        if !self.admit() {
            return 0;
        }
        if self.phase != Phase::Flushed
            || !self.valid(contexts, Some(sockets))
        {
            self.fail();
            return 0;
        }
        if !self.admit() {
            return 0;
        }
        let mut released = 0;
        let mut link = self.head;
        for _ in 0..self.count {
            let Some(State::Active {
                ack, next, flushed, ..
            }) = self.record(contexts, link)
            else {
                self.fail();
                return released;
            };
            let (ack, skip) = (*ack, *flushed);
            link = *next;
            if !skip {
                released +=
                    release_captured_inflight_for_send_queue_drain([&ack], sockets, contexts);
            }
        }
        self.settle(contexts);
        released
    }
    pub(super) fn settle(&mut self, contexts: &mut HashMap<SocketHandle, SocketCtx>) {
        if !self.admit() {
            return;
        }
        if self.phase != Phase::Flushed || !self.valid(contexts, None) {
            self.fail();
            return;
        }
        let mut link = self.head;
        for _ in 0..self.count {
            let Some(State::Active { ack, next, .. }) = self.record(contexts, link) else {
                self.fail();
                return;
            };
            if contexts[&ack.handle]
                .downlink_inflight_permits
                .quarantined()
            {
                self.fail();
                return;
            }
            link = *next;
        }
        // All proofs, ACK/refunds and exact chain validated before any Idle.
        if !self.admit() {
            return;
        }
        let mut link = self.head;
        for _ in 0..self.count {
            let Some((handle, _)) = link else { break };
            if let Some(ctx) = contexts.get_mut(&handle) {
                let record = &mut ctx.downlink_inflight_permits.capture.records[0].state;
                if let State::Active { next, .. } = record {
                    link = *next;
                }
                *record = State::Idle;
            }
        }
        self.armed = false;
    }
    #[cfg(all(test, feature = "harness"))]
    pub(super) fn lost_binding(
        &self,
        handle: SocketHandle,
        epoch: u64,
        sockets: &SocketSet<'_>,
        contexts: &HashMap<SocketHandle, SocketCtx>,
    ) -> bool {
        match self.record(contexts, Some((handle, epoch))) {
            Some(State::Active { ack, flushed, .. }) => {
                ack.binding.is_some()
                    && !flushed
                    && Binding::read(epoch, sockets.get::<TcpSocket>(handle)).is_none()
            }
            _ => false,
        }
    }
}
fn plan_flush(
    handle: SocketHandle,
    sockets: &SocketSet<'_>,
    ctx: &mut SocketCtx,
) -> Result<Option<FlushCapture>, ()> {
    if ctx.downlink_inflight_permit_bytes() == 0 {
        return Ok(None);
    }
    if ctx.downlink_inflight_permits.quarantined() {
        return Err(());
    }
    let socket = sockets.get::<TcpSocket>(handle);
    if retains_dead_ordered_inflight(handle, ctx, socket) {
        return Ok(None);
    }
    let (binding, disposition) = match ResetProof::capture(ctx, socket)? {
        Some(proof) => {
            let (epoch, local, remote) = proof.identity();
            (
                Binding {
                    epoch,
                    local,
                    remote,
                },
                CaptureDisposition::Reset(proof),
            )
        }
        None => (
            Binding::read(ctx.conn_epoch, socket).ok_or(())?,
            CaptureDisposition::Active,
        ),
    };
    if ctx.downlink_inflight_permits.quarantined()
        || ctx.downlink_inflight_permits.binding() != Some(binding)
    {
        return Err(());
    }
    Ok(Some(FlushCapture {
        handle,
        binding,
        disposition,
    }))
}

#[cfg(test)]
pub(super) mod tests;

fn physical_socket(sockets: &SocketSet<'_>, handle: SocketHandle, ctx: &SocketCtx) -> bool {
    let needs_socket = ctx.downlink_inflight_permit_bytes() != 0
        || ctx.downlink_inflight_permits.capture.active();
    !needs_socket || sockets.try_get::<TcpSocket>(handle).is_some()
}

// Chunk bytes have no egress capability. OnAdmission capabilities remain in
// their original pending owner/Flow binding until send, never in the ACK debt.
fn pending_mode_matches(ctx: &SocketCtx) -> bool {
    if ctx.downlink_delay_permit_release_until_egress {
        return ctx
            .downlink_pending_permits
            .iter()
            .all(|cap| cap.release_mode() == DownstreamPermitReleaseMode::OnEgressDrain);
    }
    if ctx.downlink_inflight_permit_bytes() != 0 {
        return false;
    }
    if ctx.downlink_pending_permits.is_empty() {
        return true;
    }
    let sum = ctx
        .downlink_pending_permits
        .iter()
        .try_fold(0usize, |sum, cap| {
            if cap.remaining_bytes() == 0
                || !ctx
                    .flow_mirror
                    .as_ref()
                    .is_some_and(|m| m.admission_cap_matches(cap))
            {
                return None;
            }
            sum.checked_add(cap.remaining_bytes())
        });
    sum == Some(ctx.downlink_pending.len())
}

// The original Flow binding must still exist, not merely agree when present.
// This same reverse check guards preparation and post-IO settlement.
fn source_inventory_matches(handle: SocketHandle, ctx: &SocketCtx) -> bool {
    let ledger = &ctx.downlink_inflight_permits;
    ledger.capture_source_matches(ctx.d16_downlink_queue.as_ref())
        && ctx.flow_mirror.as_ref().is_none_or(|mirror| {
            mirror.capture_identity_matches(handle, ctx.conn_epoch)
                && (!ctx.downlink_delay_permit_release_until_egress || mirror.egress_bound())
                && ledger.capture_source_matches(mirror.d16_queue())
                && ledger.capture_source_matches(mirror.bound_egress_queue())
        })
}

#[derive(Debug)]
struct SourceCapture {
    queue: crate::tcp_downlink_pump::PermitQueueIdentity,
    mirror: Option<(usize, SocketHandle, u64, u64)>,
    profile: u8,
}
impl SourceCapture {
    fn profile(ctx: &SocketCtx) -> u8 {
        u8::from(ctx.d16_downlink_queue.is_some())
            | (u8::from(
                ctx.flow_mirror
                    .as_ref()
                    .and_then(|m| m.d16_queue())
                    .is_some(),
            ) << 1)
            | (u8::from(
                ctx.flow_mirror
                    .as_ref()
                    .and_then(|m| m.bound_egress_queue())
                    .is_some(),
            ) << 2)
    }
    fn capture(cap: &DownstreamBytePermit, ctx: &SocketCtx) -> Self {
        Self {
            queue: cap.queue_identity(),
            mirror: ctx.flow_mirror.as_ref().map(|m| m.capture_identity()),
            profile: Self::profile(ctx),
        }
    }
    fn matches(&self, ctx: &SocketCtx) -> bool {
        let queue_matches =
            |q: Option<&AsyncLeasedByteFlowQueue>| q.is_none_or(|q| self.queue.matches_queue(q));
        self.mirror == ctx.flow_mirror.as_ref().map(|m| m.capture_identity())
            && self.profile == Self::profile(ctx)
            && ctx
                .downlink_inflight_permits
                .capture_queue_matches(&self.queue)
            && ctx
                .downlink_inflight_permits
                .permits
                .iter()
                .chain(ctx.downlink_pending_permits.iter())
                .all(|cap| {
                    self.queue
                        .matches(cap, DownstreamPermitReleaseMode::OnEgressDrain)
                })
            && queue_matches(ctx.d16_downlink_queue.as_ref())
            && ctx.flow_mirror.as_ref().is_none_or(|m| {
                queue_matches(m.d16_queue()) && queue_matches(m.bound_egress_queue())
            })
    }
}
impl Slot {
    pub(in crate::client_tun::downlink_custody) fn owner(&self) -> Option<Owner> {
        self.records.first().map(|r| r.owner)
    }
    pub(in crate::client_tun::downlink_custody) fn set_idle_owner(&mut self, owner: Owner) -> bool {
        match self.records.first_mut() {
            Some(Record {
                owner: current,
                state: State::Idle,
            }) => {
                *current = owner;
                true
            }
            _ => false,
        }
    }
    fn authorizes(&self, handle: SocketHandle, ctx: &SocketCtx) -> bool {
        match self.owner() {
            Some(Owner::Flow(identity)) => {
                // A fully settled empty context may wait for the original Root
                // Ready gate to qualify its next Flow. It cannot capture DATA.
                if ctx.downlink_pending.is_empty()
                    && ctx.downlink_pending_permits.is_empty()
                    && ctx.downlink_inflight_permits.is_empty()
                {
                    return true;
                }
                identity.1 == handle
                    && identity.2 == ctx.conn_epoch
                    && ctx.flow_mirror.as_ref().map(|m| m.capture_identity()) == Some(identity)
            }
            #[cfg(test)]
            Some(Owner::Standalone) => true,
            None => false,
        }
    }
}
