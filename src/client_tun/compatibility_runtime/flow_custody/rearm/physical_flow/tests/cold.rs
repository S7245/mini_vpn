//! Cold preparation is observed at the real Ready entry used by Service tests.
//! This is a lock probe, never a transport or success substitute.
use super::*;
use crate::client_tun::compatibility_runtime::flow_custody::transaction::OpenCustody;

pub(in crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow) fn assert_owner_unlocked(
    owner: &SessionControl,
) {
    assert!(
        owner.opens.try_lock().is_ok(),
        "original Ready cold callbacks must run outside Session opens lock"
    );
}

use crate::upstream::{NativeTcpChunk, NativeTcpReadHalf, NativeTcpReader, NativeTcpRelayStream};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) struct Probe {
    mode: u8,
    initial_refusal: std::sync::Mutex<Option<FlowIdentity>>,
    cold_before: std::sync::Mutex<Option<usize>>,
    cold_bill: AtomicUsize,
    uplink_sample: std::sync::Mutex<Option<(usize, usize, usize, usize, usize, usize)>>,
    uplink_meter: std::sync::Mutex<Option<bytes::BackingMeter>>,
    stock_wait: std::sync::Mutex<Option<(usize, usize)>>,
    original: std::sync::Mutex<Option<(FlowIdentity, usize, usize, Vec<u8>, usize)>>,
    budget: Option<bytes::BackingBudget>,
    pressure: std::sync::Mutex<Option<bytes::BytesMut>>,
    probes: AtomicUsize,
    payloads: std::sync::Mutex<[Option<Box<OriginalPanic>>; 3]>,
    payload_drops: Arc<AtomicUsize>,
    payload_ptrs: [usize; 3],
    writer_drops: AtomicUsize,
    fired: AtomicUsize,
    dropped: AtomicUsize,
    deadline: std::sync::Mutex<Option<tokio::time::Instant>>,
}
impl Probe {
    pub(in crate::client_tun::compatibility_runtime) fn cold_bill(&self) -> usize { self.cold_bill.load(Ordering::Acquire) }
    pub(in crate::client_tun::compatibility_runtime) fn stock_wait(&self) -> Option<(usize, usize)> { *self.stock_wait.lock().unwrap() }

    pub(in crate::client_tun::compatibility_runtime) fn release_pressure(&self) {
        let pressure = self.pressure.lock().unwrap().take();
        drop(pressure);
    }
    pub(in crate::client_tun::compatibility_runtime) fn fired(&self) -> usize {
        self.fired.load(Ordering::Acquire)
    }
    pub(in crate::client_tun::compatibility_runtime) fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Acquire)
    }
    pub(in crate::client_tun::compatibility_runtime) fn deadline(
        &self,
    ) -> Option<tokio::time::Instant> {
        *self.deadline.lock().unwrap()
    }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn cold_stop_probe(
        &self,
        mode: u8,
        budget: bytes::BackingBudget,
    ) -> Arc<Probe> {
        let drops = Arc::new(AtomicUsize::new(0));
        let payloads = std::array::from_fn(|site| {
            Some(Box::new(OriginalPanic {
                site,
                drops: drops.clone(),
            }))
        });
        let payload_ptrs = std::array::from_fn(|site| {
            std::ptr::from_ref(payloads[site].as_deref().unwrap()) as usize
        });
        let probe = Arc::new(Probe {
            mode,
            budget: Some(budget),
            payload_drops: drops.clone(),
            payloads: std::sync::Mutex::new(payloads),
            payload_ptrs,
            ..Probe::default()
        });
        let mut opens = self.opens();
        opens.flow.as_mut().expect("actual Flow mode").cold_probe = Some(probe.clone());
        probe
    }
}
struct Reader {
    inner: Option<NativeTcpReadHalf>,
    owner: std::sync::Weak<SessionControl>,
    probe: Arc<Probe>,
    id: FlowIdentity,
}
impl NativeTcpReader for Reader {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        let result = self.inner.as_ref().unwrap().receive_backing_meter();
        let owner = self.owner.upgrade().unwrap();
        assert_owner_unlocked(&owner);
        let probes = self.probe.probes.fetch_add(1, Ordering::AcqRel);
        assert!(probes == 0 || (self.probe.mode == 20 && probes == 1), "one cold probe per actual Flow");
        match self.probe.mode {
            2 | 6 => {
                let budget = self.probe.budget.as_ref().unwrap();
                let meter = result.as_ref().unwrap();
                let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
                let actual = bytes::BytesMut::try_zeroed_metered(
                    budget.limit() - budget.used() - header,
                    meter,
                )
                .unwrap();
                *self.probe.pressure.lock().unwrap() = Some(actual);
                assert_eq!(budget.used(), budget.limit());
                self.probe.fired.store(1, Ordering::Release);
            }
            4 => {
                self.probe.fired.store(1, Ordering::Release);
                self.probe.raise(0);
            }
            19..=21 => {},
            18 => { *self.probe.uplink_meter.lock().unwrap() = result.clone(); }
            17 => {
                assert_eq!(tokio::runtime::Handle::current().runtime_flavor(), tokio::runtime::RuntimeFlavor::CurrentThread);
                *self.probe.cold_before.lock().unwrap() = Some(self.probe.budget.as_ref().unwrap().used());
            }
            3 | 10..=14 | 16 | 31 => {} // the same cold turn stops only after actual Roles construction
            _ => fire_stop(&owner, self.id, &self.probe),
        }
        result
    }
    fn source_read_bytes(&self) -> Option<usize> {
        if self.probe.mode >= 11 { assert_owner_unlocked(&self.owner.upgrade().unwrap()); }
        self.inner.as_ref().unwrap().source_read_bytes()
    }
    fn poll_read_chunk(
        &mut self,
        cx: &mut std::task::Context<'_>,
        max: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        self.inner.as_mut().unwrap().poll_read_chunk(cx, max)
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            assert_owner_unlocked(&owner);
        }
        drop(self.inner.take());
        self.probe.dropped.fetch_add(1, Ordering::AcqRel);
        if (4..=6).contains(&self.probe.mode) {
            self.probe.raise(1);
        }
    }
}
pub(in crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow) fn decorate(
    owner: &Arc<SessionControl>,
    id: FlowIdentity,
    carrier: &mut Option<OpenedOwnedTcp>,
) {
    let probe = owner
        .opens()
        .flow
        .as_ref()
        .and_then(|flow| flow.cold_probe.clone());
    let Some(probe) = probe else {
        return;
    };
    let Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(native))) = carrier.take() else {
        panic!("actual Native carrier required");
    };
    let (reader, writer, pump) = native.into_parts();
    let probe_for_writer = probe.clone();
    let mut native = NativeTcpRelayStream::new(
        Box::new(Reader {
            inner: Some(reader),
            owner: Arc::downgrade(owner),
            probe: probe.clone(),
            id,
        }),
        Box::new(Writer {
            inner: Some(writer),
            probe: probe_for_writer,
        }),
    );
    if let Some(pump) = pump {
        native = native.with_prepared_pump(pump);
    }
    let mut opens = owner.opens();
    let flow = opens.flow_mut(id).unwrap();
    *probe.original.lock().unwrap() = Some((
        id,
        native.reader.as_ref() as *const _ as *const () as usize,
        native.writer.as_ref() as *const _ as *const () as usize,
        flow.escrow.to_vec(),
        flow.reserved,
    ));
    drop(opens);
    *carrier = Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(native)));
}

fn fire_stop(owner: &SessionControl, id: FlowIdentity, probe: &Probe) {
    assert_eq!(
        probe.fired.fetch_add(1, Ordering::AcqRel),
        0,
        "one original cold probe"
    );
    let deadline = owner.request_stop();
    *probe.deadline.lock().unwrap() = Some(deadline);
    let mut opens = owner.opens();
    assert!(opens.audit_flows());
    let flow = opens.flow_mut(id).unwrap();
    assert!(flow.preparing && flow.attempt.is_none() && flow.carrier.is_none());
    assert!(flow.w_held && flow.f_held && flow.roles.is_none() && !flow.published);
    flow.close(("root", "session_stop"));
    flow.drive(&mut std::task::Context::from_waker(std::task::Waker::noop()));
    assert_eq!(flow.stage, FlowStage::Closing);
    assert!(flow.preparing);
    if probe.mode == 1 {
        opens.slots.get_mut(id.index).unwrap().epoch += 1;
    }
    drop(opens);
    if probe.mode == 8 {
        owner.quarantine_quiet();
    }
}
pub(in crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow) fn after_roles(
    owner: &SessionControl,
    id: FlowIdentity,
) {
    assert_owner_unlocked(owner);
    let probe = owner
        .opens()
        .flow
        .as_ref()
        .and_then(|flow| flow.cold_probe.clone());
    if let Some(probe) = probe {
        if probe.mode == 17 {
            // Same current-thread synchronous production cold turn: no Service,
            // Quinn or other Flow poll can spend this budget between samples.
            let before = probe.cold_before.lock().unwrap().take().unwrap();
            let bill = probe.budget.as_ref().unwrap().used().checked_sub(before).unwrap();
            assert_eq!(probe.cold_bill.swap(bill, Ordering::AcqRel), 0);
            eprintln!("[N1-real-Roles-cold] actual_roles_prepare_bill={bill} handle={:?} includes_stock_retirement_future=true main_and_clock_already_prepared=true no_actor_spawn_yet=true", id.handle);
        } else if matches!(probe.mode, 3 | 10) {
            fire_stop(owner, id, &probe);
        }
    }
}

#[derive(Debug)]
struct OriginalPanic {
    site: usize,
    drops: Arc<AtomicUsize>,
}
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
impl Probe {
    fn raise(&self, site: usize) -> ! {
        let payload = self.payloads.lock().unwrap()[site].take().unwrap();
        std::panic::panic_any(payload)
    }
    pub(in crate::client_tun::compatibility_runtime) fn payload_ptr(&self, site: usize) -> usize {
        self.payload_ptrs[site]
    }
    pub(in crate::client_tun::compatibility_runtime) fn writer_drops(&self) -> usize {
        self.writer_drops.load(Ordering::Acquire)
    }
    pub(in crate::client_tun::compatibility_runtime) fn payload_drops(&self) -> usize {
        self.payload_drops.load(Ordering::Acquire)
    }
}
struct Writer {
    inner: Option<crate::upstream::NativeTcpWriteHalf>,
    probe: Arc<Probe>,
}
impl tokio::io::AsyncWrite for Writer {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(self.inner.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(self.inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(self.inner.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl Drop for Writer {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.probe.writer_drops.fetch_add(1, Ordering::AcqRel);
        if (4..=6).contains(&self.probe.mode) {
            self.probe.raise(2);
        }
    }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn cold_original_panic_site(
        &self,
    ) -> Option<(usize, usize)> {
        let opens = self.opens();
        opens.slots.iter().flatten().find_map(|slot| {
            let super::super::super::super::transaction::OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.panic
                .as_ref()?
                .downcast_ref::<Box<OriginalPanic>>()
                .map(|payload| (payload.site, std::ptr::from_ref(payload.as_ref()) as usize))
        })
    }
}

pub(in crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow) fn lose_turn(
    owner: &SessionControl,
) -> bool {
    assert_owner_unlocked(owner);
    owner
        .opens()
        .flow
        .as_ref()
        .and_then(|flow| flow.cold_probe.as_ref())
        .is_some_and(|probe| probe.mode == 7)
}

impl Probe {
    pub(in crate::client_tun::compatibility_runtime) fn assert_retained_originals(
        &self,
        owner: &SessionControl,
    ) {
        let original = self.original.lock().unwrap();
        let (id, reader, writer, escrow, reserved) = original.as_ref().unwrap();
        let mut opens = owner.opens();
        assert!(
            opens.slots[id.index]
                .as_ref()
                .unwrap()
                .matches_physical(*id)
        );
        let flow = opens.flow_mut(*id).unwrap();
        assert_eq!(flow.id, *id);
        assert!(flow.attempt.is_some() && flow.roles.is_none() && flow.cold_retained.is_none());
        let Some(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Native(native))) = flow.carrier.as_ref()
        else {
            panic!("original Native carrier must remain in the same Flow row");
        };
        assert_eq!(
            native.reader.as_ref() as *const _ as *const () as usize,
            *reader
        );
        assert_eq!(
            native.writer.as_ref() as *const _ as *const () as usize,
            *writer
        );
        assert_eq!(
            (flow.escrow.as_slice(), flow.escrow_bytes, flow.reserved),
            (escrow.as_slice(), escrow.len(), *reserved)
        );
        assert!(flow.w_held && flow.f_held && !flow.published);
        assert_eq!(flow.preparing, self.mode == 7);
        assert_eq!(flow.stage, FlowStage::Quarantined);
        assert_eq!(
            flow.stop_deadline,
            self.deadline().map(tokio::time::Instant::into_std)
        );
        drop(opens);
        let seen = owner.flow_observation().unwrap();
        assert_eq!(
            (
                seen.w,
                seen.f,
                seen.live,
                seen.published,
                seen.released,
                seen.running,
                seen.joined
            ),
            (1, 1, 1, 0, 0, 0, 0)
        );
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn native_source_disposal_fault(&self, id: FlowIdentity) -> bool {
        let probe = self.opens().flow.as_ref().and_then(|flow| flow.cold_probe.clone());
        let Some(probe) = probe.filter(|p| (12..=14).contains(&p.mode)) else { return false; };
        if probe.fired.fetch_add(1, Ordering::AcqRel) != 0 { return false; }
        assert_owner_unlocked(self);
        *probe.deadline.lock().unwrap() = self.ledger().shutdown_deadline;
        let mut opens = self.opens();
        let flow = opens.flow_mut(id).unwrap();
        assert!(flow.roles.as_ref().unwrap().disposition_in_flight());
        assert!(!flow.roles.as_ref().unwrap().settled());
        assert_eq!(flow.roles.as_ref().unwrap().join_count() + flow.roles.as_ref().unwrap().running_count(), 5);
        if probe.mode == 13 { opens.slots.get_mut(id.index).unwrap().epoch += 1; }
        drop(opens);
        if probe.mode == 14 { self.quarantine_quiet(); }
        probe.mode == 12 // Actual token is dropped before any opaque Source disposition.
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn native_stock_disposal_fault(&self, id: FlowIdentity, allocation: (usize, usize)) -> bool {
        let probe = self.opens().flow.as_ref().and_then(|flow| flow.cold_probe.clone());
        let Some(probe) = probe.filter(|p| p.mode == 16) else { return false; };
        if self.relay_event_test_bridge().unwrap().stock_receipt().is_none() { return false; }
        if probe.fired.fetch_add(1, Ordering::AcqRel) != 0 { return false; }
        assert_owner_unlocked(self);
        assert!(allocation.0 != 0 && allocation.1 > 0);
        *probe.stock_wait.lock().unwrap() = Some(allocation);
        *probe.deadline.lock().unwrap() = self.ledger().shutdown_deadline;
        let mut opens = self.opens();
        let flow = opens.flow_mut(id).unwrap();
        let roles = flow.roles.as_ref().unwrap();
        assert!(roles.stock_turn_held_for_test() && !roles.settled());
        assert_eq!(roles.join_count(), 5);
        assert!(flow.stop_deadline.is_some_and(|d| d <= probe.deadline().unwrap().into_std()));
        true // Drop the actual token after its actual stock wait returned Pending.
    }
}

// A scheduling/pressure observer around the actual Main extractor. It never
// supplies data, replaces the socket, or replaces the original QUIC writer.
impl OpenDispatch {
    pub(in crate::client_tun) fn observe_established_extract(
        &self,
        handle: SocketHandle,
        socket: &mut TcpSocket<'_>,
        extract: impl FnOnce(&mut TcpSocket<'_>) -> crate::client_tun::uplink::Extraction,
    ) -> crate::client_tun::uplink::Extraction {
        let probe = self.owner.as_ref().and_then(|owner| {
            owner.opens().flow.as_ref().and_then(|flow| flow.cold_probe.clone())
        });
        if let Some(probe) = probe.as_ref().filter(|p| matches!(p.mode, 20 | 21) && socket.can_recv()
            && (p.mode == 21 || socket.remote_endpoint().is_some_and(|ep| ep.port == 40004))
            && p.initial_refusal.lock().unwrap().is_none()) {
            let owner = self.owner.as_ref().unwrap();
            let contracts = owner.flow_contracts();
            let id = contracts.iter().find(|c| c.id.handle == handle).unwrap().id;
            assert_eq!(owner.flow_observation().unwrap().published, if probe.mode == 20 { 2 } else { 1 });
            *probe.initial_refusal.lock().unwrap() = Some(id);
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
            owner.ledger().shutdown_deadline = Some(deadline);
            *probe.deadline.lock().unwrap() = Some(deadline);
            let bytes = socket.recv_queue();
            *probe.uplink_sample.lock().unwrap() = Some((bytes, bytes, 0, 0, probe.budget.as_ref().unwrap().used(), probe.budget.as_ref().unwrap().used()));
            if probe.mode == 21 {
                owner.app_meter.as_ref().unwrap().quarantine();
                let value = extract(socket); // Actual typed backing admission sees the original Q.
                assert!(matches!(value, crate::client_tun::uplink::Extraction::Refused(bytes::MeteredCopyError::Quarantined)));
                assert_eq!(socket.recv_queue(), bytes);
                return value;
            }
            return crate::client_tun::uplink::Extraction::Refused(bytes::MeteredCopyError::Allocation);
        }
        let Some(probe) = probe.filter(|p| p.mode == 18 && socket.can_recv()
            && p.uplink_sample.lock().unwrap().is_none()) else { return extract(socket); };
        assert_eq!(tokio::runtime::Handle::current().runtime_flavor(), tokio::runtime::RuntimeFlavor::CurrentThread);
        let budget = probe.budget.as_ref().unwrap();
        let meter = probe.uplink_meter.lock().unwrap().clone().unwrap();
        let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
        let pressure = bytes::BytesMut::try_zeroed_metered(
            budget.limit() - budget.used() - header, &meter).unwrap();
        let before = (socket.recv_queue(), budget.used());
        assert!(before.0 > 0);
        assert_eq!(before.1, budget.limit());
        let value = extract(socket);
        let after = (socket.recv_queue(), budget.used());
        let (ptr, capacity) = match &value { crate::client_tun::uplink::Extraction::Data(v) => (v.as_ptr() as usize, v.capacity()), _ => (0, 0) };
        *probe.uplink_sample.lock().unwrap() = Some((before.0, after.0, ptr, capacity, before.1, after.1));
        eprintln!("[N1-actual-established-Full] socket_before={} socket_after={} actual_payload_ptr={ptr} actual_payload_capacity={capacity} budget_before={} budget_after={} limit={} q={} next=original_channel_and_original_QUIC_writer",
            before.0, after.0, before.1, after.1, budget.limit(), budget.quarantined());
        drop(pressure); // Real resource release; the next Main turn can resume.
        value
    }
}
impl Probe {
    pub(in crate::client_tun::compatibility_runtime) fn assert_uplink_full_preserved_socket(&self) {
        let (before, after, ptr, capacity, charged_before, charged_after) = self.uplink_sample.lock().unwrap().unwrap();
        assert_eq!(charged_before, self.budget.as_ref().unwrap().limit());
        assert_eq!(charged_after, charged_before);
        assert_eq!((after, ptr, capacity), (before, 0, 0),
            "hard Native Full must preserve the actual socket bytes before allocating uplink DATA");
    }
}

// Deterministic allocator refusal at the real Receiving copy boundary. It
// supplies only an error, never a socket, DATA, transport or successful result.
pub(in crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow) fn initial_copy_refusal(
    owner: &SessionControl, id: FlowIdentity,
) -> Option<bytes::MeteredCopyError> {
    let opens = owner.opens();
    let accounting = opens.flow.as_ref()?;
    let probe = accounting.cold_probe.as_ref()?;
    if probe.mode != 19 || accounting.published != 1 { return None; }
    let mut refusal = probe.initial_refusal.lock().unwrap();
    if refusal.is_some() { return None; }
    *refusal = Some(id);
    Some(bytes::MeteredCopyError::Allocation)
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn initial_uplink_refusal(&self) -> Option<FlowIdentity> {
        let opens = self.opens();
        let probe = opens.flow.as_ref()?.cold_probe.as_ref()?;
        let result = *probe.initial_refusal.lock().unwrap();
        result
    }
    pub(in crate::client_tun::compatibility_runtime) fn assert_initial_uplink_refusal(&self) {
        use super::super::super::super::transaction::{FlowRejection, open_origin};
        let id = self.initial_uplink_refusal().unwrap();
        let ledger = self.ledger();
        let facts = ledger.first_failure.as_ref().unwrap().facts();
        assert_eq!(facts.origin, open_origin(id.index, id.epoch, crate::shared::errors::FailurePhase::TcpOpen));
        assert_eq!(facts.io_kind, Some(std::io::ErrorKind::OutOfMemory));
        assert_eq!(ledger.additional_failures, 0, "one source, no repeated settlement report");
        drop(ledger);
        assert_eq!(self.opens().flow.as_ref().unwrap().first_rejection,
            Some(FlowRejection::ReportedNativeUplink(bytes::MeteredCopyError::Allocation)));
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn assert_established_uplink_refusal(&self) {
        use super::super::super::super::transaction::{FlowRejection, open_origin, OpenPhase};
        let id = self.initial_uplink_refusal().unwrap();
        let ledger = self.ledger();
        let facts = ledger.first_failure.as_ref().unwrap().facts();
        assert_eq!(facts.origin, open_origin(id.index, id.epoch, crate::shared::errors::FailurePhase::TcpOpen));
        assert_eq!(facts.io_kind, Some(std::io::ErrorKind::OutOfMemory));
        assert_eq!(ledger.additional_failures, 0);
        drop(ledger);
        let (sample, deadline, physical, first) = {
            let opens = self.opens();
            let probe = opens.flow.as_ref().unwrap().cold_probe.as_ref().unwrap();
            let sample = *probe.uplink_sample.lock().unwrap();
            let physical = match opens.slots.get(id.index).and_then(Option::as_ref).map(|slot| &slot.phase) {
                Some(OpenPhase::Flow(flow)) => Some((flow.id, flow.rejection, flow.stop_deadline)), _ => None,
            };
            (sample, probe.deadline().map(tokio::time::Instant::into_std), physical, opens.flow.as_ref().unwrap().first_rejection)
        };
        assert!(sample.is_some_and(|(n, after, ptr, cap, _, _)| n > 0 && n == after && ptr == 0 && cap == 0));
        if let Some((actual_id, rejection, actual_deadline)) = physical {
            assert_eq!(actual_id, id);
            assert_eq!(rejection, Some(FlowRejection::ReportedNativeUplink(bytes::MeteredCopyError::Allocation)));
            assert_eq!(actual_deadline, deadline, "actual published refusal must keep original Session absolute deadline");
        } else { assert_eq!(first, Some(FlowRejection::ReportedNativeUplink(bytes::MeteredCopyError::Allocation))); }
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn assert_established_uplink_quarantine(&self) {
        use super::super::super::super::transaction::OpenPhase;
        let id = self.initial_uplink_refusal().unwrap();
        let (stage, deadline, expected, rejection) = {
            let opens = self.opens();
            let probe = opens.flow.as_ref().unwrap().cold_probe.as_ref().unwrap();
            let Some(OpenPhase::Flow(flow)) = opens.slots.get(id.index).and_then(Option::as_ref).map(|s| &s.phase) else { panic!("unknown Q must keep original Flow"); };
            (flow.stage, flow.stop_deadline, probe.deadline().unwrap().into_std(), flow.rejection)
        };
        assert_eq!(stage, FlowStage::Quarantined);
        assert_eq!(deadline, Some(expected));
        assert!(rejection.is_none());
        let ledger = self.ledger();
        assert!(ledger.first_failure.is_none());
        assert_eq!(ledger.additional_failures, 0);
        assert!(ledger.quarantined && self.stop.stopping.load(Ordering::Acquire));
        assert_eq!(ledger.shutdown_deadline.map(tokio::time::Instant::into_std), Some(expected));
        assert!(self.app_meter.as_ref().unwrap().is_quarantined());
    }
}

// Corrupt only an actual physically committed receipt's new-epoch ledger.
// The same original alias holds F; no fabricated receipt or release witness.
pub(super) fn receipt_source_fault(opens: &mut OpenCustody, id: FlowIdentity, completion: &mut HandshakeCompletion<'_, '_>) {
    let Some(probe) = opens.flow.as_ref().and_then(|f| f.cold_probe.clone()).filter(|p| p.mode == 31) else { return; };
    let Some(ctx) = completion.socket_ctxs.get_mut(&id.handle) else { return; };
    if !ctx.flow_mirror.as_ref().is_some_and(|m| m.id == id && m.receipt.is_some()) { return; }
    if probe.fired.load(Ordering::Acquire) != 0 { return; }
    assert!(opens.slots[id.index].as_ref().unwrap().matches_physical(id));
    let flow = opens.flow_mut(id).unwrap();
    let roles = flow.roles.as_ref().unwrap();
    assert_eq!(roles.join_count(), 5);
    assert!(!roles.settled());
    assert!(ctx.flow_mirror.as_ref().unwrap().matches_epoch(ctx.conn_epoch));
    assert!(FlowMirror::validate_ordered_custody(ctx, Some(roles), flow.published));
    assert!(crate::client_tun::downlink_custody::storage::pristine(ctx));
    assert!(ctx.downlink_inflight_permits.validate_source(roles.permit_queue()));
    assert!(!crate::client_tun::downlink_custody::storage::pristine(ctx));
    assert!(ctx.downlink_inflight_permits.is_empty()); // Real zero-byte stale binding.
    assert!(!FlowMirror::validate_ordered_custody(ctx, Some(roles), flow.published));
    *probe.deadline.lock().unwrap() = flow.stop_deadline.map(tokio::time::Instant::from_std);
    probe.fired.store(1, Ordering::Release);
}
