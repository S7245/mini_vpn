//! Retained Generic TUIC open for the control root. Owns every acquired lease,
//! half, CONNECT cursor and retirement witness before the next control turn.
//! No task, timer, reconnect/probe future or DATA dispatch is created here.
//! The root must join actual carrier users before settle. Tests: ../tests/staged.rs.
use super::*;
use crate::tuic::tcp_pool_admission::TcpPoolAdmissionRoundError;
use crate::upstream::{
    TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest, TcpOpenUnavailable, TcpOpenView,
};

pub(super) fn create(
    upstream: Arc<TuicUpstream>,
    request: TcpOpenRequest,
) -> Result<Attempt, TcpOpenUnavailable> {
    if upstream.udp_runtime.node_stopping() {
        return Err(TcpOpenUnavailable::Unsupported);
    }
    if upstream.relay_policy.tuic_native_override() != TuicNativeOverride::None {
        return Err(TcpOpenUnavailable::Unsupported);
    }
    if matches!(&request.target, TargetAddr::DomainPort { host, .. } if host.is_empty() || host.len() > 255)
    {
        return Err(TcpOpenUnavailable::InvalidTarget);
    }
    let header = encode_connect(&request.target);
    Ok(Attempt {
        runtime: upstream.udp_runtime.clone(),
        upstream: Some(upstream),
        request,
        header_len: header.len(),
        header: Some(header),
        state: State::Unstarted,
        failure: None,
        cancel_requested: false,
        settle_deadline: None,
        interrupted: false,
        quarantined: false,
    })
}

pub(super) struct Attempt {
    upstream: Option<Arc<TuicUpstream>>,
    runtime: Arc<node_runtime::UdpRuntime>,
    request: TcpOpenRequest,
    header: Option<Vec<u8>>,
    header_len: usize,
    state: State,
    failure: Option<ClientError>,
    cancel_requested: bool,
    settle_deadline: Option<Instant>,
    interrupted: bool,
    quarantined: bool,
}

enum State {
    Unstarted,
    Acquiring,
    Reserved(TcpPoolSlotReservation),
    Recovering(node::recovery::Ticket),
    Maintenance(TcpPoolAuxiliaryReplacementPreparation),
    Opening(TcpPoolConnectionSelection),
    Stream(StreamAttempt),
    Transferred(Custody),
    Retiring(Custody),
    Settled(Settlement),
}

enum StreamWriter {
    Connect(ConnectWrite<quinn::SendStream>),
    Ready(TuicTcpStartupWriter<quinn::SendStream>),
    Failed {
        send: quinn::SendStream,
        written: usize,
    },
}

struct StreamAttempt {
    selection: TcpPoolConnectionSelection,
    writer: StreamWriter,
    recv: quinn::RecvStream,
    witness: quinn::BiStreamRetirement,
    metadata: Option<ReadyMetadata>,
    close: CloseReport,
}

struct ReadyMetadata {
    diag: Option<TuicTcpStreamDiagMeta>,
    stream_diag: Option<TuicTcpStreamDiag>,
    progress: quinn::SendStreamProgress,
}

struct Custody {
    lease: Option<TcpPoolSlotLease>,
    witness: Option<quinn::BiStreamRetirement>,
    pressure: Option<RetainedPressure>,
    written: usize,
    close: CloseReport,
}

#[derive(Clone, Debug, Default)]
struct CloseReport {
    reset: Option<Result<(), quinn::ClosedStream>>,
    stop: Option<Result<(), quinn::ClosedStream>>,
}

/// Value-only completion evidence. No connection, lease or registry handle survives.
struct Settlement {
    written: usize,
    _close: CloseReport,
    _lease: Option<crate::tuic::tcp_pool_admission::LeaseReleaseReport>,
    _pressure_removed: bool,
    _bi_retired: bool,
}

impl Custody {
    fn empty() -> Self {
        Self {
            lease: None,
            witness: None,
            pressure: None,
            written: 0,
            close: CloseReport::default(),
        }
    }
}

impl StreamAttempt {
    fn written(&self, header_len: usize) -> usize {
        match &self.writer {
            StreamWriter::Connect(write) => write.written,
            StreamWriter::Ready(_) => header_len,
            StreamWriter::Failed { written, .. } => *written,
        }
    }
    fn send(&mut self) -> &mut quinn::SendStream {
        match &mut self.writer {
            StreamWriter::Connect(write) => &mut write.inner,
            StreamWriter::Ready(write) => &mut write.inner,
            StreamWriter::Failed { send, .. } => send,
        }
    }
}

struct ReadyGeneration {
    conn: Connection,
    open_state: Arc<TcpPoolOpenState>,
    write_pressure: Arc<TcpWritePressure>,
    read_pressure: Arc<TcpOrderedReadPressure>,
    generation: u64,
    age: Option<u64>,
    auth_attempts: u64,
}

fn maintenance(message: &'static str) -> ClientError {
    ClientError::Io(io::Error::new(io::ErrorKind::NotConnected, message))
}

fn ready_generation(
    upstream: &TuicUpstream,
    reservation: &TcpPoolSlotReservation,
) -> Result<Option<ReadyGeneration>, ClientError> {
    let slot = upstream
        .tcp_pool_slots
        .get(reservation.index)
        .ok_or_else(|| maintenance("TUIC ready pool slot missing"))?;
    let Ok(state) = slot.state.try_lock() else {
        return Ok(None);
    };
    let current = &state.current;
    let selected = reservation
        .candidates
        .iter()
        .find(|c| c.index == reservation.index);
    if selected.and_then(|c| c.identity) != Some(current.identity())
        || !reservation
            .lease
            .generation_active
            .as_ref()
            .is_some_and(|activity| Arc::ptr_eq(activity, &current.activity))
    {
        return Err(maintenance(
            "TUIC ready pool generation/lease identity changed",
        ));
    }
    if let Some(error) = current.transport.close_reason() {
        return Err(ClientError::Io(io::Error::other(error)));
    }
    let now = upstream.clock.elapsed().as_secs();
    let path = current.transport.stats().path;
    let fresh = TcpPoolPathObservation::Known {
        identity: current.identity(),
        path_service: TcpPoolPathService::known(path.cwnd, path.rtt),
        black_holes_detected: path.black_holes_detected,
        path_generation: Some(current.transport.current_path_generation()),
        successor_service_certificate: current.successor_service_certificate,
    };
    if current.auth_attempts == 0
        || reservation.qualification != TcpPoolForwardQualification::Qualified
        || reservation.black_holes_current != Some(path.black_holes_detected)
        || fresh.successor_service_readiness().is_stale()
        || (reservation.active_before == 0 && current.open_state.needs_reconnect(reservation.index))
        || should_probe_tcp_pool_generation(
            reservation.index,
            now,
            current.open_state.last_success_secs().unwrap_or(0),
            reservation.active_before == 0,
            false,
        )
    {
        return Err(maintenance("TUIC ready pool requires maintenance"));
    }
    Ok(Some(ReadyGeneration {
        conn: current.transport.clone(),
        open_state: current.open_state.clone(),
        write_pressure: current.write_pressure.clone(),
        read_pressure: current.read_pressure.clone(),
        generation: current.open_state.generation(),
        age: current.open_state.last_success_age_secs(now),
        auth_attempts: current.auth_attempts,
    }))
}

impl Attempt {
    fn begin(&mut self) -> bool {
        if self.interrupted || self.quarantined {
            self.quarantined = true;
            return false;
        }
        self.interrupted = true;
        true
    }

    fn tighten_deadline(&mut self, deadline: Instant) {
        self.settle_deadline = Some(
            self.settle_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
    }

    fn mark_open_failure(&self) {
        let selection = match &self.state {
            State::Opening(selection) => Some(selection),
            State::Stream(stream) => Some(&stream.selection),
            _ => None,
        };
        if let Some(selection) = selection {
            selection.open_state.note_open_failure(selection.conn_index);
        }
    }

    fn quarantine(&mut self, kind: io::ErrorKind, message: String) {
        if self.failure.is_none() {
            self.failure = Some(ClientError::Io(io::Error::new(kind, message)));
        }
        self.quarantined = true;
    }

    fn fail(&mut self, error: ClientError, now: Instant) {
        if self.failure.is_none() {
            self.mark_open_failure();
            self.failure = Some(error);
        }
        self.cancel_requested = true;
        self.tighten_deadline(now + TUIC_OPEN_TIMEOUT);
    }

    fn note_cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        if matches!(self.state, State::Settled(_)) {
            return;
        }
        self.cancel_requested = true;
        self.tighten_deadline(deadline);
        if self.failure.is_none() {
            if reason == TcpOpenCancel::Deadline {
                self.mark_open_failure();
            }
            let kind = if reason == TcpOpenCancel::Deadline {
                io::ErrorKind::TimedOut
            } else {
                io::ErrorKind::Interrupted
            };
            self.failure = Some(ClientError::Io(io::Error::new(
                kind,
                format!("TUIC open {reason:?}"),
            )));
        }
    }

    fn advance(&mut self, cx: &mut Context<'_>, now: Instant) {
        if matches!(self.state, State::Transferred(_) | State::Settled(_)) {
            return;
        }
        if self.runtime.node_stopping() {
            self.note_cancel(TcpOpenCancel::Cancelled, now + TUIC_OPEN_TIMEOUT);
        }
        if now >= self.request.deadline && !self.cancel_requested {
            self.note_cancel(TcpOpenCancel::Deadline, now + TUIC_OPEN_TIMEOUT);
        }
        if self.cancel_requested {
            self.close_owned();
            self.retire(now);
            return;
        }
        let Some(upstream) = self.upstream.as_ref() else {
            self.quarantine(
                io::ErrorKind::Other,
                "active TUIC attempt lost DATA owner".into(),
            );
            return;
        };
        match &mut self.state {
            State::Unstarted => self.state = State::Acquiring,
            State::Acquiring => {
                let observations = upstream.sample_tcp_pool_observations();
                let replacement = upstream.tcp_pool_replacement_allowed();
                match upstream.tcp_pool_admission.try_decide_once(
                    &observations,
                    &replacement,
                    &[],
                    false,
                ) {
                    Ok(TcpPoolAdmissionDecision::ReserveCurrent(mut reservation)) => {
                        reservation.lease.retain_until_release();
                        self.state = State::Reserved(reservation);
                    }
                    Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement)) => {
                        self.state = State::Maintenance(replacement);
                        self.fail(maintenance("TUIC ready pool requires replacement"), now);
                    }
                    Err(
                        TcpPoolAdmissionRoundError::Contended
                        | TcpPoolAdmissionRoundError::Reservation(TcpPoolReservationError::Busy),
                    ) => {}
                    Err(error) => {
                        self.state = State::Retiring(Custody::empty());
                        self.fail(
                            ClientError::Io(io::Error::other(format!("TUIC admission {error:?}"))),
                            now,
                        );
                        if matches!(
                            error,
                            TcpPoolAdmissionRoundError::Poisoned
                                | TcpPoolAdmissionRoundError::InvalidInventory
                        ) {
                            self.quarantined = true;
                        }
                    }
                }
            }
            State::Recovering(ticket) => match upstream.observe_aux_recovery(*ticket) {
                Ok(true) => self.state = State::Acquiring,
                Ok(false) => {}
                Err(error) => self.fail(error, now),
            },
            State::Reserved(reservation) => {
                if upstream.udp_runtime.node_owned() {
                    let Some(slot) = upstream.tcp_pool_slots.get(reservation.index) else {
                        self.fail(maintenance("node recovery slot missing"), now);
                        return;
                    };
                    let Ok(slot) = slot.state.try_lock() else {
                        return;
                    };
                    if slot.current.transport.close_reason().is_some() {
                        let identity = slot.current.identity();
                        let activity = slot.current.activity.clone();
                        let ticket = upstream.request_aux_recovery(reservation.index, slot);
                        let State::Reserved(mut reservation) =
                            std::mem::replace(&mut self.state, State::Unstarted)
                        else {
                            return;
                        };
                        if upstream
                            .release_recovery_lease(
                                reservation.index,
                                identity,
                                &activity,
                                &mut reservation.lease,
                            )
                            .is_err()
                        {
                            self.state = State::Reserved(reservation);
                            self.quarantine(
                                io::ErrorKind::Other,
                                "node recovery lease release incomplete".into(),
                            );
                            return;
                        }
                        drop(reservation);
                        match ticket {
                            Ok(ticket) => self.state = State::Recovering(ticket),
                            Err(error) => self.fail(error, now),
                        }
                        return;
                    }
                }
                let ready = match ready_generation(upstream, reservation) {
                    Ok(Some(ready)) => ready,
                    Ok(None) => return,
                    Err(error) => {
                        self.fail(error, now);
                        return;
                    }
                };
                // No I/O or callbacks between removal and installation of the retained lease.
                let State::Reserved(r) = std::mem::replace(&mut self.state, State::Unstarted)
                else {
                    return;
                };
                let TcpPoolSlotReservation {
                    index,
                    lease,
                    active_before,
                    path_service,
                    path_service_tiebreak,
                    service_normalized_ordering,
                    qualification,
                    qualification_anchor,
                    black_holes_current,
                    qualification_override,
                    all_degraded_fallback,
                    candidates,
                    preparation,
                } = r;
                self.state = State::Opening(TcpPoolConnectionSelection {
                    conn_index: index,
                    conn: ready.conn,
                    lease,
                    active_before,
                    path_service,
                    path_service_tiebreak,
                    service_normalized_ordering,
                    qualification,
                    qualification_anchor,
                    black_holes_current,
                    qualification_override,
                    all_degraded_fallback,
                    candidates,
                    generation: ready.generation,
                    last_success_age_secs: ready.age,
                    probe_result: "staged_ready",
                    reconnect_reason: None,
                    open_state: ready.open_state,
                    write_pressure: ready.write_pressure,
                    read_pressure: ready.read_pressure,
                    startup_auth_attempts: ready.auth_attempts,
                    replacement_installed: false,
                });
                drop(preparation);
            }
            State::Opening(selection) => {
                if let Some(error) = selection.conn.close_reason() {
                    self.fail(ClientError::Io(io::Error::other(error)), now);
                    return;
                }
                let Some((send, recv, witness)) = selection.conn.try_open_bi_1rtt_with_retirement()
                else {
                    return;
                };
                let State::Opening(selection) =
                    std::mem::replace(&mut self.state, State::Unstarted)
                else {
                    return;
                };
                let write = ConnectWrite::new(send, self.header.take().unwrap_or_default());
                self.state = State::Stream(StreamAttempt {
                    selection,
                    writer: StreamWriter::Connect(write),
                    recv,
                    witness,
                    metadata: None,
                    close: CloseReport::default(),
                });
            }
            State::Stream(stream) => {
                if let StreamWriter::Connect(write) = &mut stream.writer {
                    if write.poll(cx).is_pending() {
                        return;
                    }
                    // The fallible poll retained the owner. This conversion only moves values.
                    let State::Stream(mut stream) =
                        std::mem::replace(&mut self.state, State::Unstarted)
                    else {
                        return;
                    };
                    let StreamWriter::Connect(write) = stream.writer else {
                        return;
                    };
                    let error = match write.into_result() {
                        Ok(send) => {
                            stream.writer = StreamWriter::Ready(send);
                            None
                        }
                        Err(failure) => {
                            stream.writer = StreamWriter::Failed {
                                send: failure.inner,
                                written: failure.written,
                            };
                            Some(ClientError::Io(failure.error))
                        }
                    };
                    self.state = State::Stream(stream);
                    if let Some(error) = error {
                        self.fail(error, now);
                    }
                }
            }
            State::Maintenance(_) | State::Retiring(_) => {}
            State::Transferred(_) | State::Settled(_) => {}
        }
    }

    /// Requests close on borrowed halves first, then installs retirement custody BEFORE Drop.
    fn close_owned(&mut self) {
        if let State::Stream(stream) = &mut self.state {
            if stream.close.reset.is_none() {
                stream.close.reset = Some(stream.send().reset(VarInt::from_u32(0)));
            }
            if stream.close.stop.is_none() {
                stream.close.stop = Some(stream.recv.stop(VarInt::from_u32(0)));
            }
        }
        let state = std::mem::replace(&mut self.state, State::Retiring(Custody::empty()));
        match state {
            State::Reserved(r) => {
                self.state = State::Retiring(Custody {
                    lease: Some(r.lease),
                    ..Custody::empty()
                });
                drop(r.preparation);
            }
            State::Opening(s) => {
                self.state = State::Retiring(Custody {
                    lease: Some(s.lease),
                    ..Custody::empty()
                });
            }
            State::Stream(stream) => {
                let written = stream.written(self.header_len);
                self.state = State::Retiring(Custody {
                    lease: Some(stream.selection.lease),
                    witness: Some(stream.witness),
                    pressure: None,
                    written,
                    close: stream.close,
                });
                drop(stream.writer);
                drop(stream.recv);
            }
            State::Maintenance(replacement) => drop(replacement),
            State::Transferred(c) | State::Retiring(c) => self.state = State::Retiring(c),
            State::Settled(receipt) => self.state = State::Settled(receipt),
            State::Unstarted | State::Acquiring | State::Recovering(_) => {}
        }
        // Typed custody is installed first. Holding DATA here would keep the
        // closed connection's physical storage alive while its witness waits.
        self.upstream = None;
    }

    fn retire(&mut self, now: Instant) {
        let State::Retiring(custody) = &mut self.state else {
            return;
        };
        if (custody.lease.is_some() || custody.witness.is_some() || custody.pressure.is_some())
            && self.settle_deadline.is_some_and(|deadline| now >= deadline)
        {
            self.quarantine(
                io::ErrorKind::TimedOut,
                "TUIC retirement observed after deadline".into(),
            );
            return;
        }
        if let Some(pressure) = &mut custody.pressure {
            match pressure.try_retire() {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    self.quarantine(
                        io::ErrorKind::Other,
                        format!("TUIC pressure retirement {error:?}"),
                    );
                    return;
                }
            }
        }
        if custody
            .witness
            .as_ref()
            .is_some_and(|witness| !witness.is_retired())
        {
            return;
        }
        if let Some(lease) = &mut custody.lease {
            let report = lease.release_once();
            if !report.complete() {
                self.quarantine(
                    io::ErrorKind::Other,
                    format!("TUIC lease retirement {report:?}"),
                );
                return;
            }
        }
        let receipt = Settlement {
            written: custody.written,
            _close: custody.close.clone(),
            _lease: custody.lease.as_ref().and_then(|lease| lease.release),
            _pressure_removed: custody.pressure.as_ref().is_some_and(|p| p.removed),
            _bi_retired: custody.witness.is_some(),
        };
        self.state = State::Settled(receipt);
        self.header = None;
    }

    fn take_ready(&mut self, now: Instant) -> Option<OpenedTcpRelay> {
        if now >= self.request.deadline
            && !matches!(self.state, State::Transferred(_) | State::Settled(_))
        {
            self.note_cancel(TcpOpenCancel::Deadline, now + TUIC_OPEN_TIMEOUT);
        }
        if self.cancel_requested {
            return None;
        }
        if !matches!(self.state, State::Stream(_)) {
            return None;
        }
        let Some(upstream) = self.upstream.as_ref() else {
            self.quarantine(
                io::ErrorKind::Other,
                "ready TUIC attempt lost DATA owner".into(),
            );
            return None;
        };
        let now_secs = upstream.clock.elapsed().as_secs();
        let mode = upstream.relay_policy.tuic_generic_mode();
        let State::Stream(stream) = &mut self.state else {
            return None;
        };
        let StreamWriter::Ready(send) = &mut stream.writer else {
            return None;
        };
        if stream.metadata.is_none() {
            let id = stream.recv.id().index();
            let diag = tcp_diag_enabled().then(|| {
                TuicTcpStreamDiagMeta::new(
                    &self.request.target,
                    stream.selection.conn_index,
                    stream.selection.conn.stable_id(),
                    id,
                )
            });
            send.set_diag_meta(diag.clone());
            stream.selection.open_state.note_open_success(now_secs);
            stream.metadata = Some(ReadyMetadata {
                stream_diag: diag.clone().map(|meta| TuicTcpStreamDiag::new(meta, now)),
                diag,
                progress: send.inner.progress_handle(),
            });
        }
        // Registration is the last potentially contended operation. Halves remain borrowed.
        let pressure = match stream
            .selection
            .write_pressure
            .try_writer(stream.recv.id().index())
        {
            Ok(Some(pressure)) => pressure,
            Ok(None) => return None,
            Err(error) => {
                self.fail(ClientError::Io(error), now);
                return None;
            }
        };
        let (retained_pressure, writer_pressure) = RetainedPressure::split(pressure);
        // The rest is only ownership movement and infallible wrapper/Box construction.
        let State::Stream(stream) = std::mem::replace(&mut self.state, State::Unstarted) else {
            return None;
        };
        let StreamWriter::Ready(send) = stream.writer else {
            return None;
        };
        let Some(metadata) = stream.metadata else {
            return None;
        };
        self.state = State::Transferred(Custody {
            lease: Some(stream.selection.lease),
            witness: Some(stream.witness),
            pressure: Some(retained_pressure),
            written: self.header_len,
            close: stream.close,
        });
        let relay = GenericCarrierParts {
            recv: stream.recv,
            send,
            send_progress: metadata.progress,
            tracking: GenericTracking {
                pressure_writer: writer_pressure,
                tcp_stream_diag: metadata.stream_diag,
                diag_meta: metadata.diag,
            },
            conn: &stream.selection.conn,
        }
        .build(mode, None);
        self.upstream = None;
        Some(OpenedTcpRelay::Generic(relay))
    }
}

impl TcpOpenAttempt<OpenedTcpRelay> for Attempt {
    fn step(&mut self, cx: &mut Context<'_>, now: Instant) {
        if !self.begin() {
            return;
        }
        self.advance(cx, now);
        self.interrupted = false;
    }
    fn view(&self) -> TcpOpenView {
        let (phase, lease, witness, written) = match &self.state {
            State::Unstarted => (TcpOpenPhase::Unstarted, false, false, 0),
            State::Acquiring | State::Recovering(_) => (TcpOpenPhase::Acquiring, false, false, 0),
            State::Reserved(_) => (TcpOpenPhase::Reserved, true, false, 0),
            State::Maintenance(_) => (TcpOpenPhase::Retiring, false, false, 0),
            State::Opening(_) => (TcpOpenPhase::Opening, true, false, 0),
            State::Stream(s) => (
                if matches!(s.writer, StreamWriter::Ready(_)) {
                    TcpOpenPhase::Ready
                } else {
                    TcpOpenPhase::Connecting
                },
                true,
                true,
                s.written(self.header_len),
            ),
            State::Transferred(c) => (
                TcpOpenPhase::Transferred,
                true,
                c.witness.is_some(),
                c.written,
            ),
            State::Retiring(c) => (
                TcpOpenPhase::Retiring,
                c.lease.is_some(),
                c.witness.is_some(),
                c.written,
            ),
            State::Settled(receipt) => (TcpOpenPhase::Settled, false, false, receipt.written),
        };
        TcpOpenView {
            phase: if self.interrupted || self.quarantined {
                TcpOpenPhase::Quarantined
            } else {
                phase
            },
            lease_held: lease,
            retirement_pending: witness,
            connect_bytes: self.header_len,
            connect_written: written,
        }
    }
    fn cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        if !self.begin() {
            return;
        }
        self.note_cancel(reason, deadline);
        self.interrupted = false;
    }
    fn take_carrier(&mut self, now: Instant) -> Option<OpenedTcpRelay> {
        if !self.begin() {
            return None;
        }
        if self.runtime.node_stopping() {
            self.note_cancel(TcpOpenCancel::Cancelled, now + TUIC_OPEN_TIMEOUT);
            self.interrupted = false;
            return None;
        }
        let result = self.take_ready(now);
        self.interrupted = false;
        result
    }
    fn settle(&mut self, _cx: &mut Context<'_>, now: Instant, deadline: Instant) {
        if !self.begin() {
            return;
        }
        self.tighten_deadline(deadline);
        if !matches!(
            self.state,
            State::Transferred(_) | State::Retiring(_) | State::Settled(_)
        ) {
            self.note_cancel(TcpOpenCancel::Cancelled, deadline);
        }
        self.close_owned();
        self.retire(now);
        self.interrupted = false;
    }
    fn failure(&self) -> Option<&ClientError> {
        self.failure.as_ref()
    }
}

#[cfg(test)]
#[path = "tests/retirement.rs"]
mod tests;
