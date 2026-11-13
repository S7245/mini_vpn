//! Retained TUIC open for the control root. Owns every acquired lease,
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
    if matches!(&request.target, TargetAddr::DomainPort { host, .. } if host.is_empty() || host.len() > 255)
    {
        return Err(TcpOpenUnavailable::InvalidTarget);
    }
    let header = encode_connect(&request.target);
    Ok(Attempt {
        failed_replacement_slots: vec![false; upstream.tcp_pool_slots.len()],
        failed_replacement: false,
        receipt: None,
        runtime: upstream.udp_runtime.clone(),
        upstream: Some(upstream),
        request,
        header_len: header.len(),
        header: Some(header),
        state: State::Unstarted,
        failure: None,
        cancel_requested: false,
        settle_deadline: None,
        recovery_bound: tokio::time::Instant::now().into_std() + TUIC_OPEN_TIMEOUT,
        interrupted: false,
        quarantined: false,
        transferred: false,
    })
}

pub(super) struct Attempt {
    upstream: Option<Arc<TuicUpstream>>,
    runtime: Arc<node_runtime::UdpRuntime>,
    request: TcpOpenRequest,
    header: Option<Vec<u8>>,
    header_len: usize,
    state: State,
    failed_replacement_slots: Vec<bool>,
    failed_replacement: bool,
    receipt: Option<node::recovery::Receipt>,
    failure: Option<ClientError>,
    cancel_requested: bool,
    settle_deadline: Option<Instant>,
    /// D154 F-N4d: an open that is only waiting on Node-held slot state — a
    /// pending closed-slot demand, or admission Busy/Contended because recovery
    /// holds the slot preparation — fails at its own 5s bound from the open,
    /// not at the demand's 2×5s budget. The shared demand is unchanged; this
    /// open neither starts nor extends a handshake.
    recovery_bound: Instant,
    interrupted: bool,
    quarantined: bool,
    /// N5-OBS (D241/D243): the carrier reached `Transferred` (Success counted once).
    transferred: bool,
}

impl Attempt {
    /// N5-OBS (D243): count Success at the first transfer, not at flow end.
    fn note_transferred(&mut self) {
        if !std::mem::replace(&mut self.transferred, true) {
            self.runtime.opens.settle(node::OpenClass::Success);
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        // Observer only: one relaxed add per open on the Node's own counters;
        // a transferred open was already counted by `note_transferred`.
        if !self.transferred {
            let class = node::OpenClass::of(false, self.failure.as_ref());
            self.runtime.opens.settle(class);
        }
    }
}

enum State {
    Unstarted,
    Acquiring,
    Reserved(TcpPoolSlotReservation),
    Recovering(node::recovery::Ticket),
    Maintenance {
        replacement: TcpPoolAuxiliaryReplacementPreparation,
        deadline: tokio::time::Instant,
    },
    HealthyRecovering {
        ticket: node::recovery::Ticket,
        index: usize,
    },
    ProbePending {
        reservation: TcpPoolSlotReservation,
        kind: node::recovery::ProbeKind,
        deadline: tokio::time::Instant,
    },
    Probing(node::recovery::Ticket),
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

enum ReadyGeneration {
    Connection(ReadyConnection),
    Probe(node::recovery::ProbeKind),
}

struct ReadyConnection {
    conn: Connection,
    open_state: Arc<TcpPoolOpenState>,
    write_pressure: Arc<TcpWritePressure>,
    read_pressure: Arc<TcpOrderedReadPressure>,
    generation: u64,
    age: Option<u64>,
    auth_attempts: u64,
    replacement_installed: bool,
}

/// Typed per-open failure; the Node records nothing (the demand stays authoritative).
fn open_bound() -> ClientError {
    ClientError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "TUIC open waited past its 5s bound on Node-held slot state",
    ))
}

fn maintenance(message: &'static str) -> ClientError {
    ClientError::Io(io::Error::new(io::ErrorKind::NotConnected, message))
}

fn ready_generation(
    upstream: &TuicUpstream,
    reservation: &TcpPoolSlotReservation,
    receipt: &mut Option<node::recovery::Receipt>,
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
    // Keep the one-time proof across a contended slot; only consume it after
    // validating the original reservation against the actual generation.
    let proof = receipt
        .take()
        .and_then(|proof| proof.consume_proof(reservation));
    let replacement_installed = proof == Some(node::recovery::Proof::Replacement);
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
    {
        return Err(maintenance("TUIC ready pool requires maintenance"));
    }
    if let Some(kind) = upstream.node_probe_kind(reservation, current, proof.is_some()) {
        if upstream.udp_runtime.node_owned() {
            return Ok(Some(ReadyGeneration::Probe(kind)));
        }
        return Err(maintenance("TUIC ready pool requires maintenance"));
    }
    Ok(Some(ReadyGeneration::Connection(ReadyConnection {
        conn: current.transport.connection()?,
        open_state: current.open_state.clone(),
        write_pressure: current.write_pressure.clone(),
        read_pressure: current.read_pressure.clone(),
        generation: current.open_state.generation(),
        age: current.open_state.last_success_age_secs(now),
        auth_attempts: current.auth_attempts,
        replacement_installed,
    })))
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
                let snapshots = upstream.sample_staged_tcp_pool();
                match upstream.tcp_pool_admission.try_decide_staged_once(
                    &snapshots,
                    &self.failed_replacement_slots,
                    self.failed_replacement,
                ) {
                    Ok(TcpPoolAdmissionDecision::ReserveCurrent(mut reservation)) => {
                        reservation.lease.retain_until_release();
                        self.state = State::Reserved(reservation);
                    }
                    Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement)) => {
                        self.state = State::Maintenance {
                            replacement,
                            deadline: tokio::time::Instant::now() + TUIC_RECONNECT_TIMEOUT,
                        };
                        if !upstream.udp_runtime.node_owned() {
                            self.fail(maintenance("TUIC ready pool requires replacement"), now);
                        }
                    }
                    Err(
                        TcpPoolAdmissionRoundError::Contended
                        | TcpPoolAdmissionRoundError::Reservation(TcpPoolReservationError::Busy),
                    ) if now >= self.recovery_bound => self.fail(open_bound(), now),
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
                Ok(false) if now >= self.recovery_bound => self.fail(open_bound(), now),
                Ok(false) => {}
                Err(error) => self.fail(error, now),
            },
            State::Maintenance { replacement, .. } => {
                let Some(slot) = upstream.tcp_pool_slots.get(replacement.index) else {
                    self.fail(maintenance("healthy replacement slot missing"), now);
                    return;
                };
                let Ok(slot) = slot.state.try_lock() else {
                    return;
                };
                let State::Maintenance {
                    replacement,
                    deadline,
                } = std::mem::replace(&mut self.state, State::Unstarted)
                else {
                    return;
                };
                let index = replacement.index;
                match upstream.submit_healthy_replacement(replacement, slot, deadline) {
                    Ok(ticket) => self.state = State::HealthyRecovering { ticket, index },
                    Err(node::recovery::SubmissionFailure { error, settled }) => {
                        if settled {
                            self.failed_replacement_slots[index] = true;
                            self.failed_replacement = true;
                            self.state = State::Acquiring;
                        } else {
                            self.fail(error, now);
                        }
                    }
                }
            }
            State::HealthyRecovering { ticket, index } => {
                match upstream.poll_maintenance_receipt(*ticket) {
                    Ok(Some(receipt)) => {
                        self.receipt = Some(receipt);
                        self.state = State::Acquiring;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if upstream.healthy_fallback_allowed(*ticket) {
                            self.failed_replacement_slots[*index] = true;
                            self.failed_replacement = true;
                            self.state = State::Acquiring;
                        } else {
                            self.fail(error, now);
                        }
                    }
                }
            }
            State::ProbePending { reservation, .. } => {
                let Some(slot) = upstream.tcp_pool_slots.get(reservation.index) else {
                    self.fail(maintenance("probe slot missing"), now);
                    return;
                };
                let Ok(slot) = slot.state.try_lock() else {
                    return;
                };
                let State::ProbePending {
                    reservation,
                    kind,
                    deadline,
                } = std::mem::replace(&mut self.state, State::Unstarted)
                else {
                    return;
                };
                match upstream.submit_probe(reservation, slot, kind, deadline) {
                    Ok(ticket) => self.state = State::Probing(ticket),
                    Err(node::recovery::ProbeSubmissionFailure { error, reservation }) => {
                        self.state = State::Reserved(reservation);
                        self.fail(error, now);
                    }
                }
            }
            State::Probing(ticket) => match upstream.poll_maintenance_receipt(*ticket) {
                Ok(Some(receipt)) => {
                    self.receipt = Some(receipt);
                    self.state = State::Acquiring;
                }
                Ok(None) => {}
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
                let ready = match ready_generation(upstream, reservation, &mut self.receipt) {
                    Ok(Some(ReadyGeneration::Connection(ready))) => ready,
                    Ok(Some(ReadyGeneration::Probe(kind))) => {
                        let State::Reserved(reservation) =
                            std::mem::replace(&mut self.state, State::Unstarted)
                        else {
                            return;
                        };
                        self.state = State::ProbePending {
                            reservation,
                            kind,
                            deadline: kind.deadline(),
                        };
                        return;
                    }
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
                    replacement_installed: ready.replacement_installed,
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
            State::Retiring(_) => {}
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
            State::Reserved(r) | State::ProbePending { reservation: r, .. } => {
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
            State::Maintenance { replacement, .. } => drop(replacement),
            State::Transferred(c) | State::Retiring(c) => self.state = State::Retiring(c),
            State::Settled(receipt) => self.state = State::Settled(receipt),
            State::Unstarted
            | State::Acquiring
            | State::Recovering(_)
            | State::HealthyRecovering { .. }
            | State::Probing(_) => {}
        }
        // Typed custody is installed first. Holding DATA here would keep the
        // closed connection's physical storage alive while its witness waits.
        self.receipt = None;
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
        let native_mode = upstream.relay_policy.tuic_native_override();
        if native_mode != TuicNativeOverride::None {
            let reader_lease = match stream.selection.lease.try_clone_retained() {
                Ok(lease) => lease,
                Err(error) => {
                    self.quarantine(
                        io::ErrorKind::Other,
                        format!("Native staged reader lease split: {error:?}"),
                    );
                    return None;
                }
            };
            let writer_lease = match stream.selection.lease.try_clone_retained() {
                Ok(lease) => lease,
                Err(error) => {
                    self.quarantine(
                        io::ErrorKind::Other,
                        format!("Native staged writer lease split: {error:?}"),
                    );
                    return None;
                }
            };
            let expected_meter = upstream.endpoint.receive_backing_meter();
            let direct_meter = if matches!(native_mode, TuicNativeOverride::NativeChunkPump | TuicNativeOverride::D16DirectOrdered) {
                match crate::tuic::tcp_readers::preflight_native_direct_source(
                    &stream.recv,
                    expected_meter.as_ref(),
                ) {
                    Ok(meter) => Some(meter),
                    Err(error) => {
                        self.quarantine(error.kind(), error.to_string());
                        return None;
                    }
                }
            } else {
                None
            };
            let preflight = if native_mode == TuicNativeOverride::NativeOrderedPump {
                match crate::tuic::tcp_readers::preflight_native_ordered_source(
                    &stream.recv,
                    expected_meter.as_ref(),
                ) {
                    Ok(preflight) => Some(preflight),
                    Err(error) => {
                        if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::OutOfMemory) {
                            self.fail(ClientError::Io(error), now);
                        } else {
                            self.quarantine(error.kind(), error.to_string());
                        }
                        return None;
                    }
                }
            } else {
                None
            };
            let State::Stream(stream) = std::mem::replace(&mut self.state, State::Unstarted) else {
                return None;
            };
            let StreamWriter::Ready(send) = stream.writer else {
                return None;
            };
            let Some(metadata) = stream.metadata else {
                return None;
            };
            let stream_id = stream.recv.id().index();
            let conn = stream.selection.conn.clone();
            let (reader, prepared): (NativeTcpReadHalf, _) =
                if native_mode == TuicNativeOverride::D16DirectOrdered {
                    // The direct reader retains the original Quinn receive half;
                    // the preflight above already bound its backing authority.
                    drop(preflight);
                    let pressure = tcp_diag_enabled().then(|| {
                        stream
                            .selection
                            .read_pressure
                            .reader(stream_id, stream.recv.progress_handle())
                    });
                    (
                        Box::new(TuicNativeOrderedReader::new(
                            stream.recv,
                            reader_lease,
                            metadata.stream_diag,
                            conn,
                            pressure,
                        )),
                        None,
                    )
                } else if native_mode == TuicNativeOverride::NativeChunkPump {
                    // The direct reader retains RecvStream; the checked source
                    // authority must be bound before the transfer/ACK point.
                    let Some(meter) = direct_meter else {
                        self.quarantine(
                            io::ErrorKind::Other,
                            "Native direct preflight missing".into(),
                        );
                        return None;
                    };
                    (
                        Box::new(TuicNativeTcpReader::new_preflighted(
                            stream.recv,
                            reader_lease,
                            metadata.stream_diag,
                            conn,
                            meter,
                        )),
                        None,
                    )
                } else if let Some(preflight) = preflight {
                    let (reader, pump) = TuicNativeOrderedPumpReader::prepare_preflighted(
                        stream.recv,
                        reader_lease,
                        metadata.stream_diag,
                        conn,
                        preflight,
                    );
                    (Box::new(reader), Some(pump))
                } else {
                    (
                        Box::new(TuicNativeTcpReader::new(
                            stream.recv,
                            reader_lease,
                            metadata.stream_diag,
                            conn,
                        )),
                        None,
                    )
                };
            let writer = Box::new(TuicNativeTcpWriter::new(
                send,
                writer_lease,
                stream.selection.write_pressure.writer(stream_id),
                metadata.progress,
            ));
            self.note_transferred();
            self.state = State::Transferred(Custody {
                lease: Some(stream.selection.lease),
                witness: Some(stream.witness),
                pressure: None,
                written: self.header_len,
                close: stream.close,
            });
            let relay = NativeTcpRelayStream::new(reader, writer);
            self.upstream = None;
            let relay = match prepared {
                Some(pump) => relay.with_prepared_pump(pump),
                None => relay,
            };
            return Some(if native_mode == TuicNativeOverride::D16DirectOrdered {
                OpenedTcpRelay::NativeByteOwned(relay)
            } else {
                OpenedTcpRelay::Native(relay)
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
        self.note_transferred();
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
            State::Acquiring
            | State::Recovering(_)
            | State::HealthyRecovering { .. }
            | State::Maintenance { .. }
            | State::Probing(_) => (TcpOpenPhase::Acquiring, false, false, 0),
            State::ProbePending { .. } => (TcpOpenPhase::Acquiring, true, false, 0),
            State::Reserved(_) => (TcpOpenPhase::Reserved, true, false, 0),
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
pub(super) mod tests;
