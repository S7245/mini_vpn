//! Original local TCP uplink transaction: channel admission before socket
//! extraction, bounded batching/FIN ordering, and writer coalescing with an
//! exact deferred command. Main owns the socket; the original writer owns IO,
//! current/deferred commands and terminal disposition. No additional queue,
//! task or lifecycle authority is introduced here. Entry: pump_established_uplink
//! and coalesce_relay_payload. Characterization: client_tun/tests/mod.rs and
//! compatibility_runtime/flow_custody/relay_writer/tests/outcomes.rs.
use super::{SocketCtx, SocketState, TcpSocket, TcpState, MAX_ESTABLISHED_UPLINK_BATCH,
    LOCAL_FINISH_DEFER_SECS, RELAY_WRITER_COALESCE_MAX_MESSAGES, RELAY_WRITER_COALESCE_MAX_BYTES};
use crate::shared::bounded_channel::{self as lane, ReserveError};
pub(super) type Sender = lane::Sender<RelayCommand>;
pub(super) type Receiver = lane::Receiver<RelayCommand>;
pub(super) type Observer = lane::Observer<RelayCommand>;
pub(super) fn channel(capacity: usize) -> (Sender, Receiver) { lane::channel(capacity) }

#[derive(Debug)]
pub(super) enum RelayCommand {
    Data(Vec<u8>),
    NativeData(bytes::Bytes),
    Finish,
}

pub(super) struct CoalescedRelayWrite {
    pub(super) payload: Payload,
    pub(super) finish_after: bool,
    pub(super) deferred: Option<RelayCommand>,
    pub(super) failure: Option<bytes::MeteredCopyError>,
}

pub(super) fn coalesce_relay_payload(
    payload: impl Into<Payload>,
    rx: &mut crate::client_tun::uplink::Receiver,
) -> CoalescedRelayWrite {
    let mut payload = payload.into();
    let mut messages = 1usize;
    let mut finish_after = false;
    while messages < RELAY_WRITER_COALESCE_MAX_MESSAGES
        && payload.len() < RELAY_WRITER_COALESCE_MAX_BYTES
    {
        match rx.try_recv() {
            Ok(command @ (RelayCommand::Data(_) | RelayCommand::NativeData(_))) => {
                let next = Payload::from_command(command);
                if payload.len().saturating_add(next.len()) > RELAY_WRITER_COALESCE_MAX_BYTES {
                    return CoalescedRelayWrite {
                        payload,
                        finish_after,
                        deferred: Some(next.into_command()),
                        failure: None,
                    };
                }
                if let Err((next, failure)) = payload.append(next) {
                    return CoalescedRelayWrite { payload, finish_after, deferred: Some(next.into_command()), failure };
                }
                messages += 1;
            }
            Ok(RelayCommand::Finish) => {
                finish_after = true;
                break;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    CoalescedRelayWrite {
        payload,
        finish_after,
        deferred: None,
        failure: None,
    }
}

/// Drain the currently available local payload from one listener slot.
/// 中文要点：这里只负责把 smoltcp 缓冲区里的数据取出来，不做任何异步外联动作。
pub(super) fn extract_socket_payload(socket: &mut TcpSocket<'_>) -> Option<Vec<u8>> {
    if !socket.can_recv() {
        return None;
    }

    let mut payload = None;
    socket
        .recv(|data| {
            payload = Some(data.to_vec());
            (data.len(), ())
        })
        .unwrap();
    payload
}

pub(super) enum Extraction { Empty, Data(Payload), Pressure, Refused(bytes::MeteredCopyError) }
impl From<Option<Vec<u8>>> for Extraction {
    fn from(value: Option<Vec<u8>>) -> Self { value.map_or(Self::Empty, |v| Self::Data(v.into())) }
}
pub(super) fn extract_for_lane(socket: &mut TcpSocket<'_>, meter: Option<&bytes::BackingMeter>) -> Extraction {
    let Some(meter) = meter else { return extract_socket_payload(socket).into(); };
    if !socket.can_recv() { return Extraction::Empty; }
    let payload = match socket.peek(socket.recv_queue()) {
        Ok(data) => match Payload::copy_native(data, meter) {
            Ok(value) => value,
            Err(bytes::MeteredCopyError::BudgetFull) => return Extraction::Pressure,
            Err(error) => return Extraction::Refused(error),
        },
        Err(_) => return Extraction::Refused(bytes::MeteredCopyError::Quarantined),
    };
    let len = payload.len();
    if socket.recv(|_| (len, ())).is_err() {
        return Extraction::Refused(bytes::MeteredCopyError::Quarantined);
    }
    Extraction::Data(payload)
}

pub(super) enum EstablishedUplink {
    NotEstablished,
    Handled,
    Closed,
    Refused(bytes::MeteredCopyError),
}

pub(super) fn pump_established_uplink<E: Into<Extraction>>(
    ctx: &mut SocketCtx,
    tcp_state: TcpState,
    now_secs: u64,
    mut next_payload: impl FnMut(Option<&bytes::BackingMeter>) -> E,
) -> EstablishedUplink {
    let Some(tx) = ctx.uplink_tx.as_mut() else {
        return EstablishedUplink::NotEstablished;
    };

    for _ in 0..MAX_ESTABLISHED_UPLINK_BATCH {
        match tx.try_reserve() {
            Ok(permit) => {
                match next_payload(tx.native_context().map(|(meter, _)| meter)).into() {
                    Extraction::Data(payload) => { permit.send(payload.into_command()); ctx.state = SocketState::Relaying; }
                    Extraction::Pressure => return EstablishedUplink::Handled,
                    Extraction::Refused(error) => return EstablishedUplink::Refused(error),
                    Extraction::Empty => {
                    if tcp_state == TcpState::CloseWait && !ctx.local_fin_sent {
                        let pending_since =
                            *ctx.local_fin_pending_since_secs.get_or_insert(now_secs);
                        let deadline_base = ctx
                            .local_fin_last_remote_progress_secs
                            .unwrap_or(pending_since)
                            .max(pending_since);
                        if now_secs.saturating_sub(deadline_base) >= LOCAL_FINISH_DEFER_SECS {
                            permit.send(RelayCommand::Finish);
                            ctx.local_fin_sent = true;
                            ctx.local_fin_pending_since_secs = None;
                            ctx.local_fin_last_remote_progress_secs = None;
                        }
                    }
                    return EstablishedUplink::Handled;
                    }
                }
            }
            Err(ReserveError::Capacity) => {
                return EstablishedUplink::Handled;
            }
            Err(ReserveError::Quarantined) => return EstablishedUplink::Refused(bytes::MeteredCopyError::Quarantined),
            Err(ReserveError::Closed) => {
                return EstablishedUplink::Closed;
            }
        }
    }

    EstablishedUplink::Handled
}


/// Actual backing travels through command/current/deferred; no independent
/// byte ticket refunds while a Native allocation is still physically alive.
#[derive(Debug, Clone)]
pub(super) enum Payload { Legacy(Vec<u8>), Native(bytes::Bytes) }
impl From<Vec<u8>> for Payload { fn from(value: Vec<u8>) -> Self { Self::Legacy(value) } }
impl Payload {
    pub(super) fn as_slice(&self) -> &[u8] { self }
    pub(super) fn copy_native(bytes: &[u8], meter: &bytes::BackingMeter) -> Result<Self, bytes::MeteredCopyError> {
        bytes::BytesMut::try_copy_from_slice_metered(bytes, meter).map(|b| Self::Native(b.freeze()))
    }

    pub(super) fn from_command(command: RelayCommand) -> Self {
        match command { RelayCommand::Data(value) => Self::Legacy(value),
            RelayCommand::NativeData(value) => Self::Native(value),
            RelayCommand::Finish => unreachable!("DATA command already matched"), }
    }
    pub(super) fn into_command(self) -> RelayCommand {
        match self { Self::Legacy(value) => RelayCommand::Data(value), Self::Native(value) => RelayCommand::NativeData(value) }
    }
    pub(super) fn capacity(&self) -> usize {
        match self { Self::Legacy(value) => value.capacity(), Self::Native(value) => value.allocation_identity().map_or(0, |id| id.capacity) }
    }
    fn append(&mut self, next: Self) -> Result<(), (Self, Option<bytes::MeteredCopyError>)> {
        match (self, next) {
            (Self::Legacy(value), Self::Legacy(mut next)) => { value.append(&mut next); Ok(()) }
            (Self::Native(value), Self::Native(next)) => {
                let Some(meter) = value.backing_meter().filter(|meter| next.backing_meter().is_some_and(|other| meter.same_authority(&other))) else {
                    return Err((Self::Native(next), Some(bytes::MeteredCopyError::Quarantined)));
                };
                let Some(len) = value.len().checked_add(next.len()) else {
                    return Err((Self::Native(next), Some(bytes::MeteredCopyError::TooLarge)));
                };
                #[cfg(test)]
                if tests::refuse_merge_now() { return Err((Self::Native(next), Some(bytes::MeteredCopyError::Allocation))); }
                match bytes::BytesMut::try_zeroed_metered(len, &meter) {
                    Ok(mut combined) => {
                        combined[..value.len()].copy_from_slice(value);
                        combined[value.len()..].copy_from_slice(&next);
                        *value = combined.freeze();
                        Ok(())
                    }
                    Err(bytes::MeteredCopyError::BudgetFull) => Err((Self::Native(next), None)),
                    Err(error) => Err((Self::Native(next), Some(error))),
                }
            }
            (_, next) => Err((next, Some(bytes::MeteredCopyError::Quarantined))),
        }
    }
}
impl std::ops::Deref for Payload { type Target = [u8]; fn deref(&self) -> &[u8] {
    match self { Self::Legacy(value) => value, Self::Native(value) => value }
} }
impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for Payload {
    fn eq(&self, other: &T) -> bool { &**self == other.as_ref() }
}
impl lane::Value for RelayCommand {
    type Disposition = ();
    fn dispose_context(_: (), _: &lane::Fault<'_>) -> Option<()> { None }
    fn lane_bytes(&self) -> usize { match self { Self::Data(value) => value.len(), Self::NativeData(value) => value.len(), Self::Finish => 0 } }
    fn valid_for(&self, meter: &bytes::BackingMeter) -> bool {
        match self { Self::Data(_) => false, Self::NativeData(value) => value.backing_meter().is_some_and(|m| m.same_authority(meter)), Self::Finish => true }
    }
    fn dispose(self, fault: &lane::Fault<'_>, _: &()) -> Option<Self> { fault.dispose(self); None }
}

impl AsRef<[u8]> for Payload { fn as_ref(&self) -> &[u8] { self } }
#[derive(Debug)]
struct AdmissionFailure(bytes::MeteredCopyError);
impl std::fmt::Display for AdmissionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Native uplink admission: {:?}", self.0) }
}
impl std::error::Error for AdmissionFailure {}
#[cfg(test)]
pub(super) fn admission_cause(source: &std::io::Error) -> Option<bytes::MeteredCopyError> {
    source.get_ref()?.downcast_ref::<AdmissionFailure>().map(|e| e.0)
}
pub(super) fn admission_error(error: bytes::MeteredCopyError) -> std::io::Error {
    let kind = match error {
        bytes::MeteredCopyError::BudgetFull | bytes::MeteredCopyError::TooLarge => std::io::ErrorKind::WouldBlock,
        bytes::MeteredCopyError::Allocation => std::io::ErrorKind::OutOfMemory,
        bytes::MeteredCopyError::Quarantined => std::io::ErrorKind::InvalidData,
    };
    std::io::Error::new(kind, AdmissionFailure(error))
}

#[cfg(test)]
pub(super) mod tests;
