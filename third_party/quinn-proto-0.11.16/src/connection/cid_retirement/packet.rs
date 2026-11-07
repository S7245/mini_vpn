//! Wire-ordered remote CID candidates and actual original-pool retirement nodes.
//! Preparation changes neither the Connection CID queue nor its Endpoint token.
//! The shared transition owns protocol semantics; no second CID authority.
//! Tests: tests/admission/packet_pressure/remaining.rs and admission/retire.rs.
use super::*;
mod replay;
use crate::connection::storage::stock::Stock;
use bytes::{BackingMeter, Bytes, MeteredCopyError};
struct Step {
    before: CidQueue,
    pending: usize,
    prepared: PreparedCid,
}
pub(in crate::connection) struct PacketCids {
    steps: Stock<Step>,
    cursor: usize,
}
pub(in crate::connection) enum PrepareError {
    Protocol(TransportError),
    Capacity(MeteredCopyError),
}
struct Candidate {
    before: CidQueue,
    after: CidQueue,
    pending: usize,
    range: std::ops::Range<u64>,
    token: Option<ResetToken>,
}
impl Connection {
    pub(in crate::connection) fn check_packet_cids<'a>(
        &mut self,
        now: Instant,
        payload: &Bytes,
        early: impl Iterator<Item = (SpaceId, u64, &'a Bytes)>,
    ) -> Result<(), TransportError> {
        self.visit_packet_cids(now, payload, early, |_| Ok(()))
            .map_err(|e| match e {
                PrepareError::Protocol(e) => e,
                PrepareError::Capacity(_) => TransportError::INTERNAL_ERROR("CID check allocated"),
            })
    }
    fn visit_packet_cids<'a>(
        &mut self,
        now: Instant,
        payload: &Bytes,
        early: impl Iterator<Item = (SpaceId, u64, &'a Bytes)>,
        mut visit: impl FnMut(Candidate) -> Result<(), PrepareError>,
    ) -> Result<(), PrepareError> {
        if !frame::Iter::new(payload.clone())
            .map_err(PrepareError::Protocol)?
            .any(|f| matches!(f, Ok(frame::Frame::NewConnectionId(_))))
        {
            return Ok(());
        }
        let mut candidate = self.rem_cids.clone();
        let mut pending = self.spaces[SpaceId::Data].pending.retire_cids.len();
        let server = self.side.is_server();
        let mut replay = replay::Replay::new(self, now).map_err(PrepareError::Protocol)?;
        for (space, number, payload) in early {
            replay
                .early(space, number, payload)
                .map_err(PrepareError::Protocol)?;
        }
        for frame in frame::Iter::new(payload.clone()).map_err(PrepareError::Protocol)? {
            match frame.map_err(|e| PrepareError::Protocol(e.into()))? {
                frame::Frame::Ack(ack) => {
                    pending = pending
                        .checked_add(
                            replay
                                .ack(SpaceId::Data, ack)
                                .map_err(PrepareError::Protocol)?,
                        )
                        .ok_or_else(|| {
                            PrepareError::Protocol(TransportError::INTERNAL_ERROR(
                                "CID pending overflow",
                            ))
                        })?;
                }
                frame::Frame::NewConnectionId(frame) => {
                    let before = candidate.clone();
                    let (range, token) = checked_transition(&mut candidate, frame, pending, server)
                        .map_err(PrepareError::Protocol)?;
                    let added = (range.end - range.start) as usize;
                    visit(Candidate {
                        before,
                        after: candidate.clone(),
                        pending,
                        range,
                        token,
                    })?;
                    pending = pending.checked_add(added).ok_or_else(|| {
                        PrepareError::Protocol(TransportError::INTERNAL_ERROR(
                            "CID pending overflow",
                        ))
                    })?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub(in crate::connection) fn prepare_packet_cids<'a>(
        &mut self,
        now: Instant,
        payload: &Bytes,
        meter: &BackingMeter,
        early: impl Iterator<Item = (SpaceId, u64, &'a Bytes)>,
    ) -> Result<PacketCids, PrepareError> {
        let mut result = PacketCids {
            steps: Stock::default(),
            cursor: 0,
        };
        let pending = &self.spaces[SpaceId::Data].pending.retire_cids;
        if !pending.authority_ready() {
            return Err(PrepareError::Protocol(TransportError::INTERNAL_ERROR(
                "retirement authority lost",
            )));
        }
        let pool = pending.empty_like();
        self.visit_packet_cids(now, payload, early, |step| {
            let mut retired = pool.empty_like();
            for sequence in step.range {
                let entry = retired.prepare(sequence).map_err(|_| {
                    PrepareError::Protocol(TransportError::INTERNAL_ERROR(
                        "retirement record capacity exhausted",
                    ))
                })?;
                retired.push(entry);
            }
            result
                .steps
                .push(
                    Step {
                        before: step.before,
                        pending: step.pending,
                        prepared: PreparedCid {
                            candidate: step.after,
                            retired,
                            token: step.token,
                        },
                    },
                    meter,
                )
                .map_err(PrepareError::Capacity)
        })?;
        Ok(result)
    }
}

impl PacketCids {
    pub(in crate::connection) fn commit_next(
        &mut self,
        conn: &mut Connection,
        now: Instant,
    ) -> Result<(), TransportError> {
        let step = self
            .steps
            .slice_mut()
            .get_mut(self.cursor)
            .and_then(Option::take)
            .ok_or_else(|| TransportError::INTERNAL_ERROR("missing CID packet preparation"))?;
        if step.before != conn.rem_cids {
            return Err(TransportError::INTERNAL_ERROR(
                "CID packet authority changed",
            ));
        }
        // Exact wire-order loss replay qualified this count before publication.
        // A mismatch is lost transaction custody, never a new protocol decision.
        if conn.spaces[SpaceId::Data].pending.retire_cids.len() != step.pending {
            return Err(TransportError::INTERNAL_ERROR(
                "CID projected pending changed",
            ));
        }
        conn.commit_remote_cid(step.prepared, now);
        self.cursor += 1;
        Ok(())
    }
}
fn checked_transition(
    candidate: &mut CidQueue,
    frame: frame::NewConnectionId,
    pending: usize,
    server: bool,
) -> Result<(std::ops::Range<u64>, Option<ResetToken>), TransportError> {
    if candidate.active().is_empty() {
        return Err(TransportError::PROTOCOL_VIOLATION(
            "NEW_CONNECTION_ID when CIDs aren't in use",
        ));
    }
    if frame.retire_prior_to > frame.sequence {
        return Err(TransportError::PROTOCOL_VIOLATION(
            "NEW_CONNECTION_ID retiring unissued CIDs",
        ));
    }
    transition(candidate, Some(frame), pending, server)
}
