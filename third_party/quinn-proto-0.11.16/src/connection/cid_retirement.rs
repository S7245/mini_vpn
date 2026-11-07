//! Owns one remote-CID change and its complete retirement obligation. The
//! fixed five-slot candidate is private until every same-Pool control record
//! is held. No new allocator/refund authority; writer, ACK/loss and final Drop
//! retain those original linear nodes. Original EventInventory guards cover
//! commit interruption; cancellation before commit returns only known stock.
//! Does not own local issued-CID routes or DATA. Entry: prepare/commit_remote_cid
//! from authenticated NEW_CONNECTION_ID, migration and clocked local Rebind.
//! Tests: crate::tests::admission::retire and control_inventory::records::tests.
use super::{control_inventory::records::Queue, Connection, SpaceId};
use crate::{
    cid_queue::{CidQueue, InsertError},
    frame, Instant, ResetToken, TransportError,
};

pub(super) mod packet;

pub(super) struct PreparedCid {
    candidate: CidQueue,
    retired: Queue<u64>,
    token: Option<ResetToken>,
}

impl Connection {
    pub(super) fn prepare_remote_cid(
        &self,
        frame: Option<frame::NewConnectionId>,
    ) -> Result<PreparedCid, TransportError> {
        let pending = &self.spaces[SpaceId::Data].pending.retire_cids;
        let mut candidate = self.rem_cids.clone();
        let mut retired = pending.empty_like();
        let (range, token) = transition(&mut candidate, frame, pending.len(), self.side.is_server())?;
        if !pending.authority_ready() {
            return Err(TransportError::INTERNAL_ERROR("retirement authority lost"));
        }
        for sequence in range {
            let record = retired.prepare(sequence).map_err(|_| {
                TransportError::INTERNAL_ERROR("retirement record capacity exhausted")
            })?;
            retired.push(record);
        }
        Ok(PreparedCid {
            candidate,
            retired,
            token,
        })
    }

    pub(super) fn commit_remote_cid(&mut self, prepared: PreparedCid, now: Instant) {
        self.rem_cids = prepared.candidate;
        self.spaces[SpaceId::Data]
            .pending
            .retire_cids
            .append(prepared.retired);
        if let Some(token) = prepared.token {
            self.set_reset_token(token, now);
        }
    }

    /// Handle an active local migration using the caller's actual protocol clock.
    pub fn local_address_changed(&mut self, now: Instant) {
        let _ = self.try_local_address_changed(now);
    }

    #[doc(hidden)]
    pub fn try_local_address_changed(&mut self, now: Instant) -> bool {
        if self.state.is_closed() {
            return false;
        }
        let prepared = match self.prepare_remote_cid(None) {
            Ok(prepared) => prepared,
            Err(_) => {
                self.fail_control_inventory(now, "local CID retirement admission failed");
                return false;
            }
        };
        if !self.endpoint_events.begin_control(1) {
            self.fail_control_inventory(now, "local CID event admission failed");
            return false;
        }
        let guard = self.endpoint_events.guard();
        self.commit_remote_cid(prepared, now);
        self.ping();
        self.endpoint_events.finish();
        guard.finish();
        true
    }
}

// Original fixed-window CID protocol transition, shared by per-frame and
// packet preparation. Only the unpublished value candidate is changed.
fn transition(candidate: &mut CidQueue, frame: Option<frame::NewConnectionId>,
    pending_len: usize, server: bool) -> Result<(std::ops::Range<u64>, Option<ResetToken>), TransportError> {
        let mut token = None;
        let range = if let Some(frame) = frame {
            match candidate.insert(frame) {
                Ok(Some((range, reset_token))) => {
                    // Preserve the original protocol rejection before capacity.
                    const MAX_PENDING: u64 = CidQueue::LEN as u64 * 10;
                    if (pending_len as u64).saturating_add(range.end - range.start) > MAX_PENDING
                    {
                        return Err(TransportError::CONNECTION_ID_LIMIT_ERROR(
                            "queued too many retired CIDs",
                        ));
                    }
                    token = Some(reset_token);
                    range
                }
                Ok(None) if server && candidate.active_seq() == 0 => {
                    if let Some((reset_token, range)) = candidate.next() {
                        token = Some(reset_token);
                        range
                    } else {
                        0..0
                    }
                }
                Ok(None) => 0..0,
                Err(InsertError::ExceedsLimit) => {
                    return Err(TransportError::CONNECTION_ID_LIMIT_ERROR(""))
                }
                Err(InsertError::Retired) => {
                    // Reordered retired IDs still owe a frame; original branch
                    // has no MAX_PENDING protocol limit. Hard capacity applies.
                    frame.sequence..frame.sequence + 1
                }
            }
        } else if let Some((reset_token, range)) = candidate.next() {
            token = Some(reset_token);
            range
        } else {
            0..0
        };
        Ok((range, token))
}
