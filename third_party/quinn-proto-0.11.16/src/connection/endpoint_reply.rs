//! One actual Connection is exclusively borrowed through Native Root reply settlement.
//! Owns event admission and receiver identity/active-state preparation; Root owns
//! routes/provider work and calls the held Pool splice before its final publication.
//! No prepared reply or Frames escape to a facade channel. Legacy installation
//! remains available separately. Entry: PreparedReply; tests: admission/registration_reply.
use super::*;

pub(crate) struct PreparedReply<'a> {
    connection: &'a mut Connection,
    guard: Option<event_inventory::OperationGuard>,
    now: Instant,
}
impl<'a> PreparedReply<'a> {
    pub(crate) fn prepare(
        connection: &'a mut Connection,
        ch: crate::ConnectionHandle,
        key: Option<crate::RegistrationKey>,
        now: Instant,
    ) -> Option<Self> {
        if key.is_none()
            || key != connection.registration_key
            || key.is_some_and(|key| key.handle() != ch)
            || !connection.endpoint_events.begin(None)
        {
            connection.endpoint_events.quarantine();
            connection.fail_control_inventory(now, "endpoint reply admission unavailable");
            return None;
        }
        let guard = connection.endpoint_events.guard();
        Some(Self {
            connection,
            guard: Some(guard),
            now,
        })
    }
    pub(crate) fn matches_receiver(
        &self,
        key: Option<crate::RegistrationKey>,
        pool: Option<&cid_frames::Pool>,
        issued: u64,
    ) -> bool {
        key == self.connection.registration_key
            && self
                .connection
                .local_cid_state
                .prepare_native(issued, 0)
                .is_ok()
            && pool
                .zip(
                    self.connection.spaces[SpaceId::Data]
                        .pending_cids
                        .cold_pool(),
                )
                .is_some_and(|(root, target)| root.same_owner(target))
    }
    pub(crate) fn prepare_identifiers(
        &self,
        sequence: u64,
        count: u64,
    ) -> Result<cid_state::PreparedNative, TransportError> {
        self.connection
            .local_cid_state
            .prepare_native(sequence, count)
    }
    pub(crate) fn refuse(&mut self, reason: &'static str) {
        self.connection.fail_control_inventory(self.now, reason);
    }
    pub(crate) fn publish_identifiers(
        &mut self,
        pool: &cid_frames::Pool,
        mut frames: cid_frames::Frames,
        active: cid_state::PreparedNative,
        expected: &[crate::shared::IssuedCid],
        publish: impl FnOnce() -> bool,
    ) -> bool {
        // Lock ordering remains Connection -> Endpoint -> Pool. All fallible
        // acquisition and physical validation finish before publish() changes Root.
        let Some(splice) = pool.prepare_reply_splice(
            &mut self.connection.spaces[SpaceId::Data].pending_cids,
            &mut frames,
            expected,
        ) else {
            return false;
        };
        if !publish() {
            return false;
        }
        splice.commit();
        self.connection.local_cid_state.install_native(active);
        // Native has no CID lifetime; the original PushNewCid reset was a no-op.
        true
    }
    pub(crate) fn finish(mut self) {
        self.connection.endpoint_events.finish();
        if let Some(guard) = self.guard.take() {
            guard.finish();
        }
    }
}
impl Connection {
    pub(super) fn install_unprepared_identifiers(
        &mut self,
        mut ids: cid_frames::Frames,
        now: Instant,
    ) {
        if !self.spaces[SpaceId::Data]
            .pending_cids
            .validate_append(&ids)
        {
            if self.error.is_none() {
                self.kill(TransportError::INTERNAL_ERROR("CID frame authority mismatch").into());
            }
            return;
        }
        if self.local_cid_state.new_cids(ids.iter(), now).is_err() {
            self.endpoint_events.quarantine();
            self.fail_control_inventory(now, "CID active inventory unavailable");
            return;
        }
        ids.reverse();
        self.spaces[SpaceId::Data].pending_cids.append(&mut ids);
        // Update Timer::PushNewCid
        if self.timers.get(Timer::PushNewCid).is_none_or(|x| x <= now) {
            self.reset_cid_retirement();
        }
    }
}
