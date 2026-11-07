//! Native Endpoint controls and actual receiver installation share this Root.
//! A mutable Connection loan holds event admission/identity and precomputes fixed
//! active state; the Pool splice lock is acquired before publishing any route.
//! Legacy uses the original entry, while Native ordinary controls without the
//! actual receiver are rejected before mutation. Tests: admission/registration_reply.
use super::*;

impl Endpoint {
    /// Apply a stamped control and install its reply through the actual owner.
    /// Terminal settlement is still returned for final Host disposal/ACK.
    #[doc(hidden)]
    pub fn handle_connection_event(
        &mut self,
        ch: ConnectionHandle,
        event: EndpointEvent,
        connection: &mut Connection,
    ) -> EndpointEventOutcome {
        if !self.native_registration() {
            let mut outcome = self.handle_event_checked(ch, event);
            if let Some(reply) = outcome.response.take() {
                connection.handle_event(reply);
            }
            return outcome;
        }
        let Some(now) = event.control_time() else {
            return self.handle_event_checked(ch, event);
        };
        // Classify an absent/old registration before borrowing or closing any
        // receiver. In particular, a delayed old event cannot consume its same
        // physical slot's successor even when a caller loans that successor.
        let Some(meta) = self.registrations.store.get(ch.0) else {
            return EndpointEventOutcome::refused(EndpointEventDisposition::Stale);
        };
        if event.registration_key().is_some() && event.registration_key() != meta.registration_key {
            return EndpointEventOutcome::refused(EndpointEventDisposition::Stale);
        }
        if connection.registration_key() != meta.registration_key {
            if let Some(meter) = &self.config.receive_backing_meter { meter.quarantine(); }
            return EndpointEventOutcome::refused(EndpointEventDisposition::Rejected);
        }
        let Some(mut receiver) = crate::connection::PreparedReply::prepare(
            connection,
            ch,
            event.registration_key(),
            now,
        ) else {
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            return EndpointEventOutcome::refused(EndpointEventDisposition::Rejected);
        };
        let mut outcome = self.handle_registration_event_checked(ch, event, Some(&mut receiver));
        if let Some(reply) = outcome.response.take() {
            match reply.0 {
                ConnectionEventInner::RegistrationRefused(_, _, reason) => receiver.refuse(reason),
                _ => {
                    if let Some(meter) = &self.config.receive_backing_meter {
                        meter.quarantine();
                    }
                    receiver.refuse("unprepared Native endpoint reply");
                    outcome.disposition = EndpointEventDisposition::Rejected;
                }
            }
        }
        if outcome.disposition == EndpointEventDisposition::Rejected {
            receiver.refuse("endpoint registration unavailable");
        }
        receiver.finish();
        outcome
    }
    pub(super) fn send_identifiers(
        &mut self,
        now: Instant,
        ch: ConnectionHandle,
        num: u64,
        retire: Option<u64>,
        receiver: Option<&mut crate::connection::PreparedReply<'_>>,
    ) -> Option<ConnectionEvent> {
        if !self.registrations.native() {
            return Some(self.send_legacy_identifiers(now, ch, num));
        }
        let Some(meta) = self.registrations.store.get(ch.0) else {
            return Some(self.registration_refusal(ch, now, "registration disappeared"));
        };
        let kept = meta.loc_cids.len()
            - usize::from(retire.is_some_and(|seq| meta.loc_cids.contains_key(&seq)));
        if num > crate::LOC_CID_COUNT
            || kept
                .checked_add(num as usize)
                .is_none_or(|len| len > crate::LOC_CID_COUNT as usize)
            || (num > 0
                && meta
                    .cids_issued
                    .checked_add(num - 1)
                    .is_none_or(|seq| seq > u64::from(crate::VarInt::MAX)))
        {
            return Some(self.registration_refusal(
                ch,
                now,
                "identifier capacity or sequence exhausted",
            ));
        }
        let sequence = meta.cids_issued;
        let Some(receiver) = receiver else {
            return Some(self.registration_refusal(ch, now, "Native reply receiver unavailable"));
        };
        let active = match receiver.prepare_identifiers(sequence, num) {
            Ok(active) => active,
            Err(_) => {
                if let Some(meter) = &self.config.receive_backing_meter {
                    meter.quarantine();
                }
                return Some(self.registration_refusal(
                    ch,
                    now,
                    "CID active inventory unavailable",
                ));
            }
        };
        let Some(pool) = meta.cid_frames.clone() else {
            return Some(self.registration_refusal(ch, now, "identifier owner unavailable"));
        };
        let Some(mut frames) = pool.prepare(num as usize) else {
            return Some(self.registration_refusal(
                ch,
                now,
                "identifier frame capacity unavailable",
            ));
        };
        let blank = IssuedCid {
            sequence: 0,
            id: ConnectionId::new(&[]),
            reset_token: ResetToken::from([0; RESET_TOKEN_SIZE]),
        };
        let mut prepared = [blank; crate::LOC_CID_COUNT as usize];
        for index in 0..num as usize {
            let Some(id) = self.native_cid_candidate(&prepared[..index]) else {
                return Some(self.registration_refusal(ch, now, "identifier generation exhausted"));
            };
            let Some(reset_token) = self
                .registrations
                .store
                .reset_token(&*self.config.reset_key, id)
            else {
                return Some(self.registration_refusal(ch, now, "reset token output unsupported"));
            };
            prepared[index] = IssuedCid {
                sequence: sequence + index as u64,
                id,
                reset_token,
            };
        }
        // Provider calls and all Pool locks finish before the Root publication point.
        frames.mark_mutated();
        for cid in &prepared[..num as usize] {
            frames.push(*cid);
        }
        let frames = frames.finish();
        if self
            .config
            .receive_backing_meter
            .as_ref()
            .is_some_and(|meter| meter.admit_precharged().is_err())
        {
            return Some(self.registration_refusal(ch, now, "identifier authority unavailable"));
        }
        if !receiver.publish_identifiers(&pool, frames, active, &prepared[..num as usize], || {
            self.registrations.store.commit_identifiers(
                ch,
                retire,
                sequence,
                &prepared[..num as usize],
            )
        }) {
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            return Some(self.registration_refusal(ch, now, "identifier publication invariant"));
        }
        None
    }
}
