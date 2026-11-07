//! Endpoint registration Authority: Meta, CID/reset routes and terminal disposal.
//! Owns issuance/publication and retirement together; does not own socket I/O,
//! TLS provider state, Connection packet processing or Service/Root settlement.
//! Identity, Meta and routing live behind one private Root. Native owns fixed
//! precharged storage; Legacy retains its original dynamic representation.
//! Entry: Endpoint::{connect,accept,handle_event}, RegistrationRoot::route; tests are
//! endpoint/tests and tests/admission/registration, plus original CID regressions.
use super::*;
mod identity;
mod ledger;
mod prepared;
mod reply;
pub(crate) use prepared::ConnectionDisposal;
pub use prepared::PreparedConnect;
mod storage;
use identity::RegistrationIdentity;
pub use identity::{EndpointEventDisposition, EndpointEventOutcome, RegistrationKey};
pub use ledger::{BoundRegistrationObserver, RegistrationObserver};
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use storage::{LocalCids, Store};

/// One Endpoint composition root owns identity and both sides of route/Meta state.
/// The immutable profile selects one physical representation and settlement ledger.
#[derive(Debug)]
pub(super) struct RegistrationRoot {
    identity: RegistrationIdentity,
    store: Store,
}
impl RegistrationRoot {
    pub(super) fn new(native: bool, meter: Option<&bytes::BackingMeter>, supported: bool) -> Self {
        Self {
            identity: RegistrationIdentity::new(native, meter),
            store: if native && !supported {
                Store::Unavailable
            } else {
                Store::new(native, meter)
            },
        }
    }
    pub(super) fn ready(&self) -> bool {
        self.identity.ready() && self.store.ready()
    }
    pub(super) fn native(&self) -> bool {
        self.identity.native()
    }
    fn inventory_valid(&self) -> bool {
        self.store.inventory_valid()
            && self.identity.inventory_matches(
                |ch| self.store.get(ch.0).and_then(|meta| meta.registration_key),
                |ch| self.store.prepared_key(ch),
            )
    }
    fn next_slot(&self) -> Option<ConnectionHandle> {
        self.store
            .vacant_key(|slot| self.identity.slot_available(slot))
            .map(ConnectionHandle)
    }
    pub(super) const fn native_layout_bytes() -> usize {
        RegistrationObserver::layout_bytes() + Store::native_layout_bytes()
    }
    pub(super) fn prepare_reset_token(
        &mut self,
        key: &dyn crypto::HmacKey,
        id: ConnectionId,
    ) -> Option<ResetToken> {
        self.store.reset_token(key, id)
    }
    pub(super) fn route(
        &self,
        addresses: &FourTuple,
        packet: &PartialDecode,
    ) -> Option<RouteDatagramTo> {
        self.store.route(addresses, packet)
    }
}

// This is the same Root's unwind guard, not another resource/refund authority.
struct RegistrationGuard {
    meter: Option<bytes::BackingMeter>,
    armed: bool,
}
impl RegistrationGuard {
    fn finish(mut self) {
        self.armed = false;
    }
}
impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
    }
}

impl Endpoint {
    /// Process `EndpointEvent`s emitted from related `Connection`s
    ///
    /// In turn, processing this event may return a `ConnectionEvent` for the same `Connection`.
    pub fn handle_event(
        &mut self,
        ch: ConnectionHandle,
        event: EndpointEvent,
    ) -> Option<ConnectionEvent> {
        let mut outcome = self.handle_event_checked(ch, event);
        outcome.finish_settlement();
        outcome.response
    }

    /// Apply a control event only to the exact originating registration.
    #[doc(hidden)]
    pub fn handle_event_checked(
        &mut self,
        ch: ConnectionHandle,
        event: EndpointEvent,
    ) -> EndpointEventOutcome {
        self.handle_registration_event_checked(ch, event, None)
    }
    fn handle_registration_event_checked(
        &mut self,
        ch: ConnectionHandle,
        event: EndpointEvent,
        mut receiver: Option<&mut crate::connection::PreparedReply<'_>>,
    ) -> EndpointEventOutcome {
        if self.registrations.native()
            && (!self.registrations.inventory_valid()
                || self
                    .config
                    .receive_backing_meter
                    .as_ref()
                    .is_some_and(|meter| meter.admit_precharged().is_err()))
        {
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            return EndpointEventOutcome::refused(EndpointEventDisposition::Rejected);
        }
        let Some(meta) = self.registrations.store.get(ch.0) else {
            return EndpointEventOutcome::refused(EndpointEventDisposition::Stale);
        };
        let key = meta.registration_key;
        if event.1.is_some() && event.1 != key {
            return EndpointEventOutcome::refused(EndpointEventDisposition::Stale);
        }
        if event.1.is_none() && self.registrations.identity.native() {
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            return EndpointEventOutcome::refused(EndpointEventDisposition::Rejected);
        }
        // Native ordinary controls must borrow their actual receiver. The old
        // public entry remains available for real terminal and Legacy events.
        if self.registrations.native() && !event.is_drained() && receiver.is_none() {
            // Ownership of the control was consumed. Without its receiver there
            // is no retry proof; never normalize that unknown disposition clean.
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            return EndpointEventOutcome::refused(EndpointEventDisposition::Rejected);
        }
        if receiver.as_ref().is_some_and(|receiver| {
            !receiver.matches_receiver(key, meta.cid_frames.as_ref(), meta.cids_issued)
        }) {
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            if let Some(receiver) = receiver.as_mut() {
                receiver.refuse("CID frame authority mismatch");
            }
            return EndpointEventOutcome::refused(EndpointEventDisposition::Rejected);
        }
        let terminal = event.is_drained();
        let guard = RegistrationGuard {
            meter: self
                .registrations
                .native()
                .then(|| self.config.receive_backing_meter.clone())
                .flatten(),
            armed: true,
        };
        let response = self.apply_registration_event(ch, event.0, receiver);
        guard.finish();
        let settlement = terminal
            .then(|| key.and_then(|key| self.registrations.identity.retire(key)))
            .flatten();
        EndpointEventOutcome {
            disposition: EndpointEventDisposition::Accepted,
            registration: key,
            terminal,
            response,
            settlement,
        }
    }

    /// Inspect identity without consuming a control message or publishing a receipt.
    #[doc(hidden)]
    pub fn registration_matches(&self, ch: ConnectionHandle, key: Option<RegistrationKey>) -> bool {
        self.registrations
            .store
            .get(ch.0)
            .is_some_and(|meta| key.is_some() && key == meta.registration_key)
    }

    /// Read-only proof for a terminal sender that may outlive Endpoint.
    #[doc(hidden)]
    pub fn registration_observer(&self) -> Option<RegistrationObserver> {
        self.registrations.identity.observer()
    }

    /// Already accepted terminal/cancellation for this exact authority and slot generation.
    #[doc(hidden)]
    pub fn registration_was_settled(&self, key: Option<RegistrationKey>) -> bool {
        key.is_some_and(|key| self.registrations.identity.was_settled(key))
    }
    /// Whether the immutable client registration profile requires stamped control.
    #[doc(hidden)]
    pub fn native_registration(&self) -> bool {
        self.registrations.identity.native()
    }

    fn registration_refusal(
        &self,
        ch: ConnectionHandle,
        now: Instant,
        reason: &'static str,
    ) -> ConnectionEvent {
        ConnectionEvent(ConnectionEventInner::RegistrationRefused(
            self.registrations
                .store
                .get(ch.0)
                .and_then(|meta| meta.registration_key),
            now,
            reason,
        ))
    }
    fn apply_registration_event(
        &mut self,
        ch: ConnectionHandle,
        event: EndpointEventInner,
        receiver: Option<&mut crate::connection::PreparedReply<'_>>,
    ) -> Option<ConnectionEvent> {
        use EndpointEventInner::*;
        match event {
            NeedIdentifiers(now, n) => return self.send_identifiers(now, ch, n, None, receiver),
            ResetToken(now, remote, token) => {
                if self
                    .registrations
                    .store
                    .reset_owner(remote, &token)
                    .is_some_and(|owner| *owner != ch)
                {
                    return Some(self.registration_refusal(ch, now, "reset token collision"));
                }
                if !self.registrations.store.replace_reset(ch, remote, token) {
                    return Some(self.registration_refusal(
                        ch,
                        now,
                        "reset route capacity unavailable",
                    ));
                }
            }
            RetireConnectionId(now, seq, allow_more) => {
                let present = self
                    .registrations
                    .store
                    .get(ch.0)
                    .is_some_and(|meta| meta.loc_cids.contains_key(&seq));
                if present && allow_more && self.registrations.native() {
                    return self.send_identifiers(now, ch, 1, Some(seq), receiver);
                }
                let cid = self
                    .registrations
                    .store
                    .get_mut(ch.0)
                    .and_then(|meta| meta.loc_cids.remove(&seq));
                if let Some(cid) = cid {
                    self.registrations.store.retire_cid(cid);
                    if allow_more {
                        return self.send_identifiers(now, ch, 1, None, receiver);
                    }
                }
            }
            Drained => {
                self.registrations.store.remove(ch);
            }
        }
        None
    }

    fn send_legacy_identifiers(
        &mut self,
        now: Instant,
        ch: ConnectionHandle,
        num: u64,
    ) -> ConnectionEvent {
        let Some(meta) = self.registrations.store.get(ch.0) else {
            return self.registration_refusal(ch, now, "registration disappeared");
        };
        let mut ids = match &meta.cid_frames {
            Some(pool) => match pool.prepare(num as usize) {
                Some(ids) => ids,
                None => {
                    return ConnectionEvent(ConnectionEventInner::IdentifierCapacityUnavailable(
                        crate::connection::cid_frames::CapacityFailure(pool.clone()),
                    ))
                }
            },
            None => crate::connection::cid_frames::Prepared::legacy(),
        };
        ids.mark_mutated();
        for _ in 0..num {
            let id = self.new_cid(ch);
            let Some(meta) = self.registrations.store.get_mut(ch.0) else {
                return self.registration_refusal(ch, now, "registration disappeared");
            };
            let sequence = meta.cids_issued;
            meta.cids_issued += 1;
            meta.loc_cids.insert(sequence, id);
            ids.push(IssuedCid {
                sequence,
                id,
                reset_token: ResetToken::new(&*self.config.reset_key, id),
            });
        }
        ConnectionEvent(ConnectionEventInner::NewIdentifiers(ids.finish(), now))
    }
    fn native_cid_candidate(&mut self, prepared: &[IssuedCid]) -> Option<ConnectionId> {
        for _ in 0..32 {
            let cid = self.local_cid_generator.generate_cid();
            if cid.len() != 8 {
                return None;
            }
            if !self.registrations.store.contains_cid(&cid)
                && !prepared.iter().any(|prior| prior.id == cid)
            {
                return Some(cid);
            }
        }
        None
    }

    /// Generate an unused CID without publishing an ownerless route.
    /// This synchronous &mut Endpoint preparation excludes concurrent insert;
    /// the client publishes only after every fallible TLS preparation succeeds.
    fn new_cid_candidate(&mut self) -> ConnectionId {
        loop {
            let cid = self.local_cid_generator.generate_cid();
            if cid.is_empty() {
                debug_assert_eq!(self.local_cid_generator.cid_len(), 0);
                return cid;
            }
            if !self.registrations.store.contains_cid(&cid) {
                return cid;
            }
        }
    }

    /// Register identifiers for the existing connection owner.
    fn new_cid(&mut self, ch: ConnectionHandle) -> ConnectionId {
        let cid = self.new_cid_candidate();
        if !cid.is_empty() {
            let _ = self.registrations.store.insert_cid(cid, ch);
        }
        cid
    }

    fn add_connection(
        &mut self,
        ch: ConnectionHandle,
        version: u32,
        init_cid: ConnectionId,
        loc_cid: ConnectionId,
        rem_cid: ConnectionId,
        addresses: FourTuple,
        now: Instant,
        tls: Box<dyn crypto::Session>,
        transport_config: Arc<TransportConfig>,
        mut cold_resources: crate::connection::ColdResources,
        side_args: SideArgs,
    ) -> Result<prepared::ConstructedConnection, TransportError> {
        let registration_key = match cold_resources.registration_key {
            Some(key) => key,
            None => self.registrations.identity.prepare(ch)?,
        };
        cold_resources.registration_key = Some(registration_key);
        cold_resources.native_registration = self.registrations.native();
        let mut rng_seed = [0; 32];
        self.rng.fill_bytes(&mut rng_seed);
        let side = side_args.side();
        let pref_addr_cid = side_args.pref_addr_cid();
        let cid_frames = cold_resources.cids.clone();
        let mut conn = Connection::new(
            self.config.clone(),
            cold_resources,
            None,
            transport_config,
            init_cid,
            loc_cid,
            rem_cid,
            addresses.remote,
            addresses.local_ip,
            tls,
            self.local_cid_generator.as_ref(),
            now,
            version,
            self.allow_mtud,
            rng_seed,
            side_args,
        )?;

        let endpoint_pacing = self
            .endpoint_pacing_service
            .as_ref()
            .map(|service| {
                ConnectionEndpointPacing::try_attach(
                    service.clone(),
                    EndpointPacingConnectionKey::for_registration(
                        ch,
                        registration_key.generation(),
                        0,
                    ),
                )
                .ok_or_else(|| TransportError::INTERNAL_ERROR("pacing inventory unavailable"))
            })
            .transpose()?;
        conn.install_endpoint_pacing(endpoint_pacing);

        let mut cids_issued = 0;
        let mut loc_cids = LocalCids::new(self.registrations.native());

        loc_cids.insert(cids_issued, loc_cid);
        cids_issued += 1;

        if let Some(cid) = pref_addr_cid {
            debug_assert_eq!(cids_issued, 1, "preferred address cid seq must be 1");
            loc_cids.insert(cids_issued, cid);
            cids_issued += 1;
        }

        Ok(prepared::ConstructedConnection {
            ch,
            conn,
            meta: ConnectionMeta {
                registration_key: Some(registration_key),
                init_cid,
                cids_issued,
                loc_cids,
                cid_frames,
                addresses,
                side,
                reset_token: None,
            },
            loc_cid,
        })
    }
}

/// Part of protocol state incoming datagrams can be routed to
#[derive(Copy, Clone, Debug)]
pub(super) enum RouteDatagramTo {
    Incoming(usize),
    Connection(ConnectionHandle),
}

/// Maps packets to existing connections
#[derive(Default, Debug)]
struct ConnectionIndex {
    /// Identifies connections based on the initial DCID the peer utilized
    ///
    /// Uses a standard `HashMap` to protect against hash collision attacks.
    ///
    /// Used by the server, not the client.
    connection_ids_initial: HashMap<ConnectionId, RouteDatagramTo>,
    /// Identifies connections based on locally created CIDs
    ///
    /// Uses a cheaper hash function since keys are locally created
    connection_ids: FxHashMap<ConnectionId, ConnectionHandle>,
    /// Identifies incoming connections with zero-length CIDs
    ///
    /// Uses a standard `HashMap` to protect against hash collision attacks.
    incoming_connection_remotes: HashMap<FourTuple, ConnectionHandle>,
    /// Identifies outgoing connections with zero-length CIDs
    ///
    /// We don't yet support explicit source addresses for client connections, and zero-length CIDs
    /// require a unique four-tuple, so at most one client connection with zero-length local CIDs
    /// may be established per remote. We must omit the local address from the key because we don't
    /// necessarily know what address we're sending from, and hence receiving at.
    ///
    /// Uses a standard `HashMap` to protect against hash collision attacks.
    outgoing_connection_remotes: HashMap<SocketAddr, ConnectionHandle>,
    /// Reset tokens provided by the peer for the CID each connection is currently sending to
    ///
    /// Incoming stateless resets do not have correct CIDs, so we need this to identify the correct
    /// recipient, if any.
    connection_reset_tokens: ResetTokenTable,
}

impl ConnectionIndex {
    /// Associate an incoming connection with its initial destination CID
    fn insert_initial_incoming(&mut self, dst_cid: ConnectionId, incoming_key: usize) {
        if dst_cid.is_empty() {
            return;
        }
        self.connection_ids_initial
            .insert(dst_cid, RouteDatagramTo::Incoming(incoming_key));
    }

    /// Remove an association with an initial destination CID
    fn remove_initial(&mut self, dst_cid: ConnectionId) {
        if dst_cid.is_empty() {
            return;
        }
        let removed = self.connection_ids_initial.remove(&dst_cid);
        debug_assert!(removed.is_some());
    }

    /// Associate a connection with its initial destination CID
    fn insert_initial(&mut self, dst_cid: ConnectionId, connection: ConnectionHandle) {
        if dst_cid.is_empty() {
            return;
        }
        self.connection_ids_initial
            .insert(dst_cid, RouteDatagramTo::Connection(connection));
    }

    /// Associate a connection with its first locally-chosen destination CID if used, or otherwise
    /// its current 4-tuple
    fn insert_conn(
        &mut self,
        addresses: FourTuple,
        dst_cid: ConnectionId,
        connection: ConnectionHandle,
        side: Side,
    ) {
        match dst_cid.len() {
            0 => match side {
                Side::Server => {
                    self.incoming_connection_remotes
                        .insert(addresses, connection);
                }
                Side::Client => {
                    self.outgoing_connection_remotes
                        .insert(addresses.remote, connection);
                }
            },
            _ => {
                self.connection_ids.insert(dst_cid, connection);
            }
        }
    }

    /// Discard a connection ID
    fn retire(&mut self, dst_cid: ConnectionId) {
        self.connection_ids.remove(&dst_cid);
    }

    /// Remove all references to a connection
    fn remove(&mut self, conn: &ConnectionMeta) {
        if conn.side.is_server() {
            self.remove_initial(conn.init_cid);
        }
        for cid in conn.loc_cids.values() {
            self.connection_ids.remove(cid);
        }
        self.incoming_connection_remotes.remove(&conn.addresses);
        self.outgoing_connection_remotes
            .remove(&conn.addresses.remote);
        if let Some((remote, token)) = conn.reset_token {
            self.connection_reset_tokens.remove(remote, token);
        }
    }

    /// Find the existing connection that `datagram` should be routed to, if any
    fn get(&self, addresses: &FourTuple, datagram: &PartialDecode) -> Option<RouteDatagramTo> {
        if !datagram.dst_cid().is_empty() {
            if let Some(&ch) = self.connection_ids.get(datagram.dst_cid()) {
                return Some(RouteDatagramTo::Connection(ch));
            }
        }
        if datagram.is_initial() || datagram.is_0rtt() {
            if let Some(&ch) = self.connection_ids_initial.get(datagram.dst_cid()) {
                return Some(ch);
            }
        }
        if datagram.dst_cid().is_empty() {
            if let Some(&ch) = self.incoming_connection_remotes.get(addresses) {
                return Some(RouteDatagramTo::Connection(ch));
            }
            if let Some(&ch) = self.outgoing_connection_remotes.get(&addresses.remote) {
                return Some(RouteDatagramTo::Connection(ch));
            }
        }
        let data = datagram.data();
        if data.len() < RESET_TOKEN_SIZE {
            return None;
        }
        self.connection_reset_tokens
            .get(addresses.remote, &data[data.len() - RESET_TOKEN_SIZE..])
            .cloned()
            .map(RouteDatagramTo::Connection)
    }
}

#[derive(Debug)]
struct ConnectionMeta {
    registration_key: Option<RegistrationKey>,
    init_cid: ConnectionId,
    /// Number of local connection IDs that have been issued in NEW_CONNECTION_ID frames.
    cids_issued: u64,
    loc_cids: LocalCids,
    cid_frames: Option<crate::connection::cid_frames::Pool>,
    /// Remote/local addresses the connection began with
    ///
    /// Only needed to support connections with zero-length CIDs, which cannot migrate, so we don't
    /// bother keeping it up to date.
    addresses: FourTuple,
    side: Side,
    /// Reset token provided by the peer for the CID we're currently sending to, and the address
    /// being sent to
    reset_token: Option<(SocketAddr, ResetToken)>,
}

/// Reset Tokens which are associated with peer socket addresses
///
/// The standard `HashMap` is used since both `SocketAddr` and `ResetToken` are
/// peer generated and might be usable for hash collision attacks.
#[derive(Default, Debug)]
struct ResetTokenTable(HashMap<SocketAddr, HashMap<ResetToken, ConnectionHandle>>);

impl ResetTokenTable {
    fn insert(&mut self, remote: SocketAddr, token: ResetToken, ch: ConnectionHandle) -> bool {
        self.0
            .entry(remote)
            .or_default()
            .insert(token, ch)
            .is_some()
    }

    fn remove(&mut self, remote: SocketAddr, token: ResetToken) {
        use std::collections::hash_map::Entry;
        match self.0.entry(remote) {
            Entry::Vacant(_) => {}
            Entry::Occupied(mut e) => {
                e.get_mut().remove(&token);
                if e.get().is_empty() {
                    e.remove_entry();
                }
            }
        }
    }

    fn get(&self, remote: SocketAddr, token: &[u8]) -> Option<&ConnectionHandle> {
        let token = ResetToken::from(<[u8; RESET_TOKEN_SIZE]>::try_from(token).ok()?);
        self.0.get(&remote)?.get(&token)
    }
}

impl Endpoint {
    /// Initiate a connection
    pub(super) fn handle_first_packet(
        &mut self,
        datagram_len: usize,
        event: DatagramConnectionEvent,
        addresses: FourTuple,
        response: &mut ResponseBuffer<'_>,
    ) -> Option<DatagramEvent> {
        let dst_cid = event.first_decode.dst_cid();
        let header = event.first_decode.initial_header().unwrap();

        let Some(server_config) = &self.server_config else {
            debug!("packet for unrecognized connection {}", dst_cid);
            return self
                .stateless_reset(event.now, datagram_len, addresses, *dst_cid, response)
                .map(DatagramEvent::Response);
        };

        let buf = response.legacy()?;

        if datagram_len < MIN_INITIAL_SIZE as usize {
            debug!("ignoring short initial for connection {}", dst_cid);
            return None;
        }

        // Saturation only happens under heavy load, where deriving initial keys per Initial just to
        // reply with CONNECTION_REFUSED would starve packet processing for existing connections.
        if self.cids_exhausted() || self.incoming_buffers.len() >= server_config.max_incoming {
            debug!(
                "ignoring initial for connection {} due to saturation",
                dst_cid
            );
            return None;
        }

        let crypto = match server_config.crypto.initial_keys(header.version, dst_cid) {
            Ok(keys) => keys,
            Err(UnsupportedVersion) => {
                // This probably indicates that the user set supported_versions incorrectly in
                // `EndpointConfig`.
                debug!(
                    "ignoring initial packet version {:#x} unsupported by cryptographic layer",
                    header.version
                );
                return None;
            }
        };

        if let Err(reason) = self.early_validate_first_packet(header) {
            return Some(DatagramEvent::Response(self.initial_close(
                header.version,
                addresses,
                &crypto,
                &header.src_cid,
                reason,
                buf,
            )));
        }

        let packet = match event.first_decode.finish(Some(&*crypto.header.remote)) {
            Ok(packet) => packet,
            Err(e) => {
                trace!("unable to decode initial packet: {}", e);
                return None;
            }
        };

        if !packet.reserved_bits_valid() {
            debug!("dropping connection attempt with invalid reserved bits");
            return None;
        }

        let Header::Initial(header) = packet.header else {
            panic!("non-initial packet in handle_first_packet()");
        };

        let server_config = self.server_config.as_ref().unwrap().clone();

        let token = match IncomingToken::from_header(&header, &server_config, addresses.remote) {
            Ok(token) => token,
            Err(InvalidRetryTokenError) => {
                debug!("rejecting invalid retry token");
                return Some(DatagramEvent::Response(self.initial_close(
                    header.version,
                    addresses,
                    &crypto,
                    &header.src_cid,
                    TransportError::INVALID_TOKEN(""),
                    buf,
                )));
            }
        };

        let incoming_idx = self.incoming_buffers.insert(IncomingBuffer::default());
        self.registrations
            .store
            .initial_incoming(header.dst_cid, incoming_idx);

        Some(DatagramEvent::NewConnection(Incoming {
            received_at: event.now,
            addresses,
            ecn: event.ecn,
            packet: InitialPacket {
                header,
                header_data: packet.header_data,
                payload: packet.payload,
            },
            rest: event.remaining,
            crypto,
            token,
            incoming_idx,
            improper_drop_warner: IncomingImproperDropWarner,
        }))
    }

    /// Attempt to accept this incoming connection (an error may still occur)
    // AcceptError cannot be made smaller without semver breakage
    #[allow(clippy::result_large_err)]
    pub fn accept(
        &mut self,
        mut incoming: Incoming,
        now: Instant,
        buf: &mut Vec<u8>,
        server_config: Option<Arc<ServerConfig>>,
    ) -> Result<(ConnectionHandle, Connection), AcceptError> {
        if self.registrations.native() {
            return Err(AcceptError {
                cause: TransportError::CONNECTION_REFUSED("native client cannot accept").into(),
                response: None,
            });
        }
        let cold_resources = match crate::connection::ColdResources::prepare(
            &self.config,
            self.local_cid_generator.cid_lifetime(),
        ) {
            Ok(inventory) => inventory,
            Err(_) => {
                self.ignore(incoming);
                return Err(AcceptError {
                    cause: ConnectionError::LocallyClosed,
                    response: None,
                });
            }
        };
        let remote_address_validated = incoming.remote_address_validated();
        incoming.improper_drop_warner.dismiss();
        let incoming_buffer = self.incoming_buffers.remove(incoming.incoming_idx);
        self.all_incoming_buffers_total_bytes -= incoming_buffer.total_bytes;

        let packet_number = incoming.packet.header.number.expand(0);
        let InitialHeader {
            src_cid,
            dst_cid,
            version,
            ..
        } = incoming.packet.header;
        let server_config =
            server_config.unwrap_or_else(|| self.server_config.as_ref().unwrap().clone());

        if server_config
            .transport
            .max_idle_timeout
            .is_some_and(|timeout| {
                incoming.received_at + Duration::from_millis(timeout.into()) <= now
            })
        {
            debug!("abandoning accept of stale initial");
            self.registrations.store.remove_initial(dst_cid);
            return Err(AcceptError {
                cause: ConnectionError::TimedOut,
                response: None,
            });
        }

        if self.cids_exhausted() {
            debug!("refusing connection");
            self.registrations.store.remove_initial(dst_cid);
            return Err(AcceptError {
                cause: ConnectionError::CidsExhausted,
                response: Some(self.initial_close(
                    version,
                    incoming.addresses,
                    &incoming.crypto,
                    &src_cid,
                    TransportError::CONNECTION_REFUSED(""),
                    buf,
                )),
            });
        }

        if incoming
            .crypto
            .packet
            .remote
            .decrypt(
                packet_number,
                &incoming.packet.header_data,
                &mut incoming.packet.payload,
            )
            .is_err()
        {
            debug!(packet_number, "failed to authenticate initial packet");
            self.registrations.store.remove_initial(dst_cid);
            return Err(AcceptError {
                cause: TransportError::PROTOCOL_VIOLATION("authentication failed").into(),
                response: None,
            });
        };

        let ch = self.registrations.next_slot().ok_or_else(|| AcceptError {
            cause: ConnectionError::CidsExhausted,
            response: None,
        })?;
        let loc_cid = self.new_cid(ch);
        let mut params = TransportParameters::new(
            &server_config.transport,
            &self.config,
            self.local_cid_generator.as_ref(),
            loc_cid,
            Some(&server_config),
            &mut self.rng,
        );
        params.stateless_reset_token = Some(ResetToken::new(&*self.config.reset_key, loc_cid));
        params.original_dst_cid = Some(incoming.token.orig_dst_cid);
        params.retry_src_cid = incoming.token.retry_src_cid;
        let mut pref_addr_cid = None;
        if server_config.has_preferred_address() {
            let cid = self.new_cid(ch);
            pref_addr_cid = Some(cid);
            params.preferred_address = Some(PreferredAddress {
                address_v4: server_config.preferred_address_v4,
                address_v6: server_config.preferred_address_v6,
                connection_id: cid,
                stateless_reset_token: ResetToken::new(&*self.config.reset_key, cid),
            });
        }

        let tls = server_config.crypto.clone().start_session(version, &params);
        let transport_config = server_config.transport.clone();
        let mut conn = self
            .add_connection(
                ch,
                version,
                dst_cid,
                loc_cid,
                src_cid,
                incoming.addresses,
                incoming.received_at,
                tls,
                transport_config,
                cold_resources,
                SideArgs::Server {
                    server_config,
                    pref_addr_cid,
                    path_validated: remote_address_validated,
                },
            )
            .and_then(|prepared| prepared.publish(self))
            .map_err(|cause| AcceptError {
                cause: cause.into(),
                response: None,
            })?;
        self.registrations.store.insert_initial(dst_cid, ch);

        match conn.handle_first_packet(
            incoming.received_at,
            incoming.addresses.remote,
            incoming.ecn,
            packet_number,
            incoming.packet,
            incoming.rest,
        ) {
            Ok(()) => {
                trace!(id = ch.0, icid = %dst_cid, "new connection");

                if conn.event_inventory_quarantined() {
                    return Err(self.reject_unpublished_inventory(ch, &conn));
                }

                for event in incoming_buffer.datagrams {
                    if conn
                        .handle_event_with_admission(
                            ConnectionEvent(ConnectionEventInner::Datagram(event)),
                            |_| Some(()),
                        )
                        .is_err()
                        || conn.event_inventory_quarantined()
                    {
                        // No Connecting/driver or application owner has been
                        // published. Retire this entire preparation rather than
                        // drop a rejected event and return a successful accept.
                        return Err(self.reject_unpublished_inventory(ch, &conn));
                    }
                }

                Ok((ch, conn))
            }
            Err(e) => {
                debug!("handshake failed: {}", e);
                self.handle_event(ch, EndpointEvent(EndpointEventInner::Drained, None));
                let response = match e {
                    ConnectionError::TransportError(ref e) => Some(self.initial_close(
                        version,
                        incoming.addresses,
                        &incoming.crypto,
                        &src_cid,
                        e.clone(),
                        buf,
                    )),
                    _ => None,
                };
                Err(AcceptError { cause: e, response })
            }
        }
    }

    /// Clean up endpoint data structures associated with an `Incoming`.
    pub(super) fn clean_up_incoming(&mut self, incoming: &Incoming) {
        self.registrations
            .store
            .remove_initial(incoming.packet.header.dst_cid);
        let incoming_buffer = self.incoming_buffers.remove(incoming.incoming_idx);
        self.all_incoming_buffers_total_bytes -= incoming_buffer.total_bytes;
    }

    #[cfg(test)]
    pub(crate) fn routes_local_cid_for_probe(&self, ch: ConnectionHandle, sequence: u64) -> bool {
        self.registrations
            .store
            .get(ch.0)
            .is_some_and(|conn| conn.loc_cids.contains_key(&sequence))
    }

    /// Number of connections that are currently open
    pub fn open_connections(&self) -> usize {
        self.registrations.store.len()
    }

    #[cfg(test)]
    pub(crate) fn known_connections(&self) -> usize {
        self.registrations.store.known_connections()
    }

    #[cfg(test)]
    pub(crate) fn known_cids(&self) -> usize {
        self.registrations.store.cid_count()
    }

    /// Whether we've used up 3/4 of the available CID space
    ///
    /// We leave some space unused so that `new_cid` can be relied upon to finish quickly. We don't
    /// bother to check when CID longer than 4 bytes are used because 2^40 connections is a lot.
    pub(super) fn cids_exhausted(&self) -> bool {
        self.local_cid_generator.cid_len() <= 4
            && self.local_cid_generator.cid_len() != 0
            && (2usize.pow(self.local_cid_generator.cid_len() as u32 * 8)
                - self.registrations.store.cid_count())
                < 2usize.pow(self.local_cid_generator.cid_len() as u32 * 8 - 2)
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
