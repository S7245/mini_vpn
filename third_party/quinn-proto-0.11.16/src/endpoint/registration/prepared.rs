//! Native connect preparation, publication and exact disposal belong to RegistrationRoot.
//! Prepared slots consume the same fixed 19-instance inventory but own no DATA route.
//! Quinn may move the real Connection into private State before publication; cancellation
//! requires the actual Connection's last-field disposal witness, not a caller marker.
//! Entry: Endpoint::{prepare_connect,publish_connect,cancel_connect}; tests: endpoint tests.
use super::*;

pub(super) struct ConstructedConnection {
    pub(super) ch: ConnectionHandle,
    pub(super) conn: Connection,
    pub(super) meta: ConnectionMeta,
    pub(super) loc_cid: ConnectionId,
}
impl ConstructedConnection {
    pub(super) fn publish(self, endpoint: &mut Endpoint) -> Result<Connection, TransportError> {
        let Self {
            ch,
            conn,
            meta,
            loc_cid,
        } = self;
        let key = meta.registration_key;
        if !endpoint.registrations.store.insert(ch, meta, loc_cid) {
            if let Some(meter) = &endpoint.config.receive_backing_meter {
                meter.quarantine();
            }
            return Err(TransportError::INTERNAL_ERROR(
                "registration publication invariant",
            ));
        }
        if let Some(key) = key {
            endpoint.registrations.identity.publish(key);
        }
        Ok(conn)
    }
}

/// A linear unpublished Native registration. Drop without publication or proven
/// cancellation quarantines its original authority; it never silently frees a slot.
#[doc(hidden)]
#[derive(Debug)]
pub struct PreparedConnect {
    key: RegistrationKey,
    meta: Option<ConnectionMeta>,
    loc_cid: ConnectionId,
    observer: RegistrationObserver,
    armed: bool,
}
impl PreparedConnect {
    /// Original identity reserved before private host construction.
    pub fn key(&self) -> RegistrationKey {
        self.key
    }
}
impl Drop for PreparedConnect {
    fn drop(&mut self) {
        if self.armed {
            self.observer.quarantine();
        }
    }
}

/// Dropped after all other real Connection fields, never by a Host receipt.
#[derive(Debug)]
pub(crate) struct ConnectionDisposal {
    observer: RegistrationObserver,
    key: RegistrationKey,
    pacing: Option<(EndpointPacingService, EndpointPacingConnectionKey)>,
}
impl Drop for ConnectionDisposal {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.observer.quarantine();
            return;
        }
        if self.observer.is_prepared(self.key) {
            if self
                .pacing
                .as_ref()
                .is_some_and(|(service, key)| !service.rollback_unpublished(*key))
            {
                self.observer.quarantine();
                return;
            }
            self.observer.connection_disposed(self.key);
        }
    }
}

impl Endpoint {
    /// Cold Host composition checks both directions across every fixed slot.
    #[doc(hidden)]
    pub fn registration_catalog_matches(
        &self,
        host: impl Fn(ConnectionHandle, Option<RegistrationKey>, Option<RegistrationKey>) -> bool,
    ) -> bool {
        self.registrations.inventory_valid()
            && (0..crate::BUDGETED_CONNECTION_LIMIT).all(|slot| {
                let ch = ConnectionHandle(slot);
                host(
                    ch,
                    self.registrations
                        .store
                        .get(slot)
                        .and_then(|meta| meta.registration_key),
                    self.registrations.store.prepared_key(ch),
                )
            })
    }
    /// Original convenience operation uses the same Native two-phase path.
    pub fn connect(
        &mut self,
        now: Instant,
        config: ClientConfig,
        remote: SocketAddr,
        server_name: &str,
    ) -> Result<(ConnectionHandle, Connection), ConnectError> {
        if !self.native_registration() {
            let prepared = self.construct_outgoing(now, config, remote, server_name)?;
            let ch = prepared.ch;
            return prepared
                .publish(self)
                .map(|conn| (ch, conn))
                .map_err(ConnectError::CryptoOutput);
        }
        let (mut ticket, conn) = self.prepare_connect(now, config, remote, server_name)?;
        match self.publish_connect(&mut ticket, &conn) {
            Ok(ch) => Ok((ch, conn)),
            Err(error) => {
                drop(conn);
                self.cancel_connect(ticket);
                Err(error)
            }
        }
    }

    /// Reserve Native identity and construct the real protocol owner without routes.
    #[doc(hidden)]
    pub fn prepare_connect(
        &mut self,
        now: Instant,
        config: ClientConfig,
        remote: SocketAddr,
        server_name: &str,
    ) -> Result<(PreparedConnect, Connection), ConnectError> {
        if !self.native_registration() {
            return Err(ConnectError::EventCapacityUnavailable);
        }
        let guard = RegistrationGuard {
            meter: self.config.receive_backing_meter.clone(),
            armed: true,
        };
        let prepared = self.construct_outgoing(now, config, remote, server_name);
        guard.finish();
        let ConstructedConnection {
            ch,
            mut conn,
            meta,
            loc_cid,
        } = prepared?;
        let key = meta
            .registration_key
            .ok_or(ConnectError::EventCapacityUnavailable)?;
        if key.handle() != ch {
            return Err(ConnectError::EventCapacityUnavailable);
        }
        let observer = self
            .registration_observer()
            .ok_or(ConnectError::EventCapacityUnavailable)?;
        if !self.registrations.store.reserve(key, loc_cid) {
            observer.quarantine();
            return Err(ConnectError::EventCapacityUnavailable);
        }
        observer.reserve(key);
        conn.track_prepared_registration(ConnectionDisposal {
            observer: observer.clone(),
            key,
            pacing: self.endpoint_pacing_service.as_ref().map(|service| {
                (
                    service.clone(),
                    EndpointPacingConnectionKey::for_registration(ch, key.generation(), 0),
                )
            }),
        });
        Ok((
            PreparedConnect {
                key,
                meta: Some(meta),
                loc_cid,
                observer,
                armed: true,
            },
            conn,
        ))
    }

    /// Validate every fallible condition first; route/Meta/ledger publication is final.
    #[doc(hidden)]
    pub fn publish_connect(
        &mut self,
        ticket: &mut PreparedConnect,
        connection: &Connection,
    ) -> Result<ConnectionHandle, ConnectError> {
        if !ticket.armed
            || connection.registration_key() != Some(ticket.key)
            || connection.is_closed()
            || connection.retained_error().is_some()
            || !self.registrations.inventory_valid()
            || !self
                .registrations
                .store
                .reserved(ticket.key, ticket.loc_cid)
            || !ticket.observer.can_publish(ticket.key)
            || ticket.meta.is_none()
        {
            return Err(ConnectError::EventCapacityUnavailable);
        }
        // insert_prepared precomputes the table cell before moving Meta; no provider
        // callback, allocation, Pool operation or Host lock occurs after that check.
        if !self
            .registrations
            .store
            .publish_prepared(ticket.key, ticket.loc_cid, &mut ticket.meta)
        {
            ticket.observer.quarantine();
            return Err(ConnectError::EventCapacityUnavailable);
        }
        self.registrations.identity.publish(ticket.key);
        ticket.armed = false;
        Ok(ticket.key.handle())
    }

    /// Cancel only after actual private Connection disposal; otherwise fail closed.
    #[doc(hidden)]
    pub fn cancel_connect(&mut self, mut ticket: PreparedConnect) -> bool {
        if !ticket.armed
            || !self.registrations.inventory_valid()
            || !self
                .registrations
                .store
                .reserved(ticket.key, ticket.loc_cid)
            || !ticket.observer.was_disposed(ticket.key)
        {
            return false;
        }
        drop(ticket.meta.take());
        self.registrations.store.cancel_reserved(ticket.key);
        ticket.observer.cancel(ticket.key);
        ticket.armed = false;
        true
    }
    fn construct_outgoing(
        &mut self,
        now: Instant,
        config: ClientConfig,
        remote: SocketAddr,
        server_name: &str,
    ) -> Result<ConstructedConnection, ConnectError> {
        if !self.registrations.ready() {
            return Err(ConnectError::EventCapacityUnavailable);
        }
        if self.registrations.native() && !self.registrations.inventory_valid() {
            if let Some(meter) = &self.config.receive_backing_meter {
                meter.quarantine();
            }
            return Err(ConnectError::EventCapacityUnavailable);
        }
        if self.registrations.identity.native()
            && self.registrations.store.occupied_len() >= crate::BUDGETED_CONNECTION_LIMIT
        {
            return Err(ConnectError::EventCapacityUnavailable);
        }
        if self.cids_exhausted() {
            return Err(ConnectError::CidsExhausted);
        }
        if remote.port() == 0 || remote.ip().is_unspecified() {
            return Err(ConnectError::InvalidRemoteAddress(remote));
        }
        if !self.config.supported_versions.contains(&config.version) {
            return Err(ConnectError::UnsupportedVersion);
        }

        let mut cold_resources = crate::connection::ColdResources::prepare(
            &self.config,
            self.local_cid_generator.cid_lifetime(),
        )
        .map_err(|_| ConnectError::EventCapacityUnavailable)?;

        let remote_id = (config.initial_dst_cid_provider)();
        trace!(initial_dcid = %remote_id);

        let ch = self
            .registrations
            .next_slot()
            .ok_or(ConnectError::EventCapacityUnavailable)?;
        cold_resources.registration_key = Some(
            self.registrations
                .identity
                .prepare(ch)
                .map_err(ConnectError::CryptoOutput)?,
        );
        // Preparing TLS can reject. Keep this CID unregistered until
        // add_connection publishes its matching Meta and original route.
        let loc_cid = if self.registrations.native() {
            self.native_cid_candidate(&[])
                .ok_or(ConnectError::CidsExhausted)?
        } else {
            self.new_cid_candidate()
        };
        let params = TransportParameters::new(
            &config.transport,
            &self.config,
            self.local_cid_generator.as_ref(),
            loc_cid,
            None,
            &mut self.rng,
        );
        let tls = config
            .crypto
            .start_session(config.version, server_name, &params)?;

        let conn = self
            .add_connection(
                ch,
                config.version,
                remote_id,
                loc_cid,
                remote_id,
                FourTuple {
                    remote,
                    local_ip: None,
                },
                now,
                tls,
                config.transport,
                cold_resources,
                SideArgs::Client {
                    token_store: config.token_store,
                    server_name: server_name.into(),
                },
            )
            .map_err(ConnectError::CryptoOutput)?;
        Ok(conn)
    }
}
