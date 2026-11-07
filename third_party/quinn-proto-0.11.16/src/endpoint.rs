use std::{
    convert::TryFrom,
    fmt, mem,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use bytes::{BufMut, Bytes, BytesMut};
mod response;
use rand::{
    rngs::{StdRng, SysRng},
    Rng, RngExt, SeedableRng,
};
pub use response::{ResponseBuffer, ResponseScratch};
use slab::Slab;
use thiserror::Error;
use tracing::{debug, trace, warn};

use crate::{
    cid_generator::ConnectionIdGenerator,
    coding::BufMutExt,
    config::{ClientConfig, EndpointConfig, ServerConfig},
    connection::{
        endpoint_pacing::{
            ConnectionEndpointPacing, EndpointPacingConnectionKey, EndpointPacingService,
            EndpointPacingSnapshot, StatelessResponseDenied, StatelessResponseReservation,
        },
        Connection, ConnectionError, SideArgs,
    },
    crypto::{self, Keys, UnsupportedVersion},
    frame,
    packet::{
        FixedLengthConnectionIdParser, Header, InitialHeader, InitialPacket, PacketDecodeError,
        PacketNumber, PartialDecode, ProtectedInitialHeader,
    },
    shared::{
        ConnectionEvent, ConnectionEventInner, ConnectionId, DatagramConnectionEvent, EcnCodepoint,
        EndpointEvent, EndpointEventInner, IssuedCid,
    },
    token::{IncomingToken, InvalidRetryTokenError, Token, TokenPayload},
    transport_parameters::{PreferredAddress, TransportParameters},
    Duration, Instant, ResetToken, Side, Transmit, TransportConfig, TransportError, INITIAL_MTU,
    MAX_CID_SIZE, MIN_INITIAL_SIZE, RESET_TOKEN_SIZE,
};

mod admission;
mod registration;
pub use admission::AdmittedDatagram;
pub(crate) use registration::ConnectionDisposal;
pub use registration::{
    BoundRegistrationObserver, EndpointEventDisposition, EndpointEventOutcome, PreparedConnect,
    RegistrationKey, RegistrationObserver,
};
use registration::{RegistrationRoot, RouteDatagramTo};

/// The main entry point to the library
///
/// This object performs no I/O whatsoever. Instead, it consumes incoming packets and
/// connection-generated events via `handle` and `handle_event`.
pub struct Endpoint {
    rng: StdRng,
    registrations: RegistrationRoot,
    local_cid_generator: Box<dyn ConnectionIdGenerator>,
    config: Arc<EndpointConfig>,
    endpoint_pacing_service: Option<EndpointPacingService>,
    server_config: Option<Arc<ServerConfig>>,
    /// Whether the underlying UDP socket promises not to fragment packets
    allow_mtud: bool,
    /// Time at which a stateless reset was most recently sent
    last_stateless_reset: Option<Instant>,
    /// Buffered Initial and 0-RTT messages for pending incoming connections
    incoming_buffers: Slab<IncomingBuffer>,
    all_incoming_buffers_total_bytes: u64,
}

impl Endpoint {
    /// Create a new endpoint
    ///
    /// `allow_mtud` enables path MTU detection when requested by `Connection` configuration for
    /// better performance. This requires that outgoing packets are never fragmented, which can be
    /// achieved via e.g. the `IPV6_DONTFRAG` socket option.
    ///
    /// If `rng_seed` is provided, it will be used to initialize the endpoint's rng (having priority
    /// over the rng seed configured in [`EndpointConfig`]). Note that the `rng_seed` parameter will
    /// be removed in a future release, so prefer setting it to `None` and configuring rng seeds
    /// using [`EndpointConfig::rng_seed`].
    pub fn new(
        config: Arc<EndpointConfig>,
        server_config: Option<Arc<ServerConfig>>,
        allow_mtud: bool,
        rng_seed: Option<[u8; 32]>,
    ) -> Self {
        let native = server_config.is_none()
            && config
                .receive_backing_meter
                .as_ref()
                .is_some_and(|m| m.budget_limit().is_some());
        let pacing = config.endpoint_pacing_service.map(|pacing| {
            EndpointPacingService::prepare(
                pacing,
                if native {
                    config.receive_backing_meter.as_ref()
                } else {
                    None
                },
            )
        });
        let pacing_ready = pacing.as_ref().is_none_or(Result::is_ok);
        let endpoint_pacing_service = pacing.and_then(Result::ok);
        let local_cid_generator = (config.connection_id_generator_factory.as_ref())();
        let supported = pacing_ready
            && (!native
                || (local_cid_generator.cid_len() == 8
                    && local_cid_generator.cid_lifetime().is_none()
                    && (RESET_TOKEN_SIZE..=64).contains(&config.reset_key.signature_len())));
        Self {
            registrations: RegistrationRoot::new(
                native,
                config.receive_backing_meter.as_ref(),
                supported,
            ),
            rng: match rng_seed.or(config.rng_seed) {
                Some(seed) => StdRng::from_seed(seed),
                None => StdRng::try_from_rng(&mut SysRng)
                    .expect("failed to seed random number generator from system"),
            },
            local_cid_generator,
            config,
            endpoint_pacing_service,
            server_config,
            allow_mtud,
            last_stateless_reset: None,
            incoming_buffers: Slab::new(),
            all_incoming_buffers_total_bytes: 0,
        }
    }

    /// Exact fixed Native registration ledger backing, used by cold budget planning.
    #[doc(hidden)]
    pub const fn native_registration_layout_bytes() -> usize {
        registration::RegistrationRoot::native_layout_bytes()
    }

    /// Exact optional Native pacing state allocation, separate from registration.
    #[doc(hidden)]
    pub const fn native_pacing_layout_bytes() -> usize {
        EndpointPacingService::native_layout_bytes()
    }

    /// Fallible Native cold construction before any driver or registration publication.
    #[doc(hidden)]
    pub fn try_new(
        config: Arc<EndpointConfig>,
        server_config: Option<Arc<ServerConfig>>,
        allow_mtud: bool,
        rng_seed: Option<[u8; 32]>,
    ) -> std::io::Result<Self> {
        let endpoint = Self::new(config, server_config, allow_mtud, rng_seed);
        if !endpoint.registrations.ready() {
            return Err(std::io::Error::other(
                "registration cold inventory unavailable",
            ));
        }
        Ok(endpoint)
    }

    /// Replace the server configuration, affecting new incoming connections only
    pub fn set_server_config(&mut self, server_config: Option<Arc<ServerConfig>>) {
        // Compatibility setter cannot downgrade an existing Native profile.
        let _ = self.try_set_server_config(server_config);
    }

    /// Reject a server transition that would bypass the fixed client profile.
    #[doc(hidden)]
    pub fn try_set_server_config(
        &mut self,
        server_config: Option<Arc<ServerConfig>>,
    ) -> std::io::Result<()> {
        if self.registrations.native() && server_config.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "native client registration cannot become a server",
            ));
        }
        self.server_config = server_config;
        Ok(())
    }

    /// Return endpoint pacing conservation state when the optional service is configured.
    #[doc(hidden)]
    pub fn endpoint_pacing_snapshot(&self) -> Option<EndpointPacingSnapshot> {
        self.endpoint_pacing_service
            .as_ref()
            .map(EndpointPacingService::snapshot)
    }

    /// Reserve immediate byte service for one stateless endpoint response.
    #[doc(hidden)]
    pub fn endpoint_pacing_reserve_stateless_response(
        &self,
        now: Instant,
        response_bytes: u64,
    ) -> Result<Option<StatelessResponseReservation>, StatelessResponseDenied> {
        let Some(service) = self.endpoint_pacing_service.as_ref() else {
            return Ok(None);
        };
        service
            .reserve_stateless_response(now, response_bytes)
            .map(Some)
            .ok_or(StatelessResponseDenied)
    }

    fn stateless_reset(
        &mut self,
        now: Instant,
        inciting_dgram_len: usize,
        addresses: FourTuple,
        dst_cid: ConnectionId,
        response: &mut ResponseBuffer<'_>,
    ) -> Option<Transmit> {
        if self
            .last_stateless_reset
            .is_some_and(|last| last + self.config.min_reset_interval > now)
        {
            debug!("ignoring unexpected packet within minimum stateless reset interval");
            return None;
        }

        /// Minimum amount of padding for the stateless reset to look like a short-header packet
        const MIN_PADDING_LEN: usize = 5;

        // Prevent amplification attacks and reset loops by ensuring we pad to at most 1 byte
        // smaller than the inciting packet.
        let max_padding_len = match inciting_dgram_len.checked_sub(RESET_TOKEN_SIZE) {
            Some(headroom) if headroom > MIN_PADDING_LEN => headroom - 1,
            _ => {
                debug!(
                    "ignoring unexpected {} byte packet: not larger than minimum stateless reset size",
                    inciting_dgram_len
                );
                return None;
            }
        };
        const IDEAL_MIN_PADDING_LEN: usize = MIN_PADDING_LEN + MAX_CID_SIZE;
        let max_output = max_padding_len + RESET_TOKEN_SIZE
            - usize::from(max_padding_len > IDEAL_MIN_PADDING_LEN);
        let buf = response.reset(self, inciting_dgram_len, max_output)?;

        debug!(
            "sending stateless reset for {} to {}",
            dst_cid, addresses.remote
        );
        let native_token = if self.registrations.native() {
            Some(
                self.registrations
                    .prepare_reset_token(&*self.config.reset_key, dst_cid)?,
            )
        } else {
            None
        };
        self.last_stateless_reset = Some(now);
        // Resets with at least this much padding can't possibly be distinguished from real packets
        let padding_len = if max_padding_len <= IDEAL_MIN_PADDING_LEN {
            max_padding_len
        } else {
            self.rng
                .random_range(IDEAL_MIN_PADDING_LEN..max_padding_len)
        };
        if !self.registrations.native() {
            buf.reserve(padding_len + RESET_TOKEN_SIZE);
        }
        buf.resize(padding_len, 0);
        self.rng.fill_bytes(&mut buf[0..padding_len]);
        buf[0] = 0b0100_0000 | (buf[0] >> 2);
        let token = match native_token {
            Some(token) => token,
            None => ResetToken::new(&*self.config.reset_key, dst_cid),
        };
        buf.extend_from_slice(&token);

        debug_assert!(buf.len() < inciting_dgram_len);

        Some(Transmit {
            destination: addresses.remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: addresses.local_ip,
        })
    }

    /// Check if we should refuse a connection attempt regardless of the packet's contents
    fn early_validate_first_packet(
        &mut self,
        header: &ProtectedInitialHeader,
    ) -> Result<(), TransportError> {
        // RFC9000 §7.2 dictates that initial (client-chosen) destination CIDs must be at least 8
        // bytes. If this is a Retry packet, then the length must instead match our usual CID
        // length. If we ever issue non-Retry address validation tokens via `NEW_TOKEN`, then we'll
        // also need to validate CID length for those after decoding the token.
        if header.dst_cid.len() < 8
            && (header.token_pos.is_empty()
                || header.dst_cid.len() != self.local_cid_generator.cid_len())
        {
            debug!(
                "rejecting connection due to invalid DCID length {}",
                header.dst_cid.len()
            );
            return Err(TransportError::PROTOCOL_VIOLATION(
                "invalid destination CID length",
            ));
        }

        Ok(())
    }

    /// Reject this incoming connection attempt
    pub fn refuse(&mut self, incoming: Incoming, buf: &mut Vec<u8>) -> Transmit {
        self.clean_up_incoming(&incoming);
        incoming.improper_drop_warner.dismiss();

        self.initial_close(
            incoming.packet.header.version,
            incoming.addresses,
            &incoming.crypto,
            &incoming.packet.header.src_cid,
            TransportError::CONNECTION_REFUSED(""),
            buf,
        )
    }

    /// Respond with a retry packet, requiring the client to retry with address validation
    ///
    /// Errors if `incoming.may_retry()` is false.
    pub fn retry(&mut self, incoming: Incoming, buf: &mut Vec<u8>) -> Result<Transmit, RetryError> {
        if !incoming.may_retry() {
            return Err(RetryError(Box::new(incoming)));
        }

        self.clean_up_incoming(&incoming);
        incoming.improper_drop_warner.dismiss();

        let server_config = self.server_config.as_ref().unwrap();

        // First Initial
        // The peer will use this as the DCID of its following Initials. Initial DCIDs are
        // looked up separately from Handshake/Data DCIDs, so there is no risk of collision
        // with established connections. In the unlikely event that a collision occurs
        // between two connections in the initial phase, both will fail fast and may be
        // retried by the application layer.
        let loc_cid = self.local_cid_generator.generate_cid();

        let payload = TokenPayload::Retry {
            address: incoming.addresses.remote,
            orig_dst_cid: incoming.packet.header.dst_cid,
            issued: server_config.time_source.now(),
        };
        let token = Token::new(payload, &mut self.rng).encode(&*server_config.token_key);

        let header = Header::Retry {
            src_cid: loc_cid,
            dst_cid: incoming.packet.header.src_cid,
            version: incoming.packet.header.version,
        };

        let encode = header.encode(buf);
        buf.put_slice(&token);
        buf.extend_from_slice(&server_config.crypto.retry_tag(
            incoming.packet.header.version,
            &incoming.packet.header.dst_cid,
            buf,
        ));
        encode.finish(buf, &*incoming.crypto.header.local, None);

        Ok(Transmit {
            destination: incoming.addresses.remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: incoming.addresses.local_ip,
        })
    }

    /// Ignore this incoming connection attempt, not sending any packet in response
    ///
    /// Doing this actively, rather than merely dropping the [`Incoming`], is necessary to prevent
    /// memory leaks due to state within [`Endpoint`] tracking the incoming connection.
    pub fn ignore(&mut self, incoming: Incoming) {
        self.clean_up_incoming(&incoming);
        incoming.improper_drop_warner.dismiss();
    }

    fn reject_unpublished_inventory(
        &mut self,
        ch: ConnectionHandle,
        conn: &Connection,
    ) -> AcceptError {
        let cause = conn.retained_error().unwrap_or_else(|| {
            TransportError::CONNECTION_REFUSED("prepared event inventory unavailable").into()
        });
        self.handle_event(ch, EndpointEvent(EndpointEventInner::Drained, None));
        AcceptError {
            cause,
            response: None,
        }
    }

    fn initial_close(
        &mut self,
        version: u32,
        addresses: FourTuple,
        crypto: &Keys,
        remote_id: &ConnectionId,
        reason: TransportError,
        buf: &mut Vec<u8>,
    ) -> Transmit {
        // We don't need to worry about CID collisions in initial closes because the peer
        // shouldn't respond, and if it does, and the CID collides, we'll just drop the
        // unexpected response.
        let local_id = self.local_cid_generator.generate_cid();
        let number = PacketNumber::U8(0);
        let header = Header::Initial(InitialHeader {
            dst_cid: *remote_id,
            src_cid: local_id,
            number,
            token: Bytes::new(),
            version,
        });

        let partial_encode = header.encode(buf);
        let max_len =
            INITIAL_MTU as usize - partial_encode.header_len - crypto.packet.local.tag_len();
        frame::Close::from(reason).encode(buf, max_len);
        buf.resize(buf.len() + crypto.packet.local.tag_len(), 0);
        partial_encode.finish(buf, &*crypto.header.local, Some((0, &*crypto.packet.local)));
        Transmit {
            destination: addresses.remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: addresses.local_ip,
        }
    }

    /// Access the configuration used by this endpoint
    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }

    /// Counter for the number of bytes currently used
    /// in the buffers for Initial and 0-RTT messages for pending incoming connections
    pub fn incoming_buffer_bytes(&self) -> u64 {
        self.all_incoming_buffers_total_bytes
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Endpoint")
            .field("rng", &self.rng)
            .field("registrations", &self.registrations)
            .field("config", &self.config)
            .field("server_config", &self.server_config)
            // incoming_buffers too large
            .field("incoming_buffers.len", &self.incoming_buffers.len())
            .field(
                "all_incoming_buffers_total_bytes",
                &self.all_incoming_buffers_total_bytes,
            )
            .finish()
    }
}

/// Buffered Initial and 0-RTT messages for a pending incoming connection
#[derive(Default)]
struct IncomingBuffer {
    datagrams: Vec<DatagramConnectionEvent>,
    total_bytes: u64,
}

/// Internal identifier for a `Connection` currently associated with an endpoint
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ConnectionHandle(pub usize);

impl From<ConnectionHandle> for usize {
    fn from(x: ConnectionHandle) -> Self {
        x.0
    }
}

/// Event resulting from processing a single datagram
pub enum DatagramEvent {
    /// The datagram is redirected to its `Connection`
    ConnectionEvent(ConnectionHandle, ConnectionEvent),
    /// The datagram may result in starting a new `Connection`
    NewConnection(Incoming),
    /// Response generated directly by the endpoint
    Response(Transmit),
}

/// An incoming connection for which the server has not yet begun its part of the handshake.
pub struct Incoming {
    received_at: Instant,
    addresses: FourTuple,
    ecn: Option<EcnCodepoint>,
    packet: InitialPacket,
    rest: Option<BytesMut>,
    crypto: Keys,
    token: IncomingToken,
    incoming_idx: usize,
    improper_drop_warner: IncomingImproperDropWarner,
}

impl Incoming {
    /// The local IP address which was used when the peer established the connection
    ///
    /// This has the same behavior as [`Connection::local_ip`].
    pub fn local_ip(&self) -> Option<IpAddr> {
        self.addresses.local_ip
    }

    /// The peer's UDP address
    pub fn remote_address(&self) -> SocketAddr {
        self.addresses.remote
    }

    /// Whether the socket address that is initiating this connection has been validated
    ///
    /// This means that the sender of the initial packet has proved that they can receive traffic
    /// sent to `self.remote_address()`.
    ///
    /// If `self.remote_address_validated()` is false, `self.may_retry()` is guaranteed to be true.
    /// The inverse is not guaranteed.
    pub fn remote_address_validated(&self) -> bool {
        self.token.validated
    }

    /// Whether it is legal to respond with a retry packet
    ///
    /// If `self.remote_address_validated()` is false, `self.may_retry()` is guaranteed to be true.
    /// The inverse is not guaranteed.
    pub fn may_retry(&self) -> bool {
        self.token.retry_src_cid.is_none()
    }

    /// The original destination connection ID sent by the client
    pub fn orig_dst_cid(&self) -> &ConnectionId {
        &self.token.orig_dst_cid
    }
}

impl fmt::Debug for Incoming {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("addresses", &self.addresses)
            .field("ecn", &self.ecn)
            // packet doesn't implement debug
            // rest is too big and not meaningful enough
            .field("token", &self.token)
            .field("incoming_idx", &self.incoming_idx)
            // improper drop warner contains no information
            .finish_non_exhaustive()
    }
}

struct IncomingImproperDropWarner;

impl IncomingImproperDropWarner {
    fn dismiss(self) {
        mem::forget(self);
    }
}

impl Drop for IncomingImproperDropWarner {
    fn drop(&mut self) {
        warn!(
            "quinn_proto::Incoming dropped without passing to Endpoint::accept/refuse/retry/ignore \
               (may cause memory leak and eventual inability to accept new connections)"
        );
    }
}

/// Errors in the parameters being used to create a new connection
///
/// These arise before any I/O has been performed.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConnectError {
    /// Initial TLS output was refused before connection routes were published.
    #[error("initial CRYPTO output refused: {0}")]
    CryptoOutput(TransportError),
    /// The endpoint could not reserve owned event capacity before connection creation.
    #[error("connection event capacity unavailable")]
    EventCapacityUnavailable,
    /// The endpoint can no longer create new connections
    ///
    /// Indicates that a necessary component of the endpoint has been dropped or otherwise disabled.
    #[error("endpoint stopping")]
    EndpointStopping,
    /// The connection could not be created because not enough of the CID space is available
    ///
    /// Try using longer connection IDs
    #[error("CIDs exhausted")]
    CidsExhausted,
    /// The given server name was malformed
    #[error("invalid server name: {0}")]
    InvalidServerName(String),
    /// The remote [`SocketAddr`] supplied was malformed
    ///
    /// Examples include attempting to connect to port 0, or using an inappropriate address family.
    #[error("invalid remote address: {0}")]
    InvalidRemoteAddress(SocketAddr),
    /// No default client configuration was set up
    ///
    /// Use `Endpoint::connect_with` to specify a client configuration.
    #[error("no default client config")]
    NoDefaultClientConfig,
    /// The local endpoint does not support the QUIC version specified in the client configuration
    #[error("unsupported QUIC version")]
    UnsupportedVersion,
}

/// Error type for attempting to accept an [`Incoming`]
#[derive(Debug)]
pub struct AcceptError {
    /// Underlying error describing reason for failure
    pub cause: ConnectionError,
    /// Optional response to transmit back
    pub response: Option<Transmit>,
}

/// Error for attempting to retry an [`Incoming`] which already bears a token from a previous retry
#[derive(Debug, Error)]
#[error("retry() with validated Incoming")]
pub struct RetryError(Box<Incoming>);

impl RetryError {
    /// Get the [`Incoming`]
    pub fn into_incoming(self) -> Incoming {
        *self.0
    }
}

/// Identifies a connection by the combination of remote and local addresses
///
/// Including the local ensures good behavior when the host has multiple IP addresses on the same
/// subnet and zero-length connection IDs are in use.
#[derive(Hash, Eq, PartialEq, Debug, Copy, Clone)]
struct FourTuple {
    remote: SocketAddr,
    // A single socket can only listen on a single port, so no need to store it explicitly
    local_ip: Option<IpAddr>,
}
