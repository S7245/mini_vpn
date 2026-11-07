//! Endpoint socket migration owns one complete rebind control transaction.
//! It prepares every fanout permit before changing socket/generation/pending stock.
//! Proto packet processing and Connection DATA remain outside this module.
//! Entry: Endpoint::rebind_abstract; tests: construction/previous_socket and original rebind tests.
use super::*;

impl Endpoint {
    /// Switch to a new UDP socket
    ///
    /// Allows the endpoint's address to be updated live, affecting all active connections. Incoming
    /// connections and connections to servers unreachable from the new address will be lost.
    ///
    /// On error, the old UDP socket is retained.
    pub fn rebind_abstract(&self, socket: Arc<dyn AsyncUdpSocket>) -> io::Result<()> {
        // Ticket custody is declared before Root so every early return unlocks
        // Root before cancelling original reservations.
        let mut fanout = fanout::Fanout::new();
        fanout.meter = self.receive_backing_meter.clone();
        fanout.candidate = Some(socket);
        let addr = fanout
            .candidate
            .as_ref()
            .ok_or_else(|| io::Error::other("missing candidate socket"))?
            .local_addr()?;
        let mut inner = self.inner.lock_state();
        let meter = inner.recv_state.backing_meter.clone();
        fanout.meter = meter.clone();
        fanout.driver = inner.driver.take();
        let _panic = fanout::RootPanic(&meter);
        if inner.driver_lost || meter.as_ref().is_some_and(BackingMeter::is_quarantined) {
            return Err(io::Error::other("endpoint stopping"));
        }
        if inner
            .recv_state
            .backing_meter
            .as_ref()
            .and_then(BackingMeter::budget_limit)
            .is_some()
            && fanout
                .candidate
                .as_ref()
                .ok_or_else(|| io::Error::other("missing candidate socket"))?
                .max_receive_segments()
                > inner.recv_state.max_receive_segments
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rebind socket exceeds reserved receive segment layout",
            ));
        }
        if !inner.registration_inventory_valid() {
            if let Some(meter) = &inner.recv_state.backing_meter {
                meter.quarantine();
            }
            return Err(io::Error::other(
                "endpoint registration inventory unavailable",
            ));
        }
        let rebind_generation = inner
            .stats
            .socket_rebinds
            .checked_add(1)
            .and_then(std::num::NonZeroU64::new)
            .ok_or_else(|| io::Error::other("socket rebind generation exhausted"))?;
        // Each live connection must own its Rebind delivery before the socket
        // changes. A full control slot leaves the previous socket untouched.
        let budgeted = inner
            .recv_state
            .backing_meter
            .as_ref()
            .and_then(BackingMeter::budget_limit)
            .is_some();
        if budgeted {
            let candidate = fanout
                .candidate
                .as_ref()
                .ok_or_else(|| io::Error::other("missing candidate socket"))?
                .clone();
            fanout
                .prepare(&inner, ControlKind::Rebind, || {
                    ConnectionEvent::Rebind(candidate.clone())
                })
                .map_err(control_admission)?;
            drop(candidate);
            fanout
                .apply(&mut inner, ControlKind::Rebind, |inner, candidate| {
                    let socket = candidate.expect("validated Rebind candidate custody");
                    let previous = mem::replace(&mut inner.socket, socket);
                    let displaced = inner.begin_socket_rebind(rebind_generation, previous);
                    inner.ipv6 = addr.is_ipv6();
                    inner.stats.socket_rebinds = rebind_generation.get();
                    displaced
                })
                .map_err(control_admission)?;
        } else {
            let socket = fanout
                .candidate
                .take()
                .ok_or_else(|| io::Error::other("missing candidate socket"))?;
            let previous = mem::replace(&mut inner.socket, socket);
            fanout.sockets = inner.begin_socket_rebind(rebind_generation, previous);
            inner.ipv6 = addr.is_ipv6();
            inner.stats.socket_rebinds = rebind_generation.get();
            for sender in inner.recv_state.connections.catalog.values() {
                let _ = sender.send_control(
                    ForwardControlPermit::Legacy,
                    ConnectionEvent::Rebind(inner.socket.clone()),
                );
            }
        }
        drop(inner);
        fanout.finish();

        Ok(())
    }
}

fn control_admission(error: proto::owned_events::ReserveError) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("rebind event admission {error:?}"),
    )
}

/// Native carries pending ownership in the fixed Catalog's same-key Live Entry.
/// Legacy retains the original set, never a second Native authority.
#[derive(Debug)]
pub(super) enum PreviousSocketInventory {
    Native,
    Legacy(PreviousSocketConnections),
}
impl PreviousSocketInventory {
    pub(super) fn new(native: bool) -> Self {
        if native {
            Self::Native
        } else {
            Self::Legacy(PreviousSocketConnections::default())
        }
    }
}
impl State {
    pub(super) fn registration_inventory_valid(&self) -> bool {
        self.recv_state.connections.catalog.matches(&self.inner)
            && match &self.prev_socket_connections {
                PreviousSocketInventory::Native => {
                    self.pending_reverse_event.is_none()
                        && self.recv_state.connections.catalog.is_native()
                        && self
                            .recv_state
                            .connections
                            .catalog
                            .previous_inventory_valid(self.stats.socket_rebinds)
                        && self.prev_socket.is_some()
                            == self.recv_state.connections.catalog.has_previous_pending()
                }
                PreviousSocketInventory::Legacy(_) => {
                    !self.recv_state.connections.catalog.is_native()
                }
            }
    }
    fn retain_previous_socket(&self) -> bool {
        match &self.prev_socket_connections {
            PreviousSocketInventory::Native => {
                self.recv_state.connections.catalog.has_previous_pending()
            }
            PreviousSocketInventory::Legacy(pending) => pending.must_retain_previous_socket(),
        }
    }
    fn begin_socket_rebind(
        &mut self,
        generation: std::num::NonZeroU64,
        previous: Arc<dyn AsyncUdpSocket>,
    ) -> [Option<Arc<dyn AsyncUdpSocket>>; 2] {
        match &mut self.prev_socket_connections {
            PreviousSocketInventory::Native => {
                self.recv_state.connections.catalog.begin_rebind(generation)
            }
            PreviousSocketInventory::Legacy(pending) => {
                *pending = PreviousSocketConnections::at_rebind(
                    generation.get(),
                    self.recv_state.connections.catalog.keys().copied(),
                )
            }
        }
        let displaced = self.prev_socket.take();
        let unused = if self.retain_previous_socket() {
            self.prev_socket = Some(previous);
            None
        } else {
            Some(previous)
        };
        [displaced, unused]
    }
    pub(super) fn note_authenticated_socket(
        &mut self,
        ch: ConnectionHandle,
        key: Option<proto::RegistrationKey>,
        generation: u64,
    ) {
        if generation != self.stats.socket_rebinds {
            return;
        }
        self.stats.current_socket_rx_rebind_generation = self
            .stats
            .current_socket_rx_rebind_generation
            .max(generation);
        match &mut self.prev_socket_connections {
            PreviousSocketInventory::Native => self
                .recv_state
                .connections
                .catalog
                .note_authenticated_socket(ch, key, generation),
            PreviousSocketInventory::Legacy(pending) => {
                pending.note_authenticated_current_socket_packet(ch, generation)
            }
        }
        if !self.retain_previous_socket() {
            self.prev_socket = None;
        }
    }
    pub(super) fn note_socket_connection_drained(&mut self, ch: ConnectionHandle) {
        if let PreviousSocketInventory::Legacy(pending) = &mut self.prev_socket_connections {
            pending.note_connection_drained(ch);
        }
        if !self.retain_previous_socket() {
            self.prev_socket = None;
        }
    }
    pub(super) fn discard_failed_previous_socket(&mut self) {
        match &mut self.prev_socket_connections {
            PreviousSocketInventory::Native => {
                self.recv_state.connections.catalog.clear_previous_pending()
            }
            PreviousSocketInventory::Legacy(pending) => {
                *pending = PreviousSocketConnections::default()
            }
        }
        self.prev_socket = None;
    }
}
