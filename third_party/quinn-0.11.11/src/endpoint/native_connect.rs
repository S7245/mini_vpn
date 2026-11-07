//! Native connect composes Proto and Host reservations at the existing Endpoint Root.
//! Opaque socket/instance construction occurs with neither registration published
//! and without Root lock. Known rejection destroys private owners before returning
//! both reservations. Unknown interruption retains the original sticky authority Q.
//! Entry: Endpoint::connect_native; tests: construction/tests and terminal19.
use super::*;

impl Endpoint {
    pub(super) fn connect_native(
        &self,
        now: Instant,
        config: ClientConfig,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<Connecting, ConnectError> {
        let mut root = self
            .inner
            .state
            .lock()
            .map_err(|_| ConnectError::EndpointStopping)?;
        if !root.registration_inventory_valid() {
            if let Some(meter) = &root.recv_state.backing_meter {
                meter.quarantine();
            }
            return Err(ConnectError::EventCapacityUnavailable);
        }
        if root.driver_lost || root.recv_state.connections.close.is_some() {
            return Err(ConnectError::EndpointStopping);
        }
        if addr.is_ipv6() && !root.ipv6 {
            return Err(ConnectError::InvalidRemoteAddress(addr));
        }
        let addr = if root.ipv6 {
            SocketAddr::V6(ensure_ipv6(addr))
        } else {
            addr
        };
        if root.recv_state.connections.catalog.occupied_len() >= BUDGETED_CONNECTION_LIMIT {
            return Err(ConnectError::EventCapacityUnavailable);
        }
        let forward = budgeted_events::connection_channels(
            root.recv_state.backing_meter.as_ref(),
            BUDGETED_FORWARD_ORDINARY_CAPACITY,
        )
        .map_err(|_| ConnectError::EventCapacityUnavailable)?;
        let (mut ticket, conn) = root.inner.prepare_connect(now, config, addr, server_name)?;
        let key = ticket.key();
        let ch = key.handle();
        if !root.recv_state.connections.catalog.reserve(key) {
            drop(conn);
            root.inner.cancel_connect(ticket);
            return Err(ConnectError::EventCapacityUnavailable);
        }
        let (terminal_send, terminal_recv) = match forward.terminal {
            Some(pair) => pair,
            None => {
                drop(conn);
                root.inner.cancel_connect(ticket);
                root.recv_state.connections.catalog.cancel(key);
                return Err(ConnectError::EventCapacityUnavailable);
            }
        };
        let entry = catalog::Entry {
            handle: ch,
            key: Some(key),
            sender: forward.forward.0,
            terminal: Some(terminal_recv),
            previous_socket: None,
        };
        let mut events = budgeted_events::EndpointEventSender::new(
            root.recv_state.connections.sender.clone(),
            Some(self.inner.self_weak.clone()),
            conn.receive_backing_meter(),
            Some(terminal_send),
        );
        events.prepare_registration();
        events.ready = Some((root.recv_state.connections.terminal_ready.clone(), ch.0));
        let socket = root.socket.clone();
        let observer = root
            .inner
            .registration_observer()
            .ok_or(ConnectError::EventCapacityUnavailable)?;
        drop(root);

        // Includes poller, all oneshots, actual State/Arc, driver future and first
        // private mutex acquisition. A panic drops the original armed Proto ticket.
        let (mut connecting, start) = Connecting::new_prepared(
            ch,
            conn,
            events,
            forward.forward.1,
            socket.clone(),
            self.runtime.clone(),
        );
        let mut entry = Some(entry);
        let result = connecting.publish_prepared(|conn| {
            let mut root = self
                .inner
                .state
                .lock()
                .map_err(|_| ConnectError::EndpointStopping)?;
            if !root.registration_inventory_valid() {
                if let Some(meter) = &root.recv_state.backing_meter {
                    meter.quarantine();
                }
                return Err(ConnectError::EventCapacityUnavailable);
            }
            if root.driver_lost || root.recv_state.connections.close.is_some() {
                return Err(ConnectError::EndpointStopping);
            }
            if !Arc::ptr_eq(&root.socket, &socket)
                || !entry
                    .as_ref()
                    .is_some_and(|entry| root.recv_state.connections.catalog.can_publish(entry))
            {
                return Err(ConnectError::EventCapacityUnavailable);
            }
            root.inner.publish_connect(&mut ticket, conn)?;
            if let Some(entry) = entry.take() {
                root.recv_state.connections.catalog.publish(entry);
            }
            root.stats.outgoing_handshakes = root.stats.outgoing_handshakes.saturating_add(1);
            Ok(observer.bind(key))
        });
        if let Err(error) = result {
            drop(start);
            drop(connecting);
            drop(entry);
            let mut root = self
                .inner
                .state
                .lock()
                .map_err(|_| ConnectError::EndpointStopping)?;
            if root.inner.cancel_connect(ticket) && !root.recv_state.connections.catalog.cancel(key)
            {
                if let Some(meter) = &root.recv_state.backing_meter {
                    meter.quarantine();
                }
            }
            let idle = root.recv_state.connections.is_empty();
            drop(root);
            if idle {
                self.inner.shared.idle.notify_waiters();
            }
            return Err(error);
        }
        start.publish();
        start.start();
        Ok(connecting)
    }
}
