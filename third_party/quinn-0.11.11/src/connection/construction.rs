//! Cold Connection instance and driver-future construction. Owns prepublication
//! preparation and start disposition; protocol DATA processing stays in State.
//! Entry: Connecting::{new,new_prepared,publish_prepared}, ConnectionStart;
//! tests: endpoint/construction and the original retirement harness.
use super::*;

struct Signals {
    handshake_data: oneshot::Sender<()>,
    connected: oneshot::Sender<bool>,
    endpoint_released: oneshot::Sender<()>,
    published: bool,
}

struct DriverRelease {
    published: Arc<AtomicBool>,
    meter: Option<bytes::BackingMeter>,
    completed: bool,
}
impl DriverRelease {
    fn complete(mut self) -> bool {
        if self.meter.as_ref().is_some_and(|meter| meter.snapshot().quarantined) {
            return false;
        }
        self.completed = true;
        true
    }
}
impl Drop for DriverRelease {
    fn drop(&mut self) {
        if !self.completed && self.published.load(Ordering::Acquire) {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
    }
}

pub(crate) struct ConnectionStart {
    runtime: Arc<dyn Runtime>,
    driver: Pin<Box<dyn Future<Output = ()> + Send>>,
    published: Arc<AtomicBool>,
    meter: Option<bytes::BackingMeter>,
}
impl ConnectionStart {
    pub(crate) fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }
    pub(crate) fn start(self) {
        let guard = DriverRelease {
            published: self.published.clone(),
            meter: self.meter.clone(),
            completed: false,
        };
        self.runtime.spawn(self.driver);
        // The runtime accepted ownership. A future dropped before its first poll
        // still owns its separate release guard; spawn panic keeps this one armed.
        guard.complete();
    }
}

impl Connecting {
    pub(crate) fn new(
        handle: ConnectionHandle,
        conn: proto::Connection,
        events: EndpointEventSender,
        recv: ForwardReceiver,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
    ) -> (Self, ConnectionStart) {
        Self::new_inner(handle, conn, events, recv, socket, runtime, true)
    }
    pub(crate) fn new_prepared(
        handle: ConnectionHandle,
        conn: proto::Connection,
        events: EndpointEventSender,
        recv: ForwardReceiver,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
    ) -> (Self, ConnectionStart) {
        Self::new_inner(handle, conn, events, recv, socket, runtime, false)
    }
    fn new_inner(
        handle: ConnectionHandle,
        conn: proto::Connection,
        endpoint_events: EndpointEventSender,
        conn_events: ForwardReceiver,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
        published: bool,
    ) -> (Self, ConnectionStart) {
        let meter = conn.receive_backing_meter();
        let disposition = Arc::new(AtomicBool::new(published));
        let release = DriverRelease {
            published: disposition.clone(),
            meter: meter.clone(),
            completed: false,
        };
        let (on_handshake_data_send, on_handshake_data_recv) = oneshot::channel();
        let (on_connected_send, on_connected_recv) = oneshot::channel();
        let (endpoint_released_send, endpoint_released_recv) = oneshot::channel();
        let (driver_released_send, driver_released_recv) = oneshot::channel();
        let conn = ConnectionRef::new(
            handle,
            conn,
            endpoint_events,
            conn_events,
            Signals {
                handshake_data: on_handshake_data_send,
                connected: on_connected_send,
                endpoint_released: endpoint_released_send,
                published,
            },
            socket,
            runtime.clone(),
        );
        if !published {
            // Materialize the actual platform mutex and optional lock-tracking
            // first-drop storage while State is still private and Root is unlocked.
            drop(conn.state.lock("prepare private connection"));
        }
        let driver = ConnectionDriver(conn.clone());
        let driver = Box::pin(
            drive_connection_and_report_release(driver, move || {
                if release.complete() {
                    let _ = driver_released_send.send(());
                }
            })
            .instrument(Span::current()),
        );
        (
            Self {
                conn: Some(conn),
                endpoint_released: Some(endpoint_released_recv),
                driver_released: Some(driver_released_recv),
                connected: on_connected_recv,
                handshake_data_ready: Some(on_handshake_data_recv),
            },
            ConnectionStart {
                runtime,
                driver,
                published: disposition,
                meter,
            },
        )
    }
    /// Lock only the still-private State before entering Endpoint Root. There is
    /// no Endpoint -> public Connection lock edge, including cancellation.
    pub(crate) fn publish_prepared(
        &mut self,
        publish: impl FnOnce(
            &proto::Connection,
        ) -> Result<proto::BoundRegistrationObserver, proto::ConnectError>,
    ) -> Result<(), proto::ConnectError> {
        let conn = self
            .conn
            .as_ref()
            .ok_or(proto::ConnectError::EventCapacityUnavailable)?;
        let mut state = conn.state.lock("publish private connection");
        if state.published {
            return Err(proto::ConnectError::EventCapacityUnavailable);
        }
        let observer = publish(&state.inner)?;
        state.endpoint_events.bind_registration(observer);
        state.published = true;
        Ok(())
    }
}
impl ConnectionRef {
    fn new(
        handle: ConnectionHandle,
        conn: proto::Connection,
        endpoint_events: EndpointEventSender,
        conn_events: ForwardReceiver,
        signals: Signals,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
    ) -> Self {
        Self(Arc::new(ConnectionInner {
            state: StateLock::new(State {
                poisoned: false,
                published: signals.published,
                inner: conn,
                driver: None,
                handle,
                on_handshake_data: Some(signals.handshake_data),
                on_connected: Some(signals.connected),
                successor_service_turn: None,
                connected: false,
                timer: None,
                timer_deadline: None,
                conn_events,
                held_proto_event: None,
                endpoint_events,
                endpoint_released: Some(signals.endpoint_released),
                current_socket_rx_rebind_generation: 0,
                blocked_writers: FxHashMap::default(),
                blocked_readers: FxHashMap::default(),
                stopped: FxHashMap::default(),
                error: None,
                io_poller: socket.clone().create_io_poller(),
                socket,
                runtime,
                send_buffer: Vec::new(),
                buffered_transmit: None,
            }),
            shared: Shared::default(),
            #[cfg(test)]
            retirement_barrier: Mutex::new(None),
            retirement_sentinel: ConnectionRetirementSentinel(Arc::new(AtomicBool::new(false))),
        }))
    }
}

impl Connection {
    /// Original endpoint-installed allocation authority; no new meter or owner.
    #[doc(hidden)]
    pub fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        self.0.state.lock("receive_backing_meter").inner.receive_backing_meter()
    }
}
