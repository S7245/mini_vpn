//! Shared open-custody test fixture for the parent custody, pressure, and settlement leaves.
//! It owns test-only upstream/device construction and cleanup, never production custody or
//! acceptance claims. Fixture borrows do not escape a test turn; test items remain in the leaves.

use super::*;

impl OpenCustody {
    pub(in crate::client_tun::compatibility_runtime) fn observe_blocked(
        &mut self,
        handle: SocketHandle,
        epoch: u64,
        socket: &mut Socket<'_>,
    ) {
        let fingerprint = socket
            .peek(socket.recv_queue())
            .unwrap_or(&[])
            .iter()
            .fold(0u64, |hash, byte| {
                hash.wrapping_mul(257).wrapping_add(u64::from(*byte))
            });
        self.blocked = Some((handle, epoch, socket.recv_queue(), fingerprint));
    }
}

pub(super) struct PendingOpens {
    pub(super) entered: AtomicUsize,
    pub(super) dropped: Arc<AtomicUsize>,
    pub(super) gates: Vec<Notify>,
    cheap: AtomicBool,
    pub(super) fail_with_drop_panic: AtomicBool,
    pub(super) error_dropped: Arc<AtomicUsize>,
    pub(super) behavior: Mutex<OpenBehavior>,
    pub(super) peer: Mutex<Option<tokio::io::DuplexStream>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum OpenBehavior {
    #[default]
    ControlledError,
    Success,
    PollPanic,
    CancelDropPanic,
}

struct CancelOpenPanic(Arc<AtomicUsize>);

impl Drop for CancelOpenPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("single cancelled open future destructor");
    }
}

pub(super) struct OpenGuard(pub(super) Arc<AtomicUsize>);

impl Drop for OpenGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl PendingOpens {
    pub(super) fn new() -> Self {
        Self {
            entered: AtomicUsize::new(0),
            dropped: Arc::new(AtomicUsize::new(0)),
            gates: (0..129).map(|_| Notify::new()).collect(),
            cheap: AtomicBool::new(false),
            fail_with_drop_panic: AtomicBool::new(false),
            error_dropped: Arc::new(AtomicUsize::new(0)),
            behavior: Mutex::new(OpenBehavior::default()),
            peer: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl ProxyUpstream for PendingOpens {
    async fn open_tcp(
        &self,
        target: &TargetAddr,
    ) -> Result<crate::upstream::RelayStream, ClientError> {
        assert_eq!(
            target,
            &TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap())
        );
        let _guard = OpenGuard(Arc::clone(&self.dropped));
        let index = self.entered.fetch_add(1, Ordering::SeqCst);
        let behavior = *self.behavior.lock().unwrap();
        let _cancel_drop = (index == 0 && matches!(behavior, OpenBehavior::CancelDropPanic))
            .then(|| CancelOpenPanic(Arc::clone(&self.error_dropped)));
        self.gates[index].notified().await;
        if index == 0 {
            match behavior {
                OpenBehavior::Success => {
                    let (stream, peer) = tokio::io::duplex(1);
                    *self.peer.lock().unwrap() = Some(peer);
                    return Ok(Box::new(stream));
                }
                OpenBehavior::PollPanic => panic!("controlled open future poll panic"),
                _ => {}
            }
        }
        if index == 0 && self.fail_with_drop_panic.load(Ordering::SeqCst) {
            return Err(ClientError::Io(std::io::Error::other(UnknownOpenError(
                Arc::clone(&self.error_dropped),
            ))));
        }
        Err(ClientError::Io(
            std::io::ErrorKind::ConnectionRefused.into(),
        ))
    }

    fn open_is_cheap(&self) -> bool {
        self.cheap.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct UnknownOpenError(Arc<AtomicUsize>);

impl std::fmt::Display for UnknownOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("opaque open error")
    }
}

impl std::error::Error for UnknownOpenError {}

impl Drop for UnknownOpenError {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("single unknown open error destructor");
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for PendingOpens {
    async fn send_udp(&self, _: Vec<u8>) {
        panic!("open custody fixture does not send UDP")
    }
}

pub(super) struct RealApps {
    pub(super) device: LoopbackTunDevice,
    pub(super) iface: Interface,
    pub(super) sockets: SocketSet<'static>,
    pub(super) handles: Vec<SocketHandle>,
    pub(super) sent: Vec<usize>,
    pub(super) windows: Vec<Option<u16>>,
    pub(super) targets: Vec<usize>,
    pub(super) send_batch: Option<usize>,
}

impl RealApps {
    pub(super) fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self::with_count(inbound, outbound, 129)
    }

    pub(super) fn with_count(inbound: PacketLink, outbound: PacketLink, count: usize) -> Self {
        let mut apps = Self::unconnected(inbound, outbound, count);
        apps.connect_range(0..count);
        apps
    }

    pub(super) fn unconnected(inbound: PacketLink, outbound: PacketLink, count: usize) -> Self {
        assert!((1..=25_536).contains(&count));
        let mut device = LoopbackTunDevice::new(inbound, outbound);
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        let local = Ipv4Address::new(10, 0, 0, 1);
        iface.update_ip_addrs(|addresses| addresses.push(IpCidr::new(local.into(), 24)).unwrap());
        iface.routes_mut().add_default_ipv4_route(local).unwrap();
        let mut sockets = SocketSet::new(vec![]);
        let handles = (0..count)
            .map(|_| {
                sockets.add(Socket::new(
                    SocketBuffer::new(vec![0; 4096]),
                    SocketBuffer::new(vec![0; 8192]),
                ))
            })
            .collect();
        Self {
            device,
            iface,
            sockets,
            handles,
            sent: vec![0; count],
            windows: vec![None; count],
            targets: (0..count)
                .map(|index| if index + 1 == count { 8192 } else { 1 })
                .collect(),
            send_batch: None,
        }
    }

    pub(super) fn connect_range(&mut self, range: std::ops::Range<usize>) {
        assert!(range.end <= self.handles.len());
        for index in range {
            let socket = self.sockets.get_mut::<Socket>(self.handles[index]);
            assert_eq!(socket.state(), smoltcp::socket::tcp::State::Closed);
            socket
                .connect(
                    self.iface.context(),
                    (IpAddress::v4(192, 0, 2, 10), 10_000),
                    40_000 + u16::try_from(index).unwrap(),
                )
                .unwrap();
        }
    }

    pub(super) async fn turn(&mut self, include_last: bool) {
        while self.device.try_recv_rx().unwrap() {
            let ip = Ipv4Packet::new_checked(self.device.rx_peek().unwrap()).unwrap();
            match ip.next_header() {
                smoltcp::wire::IpProtocol::Tcp => {
                    let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
                    let index = usize::from(tcp.dst_port() - 40_000);
                    self.windows[index] = Some(tcp.window_len());
                }
                smoltcp::wire::IpProtocol::Udp => {}
                protocol => panic!("unexpected application protocol: {protocol:?}"),
            }
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        let count = self.handles.len();
        for index in 0..if include_last { count } else { count - 1 } {
            let target_len = self.targets[index];
            let socket = self.sockets.get_mut::<Socket>(self.handles[index]);
            if socket.can_send() && self.sent[index] < target_len {
                let mut amount = target_len - self.sent[index];
                if let Some(batch) = self.send_batch {
                    if socket.send_queue() != 0 {
                        continue;
                    }
                    amount = amount.min(batch);
                }
                let payload = vec![(index % 251) as u8; amount];
                self.sent[index] += socket.send_slice(&payload).unwrap();
            }
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
    }

    pub(super) fn snapshot(
        &self,
        range: std::ops::Range<usize>,
    ) -> Vec<(
        usize,
        smoltcp::socket::tcp::State,
        usize,
        usize,
        Option<u16>,
    )> {
        range
            .map(|i| {
                (
                    i,
                    self.sockets.get::<Socket>(self.handles[i]).state(),
                    self.sent[i],
                    self.queued(i),
                    self.windows[i],
                )
            })
            .collect()
    }

    pub(super) fn queued(&self, index: usize) -> usize {
        self.sockets.get::<Socket>(self.handles[index]).send_queue()
    }

    pub(super) fn established(&self) -> usize {
        self.handles
            .iter()
            .filter(|&&handle| {
                self.sockets.get::<Socket>(handle).state()
                    == smoltcp::socket::tcp::State::Established
            })
            .count()
    }
}

pub(super) async fn capture<F: Future>(scenario: F) -> std::thread::Result<F::Output> {
    let mut scenario = Box::pin(scenario);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| scenario.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(observed)) => Poll::Ready(Ok(observed)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

pub(super) async fn ready<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    upstream: &PendingOpens,
) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        while apps.established() != 129 || upstream.entered.load(Ordering::SeqCst) != 128 {
            turn(session, apps, false).await;
        }
    })
    .await
    .is_ok()
}

pub(super) async fn turn<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    include_last: bool,
) {
    apps.turn(include_last).await;
    let mut observer = Box::pin(session.run());
    std::future::poll_fn(|cx| {
        assert!(observer.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(observer);
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
}

#[derive(Debug)]
pub(super) struct FixtureCleanup {
    pub(super) state: Option<SessionDeviceState>,
    pub(super) entered: usize,
    pub(super) dropped: usize,
    pub(super) timed_out: bool,
}

pub(super) async fn cleanup<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    upstream: &PendingOpens,
) -> FixtureCleanup {
    session.request_stop();
    let mut state = None;
    {
        let mut observer = Box::pin(session.run());
        std::future::poll_fn(|cx| {
            if let Poll::Ready(ready) = observer.as_mut().poll(cx) {
                state = Some(ready);
            }
            Poll::Ready(())
        })
        .await;
    }
    // This releases fixture waits even on legacy RED. Guard return is not task join.
    for gate in &upstream.gates {
        gate.notify_one();
    }
    let timed_out = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if state.is_none() {
                let mut observer = Box::pin(session.run());
                std::future::poll_fn(|cx| {
                    if let Poll::Ready(ready) = observer.as_mut().poll(cx) {
                        state = Some(ready);
                    }
                    Poll::Ready(())
                })
                .await;
            }
            if state.is_some()
                && upstream.dropped.load(Ordering::SeqCst)
                    == upstream.entered.load(Ordering::SeqCst)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_err();
    FixtureCleanup {
        state,
        entered: upstream.entered.load(Ordering::SeqCst),
        dropped: upstream.dropped.load(Ordering::SeqCst),
        timed_out,
    }
}

pub(super) fn fixture() -> (
    ClientTunSession<impl Future<Output = ()>>,
    RealApps,
    Arc<PendingOpens>,
    mpsc::Sender<Vec<u8>>,
) {
    fixture_with_options(4096, false, |_| {})
}

pub(super) fn fixture_with_cheap() -> (
    ClientTunSession<impl Future<Output = ()>>,
    RealApps,
    Arc<PendingOpens>,
    mpsc::Sender<Vec<u8>>,
) {
    fixture_with_options(4096, true, |_| {})
}

pub(super) fn fixture_with_rx(
    rx_bytes: usize,
) -> (
    ClientTunSession<impl Future<Output = ()>>,
    RealApps,
    Arc<PendingOpens>,
    mpsc::Sender<Vec<u8>>,
) {
    fixture_with_options(rx_bytes, false, |_| {})
}

pub(super) fn fixture_with_config(
    rx_bytes: usize,
    configure: impl FnOnce(&mut TunRuntimeConfig),
) -> (
    ClientTunSession<impl Future<Output = ()>>,
    RealApps,
    Arc<PendingOpens>,
    mpsc::Sender<Vec<u8>>,
) {
    fixture_with_options(rx_bytes, false, configure)
}

fn fixture_with_options(
    rx_bytes: usize,
    cheap: bool,
    configure: impl FnOnce(&mut TunRuntimeConfig),
) -> (
    ClientTunSession<impl Future<Output = ()>>,
    RealApps,
    Arc<PendingOpens>,
    mpsc::Sender<Vec<u8>>,
) {
    let inbound = PacketLink::bounded(2048);
    let outbound = PacketLink::bounded(2048);
    let device = LoopbackTunDevice::new(inbound.clone(), outbound.clone());
    let apps = RealApps::new(outbound, inbound);
    let upstream = Arc::new(PendingOpens::new());
    upstream.cheap.store(cheap, Ordering::Release);
    assert_eq!(upstream.open_is_cheap(), cheap);
    let (downlink, rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("129")).unwrap();
    config.tcp_socket_buffers.rx_bytes = rx_bytes;
    config.tcp_socket_buffers.tx_bytes = 4096;
    configure(&mut config);
    let session = prepare_client_tun_session(
        device,
        Arc::clone(&upstream),
        rx,
        config,
        Arc::new(Metrics::new()),
        NoopSink,
    );
    (session, apps, upstream, downlink)
}

#[derive(Debug, PartialEq)]
pub(super) struct OpenInventory {
    pub(super) slots: usize,
    pub(super) workers: usize,
    pub(super) live: usize,
    pub(super) announced: u32,
    pub(super) dropped: usize,
}

pub(super) fn inventory(control: &SessionControl, upstream: &PendingOpens) -> OpenInventory {
    let opens = control.opens();
    OpenInventory {
        slots: opens.slots.iter().flatten().count(),
        workers: opens
            .slots
            .iter()
            .flatten()
            .filter(|slot| matches!(slot.phase, OpenPhase::Worker(_)))
            .count(),
        live: opens.live,
        announced: control
            .announced
            .iter()
            .map(|word| word.load(Ordering::Acquire).count_ones())
            .sum(),
        dropped: upstream.dropped.load(Ordering::SeqCst),
    }
}
