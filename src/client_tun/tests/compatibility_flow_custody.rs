//! Non-F characterizations for the Generic outer-supervision and saved-session boundaries.
//! The Thin tracer owns no session, flow, rearm, or resource state. The harness-gated F0
//! fixture owns only one real saved-session observation; it records the current W-Retained
//! boundary and never claims F custody, relay-role settlement, or public compatibility.

use super::super::super::{
    ListenerSpec, RelayCommand, RelayEvent, RelayReadCredit, RelayStream, build_listener_socket,
    run_relay_thin,
};
use smoltcp::iface::{SocketHandle, SocketSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, watch};

#[cfg(feature = "harness")]
use super::super::super::{NoopSink, TunRuntimeConfig};
#[cfg(feature = "harness")]
use super::super::{ClientTunSession, OpenPhase, SessionDeviceState, prepare_client_tun_session};
#[cfg(feature = "harness")]
use crate::device::TunIo;
#[cfg(feature = "harness")]
use crate::harness::{LoopbackTunDevice, PacketLink};
#[cfg(feature = "harness")]
use crate::metrics::Metrics;
#[cfg(feature = "harness")]
use crate::shared::{ClientError, TargetAddr};
#[cfg(feature = "harness")]
use crate::upstream::{DatagramUpstream, ProxyUpstream};
#[cfg(feature = "harness")]
use smoltcp::iface::{Config, Interface};
#[cfg(feature = "harness")]
use smoltcp::socket::tcp::{Socket, SocketBuffer};
#[cfg(feature = "harness")]
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, Ipv4Address, Ipv4Packet, TcpPacket};
#[cfg(feature = "harness")]
use std::future::Future;
#[cfg(feature = "harness")]
use std::sync::Mutex;
#[cfg(feature = "harness")]
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "harness")]
use std::time::Duration;
#[cfg(feature = "harness")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "harness")]
use tokio::sync::Notify;

#[path = "compatibility_rearm_custody.rs"]
mod rearm_custody;

struct ThinEofStream {
    payload: Option<Vec<u8>>,
    writer_shutdown: Arc<AtomicBool>,
}

impl AsyncRead for ThinEofStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(payload) = self.payload.take() {
            buf.put_slice(&payload);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ThinEofStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.writer_shutdown.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

fn thin_test_handle(sockets: &mut SocketSet<'static>) -> SocketHandle {
    sockets.add(build_listener_socket(&ListenerSpec { local_port: 12_345 }))
}

#[tokio::test]
async fn thin_relay_outer_supervision_preserves_dispatch_then_close() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = thin_test_handle(&mut sockets);
    let payload = b"thin-staging".to_vec();
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(ThinEofStream {
        payload: Some(payload.clone()),
        writer_shutdown: Arc::clone(&writer_shutdown),
    });
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(4);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 65_536,
    });

    let task = tokio::spawn(run_relay_thin(
        handle, 119, stream, uplink_rx, back_tx, credit_rx,
    ));

    match tokio::time::timeout(std::time::Duration::from_millis(500), back_rx.recv())
        .await
        .expect("Thin staging dispatcher should deliver the controlled payload")
        .expect("Thin staging dispatcher should keep the event lane open")
    {
        (event_handle, RelayEvent::Data { epoch, bytes, .. }) => {
            assert_eq!(event_handle, handle);
            assert_eq!(epoch, 119);
            assert_eq!(bytes, payload);
        }
        other => panic!("expected Thin Data before close, got {other:?}"),
    }

    match tokio::time::timeout(std::time::Duration::from_millis(500), back_rx.recv())
        .await
        .expect("Thin EOF should close after dispatch")
        .expect("Thin EOF should publish a close event")
    {
        (event_handle, RelayEvent::Closed(close)) => {
            assert_eq!(event_handle, handle);
            assert_eq!(close.epoch, 119);
            assert_eq!(close.direction, "remote_to_local");
            assert_eq!(close.reason, "remote_eof");
        }
        other => panic!("expected Thin close after data, got {other:?}"),
    }

    tokio::time::timeout(std::time::Duration::from_millis(500), task)
        .await
        .expect("Thin outer supervision should settle after EOF")
        .expect("Thin outer supervision task should not panic");
    assert!(
        writer_shutdown.load(Ordering::SeqCst),
        "Thin EOF should stop and shut down the writer half"
    );
    drop(uplink_tx);
}

#[cfg(feature = "harness")]
struct FlowRoleUpstream {
    entered: AtomicUsize,
    release: Notify,
    peer: Mutex<Option<tokio::io::DuplexStream>>,
}

#[cfg(feature = "harness")]
impl FlowRoleUpstream {
    fn new() -> Self {
        Self {
            entered: AtomicUsize::new(0),
            release: Notify::new(),
            peer: Mutex::new(None),
        }
    }
}

#[cfg(feature = "harness")]
#[async_trait::async_trait]
impl ProxyUpstream for FlowRoleUpstream {
    fn staged_tcp_open(
        self: Arc<Self>,
        _: crate::upstream::TcpOpenRequest,
    ) -> crate::upstream::StagedTcpOpen<crate::upstream::OpenedTcpRelay> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        Err(crate::upstream::TcpOpenUnavailable::Unsupported)
    }
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        assert_eq!(
            target,
            &TargetAddr::IpPort("192.0.2.10:10000".parse().expect("fixed target"))
        );
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        let (stream, peer) = tokio::io::duplex(64);
        *self.peer.lock().expect("fixture peer mutex") = Some(peer);
        Ok(Box::new(stream))
    }

    fn open_is_cheap(&self) -> bool {
        false
    }
}

#[cfg(feature = "harness")]
#[async_trait::async_trait]
impl DatagramUpstream for FlowRoleUpstream {
    async fn send_udp(&self, _: Vec<u8>) {
        panic!("F0 fixture must not send UDP")
    }
}

#[cfg(feature = "harness")]
struct FlowRoleApps {
    device: LoopbackTunDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    handle: SocketHandle,
    sent: bool,
    received_resets: usize,
}

#[cfg(feature = "harness")]
impl FlowRoleApps {
    fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self::with_target(inbound, outbound, IpAddress::v4(192, 0, 2, 10))
    }

    fn with_target(inbound: PacketLink, outbound: PacketLink, target: IpAddress) -> Self {
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
        let mut socket = Socket::new(
            SocketBuffer::new(vec![0; 4096]),
            SocketBuffer::new(vec![0; 8192]),
        );
        socket
            .connect(iface.context(), (target, 10_000), 40_000)
            .expect("fixture app socket connects");
        Self {
            device,
            iface,
            handle: sockets.add(socket),
            sockets,
            sent: false,
            received_resets: 0,
        }
    }

    async fn turn(&mut self) {
        while self
            .device
            .try_recv_rx()
            .expect("fixture app device stays open")
        {
            let ip = Ipv4Packet::new_checked(self.device.rx_peek().expect("fixture packet"))
                .expect("fixture IPv4 packet");
            let tcp = TcpPacket::new_checked(ip.payload()).expect("fixture TCP packet");
            self.received_resets += usize::from(tcp.rst());
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        let socket = self.sockets.get_mut::<Socket>(self.handle);
        if socket.can_send() && !self.sent {
            assert_eq!(
                socket.send_slice(b"f0-retained").expect("fixture payload"),
                b"f0-retained".len()
            );
            self.sent = true;
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.expect("fixture app flush");
    }

    fn established(&self) -> bool {
        self.sockets.get::<Socket>(self.handle).state() == smoltcp::socket::tcp::State::Established
    }
}

#[cfg(feature = "harness")]
struct FlowRoleFixture<F> {
    session: ClientTunSession<F>,
    apps: FlowRoleApps,
    upstream: Arc<FlowRoleUpstream>,
    metrics: Arc<Metrics>,
    _downlink: mpsc::Sender<Vec<u8>>,
}

#[cfg(feature = "harness")]
fn flow_role_fixture() -> FlowRoleFixture<impl Future<Output = ()>> {
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let apps = FlowRoleApps::new(outbound.clone(), inbound.clone());
    let upstream = Arc::new(FlowRoleUpstream::new());
    let metrics = Arc::new(Metrics::new());
    let (downlink, downlink_rx) = mpsc::channel(1);
    let mut config = TunRuntimeConfig::from_sources(Some("1")).expect("single fixture pool");
    config.tcp_socket_buffers.rx_bytes = 4096;
    config.tcp_socket_buffers.tx_bytes = 4096;
    let session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        Arc::clone(&upstream),
        downlink_rx,
        config,
        Arc::clone(&metrics),
        NoopSink,
    );
    FlowRoleFixture {
        session,
        apps,
        upstream,
        metrics,
        _downlink: downlink,
    }
}

#[cfg(feature = "harness")]
async fn poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = Box::pin(future);
    std::future::poll_fn(|cx| {
        Poll::Ready(match future.as_mut().poll(cx) {
            Poll::Pending => None,
            Poll::Ready(value) => Some(value),
        })
    })
    .await
}

#[cfg(feature = "harness")]
async fn drive_original_turn<F: Future<Output = ()>>(fixture: &mut FlowRoleFixture<F>) {
    fixture.apps.turn().await;
    let mut observer = Box::pin(fixture.session.run());
    std::future::poll_fn(|cx| {
        assert!(observer.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::task::yield_now().await;
}

#[cfg(feature = "harness")]
async fn wait_for_worker<F: Future<Output = ()>>(
    fixture: &mut FlowRoleFixture<F>,
) -> Option<(SocketHandle, u64)> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            drive_original_turn(fixture).await;
            let identity = fixture
                .session
                .control
                .opens()
                .slots
                .iter()
                .flatten()
                .find(|slot| matches!(&slot.phase, OpenPhase::Worker(_)))
                .map(|slot| (slot.handle, slot.epoch));
            if fixture.upstream.entered.load(Ordering::SeqCst) == 1 && identity.is_some() {
                return identity;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
}

#[cfg(feature = "harness")]
async fn wait_for_retained<F: Future<Output = ()>>(
    fixture: &mut FlowRoleFixture<F>,
    (handle, epoch): (SocketHandle, u64),
) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            drive_original_turn(fixture).await;
            let retained = fixture.session.control.opens().slots.iter().flatten().any(|slot| {
                slot.handle == handle
                    && slot.epoch == epoch
                    && matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some())
            });
            if retained {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

#[cfg(feature = "harness")]
#[derive(Debug)]
struct FlowRoleCleanup {
    retained: bool,
    quarantined: bool,
    device_settled: bool,
    state: Option<SessionDeviceState>,
}

#[cfg(feature = "harness")]
async fn stop_retained_fixture<F: Future<Output = ()>>(
    fixture: &mut FlowRoleFixture<F>,
    identity: (SocketHandle, u64),
) -> FlowRoleCleanup {
    fixture.session.request_stop();
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = poll_once(fixture.session.run()).await;
            let retained = fixture.session.control.opens().slots.iter().flatten().any(|slot| {
                (slot.handle, slot.epoch) == identity
                    && matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some())
            });
            let quarantined = fixture.session.control.ledger().quarantined;
            let device_settled = fixture
                .session
                .control
                .device_settlement_observed
                .load(Ordering::SeqCst);
            if retained && quarantined && device_settled {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let retained = fixture
        .session
        .control
        .opens()
        .slots
        .iter()
        .flatten()
        .any(|slot| {
            (slot.handle, slot.epoch) == identity
                && matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some())
        });
    let quarantined = fixture.session.control.ledger().quarantined;
    let device_settled = fixture
        .session
        .control
        .device_settlement_observed
        .load(Ordering::SeqCst);
    let state = if settled {
        poll_once(fixture.session.observe_shutdown()).await
    } else {
        None
    };
    FlowRoleCleanup {
        retained,
        quarantined,
        device_settled,
        state,
    }
}

#[cfg(feature = "harness")]
#[tokio::test(flavor = "current_thread")]
async fn saved_generic_open_stays_w_retained_before_legacy_relay_spawn() {
    let mut fixture = flow_role_fixture();
    let identity = wait_for_worker(&mut fixture)
        .await
        .expect("one saved-session Generic open reaches its W worker");
    assert!(
        fixture.apps.established(),
        "fixture app completes the original TCP handshake"
    );

    fixture.upstream.release.notify_one();
    assert!(
        wait_for_retained(&mut fixture, identity).await,
        "the exact worker result becomes retained before any legacy relay installation"
    );
    assert_eq!(
        fixture.metrics.snapshot(0, 0).relays_spawned,
        0,
        "a retained success has not reached spawn_remote_relay"
    );
    let mut peer = fixture
        .upstream
        .peer
        .lock()
        .expect("fixture peer mutex")
        .take()
        .expect("controlled Generic peer");
    assert!(
        poll_once(peer.read_u8()).await.is_none(),
        "no relay writer has delivered the app payload to the Generic peer"
    );
    drop(peer);

    let cleanup = stop_retained_fixture(&mut fixture, identity).await;
    eprintln!("F0 retained-boundary cleanup={cleanup:?}");
    assert!(
        cleanup.retained,
        "legacy stop leaves the successful open retained"
    );
    assert!(
        cleanup.quarantined,
        "legacy retained success remains quarantined"
    );
    assert!(cleanup.device_settled, "fixture still settles its device");
    assert_eq!(cleanup.state, Some(SessionDeviceState::Incomplete));
    assert_eq!(fixture.metrics.snapshot(0, 0).relays_spawned, 0);
}

#[cfg(feature = "harness")]
#[path = "compatibility_f_flow.rs"]
pub(super) mod f_flow;
