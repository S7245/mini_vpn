//! Public app-window pressure and clean recovery. Owns only two bounded app
//! sockets and real peer streams; original service owns all lifecycle authority.
//! A's actual 4096-byte receive window stays full during useful B/UDP traffic.
//! This does not measure saturation of the private 64KiB ReaderWindow.
use super::*;
#[path = "observe_window.rs"]
mod observe_window;

#[derive(Clone, Copy)]
pub(super) enum Case {
    Original128KiB,
    LegacyExactWindow256KiB,
    ExactWindow256KiB,
    Continuous128KiB,
    ContinuousExactWindow256KiB,
    Permit128KiB,
    PermitExactWindow256KiB,
    Ordered128KiB,
    OrderedExactWindow256KiB,
}
impl Case {
    fn total(self) -> usize {
        match self {
            Self::Original128KiB
            | Self::Continuous128KiB
            | Self::Permit128KiB
            | Self::Ordered128KiB => 128 * 1024,
            Self::LegacyExactWindow256KiB
            | Self::ExactWindow256KiB
            | Self::ContinuousExactWindow256KiB
            | Self::PermitExactWindow256KiB
            | Self::OrderedExactWindow256KiB => 256 * 1024,
        }
    }
    fn exact(self) -> bool {
        matches!(
            self,
            Self::LegacyExactWindow256KiB
                | Self::ExactWindow256KiB
                | Self::ContinuousExactWindow256KiB
                | Self::PermitExactWindow256KiB
                | Self::OrderedExactWindow256KiB
        )
    }
    fn legacy(self) -> bool {
        matches!(self, Self::LegacyExactWindow256KiB)
    }
    fn ordered(self) -> bool {
        matches!(self, Self::Ordered128KiB | Self::OrderedExactWindow256KiB)
    }
    fn permit(self) -> bool {
        matches!(self, Self::Permit128KiB | Self::PermitExactWindow256KiB)
    }
    fn continuous(self) -> bool {
        matches!(
            self,
            Self::Continuous128KiB | Self::ContinuousExactWindow256KiB
        )
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FlowKey {
    pub(super) index: usize,
    pub(super) handle: SocketHandle,
    pub(super) epoch: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    pub(super) key: FlowKey,
    pub(super) slot_deadline: tokio::time::Instant,
    pub(super) open_deadline: std::time::Instant,
    pub(super) active: bool,
    pub(super) outstanding: usize,
}
pub(super) trait Observer {
    fn snapshot(&self, key: Option<FlowKey>) -> Option<Snapshot>;
    fn old_settled(&self) -> bool;
}
pub(super) struct NoObserver;
impl Observer for NoObserver {
    fn snapshot(&self, _: Option<FlowKey>) -> Option<Snapshot> {
        None
    }
    fn old_settled(&self) -> bool {
        false
    }
}
fn still_full(observer: &impl Observer, original: Snapshot) {
    assert_eq!(
        observer.snapshot(Some(original.key)),
        Some(original),
        "same physical full window/FlowId/deadlines"
    );
}

pub(super) async fn drive<S: TunService, F: Future>(service: &mut S, work: F) -> F::Output {
    observe_window::window(Duration::from_secs(5), async {
        tokio::select! {
            value = work => value,
            report = service.run() => panic!("downlink service ended early {report:?}"),
        }
    })
    .await
    .expect("downlink observation expired")
}
pub(crate) async fn drive_until<S: TunService, F: Future>(
    service: &mut S,
    until: tokio::time::Instant,
    work: F,
) -> F::Output {
    tokio::time::timeout_at(until, async {
        tokio::select! {
            value = work => value,
            report = service.run() => panic!("downlink service ended early {report:?}"),
        }
    })
    .await
    .expect("downlink observation expired")
}

async fn turn(app: &mut Apps) {
    app.turn().await;
    assert!(app.tcp_reply.len() <= 64);
    assert!(app.udp_reply.len() <= 64);
    tokio::time::sleep(Duration::from_millis(1)).await;
}

pub(crate) async fn auth(peer: &quinn::Connection, cfg: &TuicClientConfig) {
    let mut rx = peer.accept_uni().await.unwrap();
    let bytes = rx.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
        .unwrap();
    assert_eq!(bytes, encode_authenticate(&cfg.uuid, &token));
}

fn target(port: u16) -> TargetAddr {
    TargetAddr::IpPort(([192, 0, 2, 10], port).into())
}

pub(super) async fn accept(
    peer: &quinn::Connection,
    port: u16,
    payload: &[u8],
) -> (quinn::SendStream, quinn::RecvStream) {
    let (tx, mut rx) = peer.accept_bi().await.unwrap();
    let mut expected = encode_connect(&target(port));
    expected.extend_from_slice(payload);
    let mut bytes = vec![0; expected.len()];
    rx.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, expected);
    (tx, rx)
}

pub(super) async fn udp(peer: &quinn::Connection) -> u16 {
    let packet = peer.read_datagram().await.unwrap();
    assert!(packet.len() >= 4);
    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
    assert_eq!(
        &packet[..],
        encode_packet(assoc, &target(10001), b"service-udp")
    );
    peer.send_datagram(encode_packet(assoc, &target(10001), b"udp-reply").into())
        .unwrap();
    assoc
}

pub(super) async fn exchange(
    app: &mut Apps,
    peer: &quinn::Connection,
) -> ((quinn::SendStream, quinn::RecvStream), u16) {
    let remote = async {
        let stream = async {
            let (mut tx, rx) = accept(peer, 10000, b"service-tcp").await;
            tx.write_all(b"tcp-reply").await.unwrap();
            (tx, rx)
        };
        tokio::join!(stream, udp(peer))
    };
    let application = async {
        while app.tcp_reply != b"tcp-reply" || app.udp_reply != b"udp-reply" {
            turn(app).await;
        }
    };
    let (result, ()) = tokio::join!(remote, application);
    result
}

fn full(app: &Apps, a: SocketHandle) {
    let socket = app.sockets.get::<tcp::Socket>(a);
    assert_eq!(socket.recv_capacity(), 4096);
    assert_eq!(socket.recv_queue(), 4096);
    assert_eq!(socket.state(), tcp::State::Established);
}

pub(super) async fn retire(stream: &mut Option<(quinn::SendStream, quinn::RecvStream)>) {
    if let Some((tx, rx)) = stream.as_mut() {
        let code = tx.stopped().await.unwrap().expect("actual original STOP");
        assert!(rx.read_to_end(0).await.unwrap().is_empty());
        tx.reset(code).unwrap();
    }
    *stream = None;
}

pub(super) async fn exercise<S: TunService, O: Observer>(
    policy: Option<Arc<mini_vpn::upstream::RelayFamilyPolicy>>,
    case: Case,
    build: impl FnOnce(LoopbackTunDevice, TuicClientConfig, TunRuntimeConfig) -> (S, O),
) {
    let total = case.total();
    let mut server = server();
    let mut cfg = TuicClientConfig::from_sources(
        Some(&server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    cfg.tcp_pool = 1;
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    assert!(
        !(case.legacy() || case.continuous() || case.permit() || case.ordered())
            || policy.is_some()
    );
    if let Some(policy) = policy {
        use mini_vpn::upstream::TcpRelayEngine;
        cfg = cfg.with_relay_policy(policy.clone());
        runtime = runtime.with_relay_policy(policy.clone());
        assert!(
            Arc::ptr_eq(&policy, cfg.relay_policy())
                && Arc::ptr_eq(&policy, runtime.relay_policy())
        );
        assert_eq!(
            policy.generic_engine(),
            if case.legacy() {
                TcpRelayEngine::Legacy
            } else if case.ordered() {
                TcpRelayEngine::OrderedEgressPermitPump
            } else if case.permit() {
                TcpRelayEngine::PermitPump
            } else if case.continuous() {
                TcpRelayEngine::ContinuousPump
            } else {
                TcpRelayEngine::ThinStaging
            }
        );
    }
    let (device, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let (mut service, observer) = build(device, cfg.clone(), runtime);
    let control = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    let mut app = make_app(application);
    let original_udp = app.udp;
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2);
    let mut healthy = None;
    let scenario = capture(async {
        drive(&mut service, async {
            let peer = server.accept().await.unwrap().await.unwrap();
            peers.push(peer);
            auth(&peers[0], &cfg).await;
        })
        .await;
        let (stream, first_assoc) = drive(&mut service, exchange(&mut app, &peers[0])).await;
        healthy = Some(stream);
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        socket
            .connect(app.iface.context(), (Ipv4Address::from(app.target), 10002), 40003)
            .unwrap();
        let a = app.sockets.add(socket);
        let mut sent = false;
        let mut count = 0;
        {
        let until = tokio::time::Instant::now()+Duration::from_secs(5);
        let saturated = std::cell::Cell::new(None::<Snapshot>);
        let resume = std::cell::Cell::new(!case.exact());
        let mut work = std::pin::pin!(async {
            let remote = async {
                let (mut tx, mut rx) = accept(&peers[0], 10002, b"blocked").await;
                let mut scratch = [0; 8192];
                for offset in (0..total).step_by(scratch.len()) {
                    for (i, byte) in scratch.iter_mut().enumerate() {
                        *byte = ((offset + i) % 251) as u8;
                    }
                    tx.write_all(&scratch).await.unwrap();
                }
                tx.finish().unwrap();
                assert!(rx.read_to_end(0).await.unwrap().is_empty());
                // Both real peer halves leave custody only after app FIN/EOF.
                assert_eq!(tx.stopped().await.unwrap(), None);
            };
            let application = async {
                loop {
                    turn(&mut app).await;
                    let socket = app.sockets.get_mut::<tcp::Socket>(a);
                    if socket.can_send() && !sent {
                        socket.send_slice(b"blocked").unwrap();
                        sent = true;
                    }
                    if socket.recv_queue() == 4096 {
                        break;
                    }
                }
                full(&app, a);
                if case.exact() {
                    let original = loop {
                        full(&app, a);
                        if let Some(snapshot) = observer.snapshot(None) { break snapshot; }
                        turn(&mut app).await;
                    };
                    assert_eq!(original.outstanding, 65_536);
                    assert!(original.active);
                    println!("actual private64KiB saturated {original:?}");
                    saturated.set(Some(original));
                    while !resume.get() { tokio::task::yield_now().await; }
                    still_full(&observer, original);
                }
                app.tcp_reply.clear();
                app.udp_reply.clear();
                app.sent_udp = false;
                app.sockets.get_mut::<tcp::Socket>(app.tcp).send_slice(b"again").unwrap();
                let sibling = async {
                    let (tx, rx) = healthy.as_mut().unwrap();
                    let mut bytes = [0; 5];
                    rx.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(&bytes, b"again");
                    tx.write_all(b"sibling-reply").await.unwrap();
                    assert_eq!(udp(&peers[0]).await, first_assoc);
                };
                let useful = async {
                    while app.tcp_reply != b"sibling-reply" || app.udp_reply != b"udp-reply" {
                        full(&app, a);
                        if let Some(original) = saturated.get() { still_full(&observer, original); }
                        turn(&mut app).await;
                    }
                    full(&app, a);
                    if let Some(original) = saturated.get() { still_full(&observer, original); }
                };
                tokio::join!(sibling, useful);
                println!("actual app A window4096 full while original B+same UDP exchange succeeds");
                let mut scratch = [0; 8192];
                loop {
                    turn(&mut app).await;
                    let socket = app.sockets.get_mut::<tcp::Socket>(a);
                    while socket.can_recv() {
                        let n = socket.recv_slice(&mut scratch).unwrap();
                        assert!(count + n <= total, "no duplicate/extra payload");
                        for byte in &scratch[..n] {
                            assert_eq!(*byte, (count % 251) as u8);
                            count += 1;
                        }
                    }
                    if socket.state() == tcp::State::CloseWait {
                        assert_eq!(count, total, "actual FIN only after exact payload");
                        assert_eq!(socket.recv_queue(), 0);
                        socket.close();
                        break;
                    }
                    assert_ne!(socket.state(), tcp::State::Closed, "RST is not EOF");
                }
                while app.sockets.get::<tcp::Socket>(a).state() != tcp::State::Closed {
                    turn(&mut app).await;
                }
            };
            tokio::join!(remote, application);
        });
        if case.exact() {
            drive_until(&mut service, until, std::future::poll_fn(|cx| {
                assert!(work.as_mut().poll(cx).is_pending(), "pressure work cannot finish before release");
                if saturated.get().is_some() { Poll::Ready(()) } else { Poll::Pending }
            })).await;
            let original = saturated.get().unwrap();
            for _ in 0..4 {
                let cut = tokio::time::Instant::now()+Duration::from_millis(2);
                assert!(tokio::time::timeout_at(until.min(cut), service.run()).await.is_err());
                assert!(tokio::time::Instant::now() < until, "same original A observation deadline");
                still_full(&observer, original);
                assert_eq!(control.phase(), TuicServicePhase::Running);
            }
            println!("same original A/privatewindow65536 survived4 cancelled service borrows before {until:?}");
            resume.set(true);
        }
        drive_until(&mut service, until, work.as_mut()).await;
        }
        assert_eq!(count, total);
        app.sockets.remove(a);
        assert_eq!(control.phase(), TuicServicePhase::Running);
        assert!(control.request_rebuild());
        assert!(!control.request_rebuild());
        drive(&mut service, async {
            retire(&mut healthy).await;
            let incoming = server.accept().await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), peers[0].closed()).await.unwrap();
            assert!(peers[0].close_reason().is_some());
            if case.exact() { assert!(observer.old_settled(), "actual old root allzero/released2/mode-specific actual joins before successor handshake"); }
            app.restart();
            assert_eq!(app.udp, original_udp);
            peers.push(incoming.await.unwrap());
            auth(&peers[1], &cfg).await;
        }).await;
        let (stream, _) = drive(&mut service, exchange(&mut app, &peers[1])).await;
        healthy = Some(stream);
        assert_eq!(control.phase(), TuicServicePhase::Running);
        assert!(!control.request_rebuild(), "lifetime sole successor already consumed");
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("A exact {count}bytes+EOF/FIN, old peer closed before successor handshake; newTCP40002/sameUDP40001 restored");
    }).await;
    control.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let (report, ()) = tokio::join!(service.run(), retire(&mut healthy));
            report
        })
        .await
        .unwrap();
        println!("downlink final={report:?}");
        assert!(report.device_settled());
        assert!(!report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert_eq!(control.observe_shutdown().await, TuicServicePhase::Complete);
    })
    .await;
    drop(healthy);
    drop(service);
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}
