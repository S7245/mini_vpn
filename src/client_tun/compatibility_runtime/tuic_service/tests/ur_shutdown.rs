//! Saved Service stop through original UR carriers and their actual task joins.
//! Thin's Window belongs to its dispatcher; Legacy's belongs to its reader.
//! Each case owns only real peer/App IO and passive fixed Copy observations.
//! It never changes ReadBuf, credit, capacity, provider or settlement authority.
//! Thin nonzero gap/4-role custody remains a separate open obligation.
use super::fixture::{capture, config, Apps};
use super::*;
use crate as mini_vpn;
use crate::client_tun::TuicServicePhase;
use crate::upstream::{
    RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine, TuicGenericStreamMode,
};
use device::{Gate, GatedDevice};
use smoltcp::iface::SocketHandle;
use smoltcp::socket::tcp;
use smoltcp::wire::Ipv4Address;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::task::Poll;

#[path = "../../../../../tests/tuic_service/downlink_fixture.rs"]
pub(super) mod wire;

fn server() -> quinn::Endpoint {
    quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap()
}
fn make_app(device: LoopbackTunDevice) -> Apps {
    Apps::from_device_with_target(device, std::net::Ipv4Addr::new(192, 0, 2, 10))
}
fn parent_deadline(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn owner_facts(owner: &SessionControl) -> (bool, bool, Option<tokio::time::Instant>) {
    let ledger = owner.ledger();
    (
        ledger.first_failure.is_none(),
        ledger.quarantined,
        ledger.shutdown_deadline,
    )
}
fn original_owner(control: &Control) -> Arc<SessionControl> {
    let weak = control
        .session
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    weak.expect("actual original Session weak")
        .upgrade()
        .expect("live original Session")
}
type Halves = (quinn::SendStream, quinn::RecvStream);
async fn retire_once(stream: &mut Option<Halves>, started: &mut bool) {
    if !*started {
        *started = true;
        wire::retire(stream).await;
    }
}
async fn retire_two(streams: &mut [Option<Halves>; 2], started: &mut [bool; 2]) {
    let [a, b] = streams;
    let [first, second] = started;
    tokio::join!(retire_once(a, first), retire_once(b, second));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    ThinCurrentInventory,
    LegacyWindowTail,
}
impl Case {
    fn engine(self) -> TcpRelayEngine {
        match self {
            Self::ThinCurrentInventory => TcpRelayEngine::ThinStaging,
            Self::LegacyWindowTail => TcpRelayEngine::Legacy,
        }
    }
    fn roles_per_flow(self) -> u64 {
        match self {
            Self::ThinCurrentInventory => 4,
            Self::LegacyWindowTail => 3,
        }
    }
}
async fn exercise(case: Case) {
    let mut server = server();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        thin: (case == Case::ThinCurrentInventory).then_some("1"),
        tuic_unordered_reassembly: Some("1"),
        ..Default::default()
    }));
    assert_eq!(policy.generic_engine(), case.engine());
    assert_eq!(
        policy.tuic_generic_mode(),
        TuicGenericStreamMode::UnorderedReassembly
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    assert_eq!(cfg.tcp_pool, 1);
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy.clone());
    assert!(
        Arc::ptr_eq(&policy, cfg.relay_policy()) && Arc::ptr_eq(&policy, runtime.relay_policy())
    );
    let (device, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    let mut service = prepare(
        GatedDevice {
            inner: device,
            gate: gate.clone(),
        },
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    let saved = service.control.clone();
    let (watch, registration) = TuicUpstream::watch_ur_carrier_for_test(1);
    let mut app = make_app(application);
    let mut peer = None;
    let mut streams = [None, None];
    let mut retirement_started = [false; 2];
    let mut completed = false;
    let scenario = capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        }).await;
        let p = peer.as_ref().unwrap();
        let (healthy, assoc) = wire::drive(&mut service, wire::exchange(&mut app, p)).await;
        assert_eq!(healthy.0.id().index(), 0);
        streams[0] = Some(healthy);
        assert_eq!(watch.snapshot().created, 0, "unmatched healthy B must not consume the one observer");
        let owner = original_owner(&saved);
        let mut socket = tcp::Socket::new(tcp::SocketBuffer::new(vec![0; 4096]), tcp::SocketBuffer::new(vec![0; 4096]));
        socket.connect(app.iface.context(), (Ipv4Address::from(app.target), 10002), 40003).unwrap();
        let a = app.sockets.add(socket);
        let mut sent = false;
        let mut logged = false;
        let mut logged_app = false;
        let warm_until = tokio::time::Instant::now() + Duration::from_secs(5);
        let thin_key = std::cell::Cell::new(None);
        let thin_resume = std::cell::Cell::new(case != Case::ThinCurrentInventory);
        {
        let mut warm_work = Box::pin(async {
            let remote = async {
                let mut original = wire::accept(p, 10002, b"blocked").await;
                assert_eq!(original.0.id().index(), 1, "actual target A QUIC stream, not an inferred FlowId");
                original.0.write_all(&vec![0x5a; 262144]).await.unwrap();
                streams[1] = Some(original); // Do not finish either real peer half.
            };
            let application = async {
                loop {
                    app.turn().await;
                    let socket = app.sockets.get_mut::<tcp::Socket>(a);
                    if socket.can_send() && !sent { socket.send_slice(b"blocked").unwrap(); sent = true; }
                    let full = socket.recv_queue() == 4096;
                    let contracts = owner.flow_contracts();
                    let saturated = contracts.iter().any(|flow| flow.active && flow.published && flow.outstanding == Some(65536));
                    if full && !logged_app {
                        let fixed: [Option<_>; 2] = std::array::from_fn(|index| contracts.get(index).map(|flow| (flow.id, flow.slot_deadline, flow.open_deadline, flow.active, flow.published, flow.outstanding)));
                        let actual = watch.snapshot();
                        println!("UR first App4096 full originalRows={fixed:?} actual={actual:?}");
                        logged_app = true;
                    }
                    if full && saturated {
                        let actual = watch.snapshot();
                        if !logged { println!("UR actual Service full-window candidate {actual:?}"); logged = true; }
                        if case == Case::ThinCurrentInventory {
                            let mut matching = contracts.iter().filter(|flow| flow.active && flow.published && flow.outstanding == Some(65536));
                            let original = matching.next().expect("one original full Thin Flow");
                            assert!(matching.next().is_none());
                            thin_key.set(Some((original.id, original.slot_deadline, original.open_deadline, original.outstanding)));
                            while !thin_resume.get() { tokio::task::yield_now().await; }
                            break;
                        }
                        if actual.latest.is_some_and(|inventory| inventory.stored > 0) { break; }
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::join!(remote, application);
        });
        if case == Case::ThinCurrentInventory {
            wire::drive_until(&mut service, warm_until, std::future::poll_fn(|cx| {
                assert!(warm_work.as_mut().poll(cx).is_pending(), "same warm work remains paused");
                if thin_key.get().is_some() { Poll::Ready(()) } else { Poll::Pending }
            })).await;
            let original = thin_key.get().unwrap();
            for _ in 0..4 {
                let cut = tokio::time::Instant::now() + Duration::from_millis(2);
                assert!(tokio::time::timeout_at(warm_until.min(cut), service.run()).await.is_err());
                assert!(tokio::time::Instant::now() < warm_until);
                let now = owner.flow_contracts().into_iter().find(|flow| flow.id == original.0).unwrap();
                assert!(now.active && now.published);
                assert_eq!((now.id, now.slot_deadline, now.open_deadline, now.outstanding), original);
                assert_eq!(control.phase(), TuicServicePhase::Running);
                let current = watch.snapshot();
                assert!(current.before_drop.is_none() && !current.drop_returned);
                let inventory = current.latest.unwrap();
                assert_eq!(inventory.stream, 1);
                assert_eq!(inventory.stored, inventory.independently_summed);
            }
            println!("UR Thin same original fullWindow/key/limits survived4 cancelled2ms borrows before {warm_until:?}");
            thin_resume.set(true);
            wire::drive_until(&mut service, warm_until, warm_work.as_mut()).await;
        } else {
            wire::drive(&mut service, warm_work.as_mut()).await;
        }
        } // End the complete original warm future's borrows before healthy work.
        let rows = owner.flow_contracts();
        let saturated: Vec<_> = rows.iter().filter(|flow| flow.active && flow.published && flow.outstanding == Some(65536)).collect();
        assert_eq!(saturated.len(), 1);
        let original = saturated[0];
        let key = (original.id, original.slot_deadline, original.open_deadline, original.outstanding);
        let audit = watch.snapshot(); let held = audit.latest.unwrap();
        if case == Case::LegacyWindowTail {
            assert!(audit.last_read_capacity > 0 && audit.last_read_capacity < 65536);
            assert_eq!(audit.last_read_bytes, Some(audit.last_read_capacity), "actual original Window-tail read, not synthetic credit");
            assert!(held.chunks > 0 && held.stored > 0 && held.front.is_some());
        }
        assert_eq!(audit.created, 1); assert_eq!(held.stream, 1);
        assert_eq!(held.stored, held.independently_summed);
        assert!(audit.before_drop.is_none() && !audit.drop_returned);
        println!("UR {case:?} original A full65536 key={key:?} actualInventory={held:?}; Bytes backing Unknown");
        app.tcp_reply.clear(); app.udp_reply.clear(); app.sent_udp = false;
        app.sockets.get_mut::<tcp::Socket>(app.tcp).send_slice(b"again").unwrap();
        let healthy_work = async {
            let remote = async {
                let (tx, rx) = streams[0].as_mut().unwrap();
                let mut bytes = [0; 5]; rx.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes, b"again");
                tx.write_all(b"sibling-reply").await.unwrap(); assert_eq!(wire::udp(p).await, assoc);
            };
            let application = async {
                while app.tcp_reply != b"sibling-reply" || app.udp_reply != b"udp-reply" {
                    let socket = app.sockets.get::<tcp::Socket>(a);
                    assert_eq!((socket.recv_capacity(), socket.recv_queue()), (4096, 4096));
                    let now = owner.flow_contracts().into_iter().find(|flow| flow.id == key.0).unwrap();
                    assert_eq!((now.id, now.slot_deadline, now.open_deadline, now.outstanding), key);
                    let current = watch.snapshot();
                    assert!(current.before_drop.is_none() && !current.drop_returned);
                    let inventory = current.latest.unwrap();
                    assert_eq!(inventory.stream, 1);
                    assert_eq!(inventory.stored, inventory.independently_summed);
                    if case == Case::LegacyWindowTail { assert_eq!(current.latest, Some(held)); }
                    app.turn().await; tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::join!(remote, application);
        };
        if case == Case::ThinCurrentInventory {
            wire::drive_until(&mut service, warm_until, healthy_work).await;
        } else {
            wire::drive(&mut service, healthy_work).await;
        }
        let before_stop = watch.snapshot();
        assert!(before_stop.before_drop.is_none() && !before_stop.drop_returned);
        let stop_inventory = before_stop.latest.unwrap();
        assert_eq!(stop_inventory.stream, 1);
        assert_eq!(stop_inventory.stored, stop_inventory.independently_summed);
        if case == Case::LegacyWindowTail { assert_eq!(stop_inventory, held); }
        let facts = owner_facts(&owner);
        assert!(facts.0 && !facts.1);
        println!("UR {case:?} actual beforeStop={stop_inventory:?}; A full while healthy TCP+sameUDP={assoc} useful");
        control.request_stop();
        let parent_limit = parent_deadline(&saved).expect("original parent stop deadline");
        let mut observer = Box::pin(control.observe_shutdown());
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(1), service.run()).await.is_err());
            let actual_limit = parent_deadline(&saved);
            assert_eq!(actual_limit, Some(parent_limit));
        }
        let owner_limit = owner_facts(&owner).2.expect("original owner stop deadline");
        tokio::time::timeout_at(parent_limit, async {
            let work = async {
                retire_two(&mut streams, &mut retirement_started).await;
                while gate.polls.load(Ordering::Acquire) == 0 { app.turn().await; tokio::time::sleep(Duration::from_millis(1)).await; }
            };
            tokio::select! { () = work => (), report = service.run() => panic!("original D join gate bypassed {report:?}") }
        }).await.unwrap();
        let retired = watch.snapshot();
        assert!(retired.drop_returned);
        let dropped_inventory = retired.before_drop.expect("original carrier actual Drop sample");
        assert_eq!(dropped_inventory.stream, 1);
        assert_eq!(dropped_inventory.stored, dropped_inventory.independently_summed);
        if case == Case::LegacyWindowTail { assert_eq!(dropped_inventory, held); }
        let actual = owner.flow_observation().unwrap();
        assert_eq!((actual.live, actual.w, actual.f, actual.reserved, actual.escrow_bytes, actual.running, actual.attempts), (0,0,0,0,0,0,0));
        assert_eq!((actual.factory_started, actual.published, actual.released, actual.joined), (2,2,2,2 * case.roles_per_flow()));
        assert!(actual.audit && owner.business_complete.load(Ordering::Acquire));
        assert!(!owner.driver_complete.load(Ordering::Acquire));
        let facts = owner_facts(&owner);
        assert!(facts.0 && !facts.1);
        let actual_owner_limit = owner_facts(&owner).2;
        assert_eq!(actual_owner_limit, Some(owner_limit));
        let actual_parent = parent_deadline(&saved); assert_eq!(actual_parent, Some(parent_limit));
        assert!(gate.polls.load(Ordering::Acquire) > 0);
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0,0));
        assert!(p.close_reason().is_none() && service.completion.is_none());
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("UR {case:?} original roles perflow={} actual joins+provider retirement BEFORE D/Node {actual:?} target={retired:?} owner15={owner_limit:?} parent15={parent_limit:?}", case.roles_per_flow());
        gate.open();
        let (report, phase) = tokio::time::timeout_at(parent_limit, async { tokio::join!(service.run(), observer.as_mut()) }).await.unwrap();
        assert_eq!(phase, Phase::Complete);
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!((report.rebuilds, report.discarded_packets, report.discarded_backing), (0,0,0));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1,1));
        println!("UR actual savedService final {report:?}; same first15; no successor");
        completed = true;
    }).await;
    let cleanup = capture(async {
        control.request_stop();
        if !gate.release.load(Ordering::Acquire) {
            gate.open();
        }
        if !completed {
            for (stream, started) in streams.iter_mut().zip(retirement_started.iter()) {
                if *started && stream.is_some() {
                    eprintln!("partial original peer retirement Unknown; actual halves released, not a clean receipt");
                    drop(stream.take());
                }
            }
            let limit = parent_deadline(&saved).expect("original cleanup deadline");
            tokio::time::timeout_at(limit, async {
                tokio::join!(
                    service.run(),
                    retire_two(&mut streams, &mut retirement_started)
                );
            })
            .await
            .unwrap();
        }
    })
    .await;
    let disposal = capture(async {
        // Forced disposal is never settlement; retain the scenario error first.
        drop(streams);
        drop(service);
        drop(registration);
    })
    .await;
    if let Some(p) = peer {
        p.close(0u32.into(), b"done");
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
    if let Err(error) = disposal {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_thin_ur_current_inventory_full_window_stop_joins_four_roles_before_device_node() {
    exercise(Case::ThinCurrentInventory).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_legacy_ur_natural_window_tail_stash_stop_joins_three_roles_before_device_node() {
    exercise(Case::LegacyWindowTail).await;
}

mod gap;
