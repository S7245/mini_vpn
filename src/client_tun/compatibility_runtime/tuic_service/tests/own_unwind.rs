//! Original production saved driver fault envelope, injected once at the common
//! constructor only in test+harness. Default delegates the original future.
//! The tracer owns local endpoints, never substitute Node/device/cleanup facts.
use super::*;
use device::{Gate, GatedDevice};
use fixture::{auth, capture, config, Apps};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct OriginalFault(Arc<AtomicUsize>);
impl Drop for OriginalFault {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime::tuic_service) struct Facts {
    mode: AtomicUsize,
    fired: AtomicBool,
    polls: AtomicUsize,
    drops: AtomicUsize,
    payload: Mutex<Option<Box<OriginalFault>>>,
    payload_drops: Arc<AtomicUsize>,
    ready: AtomicUsize,
    ready_report: Mutex<Option<Report>>,
    ready_first: Mutex<Option<FirstValue>>,
}
struct Saved<F> {
    inner: Option<Pin<Box<F>>>,
    facts: Arc<Facts>,
}
impl<F: Future<Output = Completion>> Future for Saved<F> {
    type Output = Completion;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Completion> {
        // Modes zero in every original test: no counters, lock or extra behavior.
        if self.facts.mode.load(Ordering::Acquire) == 0 {
            return self.inner.as_mut().unwrap().as_mut().poll(cx);
        }
        if self.facts.mode.load(Ordering::Acquire) == 2 {
            return match self.inner.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(done) => {
                    assert_eq!(self.facts.ready.fetch_add(1, Ordering::AcqRel), 0);
                    *self.facts.ready_report.lock().unwrap() = Some(done.report);
                    *self.facts.ready_first.lock().unwrap() = first_value(&done.control);
                    Poll::Ready(done)
                }
                Poll::Pending => Poll::Pending,
            };
        }
        self.facts.polls.fetch_add(1, Ordering::AcqRel);
        assert!(
            !self.facts.fired.swap(true, Ordering::AcqRel),
            "poisoned saved future repolled"
        );
        let payload = self.facts.payload.lock().unwrap().take().unwrap();
        std::panic::panic_any(payload);
    }
}
impl<F> Drop for Saved<F> {
    fn drop(&mut self) {
        if self.facts.mode.load(Ordering::Acquire) != 0 {
            self.facts.drops.fetch_add(1, Ordering::AcqRel);
        }
        drop(self.inner.take());
        if self.facts.mode.load(Ordering::Acquire) == 2
            && self.facts.ready.load(Ordering::Acquire) == 1
        {
            let payload = self.facts.payload.lock().unwrap().take().unwrap();
            std::panic::panic_any(payload);
        }
    }
}
pub(in crate::client_tun::compatibility_runtime::tuic_service) fn wrap<
    F: Future<Output = Completion>,
>(
    inner: Pin<Box<F>>,
    facts: Arc<Facts>,
) -> Pin<Box<impl Future<Output = Completion>>> {
    Box::pin(Saved {
        inner: Some(inner),
        facts,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn service_own_saved_poll_unwind_retains_original_driver_node_device_and_requests_stop() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let gate = Arc::new(Gate::default());
    let mut service = prepare(
        GatedDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(4),
            gate: gate.clone(),
        },
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let facts = saved.saved_facts.clone();
    let mut app = Apps::new(outbound, inbound);
    let mut peer = None;
    let mut stream = None;
    let mut escaped = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            let work = async {
                let p = server.accept().await.unwrap().await.unwrap();
                auth(&p, &cfg).await;
                peer = Some(p);
                let p = peer.as_ref().unwrap();
                let tcp = async {
                    let (mut tx, mut rx) = p.accept_bi().await.unwrap();
                    let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
                    let mut want = encode_connect(&target);
                    want.extend_from_slice(b"service-tcp");
                    let mut actual = vec![0; want.len()];
                    rx.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, want);
                    tx.write_all(b"pong").await.unwrap();
                    stream = Some((tx, rx));
                };
                let udp = async {
                    let packet = p.read_datagram().await.unwrap();
                    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
                    let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
                    assert_eq!(&packet[..], encode_packet(assoc, &target, b"service-udp"));
                    p.send_datagram(encode_packet(assoc, &target, b"udp-reply").into()).unwrap();
                };
                let application = async {
                    while app.tcp_reply != b"pong" || app.udp_reply != b"udp-reply" {
                        app.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                tokio::join!(tcp, udp, application);
            };
            tokio::select! {
                () = work => (),
                report = service.run() => panic!("real service ended early {report:?}"),
            }
        }).await.unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let before = owner.flow_observation().unwrap();
        assert_eq!((before.live, before.f, before.published, before.running), (1, 1, 1, 3));
        assert!(before.audit);
        assert_eq!(handle.phase(), Phase::Running);
        let fault = Box::new(OriginalFault(facts.payload_drops.clone()));
        let identity = &*fault as *const OriginalFault as usize;
        *facts.payload.lock().unwrap() = Some(fault);
        facts.mode.store(1, Ordering::Release);
        let result = capture(tokio::time::timeout(Duration::from_millis(2), service.run())).await;
        let escaped_identity = match &result {
            Err(payload) => payload.downcast_ref::<Box<OriginalFault>>()
                .map(|payload| &**payload as *const OriginalFault as usize),
            _ => None,
        };
        println!("ownSaved POLL oracle escaped={} payload={escaped_identity:?}/original={identity} phase={:?} parentdeadline={:?} outerstop={} flow={:?} Dpoll/join/drop={}/{}/{} wrapperpoll/drop={}/{} payloaddrop={} peeropen={}",
            result.is_err(), handle.phase(), *saved.deadline.lock().unwrap(), saved.stop.stopping.load(Ordering::Acquire),
            owner.flow_observation(), gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire), facts.polls.load(Ordering::Acquire), facts.drops.load(Ordering::Acquire),
            facts.payload_drops.load(Ordering::Acquire), peer.as_ref().unwrap().close_reason().is_none());
        match result {
            Err(payload) => {
                escaped = Some(payload); // Diagnostic custody outside borrowed scenario.
                panic!("production Service root let original saved poll panic escape");
            }
            Ok(Ok(report)) => panic!("unknown poll cannot complete {report:?}"),
            Ok(Err(_)) => (),
        }
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert_eq!(payload_identity(&saved, false), identity);
        assert!(saved.stop.stopping.load(Ordering::Acquire));
        let first_deadline = saved.deadline.lock().unwrap().unwrap();
        assert!(first_deadline > tokio::time::Instant::now());
        assert_eq!(facts.payload_drops.load(Ordering::Acquire), 0);
        let polls = facts.polls.load(Ordering::Acquire);
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert_eq!(*saved.deadline.lock().unwrap(), Some(first_deadline));
            assert_eq!(facts.polls.load(Ordering::Acquire), polls);
            assert_eq!(facts.drops.load(Ordering::Acquire), 0);
            assert_eq!(facts.payload_drops.load(Ordering::Acquire), 0);
            assert_eq!(payload_identity(&saved, false), identity);
            assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)), (0, 0, 0));
            assert_eq!(owner.flow_observation().unwrap().live, 1);
            assert!(peer.as_ref().unwrap().close_reason().is_none());
            assert!(service.completion.is_none());
            assert!(!handle.request_rebuild());
        }
    }).await;
    // Do not repoll a saved future that already unwound. Forced destruction only;
    // this server drain never claims original client device/Node asynchronous join.
    drop(stream);
    drop(service);
    println!(
        "ownSaved forcedfinally Djoin/drop={}/{} wrapperdrop={}",
        gate.joins.load(Ordering::Acquire),
        gate.drops.load(Ordering::Acquire),
        facts.drops.load(Ordering::Acquire)
    );
    if let Some(p) = peer {
        p.close(0u32.into(), b"forced-finally");
    }
    server.close(0u32.into(), b"forced-finally");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    // Keep the original escaped diagnostic payload, never substitute a new one.
    if let Some(payload) = escaped {
        std::mem::forget(payload);
    }
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

fn payload_identity(control: &Control, disposal: bool) -> usize {
    let slot = if disposal {
        &control.terminal.disposal
    } else {
        &control.terminal.poll
    };
    let custody = slot.lock().unwrap();
    let payload = custody
        .as_ref()
        .expect("root must hold originalopaque payload");
    let erased: &(dyn std::any::Any + Send) = &***payload;
    let original = erased
        .downcast_ref::<Box<OriginalFault>>()
        .expect("same original Box type");
    &**original as *const OriginalFault as usize
}

type FirstValue = (usize, String, Vec<u8>);
fn first_value(owner: &SessionControl) -> Option<FirstValue> {
    let ledger = owner.ledger();
    ledger.first_failure.as_ref().map(|error| {
        (
            error as *const _ as usize,
            format!("{:?}", error.facts()),
            error.text_bytes().to_vec(),
        )
    })
}

#[tokio::test(flavor = "current_thread")]
async fn service_completed_saved_drop_unwind_retains_original_completion_first_error_and_payload() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let gate = Arc::new(Gate::default());
    gate.open();
    let mut service = Some(prepare(
        GatedDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(4),
            gate: gate.clone(),
        },
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    ));
    let handle = service.as_mut().unwrap().take_handle().unwrap();
    let saved = service.as_ref().unwrap().control.clone();
    let facts = saved.saved_facts.clone();
    let mut app = Apps::new(outbound, inbound);
    let mut peer = None;
    let mut stream = None;
    let mut escaped = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            let work = async {
                let p = server.accept().await.unwrap().await.unwrap();
                auth(&p, &cfg).await;
                peer = Some(p);
                let p = peer.as_ref().unwrap();
                let tcp = async {
                    let (mut tx, mut rx) = p.accept_bi().await.unwrap();
                    let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
                    let mut want = encode_connect(&target);
                    want.extend_from_slice(b"service-tcp");
                    let mut actual = vec![0; want.len()];
                    rx.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, want);
                    tx.write_all(b"pong").await.unwrap();
                    stream = Some((tx, rx));
                };
                let udp = async {
                    let packet = p.read_datagram().await.unwrap();
                    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
                    let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
                    assert_eq!(&packet[..], encode_packet(assoc, &target, b"service-udp"));
                    p.send_datagram(encode_packet(assoc, &target, b"udp-reply").into()).unwrap();
                };
                let application = async {
                    while app.tcp_reply != b"pong" || app.udp_reply != b"udp-reply" {
                        app.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                tokio::join!(tcp, udp, application);
            };
            tokio::select! {
                () = work => (),
                report = service.as_mut().unwrap().run() => panic!("early {report:?}"),
            }
        }).await.unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let (mut tx, mut rx) = stream.take().unwrap();
        tx.reset(77u32.into()).unwrap();
        rx.stop(78u32.into()).unwrap();
        drop((tx, rx));
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            let observe = async {
                while owner.flow_observation().unwrap().released != 1 {
                    app.turn().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::select! {
                () = observe => (),
                report = service.as_mut().unwrap().run() => panic!("reset killed service {report:?}"),
            }
        }).await.unwrap();
        let first = first_value(&owner).expect("real remote reset retains actualrole cause");
        assert!(first.1.contains("ConnectionReset"));
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.released, flow.joined), (0, 1, 3));
        assert!(flow.audit);
        let fault = Box::new(OriginalFault(facts.payload_drops.clone()));
        let identity = &*fault as *const OriginalFault as usize;
        *facts.payload.lock().unwrap() = Some(fault);
        facts.mode.store(2, Ordering::Release);
        let mut observer = Box::pin(handle.observe_shutdown());
        assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
        let original_deadline = saved.deadline.lock().unwrap().unwrap();
        let (report, phase) = tokio::time::timeout_at(original_deadline, async {
            tokio::join!(service.as_mut().unwrap().run(), observer.as_mut())
        }).await.unwrap();
        assert!(report.device_settled() && report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        println!("ownSaved COMPLETED_READY oracle phase={phase:?} rootpayload={}/original={identity} originalreport={report:?} firstVALUE={:?} Djoin/drop={}/{} wrapperReady/drop={}/{}",
            payload_identity(&saved, true), first_value(&owner), gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire), facts.ready.load(Ordering::Acquire), facts.drops.load(Ordering::Acquire));
        assert_eq!(phase, Phase::Quarantined);
        assert_eq!(payload_identity(&saved, true), identity);
        assert_eq!(facts.ready.load(Ordering::Acquire), 1);
        assert_eq!(*facts.ready_report.lock().unwrap(), Some(report));
        assert_eq!(*facts.ready_first.lock().unwrap(), Some(first.clone()));
        assert_eq!(facts.drops.load(Ordering::Acquire), 1, "root disposes actualF only AFTER originalCompletion commit");
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        service.as_ref().unwrap().completion.as_ref().unwrap()._node.as_ref().unwrap().assert_service_clean_for_test();
        assert_eq!(first_value(&owner), Some(first.clone()));
        let original = service.take().unwrap(); // Exactly one actualService Drop.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(original)));
        let escaped_identity = result.as_ref().err().and_then(|payload|
            payload.downcast_ref::<Box<OriginalFault>>().map(|payload| &**payload as *const OriginalFault as usize));
        println!("ownSaved COMPLETED_DROP oracle escaped={} payload={escaped_identity:?}/original={identity} phase={:?} parentdeadline={:?} firstVALUE={:?} originalreport={report:?} Djoin/drop={}/{} wrapperReady/drop={}/{} payloaddrop={}",
            result.is_err(), handle.phase(), *saved.deadline.lock().unwrap(), first_value(&owner),
            gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire),
            facts.ready.load(Ordering::Acquire), facts.drops.load(Ordering::Acquire), facts.payload_drops.load(Ordering::Acquire));
        assert_eq!(first_value(&owner), Some(first));
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        match result {
            Err(payload) => {
                escaped = Some(payload);
                panic!("production Service root let completed saved future Drop panic escape");
            }
            Ok(()) => (),
        }
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert_eq!(payload_identity(&saved, true), identity);
        assert_eq!(facts.payload_drops.load(Ordering::Acquire), 0);
        assert_eq!(facts.drops.load(Ordering::Acquire), 1);
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
    }).await;
    drop(stream);
    let secondary = if let Some(original) = service.take() {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(original))).err()
    } else {
        None
    };
    if let Some(p) = peer {
        p.close(0u32.into(), b"drop-finally");
    }
    server.close(0u32.into(), b"drop-finally");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Some(payload) = escaped {
        std::mem::forget(payload);
    }
    if let Some(payload) = secondary {
        std::mem::forget(payload);
    }
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}
