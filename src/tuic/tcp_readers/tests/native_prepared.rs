//! Passive fixed-value observations of the real Native ordered source.
//! This exact uses original AUTH/CONNECT and the public async factory. It never
//! supplies a replacement reader, starts a task, or opens the saved-root guards.
use crate::shared::TargetAddr;
use crate::tuic::tcp_fixture::UdpPair;
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use crate::upstream::{
    OpenedTcpRelay, ProxyUpstream, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;
use std::time::Duration;

pub(in crate::tuic) type Shared = Arc<Mutex<Audit>>;
thread_local! {
    static ACTIVE: RefCell<Option<Weak<Mutex<Audit>>>> = const { RefCell::new(None) };
}
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::tuic) struct Audit {
    stream: Option<u64>,
    created: usize,
    polls: usize,
    read_bytes: usize,
}
fn visit(stream: u64, action: impl FnOnce(&mut Audit)) {
    ACTIVE.with(|slot| {
        if let Some(shared) = slot.borrow().as_ref().and_then(Weak::upgrade) {
            let mut audit = shared.lock().unwrap();
            if audit.stream.is_none_or(|expected| expected == stream) {
                audit.stream = Some(stream);
                action(&mut audit);
            }
        }
    });
}
pub(in crate::tuic::tcp_readers) fn created(stream: u64) {
    visit(stream, |audit| audit.created += 1);
}
pub(in crate::tuic::tcp_readers) fn polled(stream: u64) {
    visit(stream, |audit| audit.polls += 1);
}
pub(in crate::tuic::tcp_readers) fn read(stream: u64, bytes: usize) {
    visit(stream, |audit| audit.read_bytes += bytes);
}
pub(in crate::tuic) struct Registration;
impl Drop for Registration {
    fn drop(&mut self) {
        ACTIVE.with(|slot| *slot.borrow_mut() = None);
    }
}
pub(in crate::tuic) fn register() -> (Shared, Registration) {
    let shared = Arc::new(Mutex::new(Audit::default()));
    ACTIVE.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Arc::downgrade(&shared));
    });
    (shared, Registration)
}
async fn capture<F: Future>(future: F) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut future = std::pin::pin!(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn actual_auth_native_public_open_keeps_source_cold_before_session_commit() {
    let limit = tokio::time::Instant::now() + Duration::from_secs(5);
    let pair = tokio::time::timeout_at(limit, UdpPair::new(8, 65536))
        .await
        .unwrap();
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 65536, 100, true)));
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    let mut cfg = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap()
    .with_relay_policy(policy);
    cfg.tcp_pool = 1;
    let mut upstream = None;
    let mut peer = None;
    let mut carrier = None;
    let mut remote = None;
    let mut actual_pump_joined = false;
    let mut committed_reader = None;
    let mut committed_writer = None;
    let mut committed_pump = None;
    let (audit, registration) = register();
    let result = capture(tokio::time::timeout_at(limit, async {
        let (u, p) = tokio::join!(
            TuicUpstream::connect(&cfg, Arc::new(crate::metrics::Metrics::default())),
            async { pair.server.accept().await.unwrap().await.unwrap() }
        );
        upstream = Some(u.unwrap()); peer = Some(p);
        let u = upstream.as_ref().unwrap(); let p = peer.as_ref().unwrap();
        let mut auth = p.accept_uni().await.unwrap();
        let actual_auth = auth.read_to_end(64).await.unwrap();
        let mut token = [0; 32];
        p.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes()).unwrap();
        assert_eq!(actual_auth, encode_authenticate(&cfg.uuid, &token));
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        carrier = Some(u.open_tcp_relay(&target).await.unwrap());
        assert!(matches!(carrier, Some(OpenedTcpRelay::Native(_))));
        remote = Some(p.accept_bi().await.unwrap());
        let (send, recv) = remote.as_mut().unwrap();
        let expected = encode_connect(&target); let mut actual = vec![0; expected.len()];
        recv.read_exact(&mut actual).await.unwrap(); assert_eq!(actual, expected);
        assert_eq!(actual.len(), 9);
        send.write_all(&[0x5a; 512]).await.unwrap(); send.finish().unwrap();
        for _ in 0..16 { tokio::task::yield_now().await; }
        let observed = *audit.lock().unwrap();
        println!("B72 actual AUTH/CONNECT9 public Native carrier retained and never polled by caller; source={observed:?}, activeLeases={}",u.tcp_pool_admission.active_total());
        assert_eq!(observed.created, 1, "actual original Native ordered source");
        assert_eq!(observed.stream, Some(recv.id().index()));
        assert_eq!(observed.polls, 0, "S1: original source must stay unpolled before session commit");
        assert_eq!(observed.read_bytes, 0, "unpublished carrier must not ingest DATA");
        let Some(OpenedTcpRelay::Native(native)) = carrier.take() else { panic!("same Native carrier"); };
        let (reader, writer, prepared) = native.into_parts();
        committed_reader = Some(reader); committed_writer = Some(writer);
        committed_pump = Some(prepared.expect("original prepared source").commit(None));
        let reader = committed_reader.as_mut().unwrap();
        let chunk = poll_fn(|cx| reader.poll_read_chunk(cx, 512)).await.unwrap().unwrap();
        assert_eq!(chunk.offset, 0);
        assert_eq!(&chunk.bytes[..], &[0x5a; 512]);
        assert!(poll_fn(|cx| reader.poll_read_chunk(cx, 512)).await.unwrap().is_none());
        assert_eq!(u.tcp_pool_admission.active_total(), 2);
        drop(committed_reader.take());
        let pump = committed_pump.as_mut().unwrap();
        let receipt = pump.settle().await.expect("actual original pump join and disposition");
        actual_pump_joined = pump.joined();
        assert!(actual_pump_joined);
        assert_eq!(receipt.forwarded_bytes, 512);
        assert_eq!(receipt.queue_discarded_bytes + receipt.reader_discarded_bytes + receipt.producer_discarded_bytes, 0);
        assert_eq!(u.tcp_pool_admission.active_total(), 1, "original Native writer lease remains");
        drop(committed_writer.take());
        assert_eq!(u.tcp_pool_admission.active_total(), 0);
    })).await;
    // Preserve the primary failure. Closing true endpoints is cleanup only;
    // endpoint cleanup is never counted as consuming an actor JoinHandle.
    let cleanup = capture(async {
        drop(committed_reader.take());
        if let Some(pump) = committed_pump.as_mut() {
            pump.stop();
            let settled = tokio::time::timeout_at(limit, pump.settle()).await;
            println!(
                "B73 retained same-handle cleanup actualJoin={} receipt={}",
                pump.joined(),
                matches!(settled, Ok(Ok(_)))
            );
        }
        drop(committed_writer.take());
        drop(committed_pump.take());
        drop(carrier.take());
        drop(remote.take());
        if let Some(p) = peer.take() {
            p.close(0u32.into(), b"B72 fixture cleanup");
        }
        drop(upstream.take());
        pair.close().await;
    })
    .await;
    drop(registration);
    println!(
        "B73 endpoint cleanup returned={}, actual pump join in scenario={actual_pump_joined}",
        cleanup.is_ok()
    );
    match result {
        Err(payload) => resume_unwind(payload),
        Ok(Err(error)) => panic!("original single5 expired: {error}"),
        Ok(Ok(())) => {}
    }
    if let Err(payload) = cleanup {
        resume_unwind(payload);
    }
}

pub(in crate::tuic) fn cold_counts(shared: &Shared) -> (usize, usize, usize) {
    let audit = shared.lock().unwrap();
    (audit.created, audit.polls, audit.read_bytes)
}
