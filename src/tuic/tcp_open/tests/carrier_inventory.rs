//! One test-only observer follows the original UR carrier, not a replacement IO.
//! Owns one weak registration and fixed Copy snapshots; never retains Bytes.
//! The exact below covers real staged-provider custody, not Service roles or gaps.
use super::*;
use crate::upstream::{RelayFamilyPolicySources, TuicGenericStreamMode};
use std::cell::RefCell;
use std::sync::Weak;

type Shared = Arc<StdMutex<Audit>>;
thread_local! {
    static OBSERVER: RefCell<Option<Weak<StdMutex<Audit>>>> = const { RefCell::new(None) };
}

pub(crate) use crate::tuic::unordered::tests::Inventory;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Audit {
    expected_stream: Option<u64>,
    pub(crate) created: usize,
    pub(crate) reads: usize,
    pub(crate) last_read_capacity: usize,
    pub(crate) last_read_bytes: Option<usize>,
    pub(crate) first_nonzero: Option<Inventory>,
    pub(crate) latest: Option<Inventory>,
    pub(crate) before_drop: Option<Inventory>,
    pub(crate) drop_returned: bool,
}
fn snapshot(shared: &Shared) -> Audit {
    *shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
pub(crate) struct Registration;
impl Drop for Registration {
    fn drop(&mut self) {
        OBSERVER.with(|slot| *slot.borrow_mut() = None);
    }
}
fn register() -> (Shared, Registration) {
    let shared = Arc::new(StdMutex::new(Audit::default()));
    OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none(), "only one bounded carrier observer");
        *slot.borrow_mut() = Some(Arc::downgrade(&shared));
    });
    (shared, Registration)
}

pub(in crate::tuic::tcp_open) struct Observed {
    inner: Option<TuicChunkRelayStream>,
    observer: Option<Weak<StdMutex<Audit>>>,
}
pub(in crate::tuic::tcp_open) fn observe(stream: TuicChunkRelayStream) -> Observed {
    let observer = OBSERVER.with(|slot| {
        let shared = slot.borrow().as_ref().and_then(Weak::upgrade)?;
        let selected = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .expected_stream
            .is_none_or(|expected| expected == stream.inventory_for_test().stream);
        if selected {
            slot.borrow_mut().take()
        } else {
            None
        }
    });
    if let Some(shared) = observer.as_ref().and_then(Weak::upgrade) {
        let mut audit = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        audit.created += 1;
        audit.latest = Some(stream.inventory_for_test());
    }
    Observed {
        inner: Some(stream),
        observer,
    }
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let capacity = buf.remaining();
        let before = buf.filled().len();
        let result =
            Pin::new(self.inner.as_mut().expect("original live carrier")).poll_read(cx, buf);
        if let Some(shared) = self.observer.as_ref().and_then(Weak::upgrade) {
            let actual = self
                .inner
                .as_ref()
                .expect("retained original carrier")
                .inventory_for_test();
            let mut audit = shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            audit.reads += 1;
            audit.last_read_capacity = capacity;
            audit.last_read_bytes = match &result {
                Poll::Ready(Ok(())) => Some(buf.filled().len() - before),
                _ => None,
            };
            if audit.first_nonzero.is_none() && actual.stored > 0 {
                audit.first_nonzero = Some(actual);
            }
            audit.latest = Some(actual);
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.inner.as_mut().expect("original live carrier")).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().expect("original live carrier")).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().expect("original live carrier")).poll_shutdown(cx)
    }
}
impl Drop for Observed {
    fn drop(&mut self) {
        let inner = self.inner.take();
        let shared = self.observer.as_ref().and_then(Weak::upgrade);
        if let (Some(stream), Some(shared)) = (inner.as_ref(), shared.as_ref()) {
            shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .before_drop = Some(stream.inventory_for_test());
        }
        drop(inner); // Only the real destructor returning proves this observation.
        if let Some(shared) = shared {
            shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .drop_returned = true;
        }
    }
}

async fn captured<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = Box::pin(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await
}
fn first(attempt: &Attempt) -> (usize, io::ErrorKind, String) {
    let Some(ClientError::Io(error)) = attempt.failure() else {
        panic!("actual original cancellation IO")
    };
    (
        error as *const io::Error as usize,
        error.kind(),
        error.to_string(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ur_nonzero_inventory_survives_read_half_drop_until_last_half_and_provider_retirement()
 {
    let pair = UdpPair::new(8, 65536).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 65536, 100, true)));
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_unordered_reassembly: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_generic_mode(),
        TuicGenericStreamMode::UnorderedReassembly
    );
    let mut cfg = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some(crate::tuic::tcp_fixture::test_ca_path().as_str()),
        Some("h3"),
    )
    .unwrap()
    .with_relay_policy(policy);
    cfg.tcp_pool = 1;
    let mut upstream = None;
    let mut peer = None;
    let mut attempt = None;
    let mut relay = None;
    let mut read_half = None;
    let mut write_half = None;
    let mut remote = None;
    let (audit, registration) = register();
    let scenario = captured(async {
        let (u, p) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(TuicUpstream::connect(&cfg, Arc::new(Metrics::default())), async {
                pair.server.accept().await.unwrap().await.unwrap()
            })
        }).await.unwrap();
        upstream = Some(Arc::new(u.unwrap())); peer = Some(p);
        let u = upstream.as_ref().unwrap(); let p = peer.as_ref().unwrap();
        let mut auth = tokio::time::timeout(Duration::from_secs(2), p.accept_uni()).await.unwrap().unwrap();
        let actual = auth.read_to_end(64).await.unwrap();
        let mut token = [0; 32]; p.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes()).unwrap();
        assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let open_limit = Instant::now() + TUIC_OPEN_TIMEOUT;
        attempt = Some(create(u.clone(), TcpOpenRequest { target: target.clone(), deadline: open_limit }).unwrap());
        let a = attempt.as_mut().unwrap();
        tokio::time::timeout_at(open_limit.into(), async {
            while a.view().phase != TcpOpenPhase::Ready {
                a.step(&mut Context::from_waker(std::task::Waker::noop()), Instant::now());
                assert!(a.failure().is_none());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        let OpenedTcpRelay::Generic(stream) = a.take_carrier(Instant::now()).unwrap() else { panic!("actual Generic carrier") };
        relay = Some(stream);
        assert_eq!(u.tcp_pool_admission.active_total(), 1);
        remote = Some(tokio::time::timeout_at(open_limit.into(), p.accept_bi()).await.unwrap().unwrap());
        let (tx, rx) = remote.as_mut().unwrap();
        let expected = encode_connect(&target); let mut header = vec![0; expected.len()];
        tokio::time::timeout_at(open_limit.into(), rx.read_exact(&mut header)).await.unwrap().unwrap();
        assert_eq!(header, expected);
        let payload = [0x5a; 4096];
        tokio::time::timeout_at(open_limit.into(), tx.write_all(&payload)).await.unwrap().unwrap();
        let mut byte = [0];
        assert_eq!(tokio::time::timeout_at(open_limit.into(), relay.as_mut().unwrap().read(&mut byte)).await.unwrap().unwrap(), 1);
        assert_eq!(byte, [0x5a]);
        let loaded = snapshot(&audit); let inventory = loaded.latest.unwrap();
        assert_eq!(loaded.created, 1); assert!(loaded.reads > 0);
        assert_eq!(inventory.next, 1); assert!(inventory.chunks > 0 && inventory.stored > 0);
        assert_eq!(inventory.stored, inventory.independently_summed);
        assert!(inventory.front.is_some_and(|(offset, pointer, len)| offset == 1 && pointer != 0 && len > 0));
        println!("UR actual AUTH/CONNECT/read1 nonzero originalinventory={inventory:?}; backing capacity Unknown");
        let (r, w) = tokio::io::split(relay.take().unwrap()); read_half = Some(r); write_half = Some(w);
        let limit = Instant::now() + TUIC_OPEN_TIMEOUT;
        a.cancel(TcpOpenCancel::Shutdown, limit);
        let original = first(a); assert_eq!(original.1, io::ErrorKind::Interrupted);
        a.cancel(TcpOpenCancel::Shutdown, limit + Duration::from_secs(20));
        assert_eq!(a.settle_deadline, Some(limit)); assert_eq!(first(a), original);
        drop(read_half.take());
        let held = snapshot(&audit);
        assert_eq!(held.latest, Some(inventory)); assert!(held.before_drop.is_none() && !held.drop_returned);
        assert!(a.view().lease_held && a.view().retirement_pending);
        assert_eq!(u.tcp_pool_admission.active_total(), 1);
        println!("UR readhalf actualDrop with writehalf alive retains originalinventory={inventory:?} active=1 first={original:?} limit={limit:?}");
        drop(write_half.take());
        let dropped = snapshot(&audit);
        assert_eq!(dropped.before_drop, Some(inventory)); assert!(dropped.drop_returned);
        assert_eq!(u.tcp_pool_admission.active_total(), 1, "carrier Drop is not admission settlement");
        a.settle(&mut Context::from_waker(std::task::Waker::noop()), Instant::now(), limit);
        assert_eq!(a.view().phase, TcpOpenPhase::Retiring);
        assert!(a.view().lease_held && a.view().retirement_pending);
        assert_eq!(u.tcp_pool_admission.active_total(), 1);
        assert_eq!(a.settle_deadline, Some(limit)); assert_eq!(first(a), original);
        println!("UR wholecarrier actualDrop returned before originalwitness; provider stillcharged {:?}", a.view());
        tokio::time::timeout_at(limit.into(), async {
            let remote_retire = async {
                let (tx, rx) = remote.as_mut().unwrap();
                let code = tx.stopped().await.unwrap().expect("original actual STOP");
                assert!(rx.read_to_end(0).await.unwrap().is_empty());
                tx.reset(code).unwrap();
            };
            let provider_retire = async {
                while a.view().phase != TcpOpenPhase::Settled {
                    a.settle(&mut Context::from_waker(std::task::Waker::noop()), Instant::now(), limit);
                    assert_eq!(a.settle_deadline, Some(limit)); assert_eq!(first(a), original);
                    assert_ne!(a.view().phase, TcpOpenPhase::Quarantined);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::join!(remote_retire, provider_retire);
        }).await.unwrap();
        let State::Settled(receipt) = &a.state else { panic!("original settlement") };
        assert!(receipt._pressure_removed && receipt._bi_retired);
        assert!(receipt._lease.unwrap().complete());
        assert_eq!(u.tcp_pool_admission.active_total(), 0);
        assert!(!a.view().lease_held && !a.view().retirement_pending);
        assert_eq!(first(a), original); assert_eq!(snapshot(&audit).before_drop, Some(inventory));
        println!("UR actual maps+pressure+lease settled active=0 first={original:?} samefirst5={limit:?} receipt={:?}", a.view());
    }).await;
    let cleanup = captured(async {
        // Forced failure cleanup is not a settlement receipt; preserve scenario first.
        drop(read_half.take());
        drop(write_half.take());
        drop(relay.take());
        drop(remote.take());
        drop(attempt.take());
        if let Some(p) = peer.take() {
            p.close(0u32.into(), b"test done");
        }
        if let Some(u) = upstream.take() {
            let endpoint = u.endpoint.clone();
            endpoint.close(0u32.into(), b"test done");
            drop(u);
            tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle())
                .await
                .unwrap();
        }
        pair.close().await;
    })
    .await;
    drop(registration);
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
}

/// Scoped passive access to this same one carrier; no module visibility export.
pub(crate) struct Watch(Shared);
impl Watch {
    pub(crate) fn snapshot(&self) -> Audit {
        snapshot(&self.0)
    }
}
impl TuicUpstream {
    pub(crate) fn watch_ur_carrier_for_test(expected_stream: u64) -> (Watch, Registration) {
        let (shared, registration) = register();
        shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .expected_stream = Some(expected_stream);
        (Watch(shared), registration)
    }
}

#[path = "carrier_inventory/ordered_cold.rs"]
mod ordered_cold;
