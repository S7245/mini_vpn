//! Real original Node/Quinn STREAM admission, including its io-facing carrier.
use super::*;
use crate::tuic::{NodeHost, NodeShutdown, NodeStartup};

#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_provider_query_and_final_drop_run_outside_opens() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 22).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_node_quinn_write_full_preserves_typed_refusal_and_clean_retirement() {
    facade(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_node_quinn_write_q_preserves_typed_refusal_and_original_inventory() {
    facade(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_node_quinn_empty_fin_q_preserves_typed_refusal_and_inventory() {
    facade(true, true).await;
}

async fn facade(quarantine: bool, finish: bool) {
    let identity = PublicCa::new();
    let server = quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut host = NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::join!(host.start(), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            peer
        })
    }).await.unwrap();
    assert_eq!(started, NodeStartup::Ready);
    let data = host.data().unwrap();
    let connection = crate::tuic::tcp_fixture::upstream_ready_connection(&data).unwrap();
    let mut stream = connection.open_uni().await.unwrap();
    let before = stream.progress_handle().sample().ok();
    let result = capture(async {
        let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
        let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
        assert_eq!(budget.used(), budget.limit());
        if quarantine { meter.quarantine(); }
        // The original call must be immediately Ready. No background receive,
        // timer or replacement IO error can intervene before classification.
        let written = if finish { std::task::Poll::Ready(stream.finish().map(|()| 0)) }
            else { std::pin::Pin::new(&mut stream).poll_write(&mut std::task::Context::from_waker(std::task::Waker::noop()), &[47; 145]) };
        let expected = if quarantine { bytes::MeteredCopyError::Quarantined } else { bytes::MeteredCopyError::BudgetFull };
        let std::task::Poll::Ready(Err(error)) = written else { panic!("actual native refusal: {written:?}"); };
        assert_eq!(error, quinn::WriteError::Admission(expected));
        let io = std::io::Error::from(error);
        assert_eq!(io.get_ref().unwrap().downcast_ref::<quinn::owned_events::AdmissionError>().unwrap().0, expected);
        let after = stream.progress_handle().sample().ok();
        if quarantine { assert_eq!(after, before, "Q before allocation must preserve the original absence"); }
        else { assert_eq!(after.unwrap().written_bytes, 0); }
        assert!(connection.close_reason().is_some());
        drop(pressure);
        assert_eq!(budget.quarantined(), quarantine);
    }).await;
    drop(stream);
    drop(connection);
    drop(data);
    if quarantine {
        drop(host);
        assert!(budget.quarantined() && budget.used() > 0);
    } else {
        let shutdown = tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown()).await.unwrap();
        assert!(matches!(shutdown, NodeShutdown::RunningResourcesSettled | NodeShutdown::FailedSettled));
        drop(host);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
    peer.close(0u32.into(), b"send inventory test");
    server.close(0u32.into(), b"send inventory test");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle()).await.unwrap();
    if let Err(payload) = result { std::panic::resume_unwind(payload); }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_provider_query_panic_preserves_original_box_and_f() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 23).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_provider_drop_panic_preserves_original_box_and_f() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 24).await;
}

struct OriginalPanic { drops: Arc<std::sync::atomic::AtomicUsize> }
impl Drop for OriginalPanic {
    fn drop(&mut self) { self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
}
pub(super) struct ProviderProbe {
    // Retire the passive observer before its captured state.
    _registration: Box<dyn std::any::Any>,
    counts: Arc<[std::sync::atomic::AtomicUsize; 4]>,
    address: usize,
    drops: Arc<std::sync::atomic::AtomicUsize>,
    mode: u8,
}
impl ProviderProbe {
    pub(super) fn install(owner: std::sync::Weak<SessionControl>, mode: u8) -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counts = Arc::new(std::array::from_fn::<_, 4, _>(|_| AtomicUsize::new(0)));
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = Box::new(OriginalPanic { drops: drops.clone() });
        let address = &*payload as *const OriginalPanic as usize;
        let payload = std::cell::RefCell::new(Some(payload));
        if mode == 28 { owner.upgrade().unwrap().arm_before_release_query_q(); }
        let captured = counts.clone();
        let registration = crate::owned_upstream::LegacyOwnedUpstream::<crate::tuic::TuicUpstream>::observe_provider_for_test(move |dropping| {
            let index = usize::from(dropping);
            captured[index].fetch_add(1, Ordering::SeqCst);
            if let Some(owner) = owner.upgrade() {
                if owner.opens.try_lock().is_err() { captured[index + 2].fetch_add(1, Ordering::SeqCst); }
                else if self_mode_reentry(mode, dropping) { owner.assert_release_reentry(); }
                if mode == 25 && !dropping { owner.invalidate_release_epoch(); }
                if mode == 26 && dropping { owner.quarantine_quiet(); }
                if mode == 27 && !dropping { owner.invalidate_release_accounting(); }
                if mode == 30 && !dropping { owner.replace_release_root(); }
            }
            if (mode == 23 && !dropping) || (mode == 24 && dropping) {
                std::panic::panic_any(payload.borrow_mut().take().expect("only the original one fault"));
            }
        });
        Self { _registration: Box::new(registration), counts, address, drops, mode }
    }
    pub(super) fn assert_clean(&self) {
        SessionControl::assert_bounded_release_audits();
        assert_eq!(self.counts.each_ref().map(|count| count.load(std::sync::atomic::Ordering::SeqCst)), [1, 1, 0, 0]);
    }
    pub(super) fn assert_fault(&self, owner: &SessionControl, deadline: tokio::time::Instant) {
        use std::sync::atomic::Ordering;
        assert_eq!(self.counts.each_ref().map(|count| count.load(Ordering::SeqCst)), [usize::from(self.mode != 28), usize::from(matches!(self.mode, 24 | 26)), 0, 0]);
        if self.mode == 25 { owner.assert_lost_release_epoch(); }
        else if matches!(self.mode, 26 | 28) { owner.assert_release_drop_q(deadline, self.mode == 28); }
        else if matches!(self.mode, 27 | 30) { owner.assert_invalid_release_accounting(); }
        else { owner.assert_release_fault::<OriginalPanic>(self.address, deadline, self.mode == 23); }
        assert_eq!(self.drops.load(Ordering::SeqCst), 0);
    }
}

fn self_mode_reentry(mode: u8, dropping: bool) -> bool { !matches!(mode, 25 | 27 | 30) || !dropping }
#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_provider_epoch_change_keeps_remaining_originals() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 25).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_provider_drop_q_keeps_upstream_and_roles() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 26).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_provider_accounting_change_keeps_remaining_originals() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 27).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_service_q_before_provider_query_keeps_all_originals() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 28).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_node_staged_connect_full_preserves_original_admission() { staged_refusal(false).await; }
#[tokio::test(flavor = "current_thread")]
async fn actual_node_staged_connect_q_preserves_original_admission() { staged_refusal(true).await; }
async fn staged_refusal(quarantine: bool) {
    use crate::owned_upstream::{LegacyOwnedUpstream, OwnedUpstream};
    use crate::upstream::{TcpOpenPhase, TcpOpenRequest};
    let identity = PublicCa::new();
    let server = quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut host = NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::join!(host.start(), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            peer
        })
    }).await.unwrap();
    assert_eq!(started, NodeStartup::Ready);
    let data = host.data().unwrap();
    let adapter = Arc::new(LegacyOwnedUpstream::new(data));
    let mut attempt = adapter.clone().staged_tcp_open(TcpOpenRequest {
        target: TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()),
        deadline: (tokio::time::Instant::now() + Duration::from_secs(5)).into_std(),
    }).unwrap();
    let box_identity = &*attempt as *const _ as *const () as usize;
    let mut source_identity = None;
    let result = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while attempt.view().phase != TcpOpenPhase::Connecting {
                attempt.step(&mut std::task::Context::from_waker(std::task::Waker::noop()), tokio::time::Instant::now().into_std());
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert_eq!(attempt.view().connect_written, 0);
        let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
        let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
        assert_eq!(budget.used(), budget.limit());
        if quarantine { meter.quarantine(); }
        attempt.step(&mut std::task::Context::from_waker(std::task::Waker::noop()), tokio::time::Instant::now().into_std());
        let expected = if quarantine { bytes::MeteredCopyError::Quarantined } else { bytes::MeteredCopyError::BudgetFull };
        let cause = |attempt: &dyn crate::upstream::TcpOpenAttempt<crate::owned_upstream::OpenedOwnedTcp>| {
            let ClientError::Io(io) = attempt.failure().expect("actual CONNECT refusal") else { panic!("original IO"); };
            let source = io.get_ref().unwrap().downcast_ref::<quinn::owned_events::AdmissionError>().unwrap();
            assert_eq!(source.0, expected);
            source as *const _ as usize
        };
        let original = cause(&*attempt);
        source_identity = Some(original);
        assert_eq!(attempt.view().connect_written, 0);
        drop(pressure);
        if !quarantine {
            let deadline = (tokio::time::Instant::now() + Duration::from_secs(15)).into_std();
            tokio::time::timeout(Duration::from_secs(15), async {
                while attempt.view().phase != TcpOpenPhase::Settled {
                    attempt.settle(&mut std::task::Context::from_waker(std::task::Waker::noop()), tokio::time::Instant::now().into_std(), deadline);
                    assert_eq!(cause(&*attempt), original);
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
        }
        assert_eq!(cause(&*attempt), original, "retirement keeps the original typed error object");
    }).await;
    let retained = if quarantine { drop(attempt); None } else { Some(attempt) };
    drop(adapter);
    if quarantine {
        drop(host);
        assert!(budget.quarantined() && budget.used() > 0);
    } else {
        let shutdown = tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown()).await.unwrap();
        assert!(matches!(shutdown, NodeShutdown::RunningResourcesSettled | NodeShutdown::FailedSettled));
        drop(host);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        let original = retained.as_ref().unwrap();
        assert_eq!(&**original as *const _ as *const () as usize, box_identity);
        let ClientError::Io(error) = original.failure().unwrap() else { panic!("original failure"); };
        let source = error.get_ref().unwrap().downcast_ref::<quinn::owned_events::AdmissionError>().unwrap();
        assert_eq!(Some(source as *const _ as usize), source_identity);
    }
    drop(retained);
    peer.close(0u32.into(), b"staged admission test");
    server.close(0u32.into(), b"staged admission test");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle()).await.unwrap();
    if let Err(payload) = result { std::panic::resume_unwind(payload); }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_provider_whole_root_replacement_invalidates_release_audit() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 30).await;
}
