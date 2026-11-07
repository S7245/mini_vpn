//! Source/receipt control tracers only; true Node/QUIC coverage lives in service tests.
use super::*;
use crate::failover::{FailoverUpstream, HealthProbe};
use crate::owned_upstream::{LocalAssociationId, UdpDownlink};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Idle;
#[async_trait::async_trait]
impl HealthProbe for Idle {
    async fn probe(&self) -> bool {
        true
    }
    async fn is_dead(&self) -> bool {
        false
    }
    async fn rx_datagrams(&self) -> Option<u64> {
        Some(1)
    }
}
fn task() -> crate::failover::HealthTask {
    Arc::new(FailoverUpstream::new(Arc::new(Idle), Arc::new(()))).spawn_health_probe()
}
struct Payload {
    drops: Arc<AtomicUsize>,
    panic: bool,
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        b"tail"
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "original source payload destructor");
    }
}
fn packet(drops: Arc<AtomicUsize>, panic: bool) -> UdpDownlink {
    UdpDownlink::new(
        LocalAssociationId::new(1).unwrap(),
        bytes::Bytes::from_owner(Payload { drops, panic }),
    )
}

#[tokio::test(start_paused = true)]
async fn actual_source_eof_waits_for_consuming_original_health_join() {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    drop(sender);
    let mut task = task();
    let owner = SessionControl::default();
    let mut saved = Box::pin(settle(
        UdpDownlinkSource::owned(receiver),
        task.join_observation(),
        &owner,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(2), saved.as_mut())
            .await
            .is_err()
    );
    assert!(!owner.ledger().quarantined);
    task.request_stop();
    assert_eq!(task.join().await, crate::failover::HealthExit::Stopped);
    tokio::time::timeout(Duration::from_millis(2), saved.as_mut())
        .await
        .unwrap();
    assert!(owner.ledger().first_failure.is_none());
}

#[tokio::test(start_paused = true)]
async fn stopped_payload_drop_failure_keeps_original_receiver_and_q_after_health_join() {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let bad = Arc::new(AtomicUsize::new(0));
    let held = Arc::new(AtomicUsize::new(0));
    sender.send(packet(bad.clone(), true)).await.unwrap();
    sender.send(packet(held.clone(), false)).await.unwrap();
    let mut task = task();
    let owner = SessionControl::default();
    let mut saved = Box::pin(settle(
        UdpDownlinkSource::owned(receiver),
        task.join_observation(),
        &owner,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(2), saved.as_mut())
            .await
            .is_err()
    );
    assert_eq!(bad.load(Ordering::SeqCst), 1);
    assert_eq!(held.load(Ordering::SeqCst), 0);
    assert!(!sender.is_closed());
    let first = {
        let ledger = owner.ledger();
        assert!(ledger.quarantined);
        assert_eq!(
            ledger.first_failure.as_ref().unwrap().facts().class,
            crate::shared::errors::FailureClass::Panic
        );
        ledger.first_failure.as_ref().unwrap() as *const RetainedFailure
    };
    task.request_stop();
    task.join().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(2), saved.as_mut())
            .await
            .is_err()
    );
    assert_eq!(
        owner.ledger().first_failure.as_ref().unwrap() as *const RetainedFailure,
        first
    );
    assert_eq!(held.load(Ordering::SeqCst), 0);
    assert!(!sender.is_closed());
    drop(saved); // forced destruction of Q, not successful asynchronous settlement
    assert_eq!(held.load(Ordering::SeqCst), 1);
    assert!(sender.is_closed());
}

#[tokio::test(start_paused = true)]
async fn unknown_source_destruction_retains_failure_and_nonzero_session_quarantine() {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let drops = Arc::new(AtomicUsize::new(0));
    sender.send(packet(drops.clone(), true)).await.unwrap();
    let mut task = task();
    let receipt = task.join_observation();
    task.request_stop();
    task.join().await;
    let owner = SessionControl::default();
    let _custody = settle(UdpDownlinkSource::owned(receiver), receipt, &owner).await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(owner.ledger().quarantined);
    assert_eq!(
        owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .origin
            .phase,
        FailurePhase::SessionLoopDrop
    );
    assert!(!owner.driver_complete.load(Ordering::Acquire));
}

struct TerminalPayload(Arc<AtomicUsize>);
impl Drop for TerminalPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("terminal payload must remain charged Q");
    }
}
struct SecondaryPayload(Arc<AtomicUsize>, Arc<AtomicUsize>);
impl Drop for SecondaryPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(TerminalPayload(self.1.clone()));
    }
}
struct NestedPayload {
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
    terminal: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for NestedPayload {
    fn as_ref(&self) -> &[u8] {
        b"tail"
    }
}
impl Drop for NestedPayload {
    fn drop(&mut self) {
        self.primary.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(SecondaryPayload(
            self.secondary.clone(),
            self.terminal.clone(),
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn secondary_panic_payload_never_escapes_or_destroys_the_original_source() {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let primary = Arc::new(AtomicUsize::new(0));
    let secondary = Arc::new(AtomicUsize::new(0));
    let terminal = Arc::new(AtomicUsize::new(0));
    let held = Arc::new(AtomicUsize::new(0));
    sender
        .send(UdpDownlink::new(
            LocalAssociationId::new(1).unwrap(),
            bytes::Bytes::from_owner(NestedPayload {
                primary: primary.clone(),
                secondary: secondary.clone(),
                terminal: terminal.clone(),
            }),
        ))
        .await
        .unwrap();
    sender.send(packet(held.clone(), false)).await.unwrap();
    let mut task = task();
    let owner = SessionControl::default();
    let mut saved = Box::pin(settle(
        UdpDownlinkSource::owned(receiver),
        task.join_observation(),
        &owner,
    ));
    let observed = {
        let observe = tokio::time::timeout(Duration::from_millis(2), saved.as_mut());
        tokio::pin!(observe);
        std::future::poll_fn(|cx| {
            match catch_unwind(AssertUnwindSafe(|| observe.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                Err(payload) => Poll::Ready(Err(payload)),
            }
        })
        .await
    };
    assert!(
        observed.is_ok(),
        "secondary panic escaped the original saved source future"
    );
    assert!(observed.unwrap().is_err());
    assert_eq!(
        (
            primary.load(Ordering::SeqCst),
            secondary.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert!(owner.ledger().quarantined);
    assert_eq!(owner.ledger().additional_failures, 1);
    assert_eq!(owner.ledger().unknown_panic_payloads, 1);
    assert_eq!(terminal.load(Ordering::SeqCst), 0);
    assert_eq!(held.load(Ordering::SeqCst), 0);
    assert!(!sender.is_closed());
    task.request_stop();
    task.join().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(2), saved.as_mut())
            .await
            .is_err()
    );
    assert_eq!(held.load(Ordering::SeqCst), 0);
    assert!(!sender.is_closed());
    drop(saved);
    assert_eq!(held.load(Ordering::SeqCst), 1);
    drop(owner);
    assert_eq!(
        terminal.load(Ordering::SeqCst),
        0,
        "forced Q shell destruction never retries terminal destructor"
    );
}
