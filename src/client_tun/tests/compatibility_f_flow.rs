//! Actual saved-loop F path over an authenticated loopback TUIC peer.
//! Uses real packet/socket/provider components, never direct slot fill.
use super::*;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use std::panic::{AssertUnwindSafe, catch_unwind};

async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = Box::pin(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

#[tokio::test]
async fn saved_f_root_forwards_real_tuic_data_and_settles_before_device() {
    run_saved_f(CloseCase::Fin).await;
}

#[tokio::test]
async fn saved_f_root_joins_original_native_chunk_carrier_before_device() {
    run_saved_f(CloseCase::NativeChunkFin).await;
}

#[tokio::test]
async fn saved_f_root_joins_original_native_permit_carrier_before_device() {
    run_saved_f(CloseCase::NativePermitFin).await;
}

#[tokio::test]
async fn saved_f_root_joins_original_byte_owned_carrier_before_device() {
    run_saved_f(CloseCase::NativeByteOwnedFin).await;
}

#[tokio::test]
async fn saved_f_root_disposes_original_byte_owned_queue_on_reset() {
    run_saved_f(CloseCase::NativeByteOwnedReset).await;
}

#[tokio::test]
async fn saved_f_root_disposes_original_native_chunk_on_reset() {
    run_saved_f(CloseCase::NativeChunkReset).await;
}

#[tokio::test]
async fn saved_f_root_disposes_original_native_permit_on_reset() {
    run_saved_f(CloseCase::NativePermitReset).await;
}

#[tokio::test]
async fn saved_f_foreign_dead_inflight_preserves_first_fault_at_flush() {
    run_saved_f(CloseCase::NativePermitForeignFlush).await;
}

#[tokio::test]
async fn saved_f_foreign_dead_inflight_preserves_first_fault_at_ack() {
    run_saved_f(CloseCase::NativePermitForeignAck).await;
}

#[tokio::test]
async fn saved_f_shutdown_drains_queued_data_under_zero_window_pressure() {
    run_saved_f(CloseCase::Shutdown).await;
}

#[tokio::test]
async fn saved_f_local_reset_disposes_queued_data_without_stopping_session() {
    run_saved_f(CloseCase::Reset).await;
}

#[tokio::test]
async fn saved_f_reset_after_remote_failure_rechecks_dead_pending() {
    run_saved_f(CloseCase::ResetAfterClose).await;
}

#[tokio::test]
async fn saved_f_reset_after_provider_retirement_disposes_dead_pending() {
    run_saved_f(CloseCase::ResetAfterRetire).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CloseCase {
    Fin,
    NativeChunkFin,
    NativePermitFin,
    NativeChunkReset,
    NativePermitReset,
    NativePermitForeignFlush,
    NativePermitForeignAck,
    NativeByteOwnedFin,
    NativeByteOwnedReset,
    Shutdown,
    Reset,
    ResetAfterClose,
    ResetAfterRetire,
    ReceiptHold,
    ReceiptWrong,
    ReceiptWindow,
}

#[path = "compatibility_f_flow/runner.rs"]
mod runner;
use runner::run_saved_f;

#[path = "compatibility_f_flow/published_receipt.rs"]
mod published_receipt;

#[path = "compatibility_f_reset.rs"]
pub(in crate::client_tun::compatibility_runtime::tests) mod reset;

#[path = "compatibility_f_flow/egress_failure.rs"]
mod egress_failure;

#[path = "compatibility_f_flow/panic_failure.rs"]
mod panic_failure;

#[path = "compatibility_f_flow/final_owner.rs"]
mod final_owner;

#[path = "compatibility_f_flow/isolation.rs"]
mod isolation;

#[path = "compatibility_f_flow/dual.rs"]
mod dual;
