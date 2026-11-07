//! Original Permit Source/Root/Main/Device, including the actual final loan.
//! The shared real Service fixture retains all lifecycle and receipt authority.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn original_permit_last_alias_keeps_f_until_actual_stock_retirement() {
    complete_service_observed(true, ServiceUdp::None, terminal::Ending::Normal, 15).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_permit_lost_retirement_turn_keeps_stock_first_deadline_and_charged_q() {
    complete_service_observed(true, ServiceUdp::None, terminal::Ending::Normal, 16).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_permit_lost_source_disposal_keeps_actual_actor() {
    complete_service_observed(true, ServiceUdp::None, terminal::Ending::Normal, 12).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_permit_two_roots_backing_full_recovers_only_from_actual_app_ack() {
    super::direct::two_flow_case(true, true, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_permit_receipt_rejects_zero_byte_old_binding_in_new_epoch() {
    complete_service_observed(true, ServiceUdp::None, terminal::Ending::Normal, 31).await;
}
