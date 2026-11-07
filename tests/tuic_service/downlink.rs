//! Public wrappers for the shared real downlink scenario.
use super::*;
#[path = "downlink_fixture.rs"]
mod fixture;
fn make_app(device: LoopbackTunDevice) -> Apps {
    Apps::new(device)
}

#[tokio::test(flavor = "current_thread")]
async fn public_full_app_window_keeps_sibling_then_clean_unique_successor_tcp_udp() {
    fixture::exercise(None, fixture::Case::Original128KiB, build).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_thin_full_app_window_keeps_sibling_then_clean_unique_successor_tcp_udp() {
    use mini_vpn::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    fixture::exercise(
        Some(Arc::new(RelayFamilyPolicy::from_sources(
            RelayFamilyPolicySources {
                thin: Some("1"),
                ..Default::default()
            },
        ))),
        fixture::Case::Original128KiB,
        build,
    )
    .await;
}

fn build(
    device: LoopbackTunDevice,
    cfg: TuicClientConfig,
    runtime: TunRuntimeConfig,
) -> (impl TunService, fixture::NoObserver) {
    (
        prepare_tuic_service(device, cfg, runtime, Arc::new(Metrics::default()), NoopSink),
        fixture::NoObserver,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn public_continuous_full_app_window_keeps_sibling_then_clean_unique_successor_tcp_udp() {
    use mini_vpn::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    fixture::exercise(
        Some(Arc::new(RelayFamilyPolicy::from_sources(
            RelayFamilyPolicySources {
                continuous: Some("1"),
                ..Default::default()
            },
        ))),
        fixture::Case::Continuous128KiB,
        build,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_permit_full_app_window_keeps_sibling_then_clean_unique_successor_tcp_udp() {
    use mini_vpn::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    fixture::exercise(
        Some(Arc::new(RelayFamilyPolicy::from_sources(
            RelayFamilyPolicySources {
                permit: Some("1"),
                ..Default::default()
            },
        ))),
        fixture::Case::Permit128KiB,
        build,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_ordered_full_app_window_keeps_sibling_then_clean_unique_successor_tcp_udp() {
    use mini_vpn::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    fixture::exercise(
        Some(Arc::new(RelayFamilyPolicy::from_sources(
            RelayFamilyPolicySources {
                ordered_egress_permit: Some("1"),
                ..Default::default()
            },
        ))),
        fixture::Case::Ordered128KiB,
        build,
    )
    .await;
}
