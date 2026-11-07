//! Real 64KiB physical pending-window pressure at the saved service. The shared
//! endpoint scenario owns IO; this observer reads the original root only. It
//! creates no receipt, lifecycle state, actor or production observation bypass.
use super::fixture::{capture, Apps};
use super::*;
use crate as mini_vpn;
use crate::client_tun::TuicServicePhase;
use smoltcp::iface::SocketHandle;
use smoltcp::socket::tcp;
use smoltcp::wire::Ipv4Address;
use std::cell::OnceCell;
use std::future::Future;
use std::task::Poll;

#[path = "../../../../../tests/tuic_service/downlink_fixture.rs"]
mod window;

fn make_app(device: LoopbackTunDevice) -> Apps {
    Apps::from_device_with_target(device, std::net::Ipv4Addr::new(192, 0, 2, 10))
}
fn server() -> quinn::Endpoint {
    quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap()
}
struct OriginalRoot {
    service: Arc<Control>,
    original: OnceCell<Arc<SessionControl>>,
    joins_per_flow: u64,
}
impl window::Observer for OriginalRoot {
    fn snapshot(&self, key: Option<window::FlowKey>) -> Option<window::Snapshot> {
        let current = self.service.session.lock().unwrap().as_ref()?.upgrade()?;
        let original = self.original.get_or_init(|| current.clone());
        assert!(
            Arc::ptr_eq(&current, original),
            "same original SessionArc throughout full-window pressure"
        );
        assert!(original.ledger().first_failure.is_none());
        let mut matches = original.flow_contracts().into_iter().filter(|flow| {
            let id = window::FlowKey {
                index: flow.id.index,
                handle: flow.id.handle,
                epoch: flow.id.epoch,
            };
            match key {
                Some(key) => id == key,
                None => flow.active && flow.published && flow.f && flow.outstanding == Some(65_536),
            }
        });
        let flow = matches.next()?;
        assert!(matches.next().is_none(), "unique original saturated A");
        assert!(flow.active && flow.published && flow.f && flow.attempt && !flow.w);
        Some(window::Snapshot {
            key: window::FlowKey {
                index: flow.id.index,
                handle: flow.id.handle,
                epoch: flow.id.epoch,
            },
            slot_deadline: flow.slot_deadline,
            open_deadline: flow.open_deadline,
            active: flow.active,
            outstanding: flow.outstanding?,
        })
    }
    fn old_settled(&self) -> bool {
        let original = self
            .original
            .get()
            .expect("original pressure root selected");
        let actual = original.flow_observation().unwrap();
        println!(
            "original private64K root after actual old peer closed BEFORE successor handshake {actual:?}"
        );
        assert!(actual.audit);
        assert_eq!(
            (
                actual.live,
                actual.w,
                actual.f,
                actual.reserved,
                actual.escrow_bytes,
                actual.running,
                actual.attempts
            ),
            (0, 0, 0, 0, 0, 0, 0)
        );
        assert_eq!(
            (
                actual.released,
                actual.joined,
                actual.published,
                actual.factory_started
            ),
            (2, 2 * self.joins_per_flow, 2, 2)
        );
        assert!(original.ledger().first_failure.is_none());
        true
    }
}
fn build(
    device: LoopbackTunDevice,
    cfg: TuicClientConfig,
    runtime: TunRuntimeConfig,
) -> (impl TunService, OriginalRoot) {
    let joins_per_flow =
        if cfg.relay_policy().generic_engine() == crate::upstream::TcpRelayEngine::Legacy {
            3
        } else {
            4
        };
    let service = prepare(device, cfg, runtime, Arc::new(Metrics::default()), NoopSink);
    let observer = OriginalRoot {
        service: service.control.clone(),
        original: OnceCell::new(),
        joins_per_flow,
    };
    (service, observer)
}
#[tokio::test(flavor = "current_thread")]
async fn real_private_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor() {
    use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        thin: Some("1"),
        ..Default::default()
    }));
    window::exercise(Some(policy), window::Case::ExactWindow256KiB, build).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_private_continuous_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor() {
    use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        continuous: Some("1"),
        ..Default::default()
    }));
    window::exercise(
        Some(policy),
        window::Case::ContinuousExactWindow256KiB,
        build,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_private_permit_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor() {
    use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        permit: Some("1"),
        ..Default::default()
    }));
    window::exercise(Some(policy), window::Case::PermitExactWindow256KiB, build).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_private_ordered_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor() {
    use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        ordered_egress_permit: Some("1"),
        ..Default::default()
    }));
    window::exercise(Some(policy), window::Case::OrderedExactWindow256KiB, build).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_private_legacy_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor() {
    use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
    let policy = Arc::new(RelayFamilyPolicy::from_sources(
        RelayFamilyPolicySources::default(),
    ));
    window::exercise(Some(policy), window::Case::LegacyExactWindow256KiB, build).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_private_ordered_chunk_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor() {
    use crate::upstream::{
        RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine, TuicGenericStreamMode,
        TuicNativeOverride,
    };
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        thin: Some("1"),
        tuic_ordered_chunk: Some("1"),
        ..Default::default()
    }));
    assert_eq!(policy.generic_engine(), TcpRelayEngine::ThinStaging);
    assert_eq!(
        policy.tuic_generic_mode(),
        TuicGenericStreamMode::OrderedChunk
    );
    assert_eq!(policy.tuic_native_override(), TuicNativeOverride::None);
    println!(
        "actual savedService TUIC Generic carrier mode={:?} engine={:?}",
        policy.tuic_generic_mode(),
        policy.generic_engine()
    );
    window::exercise(Some(policy), window::Case::ExactWindow256KiB, build).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_private_unordered_reassembly_64k_window_keeps_healthy_tcp_udp_then_clean_unique_successor(
) {
    use crate::upstream::{
        RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine, TuicGenericStreamMode,
        TuicNativeOverride,
    };
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        thin: Some("1"),
        tuic_unordered_reassembly: Some("1"),
        ..Default::default()
    }));
    assert_eq!(policy.generic_engine(), TcpRelayEngine::ThinStaging);
    assert_eq!(
        policy.tuic_generic_mode(),
        TuicGenericStreamMode::UnorderedReassembly
    );
    assert_eq!(policy.tuic_native_override(), TuicNativeOverride::None);
    println!(
        "actual savedService TUIC Generic carrier mode={:?} engine={:?}",
        policy.tuic_generic_mode(),
        policy.generic_engine()
    );
    window::exercise(Some(policy), window::Case::ExactWindow256KiB, build).await;
}
