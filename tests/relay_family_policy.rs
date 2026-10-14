//! Public policy injection and source reachability, without process-environment mutation.
use mini_vpn::client_tun::TunRuntimeConfig;
use mini_vpn::tuic::TuicClientConfig;
use mini_vpn::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine,
    TuicGenericStreamMode, TuicNativeOverride,
};
use std::sync::Arc;

#[test]
fn canonical_d16_gate_is_identical_in_both_configs() {
    for (input, enabled) in [
        (None, false),
        (Some("0"), false),
        (Some("false"), false),
        (Some(""), false),
        (Some("on"), false),
        (Some("yes"), false),
        (Some("1"), true),
        (Some(" TRUE "), true),
    ] {
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            d16_byte_owned: input,
            ..Default::default()
        }));
        let tun = TunRuntimeConfig::from_sources(Some("2"))
            .unwrap()
            .with_relay_policy(Arc::clone(&policy));
        let tuic = TuicClientConfig::from_sources(
            Some("127.0.0.1:443"),
            Some("00000000-0000-0000-0000-000000000001"),
            Some("test-only"),
            None,
            None,
            None,
        )
        .unwrap()
        .with_relay_policy(Arc::clone(&policy));
        assert!(Arc::ptr_eq(tun.relay_policy(), &policy));
        assert!(Arc::ptr_eq(tuic.relay_policy(), &policy));
        assert_eq!(tun.relay_policy().d16_byte_owned(), enabled, "{input:?}");
        assert_eq!(
            tuic.relay_policy().tuic_native_override() == TuicNativeOverride::D16DirectOrdered,
            enabled
        );
        assert_eq!(
            tuic.relay_policy().tuic_generic_mode(),
            TuicGenericStreamMode::OrderedJoin
        );
        assert!(Arc::ptr_eq(tuic.clone().relay_policy(), &policy));
    }
}

#[test]
fn every_selector_combination_preserves_family_precedence() {
    for bits in 0..2048u16 {
        let flag = |n: u32| -> Option<&'static str> {
            if bits & (1u16 << n) != 0u16 {
                Some("1")
            } else {
                None
            }
        };
        let policy = RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            ordered_egress_permit: flag(0),
            permit: flag(1),
            continuous: flag(2),
            thin: flag(3),
            native_egress_permit: flag(4),
            native_read_floor: flag(5),
            tuic_unordered_reassembly: flag(6),
            tuic_ordered_chunk: flag(7),
            tuic_native_chunk: flag(8),
            tuic_native_ordered: flag(9),
            d16_byte_owned: flag(10),
        });
        let generic = match bits & 15 {
            n if n & 1 != 0 => TcpRelayEngine::OrderedEgressPermitPump,
            n if n & 2 != 0 => TcpRelayEngine::PermitPump,
            n if n & 4 != 0 => TcpRelayEngine::ContinuousPump,
            n if n & 8 != 0 => TcpRelayEngine::ThinStaging,
            _ => TcpRelayEngine::Legacy,
        };
        assert_eq!(policy.generic_engine(), generic, "bits={bits}");
        assert_eq!(
            policy.native_engine(),
            if bits & 16 != 0 {
                NativeTcpRelayEngine::PermitPump
            } else {
                NativeTcpRelayEngine::ChunkPump
            }
        );
        assert_eq!(policy.native_read_floor(), bits & 32 != 0);
        assert_eq!(
            policy.tuic_generic_mode(),
            match bits & 192 {
                n if n & 64 != 0 => TuicGenericStreamMode::UnorderedReassembly,
                n if n & 128 != 0 => TuicGenericStreamMode::OrderedChunk,
                _ => TuicGenericStreamMode::OrderedJoin,
            }
        );
        assert_eq!(
            policy.tuic_native_override(),
            match bits & 1792 {
                n if n & 1024 != 0 => TuicNativeOverride::D16DirectOrdered,
                n if n & 512 != 0 => TuicNativeOverride::NativeOrderedPump,
                n if n & 256 != 0 => TuicNativeOverride::NativeChunkPump,
                _ => TuicNativeOverride::None,
            }
        );
    }
}

#[test]
fn tuic_truthy_spellings_do_not_enable_strict_tun_selectors() {
    for spelling in ["on", "yes", " On ", " YeS "] {
        let policy = RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            ordered_egress_permit: Some(spelling),
            permit: Some(spelling),
            continuous: Some(spelling),
            thin: Some(spelling),
            native_egress_permit: Some(spelling),
            native_read_floor: Some(spelling),
            tuic_unordered_reassembly: Some(spelling),
            tuic_ordered_chunk: Some(spelling),
            tuic_native_chunk: Some(spelling),
            tuic_native_ordered: Some(spelling),
            d16_byte_owned: Some(spelling),
        });
        assert_eq!(policy.generic_engine(), TcpRelayEngine::Legacy);
        assert_eq!(policy.native_engine(), NativeTcpRelayEngine::ChunkPump);
        assert!(!policy.native_read_floor());
        assert!(!policy.d16_byte_owned());
        assert_eq!(
            policy.tuic_generic_mode(),
            TuicGenericStreamMode::UnorderedReassembly
        );
        assert_eq!(
            policy.tuic_native_override(),
            TuicNativeOverride::NativeOrderedPump
        );
    }
}

#[test]
fn family_env_readers_and_actual_consumers_have_one_source() {
    let upstream = include_str!("../src/upstream.rs");
    let tun = include_str!("../src/client_tun.rs");
    let driver = include_str!("../src/client_tun/legacy_event_loop/driver.rs");
    assert_eq!(driver.matches("RelayFamilyPolicy::from_env()").count(), 0);
    let tuic = include_str!("../src/tuic.rs");
    let startup = include_str!("../src/tuic/node/startup.rs");
    let maintenance = include_str!("../src/tuic/node/maintenance.rs");
    let tcp_open = include_str!("../src/tuic/tcp_open.rs");
    for name in [
        "D11_ORDERED_EGRESS_PERMIT",
        "D2_PERMIT_TCP_RELAY",
        "CONTINUOUS_TCP_RELAY",
        "THIN_TCP_RELAY",
        "D6_NATIVE_EGRESS_PERMIT",
        "D15_NATIVE_READ_FLOOR",
        "TUIC_TCP_UNORDERED_REASSEMBLY",
        "TUIC_TCP_ORDERED_CHUNK",
        "TUIC_TCP_NATIVE_CHUNK_PUMP",
        "TUIC_TCP_NATIVE_ORDERED_PUMP",
        "H10D16_BYTE_OWNED_EGRESS",
    ] {
        let literal = format!("\"MINI_VPN_{name}\"");
        assert_eq!(upstream.matches(&literal).count(), 1, "{name}");
        assert_eq!(tun.matches(&literal).count(), 0, "TUN {name}");
        assert_eq!(driver.matches(&literal).count(), 0, "TUN driver {name}");
        assert_eq!(tuic.matches(&literal).count(), 0, "TUIC {name}");
        assert_eq!(startup.matches(&literal).count(), 0, "TUIC startup {name}");
        assert_eq!(
            maintenance.matches(&literal).count(),
            0,
            "TUIC maintenance {name}"
        );
        assert_eq!(tcp_open.matches(&literal).count(), 0, "TUIC open {name}");
    }
    let runtime = include_str!("../src/client_tun/compatibility_runtime.rs");
    assert_eq!(runtime.matches("RelayFamilyPolicy::from_env()").count(), 1);
    assert_eq!(
        runtime
            .matches("TuicClientConfig::from_env_with_relay_policy(Arc::clone(&relay_policy))")
            .count(),
        2
    );
    assert!(
        driver.contains(
            "handshake_done_tx.with_relay_policy(Arc::clone(&runtime_config.relay_policy))"
        )
    );
    let activity = include_str!("../src/client_tun/legacy_event_loop/activity.rs");
    assert!(tun.contains("ListenerActivity {"));
    assert!(tun.contains(".process(handle)"));
    assert!(activity.contains("self.handshake_done_tx.relay_policy(),"));
    assert!(activity.contains("dispatch.relay_policy(),"));
    assert!(startup.contains("relay_policy: Arc::clone(&cfg.relay_policy)"));
    assert!(tuic.contains("mod node;"));
    assert!(tuic.contains("mod tcp_open;"));
    assert!(tcp_open.contains("impl ProxyUpstream for TuicUpstream"));
    assert!(tcp_open.contains("self.relay_policy.tuic_generic_mode()"));
    assert!(tcp_open.contains("self.relay_policy.tuic_native_override()"));
    assert!(include_str!("../src/main.rs").contains("TuicClientConfig::standalone_from_env()"));
    let probe = include_str!("../src/tuic_direct_probe.rs");
    let validation = probe
        .find("validate_direct_probe_generic_mode(&relay_policy)?")
        .unwrap();
    assert!(validation < probe.find("TuicUpstream::connect(&config").unwrap());
}
