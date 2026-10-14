use std::fs;
use std::path::{Path, PathBuf};

#[path = "support/rust_source_scan.rs"]
mod rust_source_scan;
use rust_source_scan::{inline_cfg_test_module_names, inline_rust_module_names};

#[path = "repository_test_module_provenance/owner_bound_service.rs"]
mod owner_bound_service;

#[path = "repository_test_module_provenance/two_leg_harness.rs"]
mod two_leg_harness;

const CLIENT_TUN_SOURCE: &str = include_str!("../src/client_tun.rs");
const CLIENT_TUN_TESTS_SOURCE: &str = include_str!("../src/client_tun/tests/mod.rs");
const OWNED_UPSTREAM_TCP_SOURCE: &str = include_str!("../src/owned_upstream/tcp.rs");
const OWNED_UPSTREAM_TCP_TESTS_SOURCE: &str =
    include_str!("../src/owned_upstream/tcp/tests/mod.rs");
const TUIC_SOURCE: &str = include_str!("../src/tuic.rs");
const TUIC_TESTS_SOURCE: &str = include_str!("../src/tuic/tests/mod.rs");

// Shrink-only migration backlog. Each externalized private test Module removes
// one entry; new directly declared positive-cfg inline Modules must not add
// entries.
const LEGACY_INLINE_TEST_MODULES_AFTER_TWO_LEG_HARNESS: &[&str] = &[
    "src/dns_block.rs::tests",
    "src/failover.rs::tests",
    "src/harness.rs::tests",
    "src/lib.rs::test_support",
    "src/loop_profiler.rs::tests",
    "src/metrics.rs::tests",
    "src/owned_upstream/client_controller.rs::tests",
    "src/owned_upstream/leg_io.rs::tests",
    "src/owned_upstream/legacy.rs::tests",
    "src/owned_upstream/owner_target.rs::tests",
    "src/owned_upstream/quinn_client_root.rs::tests",
    "src/owned_upstream/quinn_leg_composition.rs::queue_classifier_tests",
    "src/owned_upstream/session.rs::tests",
    "src/owned_upstream/standby.rs::tests",
    "src/owned_upstream/supervisor.rs::tests",
    "src/owned_upstream/target.rs::tests",
    "src/owned_upstream/two_leg.rs::tests",
    "src/owned_upstream/udp.rs::tests",
    "src/quic.rs::tests",
    "src/quic_udp_send_service.rs::tests",
    "src/reality/auth.rs::tests",
    "src/reality/cert.rs::tests",
    "src/reality/client_hello.rs::tests",
    "src/reality/handshake.rs::tests",
    "src/reality/key_schedule.rs::tests",
    "src/reality/record.rs::tests",
    "src/reality/server_hello.rs::tests",
    "src/reality/vless.rs::tests",
    "src/reality_upstream.rs::tests",
    "src/resumable/auth.rs::tests",
    "src/resumable/capacity.rs::tests",
    "src/resumable/session.rs::tests",
    "src/resumable/tcp.rs::tests",
    "src/tcp_downlink_pump.rs::tests",
    "src/tcp_egress.rs::tests",
    "src/tcp_stream_service.rs::tests",
];

fn source_occurrences(source: &str, needle: &str) -> usize {
    source.match_indices(needle).count()
}

fn rust_test_attribute_count(source: &str) -> usize {
    source
        .lines()
        .map(str::trim)
        .filter(|line| *line == "#[test]" || line.starts_with("#[tokio::test"))
        .count()
}

fn rust_inline_module_declaration_count(source: &str) -> usize {
    inline_rust_module_names(source).len()
}

fn collect_production_rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .map(|entry| entry.expect("failed to read a src directory entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|error| panic!("failed to stat {}: {error}", path.display()));
        if file_type.is_dir() {
            if entry.file_name().to_string_lossy() != "tests" {
                collect_production_rust_sources(&path, sources);
            }
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            sources.push(path);
        }
    }
}

fn normalized_repository_path(path: &Path) -> String {
    path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
        .unwrap_or_else(|_| panic!("{} is outside the repository", path.display()))
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn inline_test_modules_in_production_sources() -> Vec<String> {
    let mut sources = Vec::new();
    collect_production_rust_sources(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );

    let mut modules = Vec::new();
    for source_path in sources {
        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", source_path.display()));
        let path = normalized_repository_path(&source_path);
        modules.extend(
            inline_cfg_test_module_names(&source)
                .into_iter()
                .map(|module| format!("{path}::{module}")),
        );
    }
    modules.sort();
    modules
}

#[test]
fn rust_inline_module_scanner_counts_code_and_ignores_literals() {
    let source = r###"
#[cfg(test)] mod attributed_one {}
#[allow(dead_code)] pub(crate) mod attributed_two {}
mod direct {}
#![allow(dead_code)]
mod after_inner_attribute {}
mod external_child;

const RAW_FAKE: &str = r#"
mod raw_fake_one {}
pub mod raw_fake_two {}
pub(crate) mod raw_fake_three {}
"#;

const RAW_BYTE_FAKE: &[u8] = br##"
" mod raw_byte_fake {}
"##;

const RAW_C_FAKE: &core::ffi::CStr = cr##"
" mod raw_c_fake {}
"##;

// mod line_comment_fake {}
/* pub mod block_comment_fake {} */
"###;

    assert_eq!(rust_inline_module_declaration_count(source), 4);
}

#[test]
fn inline_cfg_test_module_scanner_covers_supported_rust_forms() {
    let source = r##"
#[cfg(test)]
mod exact {}

#[cfg(all(
    test,
    unix,
))]
pub(crate) mod multiline_composite {
}

#[cfg(any(test, feature = "harness"))] mod same_line_composite {}

#[cfg(not(test))]
mod production_only {}

#[cfg(not(not(test)))]
mod double_negated {}

#[cfg(feature = "test")]
mod feature_named_test {}

#[cfg(unix /* test */)]
mod comment_named_test {}

#[cfg(all(test, /* ) */ unix))]
mod cfg_comment_parenthesis {}

#[cfg(test /* ] */)]
mod cfg_comment_bracket {}

#[cfg(not /* reason */ (test))]
mod cfg_comment_single_negation {}

#[cfg(not /* reason */ (not(test)))]
mod cfg_comment_double_negation {}

#[cfg /* reason */ (test)]
mod cfg_comment_before_parenthesis {}

#[ /* reason */ cfg(test)]
mod cfg_leading_comment {}

#[cfg(feature = r#"test"#)]
mod raw_feature_named_test {}

#[cfg(test)]
#[path = "external.rs"]
mod external;

const STRING_FAKE: &str = "#[cfg(test)] mod string_fake {}";
// #[cfg(test)] mod line_comment_fake {}
/* #[cfg(test)] mod block_comment_fake {} */
"##;

    assert_eq!(
        inline_cfg_test_module_names(source),
        [
            "exact",
            "multiline_composite",
            "same_line_composite",
            "double_negated",
            "cfg_comment_parenthesis",
            "cfg_comment_bracket",
            "cfg_comment_double_negation",
            "cfg_comment_before_parenthesis",
            "cfg_leading_comment"
        ]
    );
}

#[test]
fn repository_test_extractions_advance_the_inline_test_module_ratchet() {
    assert!(
        LEGACY_INLINE_TEST_MODULES_AFTER_TWO_LEG_HARNESS
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "the shrink-only migration backlog must stay sorted and duplicate-free",
    );

    let actual = inline_test_modules_in_production_sources();
    let expected = LEGACY_INLINE_TEST_MODULES_AFTER_TWO_LEG_HARNESS
        .iter()
        .map(|entry| (*entry).to_owned())
        .collect::<Vec<_>>();
    let missing = expected
        .iter()
        .filter(|entry| !actual.contains(entry))
        .collect::<Vec<_>>();
    let unexpected = actual
        .iter()
        .filter(|entry| !expected.contains(entry))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "inline test Module backlog is shrink-only; missing={missing:?}, unexpected={unexpected:?}",
    );
    assert!(
        TUIC_SOURCE.ends_with("#[cfg(test)]\nmod tests;\n"),
        "the TUIC test Implementation must move behind one terminal private child Module seam",
    );
    assert_eq!(
        source_occurrences(TUIC_SOURCE, "#[cfg(test)]\nmod tests;"),
        1,
        "one private child Module owns the TUIC test Implementation",
    );
    assert_eq!(
        source_occurrences(TUIC_SOURCE, "#[cfg(test)]\nmod tests {"),
        0,
        "the production TUIC source must not inline the test Implementation",
    );
    let tcp_open_tests = include_str!("../src/tuic/tcp_open/tests/mod.rs");
    let udp_send_tests = include_str!("../src/tuic/udp_send/tests/uplink.rs");
    let udp_reassembly_tests = include_str!("../src/tuic/udp_reassembly/tests/mod.rs");
    let moved_tests = [
        (udp_send_tests, "udp_send_plan_native_is_size_based"),
        (udp_send_tests, "udp_send_plan_quic_is_always_stream"),
        (
            udp_reassembly_tests,
            "reassemble_single_fragment_passthrough",
        ),
        (udp_reassembly_tests, "reassemble_two_fragments_in_order"),
        (
            udp_reassembly_tests,
            "reassemble_two_fragments_out_of_order",
        ),
        (
            udp_reassembly_tests,
            "reassemble_duplicate_fragment_last_writer_wins",
        ),
        (udp_reassembly_tests, "reassemble_incomplete_swept_by_ttl"),
        (udp_reassembly_tests, "reassemble_rejects_bad_frag_id"),
        (udp_reassembly_tests, "reassemble_cap_evicts_oldest"),
    ];
    for (source, name) in moved_tests {
        assert_eq!(
            source_occurrences(source, &format!("#[test]\nfn {name}()")),
            1
        );
    }
    assert_eq!(
        rust_test_attribute_count(TUIC_TESTS_SOURCE)
            + moved_tests.len()
            + rust_test_attribute_count(tcp_open_tests),
        162,
        "the external private Modules retain all original TUIC tests after the UDP and TCP open Moves",
    );
    assert!(
        TUIC_TESTS_SOURCE.starts_with("use super::*;\n"),
        "the external TUIC test Implementation starts at the unchanged parent seam",
    );
    assert_eq!(
        rust_inline_module_declaration_count(TUIC_TESTS_SOURCE),
        0,
        "the external TUIC file must not add another inline Module wrapper around the extracted body",
    );
    assert_eq!(
        source_occurrences(
            tcp_open_tests,
            "#[tokio::test]\nasync fn tcp_startup_service_is_one_atomic_business_write_after_connect()",
        ),
        1,
    );
    assert!(TUIC_TESTS_SOURCE.contains("fn config_debug_redacts_credentials()"));
    assert_eq!(
        source_occurrences(TUIC_TESTS_SOURCE, "use super::*;"),
        1,
        "the external TUIC test Implementation keeps one parent-Module import",
    );
}

#[test]
fn client_tun_test_module_stays_external_and_private() {
    assert_eq!(
        source_occurrences(CLIENT_TUN_SOURCE, "#[cfg(test)]\nmod tests;"),
        1,
        "one terminal private child Module owns the client TUN test Implementation",
    );
    assert_eq!(
        source_occurrences(CLIENT_TUN_SOURCE, "#[cfg(test)]\nmod tests {"),
        0,
        "the production source must not inline the client TUN test Implementation",
    );
    assert!(
        CLIENT_TUN_SOURCE.ends_with("#[cfg(test)]\nmod tests;\n"),
        "the external private test Module stays the terminal production declaration",
    );
    let compatibility_tests = include_str!("../src/client_tun/tests/compatibility_lifecycle.rs");
    let listener_capacity_tests =
        include_str!("../src/client_tun/tests/compatibility_listener_capacity.rs");
    let flow_custody_tests = include_str!("../src/client_tun/tests/compatibility_flow_custody.rs");
    let rearm_custody_tests =
        include_str!("../src/client_tun/tests/compatibility_rearm_custody.rs");
    let reader_batch_tests = include_str!("../src/client_tun/tests/relay_reader_batch.rs");
    let local_egress_tests = include_str!("../src/client_tun/tests/local_egress_service.rs");
    let modules = rust_source_scan::external_rust_modules(CLIENT_TUN_TESTS_SOURCE);
    let routes = modules
        .iter()
        .filter(|module| module.name == "compatibility_lifecycle")
        .collect::<Vec<_>>();
    assert!(matches!(
        routes.as_slice(),
        [route] if route.is_private && route.is_unconditional
            && route.uses_default_path && route.module_context.is_empty()
    ));
    let reader_routes = modules
        .iter()
        .filter(|module| module.name == "relay_reader_batch")
        .collect::<Vec<_>>();
    assert!(matches!(
        reader_routes.as_slice(),
        [route] if route.is_private && route.is_unconditional
            && route.uses_default_path && route.module_context.is_empty()
    ));
    assert_eq!(rust_inline_module_declaration_count(reader_batch_tests), 0);
    assert_eq!(rust_test_attribute_count(reader_batch_tests), 4);
    for name in [
        "relay_remote_ready_burst_forwards_immediate_chunks",
        "relay_remote_ready_burst_respects_byte_limit_without_overshoot",
        "relay_remote_ready_burst_tapers_when_global_rx_queue_is_busy",
        "relay_remote_ready_burst_stops_extra_reads_at_global_rx_critical_edge",
    ] {
        let declaration = format!("#[tokio::test]\nasync fn {name}()");
        assert_eq!(source_occurrences(reader_batch_tests, &declaration), 1);
        assert_eq!(source_occurrences(CLIENT_TUN_TESTS_SOURCE, &declaration), 0);
    }
    let egress_routes = modules
        .iter()
        .filter(|module| module.name == "local_egress_service")
        .collect::<Vec<_>>();
    assert!(
        matches!(egress_routes.as_slice(), [route] if route.is_private
        && route.is_unconditional && route.uses_default_path && route.module_context.is_empty())
    );
    assert_eq!(rust_inline_module_declaration_count(local_egress_tests), 0);
    assert_eq!(rust_test_attribute_count(local_egress_tests), 2);
    for name in [
        "local_egress_service_stops_on_no_work_hard_pause_and_no_progress",
        "d16_drain_only_keeps_poll_and_flush_progress",
    ] {
        let declaration = format!("#[tokio::test]\nasync fn {name}()");
        assert_eq!(source_occurrences(local_egress_tests, &declaration), 1);
        assert_eq!(source_occurrences(CLIENT_TUN_TESTS_SOURCE, &declaration), 0);
    }
    let child_tests = syn::parse_file(compatibility_tests).unwrap();
    let parent_tests = syn::parse_file(CLIENT_TUN_TESTS_SOURCE).unwrap();
    for name in [
        "event_loop_stops_after_terminal_tun_failure",
        "bounded_tun_drain_services_dirty_relay_once_per_tcp_batch",
        "bounded_tun_drain_skips_tcp_relay_for_udp_only_batch",
        "bounded_tun_drain_services_mixed_batch_once_when_tcp_is_present",
    ] {
        let declarations = child_tests
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Fn(function) if function.sig.ident == name => Some(function),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declarations.len(),
            1,
            "{name} must remain unique in the private compatibility child",
        );
        let test = declarations[0];
        assert!(test.sig.asyncness.is_some());
        assert!(test.attrs.iter().any(|attribute| {
            let path = attribute.path();
            path.leading_colon.is_none()
                && path.segments.len() == 2
                && path.segments[0].ident == "tokio"
                && path.segments[1].ident == "test"
        }));
        assert!(
            parent_tests
                .items
                .iter()
                .all(|item| !matches!(item, syn::Item::Fn(function) if function.sig.ident == name))
        );
    }
    assert_eq!(rust_inline_module_declaration_count(compatibility_tests), 0);
    assert_eq!(rust_test_attribute_count(compatibility_tests), 7);
    assert_eq!(
        rust_test_attribute_count(CLIENT_TUN_TESTS_SOURCE)
            + rust_test_attribute_count(include_str!(
                "../src/client_tun/tests/dns_characterization.rs"
            ))
            + rust_test_attribute_count(compatibility_tests)
            + rust_test_attribute_count(listener_capacity_tests)
            + rust_test_attribute_count(flow_custody_tests)
            + rust_test_attribute_count(rearm_custody_tests)
            + rust_test_attribute_count(reader_batch_tests)
            + rust_test_attribute_count(local_egress_tests),
        349,
        "the external private Module retains the complete client TUN test inventory",
    );
    assert!(
        CLIENT_TUN_TESTS_SOURCE.starts_with("use super::*;\n"),
        "the external test Implementation must not add a second tests Module layer",
    );
    assert_eq!(
        rust_inline_module_declaration_count(CLIENT_TUN_TESTS_SOURCE),
        0,
        "the external file must not add another inline Module wrapper around the extracted body",
    );
    assert!(CLIENT_TUN_TESTS_SOURCE.contains("fn upstream_kind_selector()"));
    assert!(CLIENT_TUN_TESTS_SOURCE.contains("fn handles_for_port_returns_pool_handles()"));
    assert_eq!(
        source_occurrences(CLIENT_TUN_TESTS_SOURCE, "use super::*;"),
        1,
        "the external test Implementation keeps one parent-Module import",
    );

    let runtime = include_str!("../src/client_tun/compatibility_runtime.rs");
    let runtime_ast = syn::parse_file(runtime).unwrap();
    let custody_routes = runtime_ast
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if module.ident == "tests" => Some(module),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(custody_routes.len(), 1);
    let route = custody_routes[0];
    assert!(matches!(route.vis, syn::Visibility::Inherited));
    assert!(route.content.is_none());
    assert_eq!(route.attrs.len(), 2);
    assert!(route.attrs.iter().any(|attribute| {
        attribute.path().is_ident("cfg")
            && attribute
                .parse_args::<syn::Ident>()
                .is_ok_and(|ident| ident == "test")
    }));
    assert!(route.attrs.iter().any(|attribute| match &attribute.meta {
        syn::Meta::NameValue(value) if value.path.is_ident("path") => {
            matches!(&value.value, syn::Expr::Lit(expression)
                if matches!(&expression.lit, syn::Lit::Str(path)
                    if path.value() == "tests/compatibility_relay_custody.rs"))
        }
        _ => false,
    }));
    let custody_source = include_str!("../src/client_tun/tests/compatibility_relay_custody.rs");
    assert_eq!(rust_inline_module_declaration_count(custody_source), 0);
    let custody_ast = syn::parse_file(custody_source).unwrap();
    let listener_capacity_routes = custody_ast
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if module.ident == "listener_capacity" => Some(module),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        listener_capacity_routes.as_slice(),
        [route] if matches!(route.vis, syn::Visibility::Inherited)
            && route.content.is_none()
            && route.attrs.len() == 1
            && route.attrs.iter().any(|attribute| match &attribute.meta {
                syn::Meta::NameValue(value) if value.path.is_ident("path") => {
                    matches!(&value.value, syn::Expr::Lit(expression)
                        if matches!(&expression.lit, syn::Lit::Str(path)
                            if path.value() == "compatibility_listener_capacity.rs"))
                }
                _ => false,
            })
    ));
    let flow_custody_routes = custody_ast
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if module.ident == "flow_custody" => Some(module),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        flow_custody_routes.as_slice(),
        [route] if matches!(route.vis, syn::Visibility::Inherited)
            && route.content.is_none()
            && route.attrs.len() == 1
            && route.attrs.iter().any(|attribute| match &attribute.meta {
                syn::Meta::NameValue(value) if value.path.is_ident("path") => {
                    matches!(&value.value, syn::Expr::Lit(expression)
                        if matches!(&expression.lit, syn::Lit::Str(path)
                            if path.value() == "compatibility_flow_custody.rs"))
                }
                _ => false,
            })
    ));
    let flow_custody_ast = syn::parse_file(flow_custody_tests).unwrap();
    let rearm_custody_routes = flow_custody_ast
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if module.ident == "rearm_custody" => Some(module),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        rearm_custody_routes.as_slice(),
        [route] if matches!(route.vis, syn::Visibility::Inherited)
            && route.content.is_none()
            && route.attrs.len() == 1
            && route.attrs.iter().any(|attribute| match &attribute.meta {
                syn::Meta::NameValue(value) if value.path.is_ident("path") => {
                    matches!(&value.value, syn::Expr::Lit(expression)
                        if matches!(&expression.lit, syn::Lit::Str(path)
                            if path.value() == "compatibility_rearm_custody.rs"))
                }
                _ => false,
            })
    ));
    let capacity_test_name = "fn ensure_spare_listeners_respects_global_cap";
    assert_eq!(
        (
            source_occurrences(listener_capacity_tests, capacity_test_name),
            source_occurrences(CLIENT_TUN_TESTS_SOURCE, capacity_test_name),
        ),
        (1, 0),
        "the moved capacity characterizer must appear once in its private leaf and zero times in the oversized parent",
    );
    let thin_characterizer_name = "fn thin_relay_outer_supervision_preserves_dispatch_then_close";
    assert_eq!(
        (
            source_occurrences(flow_custody_tests, thin_characterizer_name),
            source_occurrences(custody_source, thin_characterizer_name),
        ),
        (1, 0),
        "the Thin characterizer must appear once in its private leaf and zero times in its custody parent",
    );
    let f0_characterizer_name = "fn saved_generic_open_stays_w_retained_before_legacy_relay_spawn";
    assert_eq!(
        (
            source_occurrences(flow_custody_tests, f0_characterizer_name),
            source_occurrences(custody_source, f0_characterizer_name),
        ),
        (1, 0),
        "the F0 retained-boundary characterizer must appear once in its private leaf and zero times in its custody parent",
    );
    assert_eq!(rust_test_attribute_count(rearm_custody_tests), 6);
    for characterizer_name in [
        "fn d16_terminal_socket_rearm_preserves_local_terminal_cause",
        "fn d16_rearm_detaches_old_epoch_queue_and_ignores_late_wake",
        "fn reaper_restores_listening_state_and_releases_fake_ip",
    ] {
        assert_eq!(
            (
                source_occurrences(rearm_custody_tests, characterizer_name),
                source_occurrences(CLIENT_TUN_TESTS_SOURCE, characterizer_name),
            ),
            (1, 0),
            "the moved G0-T characterizer must appear once in its private child and zero times in the oversized parent",
        );
    }
    for characterizer_name in [
        "fn g0_physical_rearm_is_private_to_the_gateway",
        "fn current_closed_event_rearms_through_original_loop_gateway",
    ] {
        assert_eq!(
            (
                source_occurrences(rearm_custody_tests, characterizer_name),
                source_occurrences(flow_custody_tests, characterizer_name),
                source_occurrences(CLIENT_TUN_TESTS_SOURCE, characterizer_name),
            ),
            (1, 0, 0),
            "the G0 gateway characterizer must appear once in its private child and in neither parent",
        );
    }
    let generic_launcher_characterizer_name =
        "fn generic_launcher_public_loop_dispatches_then_closes";
    assert_eq!(
        (
            source_occurrences(rearm_custody_tests, generic_launcher_characterizer_name),
            source_occurrences(flow_custody_tests, generic_launcher_characterizer_name),
            source_occurrences(CLIENT_TUN_TESTS_SOURCE, generic_launcher_characterizer_name),
        ),
        (1, 0, 0),
        "the G0-M launcher characterizer must appear once in its private child and in neither parent",
    );
    let mut custody_names = custody_ast
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Fn(function)
                if function.attrs.iter().any(|attribute| {
                    let path = attribute.path();
                    path.leading_colon.is_none()
                        && path.segments.len() == 2
                        && path.segments[0].ident == "tokio"
                        && path.segments[1].ident == "test"
                }) =>
            {
                assert!(function.sig.asyncness.is_some());
                Some(function.sig.ident.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    custody_names.sort();
    let mut expected_custody_names = [
        "session_observer_cancellation_retains_original_loop_and_device_join",
        "session_inner_poll_panic_retains_device_until_actual_join",
        "session_settled_device_drop_panic_is_retained_once",
        "session_inner_destructor_panic_still_joins_original_device",
        "session_original_ingress_error_preserves_os_facts_through_device_settlement",
        "session_kind_only_device_failure_is_bounded_and_sticky",
        "session_unknown_device_callbacks_preserve_custody_in_sticky_quarantine",
        "session_shutdown_deadline_does_not_reset_or_discard_pending_device",
        "session_real_pump_failure_delta_moves_once_and_saturates",
        "session_running_data_poll_does_not_acquire_control_ledger",
    ];
    expected_custody_names.sort();
    assert_eq!(custody_names, expected_custody_names);
    assert_eq!(rust_test_attribute_count(custody_source), 10);
}

#[test]
fn owned_upstream_tcp_test_module_moves_external_and_private() {
    assert!(
        OWNED_UPSTREAM_TCP_SOURCE.ends_with("#[cfg(test)]\nmod tests;\n"),
        "the owned-upstream TCP test Implementation must move behind one terminal private child Module seam",
    );
    assert_eq!(
        source_occurrences(OWNED_UPSTREAM_TCP_SOURCE, "#[cfg(test)]\nmod tests;",),
        1,
        "one private child Module owns the owned-upstream TCP test Implementation",
    );
    assert_eq!(
        source_occurrences(OWNED_UPSTREAM_TCP_SOURCE, "#[cfg(test)]\nmod tests {",),
        0,
        "the production owned-upstream TCP source must not inline the test Implementation",
    );
    assert_eq!(
        rust_test_attribute_count(OWNED_UPSTREAM_TCP_TESTS_SOURCE),
        65,
        "the external private Module retains the complete owned-upstream TCP test inventory",
    );
    assert!(
        OWNED_UPSTREAM_TCP_TESTS_SOURCE
            .starts_with("use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};\n"),
        "the external owned-upstream TCP test Implementation starts at the unchanged body seam",
    );
    assert_eq!(
        rust_inline_module_declaration_count(OWNED_UPSTREAM_TCP_TESTS_SOURCE),
        0,
        "the external owned-upstream TCP file must not add another inline Module wrapper around the extracted body",
    );
    assert!(
        OWNED_UPSTREAM_TCP_TESTS_SOURCE
            .contains("fn downlink_refuses_delivery_when_completion_lane_has_no_slot()")
    );
    assert!(
        OWNED_UPSTREAM_TCP_TESTS_SOURCE
            .contains("fn debug_is_redacted_and_endpoint_drop_releases_all_port_ownership()")
    );
    assert_eq!(
        source_occurrences(OWNED_UPSTREAM_TCP_TESTS_SOURCE, "use super::*;"),
        1,
        "the external owned-upstream TCP test Implementation keeps one parent-Module import",
    );
}
