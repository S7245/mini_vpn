//! Repository provenance for the private two-leg harness test Module tree.
//!
//! This child owns structural assertions for that test tree. It does not own
//! production behavior, capacity arithmetic, or repository-wide scanners.

use super::rust_source_scan::{
    external_rust_modules, inline_rust_module_names, rust_test_functions,
    source_include_macro_count,
};
use super::*;

const TWO_LEG_HARNESS_SOURCE: &str = include_str!("../../src/owned_upstream/two_leg_harness.rs");
const TWO_LEG_HARNESS_TESTS_SOURCE: &str =
    include_str!("../../src/owned_upstream/two_leg_harness/tests/mod.rs");
const R7_CAPACITY_SOURCE: &str =
    include_str!("../../src/owned_upstream/two_leg_harness/tests/r7_capacity.rs");
const R7_PROGRESS_SCAN_SOURCE: &str =
    include_str!("../../src/owned_upstream/two_leg_harness/tests/r7_capacity/progress_scan.rs");
const MOVED_PROGRESS_SCAN_TESTS: &[&str] = &[
    "r7_owner_scan_capacity_uses_two_directions_ceiling_and_bounded_episodes",
    "r7_owner_scan_capacity_rejects_zero_flow_or_episode_domains_exactly",
    "r7_owner_scan_capacity_rejects_direction_and_episode_overflow_exactly",
    "r7_manifest_scan_capacity_uses_live_flow_baseline_characterization",
    "r7_manifest_flow_and_terminal_bounds_do_not_borrow_characterization",
];

#[test]
fn external_module_scanner_reports_public_visibility_across_trivia() {
    let source = "pub /* visibility */ (super) /* seam */ mod /* name */ r7_capacity;";
    let modules = external_rust_modules(source);

    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].name, "r7_capacity");
    assert!(!modules[0].is_private);
    assert!(modules[0].is_unconditional);
    assert!(modules[0].uses_default_path);
    assert!(modules[0].module_context.is_empty());
}

#[test]
fn external_module_scanner_reports_cfg_not_test_as_conditional() {
    for source in [
        "#[cfg(not(test))]\nmod r7_capacity;",
        "#![cfg(not(test))]\nmod r7_capacity;",
    ] {
        let modules = external_rust_modules(source);

        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].name, "r7_capacity");
        assert!(modules[0].is_private);
        assert!(!modules[0].is_unconditional);
        assert!(modules[0].uses_default_path);
        assert!(modules[0].module_context.is_empty());
    }
}

#[test]
fn external_module_scanner_rejects_an_unsupported_target_path() {
    let modules =
        external_rust_modules("#[path = concat!(\"r7_\", \"capacity.rs\")]\nmod r7_capacity;");

    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].name, "r7_capacity");
    assert!(!modules[0].uses_default_path);
}

#[test]
fn test_function_scanner_ignores_comment_and_raw_string_fakes() {
    let source = r###"
        // #[test]
        // fn comment_fake() {}
        const RAW_FAKE: &str = r#"
#[test]
fn raw_fake() {}
"#;
        #[test]
        fn real_test() {}
    "###;
    let declarations = rust_test_functions(source);

    assert_eq!(declarations.len(), 1);
    assert_eq!(declarations[0].name, "real_test");
    assert!(declarations[0].module_context.is_empty());
}

#[test]
fn test_function_scanner_reports_module_context_across_extra_attributes() {
    let source = "mod wrapper { #[test] #[allow(dead_code)] fn moved() {} }";
    let declarations = rust_test_functions(source);

    assert_eq!(declarations.len(), 1);
    assert_eq!(declarations[0].name, "moved");
    assert_eq!(declarations[0].module_context, ["wrapper"]);
}

fn assert_one_private_external_test_module(source: &str, expected_name: &str) {
    let modules = external_rust_modules(source);
    let matching_modules = modules
        .iter()
        .filter(|module| module.name == expected_name && module.module_context.is_empty())
        .collect::<Vec<_>>();

    assert_eq!(
        matching_modules.len(),
        1,
        "{expected_name} must have exactly one code-aware external Module declaration",
    );
    let module = matching_modules[0];
    assert!(
        module.module_context.is_empty(),
        "{expected_name} must remain a direct child of its declaring test Module",
    );
    assert!(
        module.is_private,
        "{expected_name} must remain a private Module",
    );
    assert!(
        module.is_unconditional,
        "{expected_name} must remain unconditionally compiled inside its inherited test scope",
    );
    assert!(
        module.uses_default_path,
        "{expected_name} must use the repository-closed default external path",
    );
    assert_eq!(
        inline_rust_module_names(source)
            .iter()
            .filter(|name| name.as_str() == expected_name)
            .count(),
        0,
        "{expected_name} must not be replaced by an inline wrapper",
    );
    assert_eq!(
        source_include_macro_count(source),
        0,
        "the declaring source must not replace {expected_name} with include!",
    );
}

#[test]
fn two_leg_harness_nested_capacity_route_stays_external_and_private() {
    assert_one_private_external_test_module(TWO_LEG_HARNESS_TESTS_SOURCE, "r7_capacity");
    assert_one_private_external_test_module(R7_CAPACITY_SOURCE, "progress_scan");

    assert_eq!(
        source_include_macro_count(R7_PROGRESS_SCAN_SOURCE),
        0,
        "the progress-scan leaf must not replace moved tests with include!",
    );

    let leaf_tests = rust_test_functions(R7_PROGRESS_SCAN_SOURCE);
    let parent_tests = rust_test_functions(R7_CAPACITY_SOURCE);
    for test_name in MOVED_PROGRESS_SCAN_TESTS {
        let matching_leaf_tests = leaf_tests
            .iter()
            .filter(|declaration| {
                declaration.name == *test_name && declaration.module_context.is_empty()
            })
            .count();
        assert_eq!(
            matching_leaf_tests, 1,
            "{test_name} must remain a direct test in the private progress-scan child",
        );
        assert_eq!(
            parent_tests
                .iter()
                .filter(|declaration| declaration.name == *test_name)
                .count(),
            0,
            "{test_name} must stay absent from the oversized capacity parent",
        );
    }
}

#[test]
fn nested_route_contract_allows_unrelated_module_shapes() {
    let source = r#"
mod progress_scan;
mod unrelated_inline {}
#[path = "elsewhere.rs"]
mod unrelated_external;
"#;

    assert_one_private_external_test_module(source, "progress_scan");
}

#[test]
fn two_leg_harness_test_module_moves_external_and_private() {
    assert!(
        TWO_LEG_HARNESS_SOURCE.ends_with("#[cfg(test)]\nmod tests;\n"),
        "the two-leg harness test Implementation must move behind one terminal private child Module seam",
    );
    assert_eq!(
        source_occurrences(TWO_LEG_HARNESS_SOURCE, "#[cfg(test)]\nmod tests;"),
        1,
        "one private child Module owns the two-leg harness test Implementation",
    );
    assert_eq!(
        source_occurrences(TWO_LEG_HARNESS_SOURCE, "#[cfg(test)]\nmod tests {"),
        0,
        "the production two-leg harness source must not inline the test Implementation",
    );
    assert!(
        TWO_LEG_HARNESS_TESTS_SOURCE.starts_with("use bytes::Bytes;\n"),
        "the external two-leg harness test Implementation starts at the unchanged body seam",
    );
    assert_eq!(
        rust_inline_module_declaration_count(TWO_LEG_HARNESS_TESTS_SOURCE),
        0,
        "the external two-leg harness file must not add another inline Module wrapper around the extracted body",
    );
    assert!(
        TWO_LEG_HARNESS_TESTS_SOURCE
            .contains("fn global_scheduler_releases_exactly_one_wire_message_per_turn()")
    );
    assert!(
        TWO_LEG_HARNESS_TESTS_SOURCE
            .contains("fn production_baseline_retries_retained_attach_after_transport_pressure()")
    );
    assert_eq!(
        source_occurrences(TWO_LEG_HARNESS_TESTS_SOURCE, "use super::*;"),
        1,
        "the external two-leg harness test Implementation keeps one parent-Module import",
    );
}
