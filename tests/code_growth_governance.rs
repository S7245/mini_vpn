use std::fs;
use std::path::{Path, PathBuf};

#[path = "support/rust_source_scan.rs"]
mod rust_source_scan;
use rust_source_scan::{
    ExternalCfgTestModule, LintSuppressionCounts, RustPathAttribute, external_cfg_test_modules,
    lint_suppression_counts, rust_path_attributes, source_include_macro_count,
};

const NEW_PRODUCTION_FILE_LINE_LIMIT: usize = 1_500;
const NEW_TEST_INDEX_LINE_LIMIT: usize = 300;
const NEW_TEST_LEAF_LINE_LIMIT: usize = 800;

// Exact existing debt at the governance cutover. This inventory is
// shrink-only: never add a path. When a file gets smaller, the stage Integrator
// lowers its recorded value in the same promoted change; remove it once it
// reaches the new-file limit.
const GRANDFATHERED_PRODUCTION_LINES: &[(&str, usize)] = &[
    ("src/client_tun.rs", 10_561),
    ("src/harness.rs", 4_099),
    ("src/owned_upstream/client_controller.rs", 8_577),
    ("src/owned_upstream/leg.rs", 2_699),
    ("src/owned_upstream/leg_io.rs", 7_937),
    ("src/owned_upstream/owner_session_controller.rs", 2_956),
    ("src/owned_upstream/owner_target.rs", 8_498),
    ("src/owned_upstream/quinn_client_root.rs", 6_468),
    ("src/owned_upstream/quinn_leg_actor.rs", 6_583),
    ("src/owned_upstream/quinn_leg_composition.rs", 9_447),
    ("src/owned_upstream/quinn_leg_composition/owner.rs", 1_860),
    (
        "src/owned_upstream/quinn_leg_composition/owner_initial.rs",
        2_067,
    ),
    ("src/owned_upstream/reverse_switch.rs", 2_768),
    ("src/owned_upstream/standby.rs", 8_108),
    ("src/owned_upstream/supervisor.rs", 4_834),
    ("src/owned_upstream/tcp.rs", 2_766),
    ("src/owned_upstream/two_leg.rs", 3_847),
    ("src/owned_upstream/two_leg_harness.rs", 3_423),
    ("src/resumable/auth.rs", 2_954),
    ("src/resumable/capacity.rs", 1_547),
    ("src/resumable/protocol.rs", 1_992),
    ("src/resumable/session.rs", 5_364),
    ("src/resumable/tcp.rs", 1_770),
    ("src/tcp_downlink_pump.rs", 1_839),
    ("src/tuic.rs", 4_690),
];

// Exact too-many-arguments allowance debt at the governance cutover. This
// inventory spans all governed Rust and is shrink-only; a new allowance must
// not be normalized by adding it.
const GRANDFATHERED_TOO_MANY_ARGUMENTS: &[(&str, usize)] = &[
    ("src/client_tun.rs", 18),
    ("src/owned_upstream/quinn_client_root.rs", 1),
    ("src/owned_upstream/quinn_leg_actor.rs", 6),
    ("src/owned_upstream/standby.rs", 1),
    ("src/owned_upstream/target.rs", 1),
    ("src/owned_upstream/two_leg.rs", 1),
    ("src/reality_upstream.rs", 1),
    ("src/tuic.rs", 3),
];

// Exact test-source debt at the governance cutover. Index Modules have a
// 300-line new-file limit; test leaves have an 800-line limit. This inventory
// is shrink-only and must move down with every behavior-preserving split.
const GRANDFATHERED_TEST_LINES: &[(&str, usize)] = &[
    ("src/client_tun/tests/mod.rs", 12_886),
    ("src/owned_upstream/owner_output_drain/tests/mod.rs", 986),
    (
        "src/owned_upstream/owner_session_controller/tests/attached_recovery.rs",
        823,
    ),
    (
        "src/owned_upstream/owner_session_controller/tests/mod.rs",
        1_355,
    ),
    ("src/owned_upstream/quinn_leg_actor/tests/mod.rs", 15_814),
    (
        "src/owned_upstream/quinn_leg_actor/tests/r7_client_b_control_dispatch.rs",
        1_345,
    ),
    (
        "src/owned_upstream/quinn_leg_actor/tests/r7_client_recovery_tail_settlement.rs",
        835,
    ),
    (
        "src/owned_upstream/quinn_leg_actor/tests/r7_client_replacement_supersession.rs",
        997,
    ),
    (
        "src/owned_upstream/quinn_leg_actor/tests/r7_owner_b_freshness.rs",
        1_576,
    ),
    (
        "src/owned_upstream/quinn_leg_composition/owner/continuation/tests/mod.rs",
        330,
    ),
    ("src/owned_upstream/reverse_switch/tests/mod.rs", 2_393),
    ("src/owned_upstream/tcp/tests/mod.rs", 2_975),
    ("src/owned_upstream/two_leg_harness/tests/mod.rs", 496),
    (
        "src/owned_upstream/two_leg_harness/tests/r7_capacity.rs",
        936,
    ),
    ("src/quic_path_probe.rs", 1_235),
    ("src/resumable/protocol/tests/mod.rs", 436),
    ("src/tuic/tests/mod.rs", 5_285),
    ("tests/knife16_owned_leg_provenance.rs", 2_454),
    ("tests/knife16_resumable_protocol.rs", 1_444),
];

fn repository_root() -> PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().expect("current directory must be readable"))
}

fn excluded_governance_directory(root: &Path, directory: &Path) -> bool {
    let relative = directory
        .strip_prefix(root)
        .unwrap_or_else(|_| panic!("{} must be below {}", directory.display(), root.display()));
    let directory_name = directory.file_name().and_then(|name| name.to_str());
    matches!(directory_name, Some(".git" | "third_party"))
        || relative == Path::new("target")
        || relative == Path::new(".claude")
        || relative == Path::new(".codex")
        || relative.starts_with(Path::new("docs/archive"))
        || relative.starts_with(Path::new(".learnings/archive"))
}

fn collect_repository_rust_sources(root: &Path, directory: &Path, sources: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .map(|entry| entry.expect("failed to read a repository directory entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|error| panic!("failed to stat {}: {error}", path.display()));
        if file_type.is_dir() {
            if !excluded_governance_directory(root, &path) {
                collect_repository_rust_sources(root, &path, sources);
            }
        } else if file_type.is_symlink() {
            panic!("governed source symlink is not allowed: {}", path.display());
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            sources.push(path);
        }
    }
}

fn path_is_in_test_tree(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root)
        .unwrap_or_else(|_| panic!("{} must be below {}", path.display(), root.display()))
        .components()
        .any(|component| component.as_os_str() == "tests")
}

fn source_child_module_base(root: &Path, source: &Path) -> PathBuf {
    let parent = source
        .parent()
        .unwrap_or_else(|| panic!("{} must have a parent directory", source.display()));
    let file_name = source
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let crate_root = matches!(file_name, "lib.rs" | "main.rs" | "build.rs")
        || parent == root.join("tests")
        || parent == root.join("src/bin");
    if file_name == "mod.rs" || crate_root {
        parent.to_path_buf()
    } else {
        parent.join(
            source
                .file_stem()
                .unwrap_or_else(|| panic!("{} must have a file stem", source.display())),
        )
    }
}

fn default_external_module_candidates(
    root: &Path,
    source: &Path,
    module_context: &[String],
    module: &str,
) -> [PathBuf; 2] {
    let module_base = module_context
        .iter()
        .fold(source_child_module_base(root, source), |base, component| {
            base.join(component)
        });
    [
        module_base.join(format!("{module}.rs")),
        module_base.join(module).join("mod.rs"),
    ]
}

#[derive(Debug)]
struct GovernedRustSources {
    production: Vec<PathBuf>,
    tests: Vec<PathBuf>,
    reference_violations: Vec<String>,
}

fn governed_rust_sources(root: &Path) -> GovernedRustSources {
    let mut all_sources = Vec::new();
    collect_repository_rust_sources(root, root, &mut all_sources);
    all_sources.sort();

    let canonical_sources = all_sources
        .iter()
        .map(|path| {
            fs::canonicalize(path).unwrap_or_else(|error| {
                panic!("failed to canonicalize {}: {error}", path.display())
            })
        })
        .collect::<Vec<_>>();
    let canonical_root = fs::canonicalize(root)
        .unwrap_or_else(|error| panic!("failed to canonicalize {}: {error}", root.display()));
    let mut test_targets = Vec::new();
    let mut reference_violations = Vec::new();

    for source_path in &all_sources {
        let source = fs::read_to_string(source_path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", source_path.display()));
        let relative = repository_relative(root, source_path);

        let include_count = source_include_macro_count(&source);
        if include_count > 0 {
            reference_violations.push(format!(
                "{relative} contains {include_count} forbidden include! source macro(s)",
            ));
        }

        for attribute in rust_path_attributes(&source) {
            let RustPathAttribute::Literal(reference) = attribute else {
                reference_violations.push(format!(
                    "{relative} contains an unsupported #[path] attribute",
                ));
                continue;
            };
            let candidate = source_path
                .parent()
                .expect("a governed source must have a parent")
                .join(&reference);
            let Ok(canonical) = fs::canonicalize(&candidate) else {
                reference_violations.push(format!(
                    "{relative} references missing Rust source {reference}",
                ));
                continue;
            };
            if candidate
                .extension()
                .is_none_or(|extension| extension != "rs")
            {
                reference_violations.push(format!(
                    "{relative} uses #[path] for non-.rs source {reference}",
                ));
            }
            if !canonical.starts_with(&canonical_root) || !canonical_sources.contains(&canonical) {
                reference_violations.push(format!(
                    "{relative} references Rust source outside the governed inventory: {reference}",
                ));
            }
        }

        for module in external_cfg_test_modules(&source) {
            let explicit_candidate_count = module.explicit_paths.len();
            let candidates = if explicit_candidate_count > 0 {
                let explicit_base = if module.module_context.is_empty() {
                    source_path
                        .parent()
                        .expect("a governed source must have a parent")
                        .to_path_buf()
                } else {
                    module.module_context.iter().fold(
                        source_child_module_base(root, source_path),
                        |base, component| base.join(component),
                    )
                };
                module
                    .explicit_paths
                    .into_iter()
                    .map(|reference| explicit_base.join(reference))
                    .collect::<Vec<_>>()
            } else {
                default_external_module_candidates(
                    root,
                    source_path,
                    &module.module_context,
                    &module.name,
                )
                .to_vec()
            };
            let existing = candidates
                .into_iter()
                .filter(|candidate| candidate.is_file())
                .collect::<Vec<_>>();
            let expected_candidates = explicit_candidate_count.max(1);
            if existing.len() != expected_candidates {
                reference_violations.push(format!(
                    "{relative} cfg-test Module {} resolves to {} source candidates; expected {expected_candidates}",
                    module.name,
                    existing.len(),
                ));
                continue;
            }
            for candidate in existing {
                let canonical = fs::canonicalize(&candidate).unwrap_or_else(|error| {
                    panic!("failed to canonicalize {}: {error}", candidate.display())
                });
                if !canonical_sources.contains(&canonical) {
                    reference_violations.push(format!(
                        "{relative} cfg-test Module {} resolves outside the governed Rust inventory",
                        module.name,
                    ));
                    continue;
                }
                test_targets.push(canonical);
            }
        }
    }

    test_targets.sort();
    test_targets.dedup();
    let mut production = Vec::new();
    let mut tests = Vec::new();
    for (path, canonical) in all_sources.into_iter().zip(canonical_sources) {
        if path_is_in_test_tree(root, &path) || test_targets.binary_search(&canonical).is_ok() {
            tests.push(path);
        } else {
            production.push(path);
        }
    }
    GovernedRustSources {
        production,
        tests,
        reference_violations,
    }
}

fn source_line_count(source: &str) -> usize {
    source.bytes().filter(|byte| *byte == b'\n').count()
        + usize::from(!source.is_empty() && !source.ends_with('\n'))
}

fn repository_relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or_else(|_| panic!("{} must be below {}", path.display(), root.display()))
        .to_string_lossy()
        .replace('\\', "/")
}

fn grandfathered_production_lines(path: &str) -> Option<usize> {
    GRANDFATHERED_PRODUCTION_LINES
        .binary_search_by_key(&path, |(entry, _)| *entry)
        .ok()
        .map(|index| GRANDFATHERED_PRODUCTION_LINES[index].1)
}

fn grandfathered_too_many_arguments(path: &str) -> Option<usize> {
    GRANDFATHERED_TOO_MANY_ARGUMENTS
        .binary_search_by_key(&path, |(entry, _)| *entry)
        .ok()
        .map(|index| GRANDFATHERED_TOO_MANY_ARGUMENTS[index].1)
}

fn grandfathered_test_lines(path: &str) -> Option<usize> {
    GRANDFATHERED_TEST_LINES
        .binary_search_by_key(&path, |(entry, _)| *entry)
        .ok()
        .map(|index| GRANDFATHERED_TEST_LINES[index].1)
}

#[test]
fn production_rust_source_line_debt_is_exact_and_shrink_only() {
    assert!(
        GRANDFATHERED_PRODUCTION_LINES
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0),
        "the grandfathered production inventory must stay sorted and duplicate-free",
    );

    let root = repository_root();
    let inventory = governed_rust_sources(&root);

    let mut seen_grandfathered = Vec::new();
    let mut violations = inventory.reference_violations;
    for path in inventory.production {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let lines = source_line_count(&source);
        let relative = repository_relative(&root, &path);
        match grandfathered_production_lines(&relative) {
            Some(recorded) => {
                seen_grandfathered.push(relative.clone());
                if lines <= NEW_PRODUCTION_FILE_LINE_LIMIT {
                    violations.push(format!(
                        "{relative} reached {lines} lines; remove its grandfathered entry",
                    ));
                } else if lines > recorded {
                    violations.push(format!(
                        "{relative} grew from its exact baseline {recorded} to {lines} lines",
                    ));
                } else if lines < recorded {
                    violations.push(format!(
                        "{relative} shrank from {recorded} to {lines} lines; lower the ratchet in the same change",
                    ));
                }
            }
            None if lines > NEW_PRODUCTION_FILE_LINE_LIMIT => violations.push(format!(
                "{relative} has {lines} lines above the {NEW_PRODUCTION_FILE_LINE_LIMIT}-line new-file limit",
            )),
            None => {}
        }
    }

    let missing = GRANDFATHERED_PRODUCTION_LINES
        .iter()
        .map(|(path, _)| *path)
        .filter(|path| !seen_grandfathered.iter().any(|seen| seen == path))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        violations.push(format!(
            "remove missing grandfathered production paths: {}",
            missing.join(", "),
        ));
    }

    assert!(
        violations.is_empty(),
        "production Rust line-growth governance failed:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn rust_too_many_arguments_allowance_debt_is_exact_and_shrink_only() {
    assert!(
        GRANDFATHERED_TOO_MANY_ARGUMENTS
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0),
        "the grandfathered too-many-arguments inventory must stay sorted and duplicate-free",
    );

    let root = repository_root();
    let inventory = governed_rust_sources(&root);
    let mut sources = inventory.production;
    sources.extend(inventory.tests);

    let mut seen_grandfathered = Vec::new();
    let mut violations = inventory.reference_violations;
    for path in sources {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let suppressions = lint_suppression_counts(&source);
        let allowances = suppressions.too_many_arguments;
        let relative = repository_relative(&root, &path);
        if suppressions.broad > 0 {
            violations.push(format!(
                "{relative} contains {} forbidden broad warnings/clippy::all suppression(s)",
                suppressions.broad,
            ));
        }
        match grandfathered_too_many_arguments(&relative) {
            Some(recorded) => {
                seen_grandfathered.push(relative.clone());
                if allowances == 0 {
                    violations.push(format!(
                        "{relative} has no remaining allowance; remove its grandfathered entry",
                    ));
                } else if allowances > recorded {
                    violations.push(format!(
                        "{relative} grew from {recorded} to {allowances} too-many-arguments allowances",
                    ));
                } else if allowances < recorded {
                    violations.push(format!(
                        "{relative} reduced its allowances from {recorded} to {allowances}; lower the ratchet in the same change",
                    ));
                }
            }
            None if allowances > 0 => violations.push(format!(
                "{relative} introduced {allowances} unapproved too-many-arguments allowance(s)",
            )),
            None => {}
        }
    }

    let missing = GRANDFATHERED_TOO_MANY_ARGUMENTS
        .iter()
        .map(|(path, _)| *path)
        .filter(|path| !seen_grandfathered.iter().any(|seen| seen == path))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        violations.push(format!(
            "remove missing grandfathered allowance paths: {}",
            missing.join(", "),
        ));
    }

    assert!(
        violations.is_empty(),
        "Rust lint-suppression governance failed:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn rust_test_source_line_debt_is_exact_and_shrink_only() {
    assert!(
        GRANDFATHERED_TEST_LINES
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0),
        "the grandfathered test inventory must stay sorted and duplicate-free",
    );

    let root = repository_root();
    let inventory = governed_rust_sources(&root);

    let mut seen_grandfathered = Vec::new();
    let mut violations = inventory.reference_violations;
    for path in inventory.tests {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let lines = source_line_count(&source);
        let relative = repository_relative(&root, &path);
        let limit = if path.file_name().is_some_and(|name| name == "mod.rs") {
            NEW_TEST_INDEX_LINE_LIMIT
        } else {
            NEW_TEST_LEAF_LINE_LIMIT
        };
        match grandfathered_test_lines(&relative) {
            Some(recorded) => {
                seen_grandfathered.push(relative.clone());
                if lines <= limit {
                    violations.push(format!(
                        "{relative} reached its {limit}-line limit; remove its grandfathered entry",
                    ));
                } else if lines > recorded {
                    violations.push(format!(
                        "{relative} grew from its exact baseline {recorded} to {lines} lines",
                    ));
                } else if lines < recorded {
                    violations.push(format!(
                        "{relative} shrank from {recorded} to {lines} lines; lower the ratchet in the same change",
                    ));
                }
            }
            None if lines > limit => violations.push(format!(
                "{relative} has {lines} lines above its {limit}-line new-file limit",
            )),
            None => {}
        }
    }

    let missing = GRANDFATHERED_TEST_LINES
        .iter()
        .map(|(path, _)| *path)
        .filter(|path| !seen_grandfathered.iter().any(|seen| seen == path))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        violations.push(format!(
            "remove missing grandfathered test paths: {}",
            missing.join(", "),
        ));
    }

    assert!(
        violations.is_empty(),
        "test Rust line-growth governance failed:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn governed_target_sources_keep_their_classes_and_build_artifacts_stay_excluded() {
    let root = repository_root();
    let inventory = governed_rust_sources(&root);
    for (relative, production) in [
        ("src/owned_upstream/target/memory_io.rs", true),
        ("src/owned_upstream/target/memory_ready.rs", true),
        ("src/owned_upstream/target/tests/owner_readiness.rs", false),
    ] {
        let path = root.join(relative);
        assert_eq!(
            inventory
                .production
                .iter()
                .filter(|entry| **entry == path)
                .count(),
            usize::from(production),
            "{relative} must have its exact production inventory membership",
        );
        assert_eq!(
            inventory
                .tests
                .iter()
                .filter(|entry| **entry == path)
                .count(),
            usize::from(!production),
            "{relative} must have its exact test inventory membership",
        );
    }
    assert!(
        inventory.reference_violations.is_empty(),
        "actual source routes must close: {:?}",
        inventory.reference_violations,
    );
    for relative in [
        "target",
        ".git",
        "third_party",
        ".claude",
        ".codex",
        "docs/archive",
        ".learnings/archive",
        "src/nested/.git",
        "src/nested/third_party",
    ] {
        let directory = root.join(relative);
        assert!(
            excluded_governance_directory(&root, &directory),
            "{relative} must remain excluded",
        );
        assert!(
            inventory
                .production
                .iter()
                .chain(&inventory.tests)
                .all(|path| !path.starts_with(&directory)),
            "{relative} must not contribute governed Rust sources",
        );
    }
}

#[test]
fn rust_source_reference_and_test_classification_are_closed() {
    let root = repository_root();
    let inventory = governed_rust_sources(&root);
    assert!(
        inventory.reference_violations.is_empty(),
        "Rust source-reference governance failed:\n{}",
        inventory.reference_violations.join("\n"),
    );
    assert!(
        inventory
            .production
            .iter()
            .all(|path| !inventory.tests.contains(path)),
        "a governed Rust path must have exactly one production/test classification",
    );
}

#[test]
fn source_scanner_closes_external_test_path_and_include_bypasses() {
    let source = r###"
#[cfg(test)]
#[path = "external_test.rs"]
mod external_test;

#[cfg(test)]
#[cfg_attr(test, path = "nested_test.rs")]
mod nested_test;

#[cfg(test)]
mod inherited_test_context {
    mod inherited_child;
}

mod inner_attribute_test_context {
    #![cfg(test)]
    mod inner_attribute_child;
}

use std::include as source_include;
include!("outside.inc");

const STRING_FAKE: &str = "#[path = \"fake.rs\"] include!(\"fake.inc\")";
// #[cfg(test)] #[path = "comment.rs"] mod comment;
"###;

    assert_eq!(
        external_cfg_test_modules(source),
        [
            ExternalCfgTestModule {
                name: "external_test".to_owned(),
                explicit_paths: vec!["external_test.rs".to_owned()],
                module_context: Vec::new(),
            },
            ExternalCfgTestModule {
                name: "nested_test".to_owned(),
                explicit_paths: vec!["nested_test.rs".to_owned()],
                module_context: Vec::new(),
            },
            ExternalCfgTestModule {
                name: "inherited_child".to_owned(),
                explicit_paths: Vec::new(),
                module_context: vec!["inherited_test_context".to_owned()],
            },
            ExternalCfgTestModule {
                name: "inner_attribute_child".to_owned(),
                explicit_paths: Vec::new(),
                module_context: vec!["inner_attribute_test_context".to_owned()],
            }
        ],
    );
    assert_eq!(
        rust_path_attributes(source),
        [
            RustPathAttribute::Literal("external_test.rs".to_owned()),
            RustPathAttribute::Literal("nested_test.rs".to_owned()),
        ],
    );
    assert_eq!(source_include_macro_count(source), 2);
}

#[test]
fn lint_suppression_scanner_covers_direct_broad_and_test_attributes() {
    let source = r###"
#[allow(clippy::too_many_arguments)]
fn direct() {}

#[allow(clippy :: too_many_arguments)]
fn spaced_direct() {}

#[cfg_attr(test, expect(clippy::all))]
fn broad_clippy() {}

#[expect(clippy /* structural trivia */ :: all)]
fn commented_broad_clippy() {}

#[allow(warnings)]
fn broad_rustc() {}

const STRING_FAKE: &str = "#[allow(clippy::too_many_arguments)]";
// #[allow(clippy::all)]
"###;

    assert_eq!(
        lint_suppression_counts(source),
        LintSuppressionCounts {
            too_many_arguments: 2,
            broad: 3,
        },
    );
}
