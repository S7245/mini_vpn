//! Declaration locality for real root scenarios and their inward metadata.
//!
//! Owns their private routes, source inventory and metadata declaration scopes, not runtime
//! behavior, arbitrary cfg/macro semantics or the future Bound implementation.

use super::rust_source_scan::{
    external_rust_modules, inline_rust_module_names, rust_test_functions,
    source_include_macro_count,
};
use super::*;

#[test]
fn flow_lifecycle_completion_test_has_one_private_route() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/owned_upstream/owner_target/commands/tests/bound");
    assert_private_default_child(&directory.join("flow_lifecycle.rs"), "completion");
    let child = directory.join("flow_lifecycle/completion.rs");
    let mut paths = Vec::new();
    collect_root_rust_sources(&directory, &mut paths);
    let declarations = paths
        .iter()
        .flat_map(|path| {
            rust_test_functions(&fs::read_to_string(path).unwrap())
                .into_iter()
                .filter(|test| {
                    test.name == "received_reset_retries_once_after_real_terminal_pressure_releases"
                })
                .map(move |test| (path, test))
        })
        .collect::<Vec<_>>();
    assert_eq!(declarations.len(), 1, "the pressure test has one real body");
    assert_eq!(declarations[0].0, &child);
    assert!(declarations[0].1.module_context.is_empty());
}

#[test]
fn bound_failure_completion_tests_have_one_private_route() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/owned_upstream/owner_target/tests/r7_nested_abort");
    let parent = directory.join("bound_failure.rs");
    assert_private_default_child(&parent, "completion");
    let child = directory.join("bound_failure/completion.rs");
    let mut paths = Vec::new();
    collect_root_rust_sources(&directory, &mut paths);
    for name in [
        "cold_local_failure_retries_without_borrowing_another_flows_driver_charge",
        "real_z_pressure_retains_failed_work_and_driver_charge_until_actual_grace_release",
    ] {
        let declarations = paths
            .iter()
            .flat_map(|path| {
                rust_test_functions(&fs::read_to_string(path).unwrap())
                    .into_iter()
                    .filter(move |test| test.name == name)
                    .map(move |test| (path, test))
            })
            .collect::<Vec<_>>();
        assert_eq!(declarations.len(), 1, "{name} must have one real test body");
        assert_eq!(declarations[0].0, &child);
        assert!(declarations[0].1.module_context.is_empty());
    }
}

const ROOT: &str = "src/owned_upstream/quinn_leg_composition/owner/server_root";
const MOVED_DISPATCH_TESTS: &[(&str, &[&str])] = &[
    (
        "tests/ready_dispatch.rs",
        &[
            "ready_session_enqueued_during_cursor_handoff_is_not_lost",
            "scheduler_claim_corruption_stops_all_residents_without_discarding_commands",
            "unrelated_lane_rearm_registers_remaining_full_before_root_parks",
        ],
    ),
    (
        "tests/ready_dispatch/publisher_fatal.rs",
        &[
            "publisher_revision_exhaustion_closes_admission_before_another_resident_send",
            "process_revision_exhaustion_is_published_before_root_selection",
            "slot_poison_is_published_before_root_selection",
            "poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition",
            "first_published_poison_survives_later_process_revision_exhaustion",
            "publisher_fault_during_send_preserves_admitted_completion",
        ],
    ),
    (
        "tests/ready_dispatch/publisher_fatal/notify_unwind.rs",
        &["committed_publisher_fault_survives_root_notify_unwind"],
    ),
    (
        "tests/ready_dispatch/publisher_fatal/park_fatal.rs",
        &["fatal_during_cached_waker_replacement_prevents_repark"],
    ),
    (
        "tests/ready_dispatch/publisher_fatal/source_unknown.rs",
        &["healthy_source_unwind_quarantines_only_its_resident"],
    ),
];

fn assert_private_default_child(parent: &Path, name: &str) {
    let source = fs::read_to_string(parent).unwrap();
    let modules = external_rust_modules(&source);
    let matches = modules
        .iter()
        .filter(|module| module.name == name && module.module_context.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "{} must declare exactly one direct external {name} Module",
        parent.display(),
    );
    assert!(matches[0].is_private, "{name} must remain private");
    assert!(
        matches[0].is_unconditional,
        "{name} must inherit its test scope"
    );
    assert!(
        matches[0].uses_default_path,
        "{name} must use its default path"
    );
    assert!(
        !inline_rust_module_names(&source)
            .iter()
            .any(|module| module == name)
    );
    assert_eq!(source_include_macro_count(&source), 0);
}

fn collect_root_rust_sources(directory: &Path, paths: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let kind = entry.file_type().unwrap();
        let path = entry.path();
        assert!(
            !kind.is_symlink(),
            "root source inventory must not escape through symlinks"
        );
        if kind.is_dir() {
            collect_root_rust_sources(&path, paths);
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            paths.push(path);
        }
    }
}

fn inward_scheduler_aliases(tree: &syn::UseTree, prefix: &str) -> usize {
    match tree {
        syn::UseTree::Path(path) => {
            inward_scheduler_aliases(&path.tree, &format!("{prefix}{}::", path.ident))
        }
        syn::UseTree::Group(group) => group
            .items
            .iter()
            .map(|item| inward_scheduler_aliases(item, prefix))
            .sum(),
        syn::UseTree::Rename(rename)
            if rename.rename == "scheduler"
                && ((prefix == "crate::owned_upstream::owner_ready::"
                    && rename.ident == "self")
                    || (prefix == "crate::owned_upstream::" && rename.ident == "owner_ready")) =>
        {
            1
        }
        _ => 0,
    }
}

#[test]
fn owner_ready_metadata_has_one_inward_implementation_route() {
    use syn::{Item, Visibility};

    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let inward_path = repository.join("src/owned_upstream/owner_ready.rs");
    let inward_source = fs::read_to_string(&inward_path).unwrap();
    let inward = syn::parse_file(&inward_source).unwrap();
    // Check existing-file AST ownership first: the expected pre-Move RED is
    // absent ReadyIndex, not a missing test-child file or failed compilation.
    assert_eq!(
        inward
            .items
            .iter()
            .filter(|item| matches!(item, Item::Struct(item) if item.ident == "ReadyIndex"))
            .count(),
        1,
        "owner_ready must directly own the complete ReadyIndex declaration",
    );

    assert_private_default_child(&repository.join("src/owned_upstream/mod.rs"), "owner_ready");
    let root_path = repository.join(ROOT).with_extension("rs");
    let root_source = fs::read_to_string(&root_path).unwrap();
    let root = syn::parse_file(&root_source).unwrap();
    assert!(
        !root
            .items
            .iter()
            .any(|item| matches!(item, Item::Mod(item) if item.ident == "scheduler")),
        "the old scheduler Module must be removed, not forwarded"
    );
    let aliases = root
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Use(item) => Some((item, inward_scheduler_aliases(&item.tree, ""))),
            _ => None,
        })
        .filter(|(_, count)| *count != 0)
        .collect::<Vec<_>>();
    assert_eq!(
        aliases.len(),
        1,
        "Root must directly import its one inward scheduler"
    );
    assert_eq!(aliases[0].1, 1);
    assert!(matches!(aliases[0].0.vis, Visibility::Inherited));

    let is_super = |visibility: &Visibility| {
        matches!(visibility,
            Visibility::Restricted(restricted)
                if restricted.in_token.is_none() && restricted.path.is_ident("super")
        )
    };
    for (name, caller_visible) in [
        ("OwnerReadySource", true),
        ("SendLane", true),
        ("SendCursor", true),
        ("ReadyFault", true),
        ("ReadyFailureGate", false),
        ("ReadyKey", false),
        ("ReadyPhase", false),
        ("ReadyCell", false),
        ("ReadySlot", false),
        ("ReadyTree", false),
        ("ReadyShared", false),
        ("CapacityWake", false),
        ("ReadyClaim", true),
        ("ReadyIndex", true),
        ("ReadyGeometry", true),
        ("ReadyGeometryError", true),
    ] {
        let declarations = inward
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Struct(item) if item.ident == name => {
                    Some((&item.vis, &item.attrs, Some(&item.fields)))
                }
                Item::Enum(item) if item.ident == name => Some((&item.vis, &item.attrs, None)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declarations.len(),
            1,
            "{name} must be owned directly inward"
        );
        let (visibility, attributes, fields) = declarations[0];
        assert!(
            attributes
                .iter()
                .all(|attribute| !attribute.path().is_ident("cfg")
                    && !attribute.path().is_ident("cfg_attr")),
            "{name} must not disappear behind configuration"
        );
        assert!(
            if caller_visible {
                is_super(visibility)
            } else {
                matches!(visibility, Visibility::Inherited)
            },
            "{name} must keep its frozen caller visibility"
        );
        if let Some(fields) = fields {
            for field in fields {
                let original_claim_value = name == "ReadyClaim"
                    && field
                        .ident
                        .as_ref()
                        .is_some_and(|field| field == "slot" || field == "source");
                assert!(
                    if original_claim_value {
                        is_super(&field.vis)
                    } else {
                        matches!(field.vis, Visibility::Inherited)
                    },
                    "{name} must not expose private storage"
                );
            }
        }
        assert!(
            !root.items.iter().any(|item| match item {
                Item::Struct(item) => item.ident == name,
                Item::Enum(item) => item.ident == name,
                _ => false,
            }),
            "{name} must not have an outer duplicate"
        );
    }
    for source in [&inward_source, &root_source] {
        assert_eq!(source_include_macro_count(source), 0);
    }
    let test_routes = external_rust_modules(&inward_source)
        .into_iter()
        .filter(|module| module.name == "tests" && module.module_context.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(test_routes.len(), 1);
    assert!(test_routes[0].is_private && test_routes[0].uses_default_path);
    let test_modules = inward
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(item) if item.ident == "tests" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(test_modules.len(), 1);
    assert!(test_modules[0].content.is_none());
    assert_eq!(test_modules[0].attrs.len(), 1);
    assert!(test_modules[0].attrs[0].path().is_ident("cfg"));
    assert!(
        test_modules[0].attrs[0]
            .parse_args::<syn::Path>()
            .is_ok_and(|path| path.is_ident("test"))
    );
    let tests = repository.join("src/owned_upstream/owner_ready/tests");
    assert_private_default_child(&tests.join("mod.rs"), "ready_dispatch");
    assert_private_default_child(&tests.join("ready_dispatch.rs"), "publisher_fatal");
    for name in ["notify_unwind", "park_fatal", "source_unknown"] {
        assert_private_default_child(&tests.join("ready_dispatch/publisher_fatal.rs"), name);
    }
    for relative in [
        "scheduler.rs",
        "scheduler/tests/mod.rs",
        "scheduler/tests/ready_dispatch.rs",
        "scheduler/tests/ready_dispatch/publisher_fatal.rs",
        "scheduler/tests/ready_dispatch/publisher_fatal/notify_unwind.rs",
        "scheduler/tests/ready_dispatch/publisher_fatal/park_fatal.rs",
        "scheduler/tests/ready_dispatch/publisher_fatal/source_unknown.rs",
    ] {
        assert!(
            !repository.join(ROOT).join(relative).exists(),
            "the moved source {relative} must not remain as a second implementation"
        );
    }
}

#[test]
fn owner_root_dispatch_scenarios_have_one_outer_test_route() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(ROOT);
    // Check the route before reading any destination, so the pre-Move RED is
    // the missing ownership declaration rather than a missing include_str! file.
    assert_private_default_child(&root.join("tests/mod.rs"), "ready_dispatch");
    assert_private_default_child(&root.join("tests/ready_dispatch.rs"), "publisher_fatal");
    for child in ["notify_unwind", "park_fatal", "source_unknown"] {
        assert_private_default_child(&root.join("tests/ready_dispatch/publisher_fatal.rs"), child);
    }

    let mut paths = vec![root.with_extension("rs")];
    collect_root_rust_sources(&root, &mut paths);
    let declarations = paths
        .into_iter()
        .map(|path| {
            let source = fs::read_to_string(&path).unwrap();
            assert_eq!(source_include_macro_count(&source), 0);
            (path, rust_test_functions(&source))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        MOVED_DISPATCH_TESTS
            .iter()
            .map(|(_, tests)| tests.len())
            .sum::<usize>(),
        12
    );
    for (relative_path, names) in MOVED_DISPATCH_TESTS {
        let expected_path = root.join(relative_path);
        for name in *names {
            let sites = declarations
                .iter()
                .flat_map(|(path, tests)| {
                    tests
                        .iter()
                        .filter(|test| test.name == *name)
                        .map(move |test| (path, test))
                })
                .collect::<Vec<_>>();
            assert_eq!(
                sites.len(),
                1,
                "{name} must have one definition across the root source tree"
            );
            assert_eq!(
                sites[0].0, &expected_path,
                "{name} must leave the metadata test tree"
            );
            assert!(
                sites[0].1.module_context.is_empty(),
                "{name} must remain a direct leaf test"
            );
        }
    }
}
// Actor implementation locality is separate from owner runtime acceptance.
#[test]
fn actor_readiness_and_output_implementations_have_one_private_route() {
    use syn::{ImplItem, Item, ItemImpl, Type, Visibility};

    fn implementations<'a>(file: &'a syn::File, name: &str) -> Vec<&'a ItemImpl> {
        file.items
            .iter()
            .filter_map(|item| match item {
                Item::Impl(item)
                    if matches!(item.self_ty.as_ref(), Type::Path(ty)
                        if ty.qself.is_none() && ty.path.is_ident(name)) =>
                {
                    Some(item)
                }
                _ => None,
            })
            .collect()
    }

    fn method<'a>(implementation: &'a ItemImpl, name: &str) -> &'a syn::ImplItemFn {
        let matches = implementation
            .items
            .iter()
            .filter_map(|item| match item {
                ImplItem::Fn(item) if item.sig.ident == name => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{name} must have one direct method body");
        matches[0]
    }

    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let parent_path = repository.join("src/owned_upstream/quinn_leg_actor.rs");
    // This checks the actual parent before opening destinations. The expected
    // pre-Move RED is the missing private route, not a missing-file failure.
    for name in ["readiness", "output"] {
        assert_private_default_child(&parent_path, name);
    }
    let parent = syn::parse_file(&fs::read_to_string(&parent_path).unwrap()).unwrap();
    let children = ["readiness", "output"].map(|name| {
        let source = fs::read_to_string(
            repository.join(format!("src/owned_upstream/quinn_leg_actor/{name}.rs")),
        )
        .unwrap();
        assert_eq!(source_include_macro_count(&source), 0);
        // Preserve bootstrap's original whole-parent negative-check domain.
        // Its unchanged parent check and these actual children cover the Move.
        for forbidden in [
            "into_bound_inbound",
            "into_inbound",
            "as_bound_inbound",
            "QuinnLegActorInbound) -> BoundInbound",
        ] {
            assert!(
                !source.contains(forbidden),
                "production naked inbound seam reappeared in {name} as {forbidden}",
            );
        }
        syn::parse_file(&source).unwrap()
    });
    for file in [&parent, &children[0], &children[1]] {
        assert!(file.attrs.iter().all(|attribute| {
            !attribute.path().is_ident("cfg") && !attribute.path().is_ident("cfg_attr")
        }));
    }
    for name in ["Readiness", "ActorEncodedTransport", "StartedQuinnLegActor"] {
        let anchors = parent
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Struct(item) if item.ident == name => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(anchors.len(), 1, "{name} storage must remain in the actor");
        for field in &anchors[0].fields {
            assert!(matches!(field.vis, Visibility::Inherited));
        }
        for child in &children {
            assert!(
                !child
                    .items
                    .iter()
                    .any(|item| matches!(item, Item::Struct(item) if item.ident == name))
            );
        }
    }

    let readiness = implementations(&children[0], "Readiness");
    assert_eq!(readiness.len(), 1);
    assert!(readiness[0].trait_.is_none() && readiness[0].attrs.is_empty());
    assert!(implementations(&parent, "Readiness").is_empty());
    assert!(implementations(&children[1], "Readiness").is_empty());
    for name in [
        "new",
        "current",
        "reserved",
        "same_source",
        "advance",
        "reserve_next",
        "publish",
        "wait_after",
        "wait_after_or_cancel",
    ] {
        let item = method(readiness[0], name);
        assert!(item.attrs.is_empty());
        assert!(matches!(&item.vis, Visibility::Restricted(vis)
            if vis.in_token.is_none() && vis.path.is_ident("super")));
    }

    let transport = implementations(&children[1], "ActorEncodedTransport");
    assert_eq!(transport.len(), 1);
    assert!(transport[0].attrs.is_empty());
    let (negative, trait_path, _) = transport[0].trait_.as_ref().unwrap();
    assert!(negative.is_none() && trait_path.is_ident("EncodedLegTransport"));
    assert!(implementations(&parent, "ActorEncodedTransport").is_empty());
    assert!(implementations(&children[0], "ActorEncodedTransport").is_empty());
    for (name, test_only) in [
        ("submit_encoded", false),
        ("send_encoded", true),
        ("send_attach_ordered_encoded", true),
    ] {
        let item = method(transport[0], name);
        assert!(matches!(item.vis, Visibility::Inherited));
        if test_only {
            assert_eq!(item.attrs.len(), 1);
            assert!(item.attrs[0].path().is_ident("cfg"));
            assert!(
                item.attrs[0]
                    .parse_args::<syn::Path>()
                    .is_ok_and(|path| path.is_ident("test"))
            );
        } else {
            assert!(item.attrs.is_empty());
        }
    }
    let output = implementations(&children[1], "StartedQuinnLegActor");
    assert_eq!(output.len(), 1);
    assert!(output[0].attrs.is_empty() && output[0].trait_.is_none());
    let flush = method(output[0], "try_flush");
    assert!(flush.attrs.is_empty());
    assert!(matches!(&flush.vis, Visibility::Restricted(vis)
        if vis.in_token.is_none() && vis.path.is_ident("crate")));
    for file in [&parent, &children[0]] {
        assert!(
            implementations(file, "StartedQuinnLegActor")
                .iter()
                .all(|implementation| !implementation.items.iter().any(
                    |item| matches!(item, ImplItem::Fn(item) if item.sig.ident == "try_flush")
                ))
        );
    }
}

#[test]
fn target_join_retirement_has_one_private_completion_owner() {
    use syn::{ImplItem, Item, ItemImpl, Type, Visibility};

    fn methods<'a>(file: &'a syn::File, name: &str) -> Vec<(&'a ItemImpl, &'a syn::ImplItemFn)> {
        file.items
            .iter()
            .filter_map(|item| match item {
                Item::Impl(item)
                    if matches!(item.self_ty.as_ref(), Type::Path(ty)
                        if ty.qself.is_none() && ty.path.segments.len() == 1
                            && ty.path.segments[0].ident == "OwnerTargetExecutor") =>
                {
                    Some(item)
                }
                _ => None,
            })
            .flat_map(|implementation| {
                implementation
                    .items
                    .iter()
                    .filter_map(move |item| match item {
                        ImplItem::Fn(method) if method.sig.ident == name => {
                            Some((implementation, method))
                        }
                        _ => None,
                    })
            })
            .collect()
    }

    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let parent_path = repository.join("src/owned_upstream/owner_target.rs");
    let directory = repository.join("src/owned_upstream/owner_target");
    let completion_path = directory.join("completion.rs");
    assert_private_default_child(&parent_path, "completion");
    assert_private_default_child(&completion_path, "retirement");
    let retirement_path = directory.join("completion/retirement.rs");
    let bound_path = directory.join("commands/tests/bound.rs");
    assert_private_default_child(&bound_path, "retirement");
    let test_path = directory.join("commands/tests/bound/retirement.rs");
    let mut paths = vec![parent_path];
    collect_root_rust_sources(&directory, &mut paths);
    let files = paths
        .iter()
        .map(|path| {
            let source = fs::read_to_string(path).unwrap();
            (path, syn::parse_file(&source).unwrap())
        })
        .collect::<Vec<_>>();

    for (name, parent_called) in [
        ("finish_target_flow", true),
        ("retire_target_tombstone", true),
        ("retire_recorded_target_tombstone", false),
        ("try_join_target_flow", true),
        ("settle_terminal_receipt", true),
        ("retire_just_joined_tombstone", false),
        ("drain_pending_target_joins", false),
        ("expire_next_target", true),
    ] {
        let declarations = files
            .iter()
            .flat_map(|(path, file)| {
                methods(file, name)
                    .into_iter()
                    .map(move |(implementation, method)| (*path, file, implementation, method))
            })
            .collect::<Vec<_>>();
        assert_eq!(declarations.len(), 1, "{name} must have one actual body");
        let (path, file, implementation, method) = declarations[0];
        assert_eq!(
            path, &retirement_path,
            "{name} must live in the private retirement implementation"
        );
        assert!(implementation.trait_.is_none());
        for attributes in [&file.attrs, &implementation.attrs, &method.attrs] {
            assert!(attributes.iter().all(|attribute| {
                !attribute.path().is_ident("cfg") && !attribute.path().is_ident("cfg_attr")
            }));
        }
        if parent_called {
            assert!(matches!(&method.vis, Visibility::Restricted(vis)
                if vis.in_token.is_some()
                    && vis.path.leading_colon.is_none()
                    && vis.path.segments.len() == 3
                    && vis.path.segments.iter()
                        .zip(["crate", "owned_upstream", "owner_target"])
                        .all(|(segment, name)| segment.ident == name
                            && matches!(segment.arguments, syn::PathArguments::None))));
        } else {
            assert!(matches!(method.vis, Visibility::Inherited));
        }
    }

    let name = "cold_retiring_keeps_original_terminal_through_ready_installation";
    let declarations = paths
        .iter()
        .flat_map(|path| {
            rust_test_functions(&fs::read_to_string(path).unwrap())
                .into_iter()
                .filter(move |test| test.name == name)
                .map(move |test| (path, test))
        })
        .collect::<Vec<_>>();
    assert_eq!(declarations.len(), 1, "{name} must have one real test body");
    assert_eq!(declarations[0].0, &test_path);
    assert!(declarations[0].1.module_context.is_empty());
}

#[test]
fn owner_ready_publication_has_one_private_implementation_owner() {
    use syn::{ImplItem, Item, Type, Visibility};
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let directory = repository.join("src/owned_upstream/owner_ready");
    let parent = directory.join("flow.rs");
    let child = directory.join("flow/publication.rs");
    assert_private_default_child(&parent, "publication");
    let mut paths = vec![repository.join("src/owned_upstream/owner_ready.rs")];
    collect_root_rust_sources(&directory, &mut paths);
    let files = paths
        .iter()
        .map(|path| syn::parse_file(&fs::read_to_string(path).unwrap()).unwrap())
        .collect::<Vec<_>>();
    let unconditional = |attrs: &[syn::Attribute]| {
        attrs
            .iter()
            .all(|a| !a.path().is_ident("cfg") && !a.path().is_ident("cfg_attr"))
    };
    for (owner, names, visible, wake) in [
        (
            "OwnerReadySessionBinding",
            ["request_retained_flow_work", "begin_publication"],
            true,
            false,
        ),
        (
            "OwnerPublicationTurn",
            ["finish_quarantined", "finish"],
            true,
            false,
        ),
        (
            "ReadyShared",
            ["publish_bound", "notify_bound"],
            false,
            false,
        ),
        ("FlowWake", ["wake", "wake_by_ref"], false, true),
        ("SessionWake", ["wake", "wake_by_ref"], false, true),
    ] {
        for name in names {
            let mut declarations = Vec::new();
            for (path, file) in paths.iter().zip(&files) {
                for item in &file.items {
                    let Item::Impl(implementation) = item else {
                        continue;
                    };
                    if !matches!(implementation.self_ty.as_ref(), Type::Path(ty)
                        if ty.qself.is_none() && ty.path.is_ident(owner))
                    {
                        continue;
                    }
                    for item in &implementation.items {
                        if let ImplItem::Fn(method) = item
                            && method.sig.ident == name
                        {
                            declarations.push((path, file, implementation, method));
                        }
                    }
                }
            }
            assert_eq!(declarations.len(), 1, "{owner}::{name} must have one body");
            let (path, file, implementation, method) = declarations[0];
            assert_eq!(path, &child, "{owner}::{name} must leave the old parent");
            match &implementation.trait_ {
                Some((negative, trait_path, _)) => {
                    assert!(wake && negative.is_none() && trait_path.is_ident("Wake"));
                    assert_eq!(implementation.items.len(), 2, "complete Wake impl");
                }
                None => assert!(!wake),
            }
            for attrs in [&file.attrs, &implementation.attrs, &method.attrs] {
                assert!(unconditional(attrs));
            }
            if visible {
                assert!(matches!(&method.vis, Visibility::Restricted(vis)
                    if vis.in_token.is_some() && vis.path.leading_colon.is_none()
                        && vis.path.segments.len() == 2
                        && vis.path.segments.iter().zip(["crate", "owned_upstream"])
                            .all(|(s, name)| s.ident == name
                                && matches!(s.arguments, syn::PathArguments::None))));
            } else {
                assert!(matches!(method.vis, Visibility::Inherited));
            }
        }
    }
    let enums = paths
        .iter()
        .zip(&files)
        .flat_map(|(path, file)| {
            file.items.iter().filter_map(move |item| match item {
                Item::Enum(item) if item.ident == "BoundRequest" => Some((path, item)),
                _ => None,
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(enums.len(), 1);
    assert_eq!(enums[0].0, &child);
    assert!(matches!(enums[0].1.vis, Visibility::Inherited));
    assert!(unconditional(&enums[0].1.attrs));

    let child_file = &files[paths.iter().position(|path| path == &child).unwrap()];
    let routes = child_file
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(route) if route.ident == "tests" => Some(route),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(routes.len(), 1);
    let route = routes[0];
    assert!(matches!(route.vis, Visibility::Inherited) && route.content.is_none());
    assert_eq!(route.attrs.len(), 1);
    let syn::Meta::List(cfg) = &route.attrs[0].meta else {
        panic!("cfg(test) route");
    };
    assert!(cfg.path.is_ident("cfg") && cfg.parse_args::<syn::Path>().unwrap().is_ident("test"));
    assert_private_default_child(&directory.join("flow/publication/tests/mod.rs"), "retained");
}
