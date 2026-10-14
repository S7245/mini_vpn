//! Original Controller ledger provenance guard; no runtime lifecycle authority.
use super::*;

#[test]
fn controller_aborted_pages_use_one_checked_constructor_and_complete_cold_inventory() {
    let syntax = syn::parse_file(OWNER_SESSION_CONTROLLER_SOURCE)
        .expect("owner session controller remains valid Rust syntax");
    let cold_syntax = syn::parse_file(include_str!(
        "../../src/owned_upstream/owner_session_controller/cold_inventory.rs"
    ))
    .expect("the original cold audit remains valid Rust syntax");
    let owner_syntax = syn::parse_file(QUINN_OWNER_COMPOSITION_SOURCE).unwrap();
    for (parent, name) in [(&syntax, "cold_inventory"), (&owner_syntax, "inventory")] {
        let modules: Vec<_> = parent
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Mod(module) if module.ident == name => Some(module),
                _ => None,
            })
            .collect();
        assert_eq!(modules.len(), 1);
        assert!(matches!(modules[0].vis, syn::Visibility::Inherited));
        assert!(modules[0].attrs.is_empty() && modules[0].content.is_none());
    }
    assert!(impl_methods_named(&syntax, "cold_inventory").is_empty());
    assert!(impl_methods_named(&owner_syntax, "is_output_exact_zero").is_empty());
    let output_syntax = syn::parse_file(QUINN_OWNER_INVENTORY_SOURCE).unwrap();
    let target_syntax = syn::parse_file(OWNER_TARGET_SOURCE).unwrap();
    let routes: Vec<_> = target_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(module) if module.ident == "error_inventory" => Some(module),
            _ => None,
        })
        .collect();
    assert_eq!(routes.len(), 1);
    assert!(routes[0].attrs.is_empty() && routes[0].content.is_none());
    let syn::Visibility::Restricted(scope) = &routes[0].vis else {
        panic!("the diagnostic module must stay inside owned_upstream");
    };
    assert!(scope.in_token.is_some());
    assert_eq!(scope.path.segments.len(), 2);
    assert!(path_ends_with(&scope.path, &["crate", "owned_upstream"]));
    let error_syntax = syn::parse_file(include_str!(
        "../../src/owned_upstream/owner_target/error_inventory.rs"
    ))
    .unwrap();
    let error_methods: Vec<_> = error_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item)
                if item.trait_.is_none() && type_is_named(&item.self_ty, "OwnerTargetError") =>
            {
                Some(item)
            }
            _ => None,
        })
        .flat_map(|item| item.items.iter())
        .filter_map(|item| match item {
            ImplItem::Fn(method) if method.sig.ident == "cold_inventory" => Some(method),
            _ => None,
        })
        .collect();
    assert_eq!(error_methods.len(), 1);
    assert!(impl_methods_named(&target_syntax, "cold_inventory").is_empty());
    assert_eq!(
        impl_methods_named(&output_syntax, "is_output_exact_zero").len(),
        1
    );
    let aborted_structs = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == ABORTED_CONTROLLER_TURN => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        aborted_structs.len(),
        1,
        "the controller must retain one aborted-page ledger type",
    );
    let aborted_impls = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item) if item.trait_.is_none() => {
                let Type::Path(self_type) = item.self_ty.as_ref() else {
                    return None;
                };
                path_ends_with(&self_type.path, &[ABORTED_CONTROLLER_TURN]).then_some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        aborted_impls.len(),
        1,
        "the aborted-page ledger must retain one auditable inherent implementation",
    );
    let mut construction = AbortedControllerConstructionAudit::default();
    construction.visit_file(&syntax);
    construction.visit_file(&cold_syntax);
    construction.visit_file(
        &syn::parse_file(include_str!(
            "../../src/owned_upstream/owner_session_controller/terminal_grace.rs"
        ))
        .expect("owner terminal-grace implementation remains valid Rust syntax"),
    );
    assert_eq!(
        construction.explicit_named_literals, 0,
        "production code must not bypass AbortedOwnerControllerTurn::new with a named literal",
    );
    assert_eq!(
        construction.constructor_calls, 3,
        "the controller retains three closed returned/aborted-page construction paths",
    );
    assert_eq!(
        construction.checked_slot_installations, 3,
        "every controller abort must install the checked page ledger",
    );
    for item in &aborted_impls[0].items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let mut self_literals = SelfStructLiteralAudit::default();
        self_literals.visit_block(&method.block);
        if method.sig.ident == "new" {
            assert_eq!(
                self_literals.count, 1,
                "the checked constructor must materialize exactly one ledger",
            );
        } else {
            assert_eq!(
                self_literals.count, 0,
                "only AbortedOwnerControllerTurn::new may materialize Self",
            );
        }
    }
    let inventory_structs = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "OwnerSessionControllerColdInventory" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(inventory_structs.len(), 1);
    let mut actual_inventory_fields = inventory_structs[0]
        .fields
        .iter()
        .map(|field| {
            field
                .ident
                .as_ref()
                .expect("controller cold inventory uses named fields")
                .to_string()
        })
        .collect::<Vec<_>>();
    actual_inventory_fields.sort_unstable();
    let mut expected_inventory_fields = vec![
        "actor_origin_witnesses",
        "controller_retained_recovery_turns",
        "independently_summed_frame_bytes",
        "local_origin_witnesses",
        "malformed_page_arrival_witnesses",
        "malformed_page_origin_witnesses",
        "malformed_raw_continuations",
        "pending_continuations",
        "retained_application_ack_observations",
        "retained_continuations",
        "retained_error_payload",
        "retained_recovery_payload",
        "retained_observations",
        "retained_outputs",
        "retained_target_errors",
        "stored_frame_bytes",
        "target_aborted_effect_guards",
        "target_aborted_effects",
        "target_retained_ordinary_observations",
        "target_retained_ordinary_outputs",
        "target_retained_recovery_guards",
        "target_retained_recovery_observations",
        "target_retained_recovery_outputs",
        "total_continuations",
    ];
    expected_inventory_fields.sort_unstable();
    assert_eq!(
        actual_inventory_fields, expected_inventory_fields,
        "controller cold inventory field coverage must remain exhaustive",
    );
    assert_eq!(impl_methods_named(&cold_syntax, "cold_inventory").len(), 2);
    // The new opaque error visitor is not the original generic C audit. Lock
    // both exact implementation owners rather than weakening the C byte guard.
    let methods_for = |name: &str| {
        cold_syntax
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Impl(item) if item.trait_.is_none() && type_is_named(&item.self_ty, name) => {
                    Some(item)
                }
                _ => None,
            })
            .flat_map(|item| item.items.iter())
            .filter_map(|item| match item {
                ImplItem::Fn(method) if method.sig.ident == "cold_inventory" => Some(method),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let cold_inventory_methods = methods_for("OwnerSessionControllerCore");
    assert_eq!(cold_inventory_methods.len(), 1);
    assert_eq!(methods_for("OwnerSessionControllerError").len(), 1);
    let mut cold_calls = ControllerColdAuditCalls::default();
    cold_calls.visit_block(&cold_inventory_methods[0].block);
    assert_eq!(cold_calls.independent_error_mapping, 1);
    assert_eq!(cold_calls.stored_byte_audits, 1);
    assert_eq!(cold_calls.page_origin_reads, 1);
    // A nested diagnostic Result would hide failure from the existing Root
    // outer-Err branch. Lock this exact owning field's typed outer propagation.
    #[derive(Default)]
    struct RecoveryAuditField(usize);
    impl<'ast> syn::visit::Visit<'ast> for RecoveryAuditField {
        fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
            if path_ends_with(&node.path, &["OwnerSessionControllerColdInventory"]) {
                for field in &node.fields {
                    if !matches!(&field.member, syn::Member::Named(name) if name == "retained_recovery_payload")
                    {
                        continue;
                    }
                    let syn::Expr::Try(value) = &field.expr else {
                        panic!("audit failure must escape C");
                    };
                    let syn::Expr::MethodCall(map) = value.expr.as_ref() else {
                        panic!("typed mapping");
                    };
                    assert_eq!(map.method, "map_err");
                    assert_eq!(map.args.len(), 1);
                    let syn::Expr::Path(path) = &map.args[0] else {
                        panic!("exact error constructor");
                    };
                    assert!(path_ends_with(
                        &path.path,
                        &[
                            "OwnerSessionControllerColdInventoryError",
                            "RecoveryPayload"
                        ]
                    ));
                    let syn::Expr::MethodCall(audit) = map.receiver.as_ref() else {
                        panic!("original C traversal");
                    };
                    assert_eq!(audit.method, "audit_recovery_payload");
                    assert!(audit.args.is_empty());
                    self.0 += 1;
                }
            }
            syn::visit::visit_expr_struct(self, node);
        }
    }
    let mut recovery = RecoveryAuditField::default();
    recovery.visit_block(&cold_inventory_methods[0].block);
    assert_eq!(recovery.0, 1);
}
