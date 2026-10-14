//! Knife16 provenance for the private owner continuation authority.
//!
//! Owns source-layout characterization only; production terminal behavior and
//! physical-root lifecycle remain outside this test child.

use super::*;

#[path = "../support/rust_source_scan.rs"]
mod rust_source_scan;

#[test]
fn owner_continuation_authority_is_private_and_binds_one_controller_output_root() {
    let owner_syntax = syn::parse_file(QUINN_OWNER_COMPOSITION_SOURCE)
        .expect("owner composition remains valid Rust syntax");
    let continuation_modules = owner_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Mod(module) if module.ident == "continuation" => Some(module),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [continuation_module] = continuation_modules.as_slice() else {
        panic!(
            "owner composition must declare exactly one private continuation leaf; found {}",
            continuation_modules.len(),
        );
    };
    assert!(
        matches!(continuation_module.vis, syn::Visibility::Inherited),
        "the continuation leaf must stay private to the owner module",
    );
    assert!(
        continuation_module.content.is_none(),
        "continuation authority must live in its external leaf file",
    );
    assert!(
        !has_direct_cfg_test(&continuation_module.attrs),
        "the controller-output authority must compile outside tests",
    );
    assert!(
        !QUINN_COMPOSITION_SOURCE.contains("struct QuinnOwnerContinuationPermit"),
        "the parent module must not retain a child-forgeable permit constructor",
    );
    assert!(
        !QUINN_OWNER_COMPOSITION_SOURCE.contains("struct QuinnOwnerOutputContinuationTransaction"),
        "the legacy transaction must not remain forgeable by owner siblings",
    );

    let continuation_source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/owned_upstream/quinn_leg_composition/owner/continuation.rs"
    ))
    .expect("the private continuation leaf must exist");
    let continuation_syntax = syn::parse_file(&continuation_source)
        .expect("the private continuation leaf remains valid Rust syntax");

    let lease = continuation_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerContinuationLease" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [lease] = lease.as_slice() else {
        panic!(
            "the continuation leaf must retain exactly one nominal lease; found {}",
            lease.len(),
        );
    };
    assert!(
        matches!(lease.vis, syn::Visibility::Inherited),
        "only the continuation leaf may construct the nominal lease",
    );

    let permits = continuation_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerContinuationPermit" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [permit] = permits.as_slice() else {
        panic!(
            "the continuation leaf must retain exactly one permit; found {}",
            permits.len(),
        );
    };
    let syn::Fields::Named(permit_fields) = &permit.fields else {
        panic!("the continuation permit must hold one real nominal-lease borrow");
    };
    assert_eq!(permit_fields.named.len(), 1);
    let permit_lease = permit_fields
        .named
        .first()
        .expect("the continuation permit retains its exact lease borrow");
    let Type::Reference(permit_lease_type) = &permit_lease.ty else {
        panic!("the continuation permit must hold a reference to its nominal lease");
    };
    assert!(
        permit_lease_type.mutability.is_some(),
        "the permit must linearly borrow the nominal lease",
    );
    assert!(matches!(
        permit_lease_type.elem.as_ref(),
        Type::Path(path)
            if path_ends_with(&path.path, &["QuinnOwnerContinuationLease"])
    ));
    assert!(
        !continuation_source.contains("PhantomData"),
        "a marker-only lifetime does not prove custody of the nominal lease",
    );

    let roots = continuation_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerControllerOutputRoot" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [root] = roots.as_slice() else {
        panic!(
            "the continuation leaf must retain exactly one controller-output root; found {}",
            roots.len(),
        );
    };
    assert!(
        !has_direct_cfg_test(&root.attrs),
        "the controller-output root must be a non-test production type",
    );
    assert!(
        !matches!(root.vis, syn::Visibility::Public(_)),
        "the controller-output root must not become a crate-wide public authority",
    );
    let syn::Fields::Named(root_fields) = &root.fields else {
        panic!("the controller-output root must name every owned authority");
    };
    for (name, count) in [
        (
            "OwnerSessionController",
            root_fields
                .named
                .iter()
                .filter(|field| type_is_named(&field.ty, "OwnerSessionController"))
                .count(),
        ),
        (
            "QuinnOwnerOutputSubroot",
            root_fields
                .named
                .iter()
                .filter(|field| type_is_named(&field.ty, "QuinnOwnerOutputSubroot"))
                .count(),
        ),
        (
            "QuinnOwnerControllerFailureLedger",
            root_fields
                .named
                .iter()
                .filter(|field| type_is_named(&field.ty, "QuinnOwnerControllerFailureLedger"))
                .count(),
        ),
        (
            "QuinnOwnerContinuationLease",
            root_fields
                .named
                .iter()
                .filter(|field| type_is_named(&field.ty, "QuinnOwnerContinuationLease"))
                .count(),
        ),
    ] {
        assert_eq!(
            count, 1,
            "the exact root must own one {name}; found {count}",
        );
    }
    assert_eq!(
        root_fields
            .named
            .iter()
            .filter(|field| type_is_option_of(&field.ty, "OwnerSessionControllerError"))
            .count(),
        0,
        "an Option-style first-error slot can silently discard a second ownership-carrying error",
    );

    let failure_ledgers = continuation_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Enum(item) if item.ident == "QuinnOwnerControllerFailureLedger" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [failure_ledger] = failure_ledgers.as_slice() else {
        panic!(
            "the controller-output leaf must retain exactly one closed failure ledger; found {}",
            failure_ledgers.len(),
        );
    };
    assert!(
        matches!(failure_ledger.vis, syn::Visibility::Inherited),
        "the failure ledger must not be externally writable",
    );
    let ready = failure_ledger
        .variants
        .iter()
        .find(|variant| variant.ident == "Ready")
        .expect("the failure ledger retains one clean Ready state");
    assert!(matches!(ready.fields, syn::Fields::Unit));
    let failed = failure_ledger
        .variants
        .iter()
        .find(|variant| variant.ident == "Failed")
        .expect("the failure ledger retains one ownership-carrying Failed state");
    let syn::Fields::Unnamed(failed_fields) = &failed.fields else {
        panic!("Failed must directly own the exact controller error");
    };
    assert_eq!(failed_fields.unnamed.len(), 1);
    assert!(
        failed_fields
            .unnamed
            .first()
            .is_some_and(|field| type_is_named(&field.ty, "OwnerSessionControllerError")),
        "Failed must retain the exact controller error without an Option writer",
    );
    assert_eq!(
        source_occurrences(&continuation_source, "Option<OwnerSessionControllerError>"),
        0,
        "the unified root must not reintroduce an Option-style controller-failure slot",
    );

    let transactions = continuation_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerOutputContinuationTransaction" => {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [transaction] = transactions.as_slice() else {
        panic!(
            "the exact root must mint one private continuation transaction type; found {}",
            transactions.len(),
        );
    };
    assert!(
        !matches!(transaction.vis, syn::Visibility::Public(_)),
        "the continuation transaction must not become a crate-wide authority",
    );
    let syn::Fields::Named(transaction_fields) = &transaction.fields else {
        panic!("the continuation transaction must name its exact root borrow");
    };
    assert_eq!(
        transaction_fields
            .named
            .iter()
            .filter(|field| {
                type_is_mutable_reference_to(&field.ty, "QuinnOwnerControllerOutputRoot")
            })
            .count(),
        1,
        "the transaction must directly and exclusively borrow one controller-output root",
    );
    assert_eq!(
        transaction_fields.named.len(),
        1,
        "continuation phase and marker ownership belong to the borrowed root, not guard-local fields",
    );
    assert!(
        transaction_fields
            .named
            .iter()
            .all(|field| matches!(field.vis, syn::Visibility::Inherited)),
        "transaction construction fields must remain private to the continuation leaf",
    );
    let continue_methods = impl_methods_named(&continuation_syntax, "continue_and_settle");
    let [continue_method] = continue_methods.as_slice() else {
        panic!(
            "the continuation leaf must retain one closed continue-and-settle method; found {}",
            continue_methods.len(),
        );
    };
    assert_eq!(
        continue_method.sig.inputs.len(),
        1,
        "continue-and-settle must consume only self; an arbitrary controller parameter permits cross-root pairing",
    );
    assert!(matches!(
        continue_method.sig.inputs.first(),
        Some(syn::FnArg::Receiver(receiver)) if receiver.reference.is_none()
    ));
    assert!(
        continue_method.sig.asyncness.is_none(),
        "controller mutation and output settlement must stay synchronous",
    );

    let classified = owner_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Enum(item) if item.ident == "QuinnOwnerRecoveryClassified" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [classified] = classified.as_slice() else {
        panic!(
            "owner input must retain exactly one closed classification enum; found {}",
            classified.len(),
        );
    };
    for (variant_name, expected_type) in [
        ("Attach", "OwnerSessionRegisteredAttachDrive"),
        ("Ordinary", "QuinnOwnerOrdinaryClassified"),
    ] {
        let variant = classified
            .variants
            .iter()
            .find(|variant| variant.ident == variant_name)
            .unwrap_or_else(|| panic!("classification is missing {variant_name}"));
        let syn::Fields::Unnamed(fields) = &variant.fields else {
            panic!("{variant_name} must directly carry one settled success value");
        };
        assert_eq!(fields.unnamed.len(), 1);
        assert!(
            fields
                .unnamed
                .first()
                .is_some_and(|field| type_is_named(&field.ty, expected_type)),
            "{variant_name} must not wrap controller/output failure in a nested Result success",
        );
    }

    let physical_root_syntax = syn::parse_file(QUINN_OWNER_PHYSICAL_ROOT_SOURCE)
        .expect("the physical-root child remains valid Rust syntax");
    let recovery_adapters = physical_root_syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerRecoverySessionComposition" => {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [recovery_adapter] = recovery_adapters.as_slice() else {
        panic!(
            "the compatibility recovery adapter must remain unique; found {}",
            recovery_adapters.len(),
        );
    };
    let syn::Fields::Named(adapter_fields) = &recovery_adapter.fields else {
        panic!("the compatibility recovery adapter must name its owned roots");
    };
    assert_eq!(
        adapter_fields
            .named
            .iter()
            .filter(|field| type_is_named(&field.ty, "QuinnOwnerControllerOutputRoot"))
            .count(),
        1,
        "the adapter must hold the controller/output authority only through one root",
    );
    for forbidden in [
        "OwnerSessionController",
        "QuinnOwnerOutputSubroot",
        "OwnerSessionControllerError",
    ] {
        assert_eq!(
            adapter_fields
                .named
                .iter()
                .filter(|field| {
                    type_is_named(&field.ty, forbidden) || type_is_option_of(&field.ty, forbidden)
                })
                .count(),
            0,
            "the adapter must not retain split {forbidden} authority",
        );
    }
}

#[test]
fn owner_continuation_hard_terminal_implementation_has_one_closed_route() {
    let parent_source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/owned_upstream/quinn_leg_composition/owner/continuation.rs"
    ))
    .expect("the production continuation parent must remain readable");
    let modules = rust_source_scan::external_rust_modules(&parent_source);
    let routes = modules
        .iter()
        .filter(|module| module.name == "hard_terminal")
        .collect::<Vec<_>>();
    let [route] = routes.as_slice() else {
        panic!(
            "production continuation must declare exactly one external hard_terminal Module; found {}",
            routes.len(),
        );
    };
    assert!(
        route.module_context.is_empty()
            && route.is_private
            && route.is_unconditional
            && route.uses_default_path,
        "hard_terminal must be a direct private unconditional default-path child",
    );
    assert_eq!(
        (
            rust_source_scan::inline_rust_module_names(&parent_source)
                .iter()
                .filter(|name| name.as_str() == "hard_terminal")
                .count(),
            rust_source_scan::source_include_macro_count(&parent_source),
        ),
        (0, 0),
        "hard_terminal must not use an inline or source-include bypass",
    );

    let child_source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/owned_upstream/quinn_leg_composition/owner/continuation/hard_terminal.rs"
    ))
    .expect("the private hard-terminal implementation child must exist");
    assert_eq!(
        rust_source_scan::source_include_macro_count(&child_source),
        0,
    );
    let parent =
        syn::parse_file(&parent_source).expect("the continuation parent must remain valid Rust");
    let child =
        syn::parse_file(&child_source).expect("the hard-terminal child must remain valid Rust");

    let is_super = |visibility: &syn::Visibility| {
        matches!(
            visibility,
            syn::Visibility::Restricted(restricted)
                if restricted.in_token.is_none() && restricted.path.is_ident("super")
        )
    };
    let is_owner = |visibility: &syn::Visibility| {
        matches!(
            visibility,
            syn::Visibility::Restricted(restricted)
                if restricted.in_token.is_some()
                    && restricted
                        .path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect::<Vec<_>>()
                        .join("::")
                        == "crate::owned_upstream::quinn_leg_composition::owner"
        )
    };
    let has_cfg_not_test = |attributes: &[syn::Attribute]| {
        attributes.iter().any(|attribute| {
            attribute.path().is_ident("cfg")
                && attribute.parse_args::<syn::Meta>().is_ok_and(|meta| {
                    matches!(
                        meta,
                        syn::Meta::List(list)
                            if list.path.is_ident("not") && list.tokens.to_string() == "test"
                    )
                })
        })
    };
    let enum_count = |syntax: &syn::File, name: &str| {
        syntax
            .items
            .iter()
            .filter(|item| matches!(item, Item::Enum(item) if item.ident == name))
            .count()
    };
    let roots = parent
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerControllerOutputRoot" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        roots.len(),
        1,
        "parent must retain one controller/output root"
    );
    let root = roots[0];
    assert!(is_super(&root.vis));
    let syn::Fields::Named(fields) = &root.fields else {
        panic!("controller/output root must retain named storage");
    };
    let terminal_fields = fields
        .named
        .iter()
        .filter(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident == "terminal")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        terminal_fields.len(),
        1,
        "parent must retain terminal storage"
    );
    let terminal = terminal_fields[0];
    assert!(matches!(terminal.vis, syn::Visibility::Inherited));
    assert!(type_is_named(
        &terminal.ty,
        "QuinnOwnerControllerOutputTerminalState",
    ));
    for stable in [
        "QuinnOwnerControllerOutputTerminalObservation",
        "QuinnOwnerControllerOutputTerminalBeginError",
        "QuinnOwnerControllerOutputTerminalCallbackDrive",
    ] {
        let declarations = parent
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Enum(item) if item.ident == stable => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(declarations.len(), 1, "parent must retain one {stable}");
        let declaration = declarations[0];
        assert!(has_direct_cfg_test(&declaration.attrs));
        assert!(is_super(&declaration.vis));
    }

    for retained in ["new", "controller_failed", "output_inventory"] {
        let methods = impl_methods_named(&parent, retained);
        assert_eq!(methods.len(), 1, "parent must retain {retained}");
        assert!(is_super(&methods[0].vis));
    }

    let child_root_impls = child
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item)
                if item.trait_.is_none()
                    && type_is_named(&item.self_ty, "QuinnOwnerControllerOutputRoot") =>
            {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(child_root_impls.len(), 1, "child must own one root impl");
    let child_root_impl = child_root_impls[0];
    let child_root_methods = child_root_impl
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut child_root_names = child_root_methods
        .iter()
        .map(|method| method.sig.ident.to_string())
        .collect::<Vec<_>>();
    child_root_names.sort();
    assert_eq!(
        child_root_names.join(","),
        "arm_terminal_bind_postcommit_panic_for_test,begin_terminal_transaction,terminal_observation",
    );
    for moved in &child_root_methods {
        assert!(impl_methods_named(&parent, &moved.sig.ident.to_string()).is_empty());
        assert!(has_direct_cfg_test(&moved.attrs) && is_owner(&moved.vis));
    }

    let states = child
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Enum(item) if item.ident == "QuinnOwnerControllerOutputTerminalState" => {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(states.len(), 2);
    assert!(states.iter().all(|state| is_super(&state.vis)));
    assert_eq!(
        (
            states
                .iter()
                .filter(|state| has_direct_cfg_test(&state.attrs))
                .count(),
            states
                .iter()
                .filter(|state| has_cfg_not_test(&state.attrs))
                .count(),
        ),
        (1, 1),
    );
    let state_impls = child
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item)
                if item.trait_.is_none()
                    && type_is_named(&item.self_ty, "QuinnOwnerControllerOutputTerminalState") =>
            {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(state_impls.len(), 2);
    assert_eq!(
        (
            state_impls
                .iter()
                .filter(|item| has_direct_cfg_test(&item.attrs))
                .count(),
            state_impls
                .iter()
                .filter(|item| has_cfg_not_test(&item.attrs))
                .count(),
        ),
        (1, 1),
    );
    assert!(state_impls.iter().all(|item| {
        matches!(
            item.items.as_slice(),
            [ImplItem::Fn(method)]
                if method.sig.ident == "inventory" && is_super(&method.vis)
        )
    }));
    assert!(parent.items.iter().all(|item| match item {
        Item::Enum(item) => item.ident != "QuinnOwnerControllerOutputTerminalState",
        Item::Impl(item) => {
            !type_is_named(&item.self_ty, "QuinnOwnerControllerOutputTerminalState")
        }
        _ => true,
    }));

    for payload in [
        "QuinnOwnerTerminalCorrelation",
        "QuinnOwnerTerminalQuarantineCause",
    ] {
        let declarations = child
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Enum(item) if item.ident == payload => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(declarations.len(), 1, "child must own one {payload}");
        let declaration = declarations[0];
        assert!(has_direct_cfg_test(&declaration.attrs) && is_super(&declaration.vis));
        assert_eq!(enum_count(&parent, payload), 0);
    }

    let hard_terminal_uses = parent
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Use(item) => Some(item),
            _ => None,
        })
        .filter(|item| {
            matches!(
                &item.tree,
                syn::UseTree::Path(path) if path.ident == "hard_terminal"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(hard_terminal_uses.len(), 1, "parent must import one state");
    let state_import = hard_terminal_uses[0];
    assert!(matches!(state_import.vis, syn::Visibility::Inherited));
    assert!(matches!(
        &state_import.tree,
        syn::UseTree::Path(path)
            if matches!(
                path.tree.as_ref(),
                syn::UseTree::Name(name)
                    if name.ident == "QuinnOwnerControllerOutputTerminalState"
            )
    ));

    let transaction_name = "QuinnOwnerControllerOutputTerminalTransaction";
    assert!(parent.items.iter().all(|item| match item {
        Item::Struct(item) => item.ident != transaction_name,
        Item::Impl(item) => !type_is_named(&item.self_ty, transaction_name),
        _ => true,
    }));
    let transactions = child
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == transaction_name => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(transactions.len(), 1, "child must own one terminal guard");
    let transaction = transactions[0];
    assert!(has_direct_cfg_test(&transaction.attrs) && is_owner(&transaction.vis));
    let syn::Fields::Named(fields) = &transaction.fields else {
        panic!("terminal guard must retain named root ownership");
    };
    assert!(
        fields.named.len() == 1
            && matches!(fields.named[0].vis, syn::Visibility::Inherited)
            && type_is_mutable_reference_to(&fields.named[0].ty, "QuinnOwnerControllerOutputRoot",)
    );

    let transaction_impls = child
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item)
                if item.trait_.is_none() && type_is_named(&item.self_ty, transaction_name) =>
            {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(transaction_impls.len(), 1, "child must own one guard impl");
    let transaction_impl = transaction_impls[0];
    assert!(has_direct_cfg_test(&transaction_impl.attrs));
    let methods = transaction_impl
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut owner_methods = methods
        .iter()
        .filter(|method| is_owner(&method.vis))
        .map(|method| method.sig.ident.to_string())
        .collect::<Vec<_>>();
    owner_methods.sort();
    assert_eq!(
        owner_methods.join(","),
        "apply_reported,enter_actor_call,quarantine_actor_uncertain,restore_actor_retained",
    );
    let mut private_methods = methods
        .iter()
        .filter(|method| matches!(method.vis, syn::Visibility::Inherited))
        .map(|method| method.sig.ident.to_string())
        .collect::<Vec<_>>();
    private_methods.sort();
    assert_eq!(
        private_methods.join(","),
        "close_controller_applied,close_standby_retired,commit_hard_terminal_output_stop,quarantine,reject_precommit,stage_binding_stamp,stage_bound,stage_controller_bound,stage_standby_bound,take_for_close",
    );
    let drop_impls = child
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item)
                if type_is_named(&item.self_ty, transaction_name)
                    && item
                        .trait_
                        .as_ref()
                        .is_some_and(|(_, path, _)| path_ends_with(path, &["Drop"])) =>
            {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(drop_impls.len(), 1, "child must own one guard Drop impl");
    let drop_impl = drop_impls[0];
    assert!(has_direct_cfg_test(&drop_impl.attrs));
    assert!(matches!(
        drop_impl.items.as_slice(),
        [ImplItem::Fn(method)]
            if method.sig.ident == "drop"
                && matches!(method.vis, syn::Visibility::Inherited)
    ));
}
