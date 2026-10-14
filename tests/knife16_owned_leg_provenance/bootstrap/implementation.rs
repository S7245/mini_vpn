//! Closed-route and declaration ownership audit for server bootstrap.
//!
//! Owns the source-only P2B extraction tracer, not runtime or custody behavior.
//! Requires one private production route and exact parent/child ownership.
//! Entry: the ancestor's private `implementation` Module; the test lives here.

use super::*;

fn assert_bootstrap_route(source: &str) {
    let routes = rust_source_scan::external_rust_modules(source)
        .into_iter()
        .filter(|route| route.name == "bootstrap")
        .collect::<Vec<_>>();
    assert_eq!(
        routes.len(),
        1,
        "actor must declare exactly one external bootstrap Module"
    );
    let route = &routes[0];
    assert!(
        route.module_context.is_empty()
            && route.is_private
            && route.is_unconditional
            && route.uses_default_path
    );
    assert!(
        !rust_source_scan::inline_rust_module_names(source)
            .iter()
            .any(|name| name == "bootstrap")
    );
    assert_eq!(rust_source_scan::source_include_macro_count(source), 0);
}

#[derive(Default)]
struct BootstrapDeclarations(Vec<String>);

impl<'ast> Visit<'ast> for BootstrapDeclarations {
    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        if [
            "ServerBootstrapInput",
            "ServerBootstrapHandoff",
            "BootstrapTaskTerminationGuard",
        ]
        .iter()
        .any(|name| item.ident == name)
        {
            self.0.push(format!("struct:{}", item.ident));
        }
        visit::visit_item_struct(self, item);
    }
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if item.sig.ident == "run_server_bootstrap" {
            self.0.push("fn:run_server_bootstrap".into());
        }
        visit::visit_item_fn(self, item);
    }
    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        if let Type::Path(path) = &*item.self_ty {
            if [
                "ServerBootstrapHandoff",
                "BootstrapTaskTerminationGuard",
                "QuinnLegActorBootstrap",
            ]
            .iter()
            .any(|name| path.path.is_ident(name))
            {
                let kind = if item.trait_.is_some() {
                    "trait"
                } else {
                    "inherent"
                };
                self.0
                    .push(format!("{kind}:{}", path.path.segments[0].ident));
            }
        }
        visit::visit_item_impl(self, item);
    }
}

#[test]
fn server_bootstrap_implementation_has_one_closed_private_route() {
    assert_bootstrap_route(QUINN_ACTOR_SOURCE);
    let parent = syn::parse_file(QUINN_ACTOR_SOURCE).expect("actor syntax");
    let mut retained = BootstrapDeclarations::default();
    retained.visit_file(&parent);
    assert!(
        retained.0.is_empty(),
        "moved declarations remain in parent: {:?}",
        retained.0
    );
    for anchor in ["QuinnLegActorBootstrap", "QuinnLegActorListener"] {
        assert_eq!(
            parent
                .items
                .iter()
                .filter(|item| matches!(item, Item::Struct(item) if item.ident == anchor))
                .count(),
            1
        );
    }
    let source = read_bootstrap_implementation();
    let child = syn::parse_file(&source).expect("bootstrap syntax");
    assert!(
        child
            .attrs
            .iter()
            .all(|attribute| attribute.path().is_ident("doc"))
    );
    let mut declarations = BootstrapDeclarations::default();
    declarations.visit_file(&child);
    declarations.0.sort();
    assert_eq!(
        declarations.0,
        [
            "fn:run_server_bootstrap",
            "inherent:BootstrapTaskTerminationGuard",
            "inherent:QuinnLegActorBootstrap",
            "inherent:ServerBootstrapHandoff",
            "struct:BootstrapTaskTerminationGuard",
            "struct:ServerBootstrapHandoff",
            "struct:ServerBootstrapInput",
            "trait:BootstrapTaskTerminationGuard",
            "trait:QuinnLegActorBootstrap",
            "trait:ServerBootstrapHandoff",
        ]
    );
    for item in &child.items {
        match item {
            Item::Use(item) => assert!(matches!(item.vis, syn::Visibility::Inherited)),
            Item::Struct(item) => {
                let expected = if item.ident == "ServerBootstrapInput" {
                    "self"
                } else {
                    "super"
                };
                assert_eq!(bootstrap_visibility(&item.vis), expected);
                assert!(item.attrs.is_empty());
                assert!(
                    item.fields
                        .iter()
                        .all(|field| matches!(field.vis, syn::Visibility::Inherited))
                );
            }
            Item::Fn(item) => assert!(
                item.sig.ident == "run_server_bootstrap"
                    && item.attrs.is_empty()
                    && matches!(item.vis, syn::Visibility::Inherited)
            ),
            Item::Impl(item) => {
                assert!(item.attrs.is_empty());
                let Type::Path(owner) = &*item.self_ty else {
                    panic!("nominal bootstrap owner")
                };
                let owner = owner.path.get_ident().expect("direct owner").to_string();
                let expected: &[(&str, &str)] = if let Some((_, path, _)) = &item.trait_ {
                    assert!(path.is_ident("Drop"));
                    &[("drop", "self")]
                } else {
                    match owner.as_str() {
                        "ServerBootstrapHandoff" => &[("new", "super"), ("claim", "self")],
                        "BootstrapTaskTerminationGuard" => {
                            &[("new", "super"), ("mark_clean", "self")]
                        }
                        "QuinnLegActorBootstrap" => {
                            &[("start", "crate"), ("start_with_clock", "crate")]
                        }
                        _ => panic!("unexpected bootstrap owner"),
                    }
                };
                let actual = item
                    .items
                    .iter()
                    .map(|item| {
                        let syn::ImplItem::Fn(method) = item else {
                            panic!("method-only bootstrap impl")
                        };
                        assert!(method.attrs.is_empty());
                        (
                            method.sig.ident.to_string(),
                            bootstrap_visibility(&method.vis),
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual,
                    expected
                        .iter()
                        .map(|(name, scope)| (name.to_string(), *scope))
                        .collect::<Vec<_>>()
                );
            }
            _ => panic!("no extra Module/macro/alias/Interface in bootstrap child"),
        }
    }
    assert_eq!(rust_source_scan::source_include_macro_count(&source), 0);
}

fn bootstrap_visibility(visibility: &syn::Visibility) -> &str {
    match visibility {
        syn::Visibility::Inherited => "self",
        syn::Visibility::Restricted(scope) if scope.path.is_ident("super") => "super",
        syn::Visibility::Restricted(scope) if scope.path.is_ident("crate") => "crate",
        _ => "widened",
    }
}
