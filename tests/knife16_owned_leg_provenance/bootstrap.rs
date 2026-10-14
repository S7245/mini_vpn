//! Bootstrap and inbound/admission cleanup provenance.
//!
//! Owns the existing source contract and P2A code-aware allowance guard.
//! Uses ancestor fixtures and the shared scanner; owns no production custody.
//! Entry: the integration root's private `bootstrap` route; its test lives here.

use super::*;

#[path = "bootstrap/implementation.rs"]
mod implementation;
#[path = "../support/rust_source_scan.rs"]
mod rust_source_scan;

#[derive(Default)]
struct BootstrapParameterAudit {
    bootstrap_functions: usize,
    bootstrap_parameters: Vec<usize>,
    allowed_functions: Vec<(String, usize)>,
}

fn attribute_allowances(attributes: &[syn::Attribute]) -> usize {
    attributes
        .iter()
        .filter_map(|attribute| match &attribute.meta {
            syn::Meta::List(list) => Some(format!(
                "{}({})",
                list.path
                    .get_ident()
                    .map_or_else(String::new, ToString::to_string),
                list.tokens,
            )),
            _ => None,
        })
        .map(|source| rust_source_scan::lint_suppression_counts(&source).too_many_arguments)
        .sum()
}

impl<'ast> Visit<'ast> for BootstrapParameterAudit {
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        let count = attribute_allowances(&item.attrs);
        if count != 0 {
            self.allowed_functions
                .push((item.sig.ident.to_string(), count));
        }
        if item.sig.ident == "run_server_bootstrap" {
            self.bootstrap_functions += 1;
            self.bootstrap_parameters.push(item.sig.inputs.len());
        }
        visit::visit_item_fn(self, item);
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        let count = attribute_allowances(&item.attrs);
        if count != 0 {
            self.allowed_functions
                .push((item.sig.ident.to_string(), count));
        }
        visit::visit_impl_item_fn(self, item);
    }
}

fn audit_bootstrap_parameters(source: &str) -> BootstrapParameterAudit {
    let syntax = syn::parse_file(source).expect("valid Rust source for allowance audit");
    let mut audit = BootstrapParameterAudit::default();
    audit.visit_file(&syntax);
    audit.allowed_functions.sort();
    audit
}

fn assert_no_child_allowances(path: &std::path::Path) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("actor child directory") {
            assert_no_child_allowances(&entry.expect("actor child entry").path());
        }
    } else if path.extension().is_some_and(|extension| extension == "rs") {
        let source = std::fs::read_to_string(path).expect("actor child source");
        let counts = rust_source_scan::lint_suppression_counts(&source);
        assert_eq!(
            (counts.too_many_arguments, counts.broad),
            (0, 0),
            "no bootstrap allowance relocation or broad suppression at {}",
            path.display(),
        );
    }
}

fn read_bootstrap_implementation() -> String {
    std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/owned_upstream/quinn_leg_actor/bootstrap.rs"),
    )
    .expect("closed bootstrap Implementation source")
}

#[test]
fn server_bootstrap_parameter_aggregate_has_no_relocated_allowance() {
    let bootstrap = read_bootstrap_implementation();
    let audit = audit_bootstrap_parameters(&bootstrap);
    assert!(
        !audit
            .allowed_functions
            .iter()
            .any(|(name, _)| name == "run_server_bootstrap"),
        "run_server_bootstrap must lose its too_many_arguments allowance before P2B",
    );
    assert_eq!(audit.bootstrap_functions, 1);
    assert_eq!(audit.bootstrap_parameters, [1]);
    let syntax = syn::parse_file(&bootstrap).expect("bootstrap syntax");
    let inputs = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "ServerBootstrapInput" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [input] = inputs.as_slice() else {
        panic!("one nominal bootstrap input")
    };
    assert!(matches!(input.vis, syn::Visibility::Inherited));
    assert!(
        input.attrs.is_empty(),
        "no Clone/Copy derive or conditional aggregate"
    );
    let names = [
        "connecting",
        "reservation",
        "leg",
        "limits",
        "peer_policy",
        "session_clock",
        "deadline",
        "fail_start_before_publication",
    ];
    assert_eq!(
        input
            .fields
            .iter()
            .map(|field| field.ident.as_ref().expect("named input field").to_string())
            .collect::<Vec<_>>(),
        names
    );
    assert!(
        input
            .fields
            .iter()
            .all(|field| matches!(field.vis, syn::Visibility::Inherited))
    );
    assert!(!syntax.items.iter().any(|item| matches!(item, Item::Impl(item)
        if matches!(&*item.self_ty, Type::Path(path) if path.path.is_ident("ServerBootstrapInput")))),
        "input is a move-only data aggregate, without trait/forwarding implementation");
    let function = syntax
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(item) if item.sig.ident == "run_server_bootstrap" => Some(item),
            _ => None,
        })
        .expect("direct bootstrap consumer");
    assert!(matches!(function.vis, syn::Visibility::Inherited));
    let syn::FnArg::Typed(argument) = &function.sig.inputs[0] else {
        panic!("owned aggregate argument")
    };
    assert!(
        matches!(&*argument.ty, Type::Path(path) if path.path.is_ident("ServerBootstrapInput"))
    );
    let syn::Pat::Struct(pattern) = &*argument.pat else {
        panic!("destructure on entry, preserving the entered-callee local drop order");
    };
    assert!(pattern.path.is_ident("ServerBootstrapInput") && pattern.rest.is_none());
    assert_eq!(
        pattern
            .fields
            .iter()
            .map(|field| match &field.member {
                Member::Named(name) => name.to_string(),
                _ => panic!("named pattern field"),
            })
            .collect::<Vec<_>>(),
        names
    );
    assert_eq!(
        audit_bootstrap_parameters(QUINN_ACTOR_SOURCE).allowed_functions,
        [
            "actor_root",
            "data_reader_task",
            "data_writer_task",
            "delivery_writer_task",
            "new",
            "start_actor"
        ]
        .map(|name| (name.to_owned(), 1)),
        "only the six unmoved legacy function declarations retain allowances",
    );
    let counts = rust_source_scan::lint_suppression_counts(QUINN_ACTOR_SOURCE);
    assert_eq!((counts.too_many_arguments, counts.broad), (6, 0));
    assert_no_child_allowances(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/owned_upstream/quinn_leg_actor"),
    );
}

#[test]
fn bootstrap_allowance_audit_ignores_trivia_but_counts_real_suppressions() {
    let fake = r###"
        // #[allow(clippy::too_many_arguments)] async fn run_server_bootstrap(a: A,b: B){}
        const FAKE: &str = r#"#[allow(clippy::too_many_arguments)] fn run_server_bootstrap() {}"#;
        #[doc = "#[allow(clippy::too_many_arguments)]"]
        async fn run_server_bootstrap(input: Input) {}
    "###;
    let audit = audit_bootstrap_parameters(fake);
    assert_eq!(audit.bootstrap_functions, 1);
    assert_eq!(audit.bootstrap_parameters, [1]);
    assert!(audit.allowed_functions.is_empty());
    assert_eq!(
        rust_source_scan::lint_suppression_counts(fake).too_many_arguments,
        0
    );
    for attribute in [
        "#[allow(clippy::too_many_arguments)]",
        "#[expect(clippy :: too_many_arguments)]",
        "#[cfg_attr(test, allow(clippy::too_many_arguments))]",
    ] {
        let source = format!(
            "mod child {{ {attribute} #[inline] async fn run_server_bootstrap(input: Input) {{}} }}"
        );
        assert_eq!(
            audit_bootstrap_parameters(&source).allowed_functions,
            [("run_server_bootstrap".to_owned(), 1)]
        );
    }
    let inherited = "#[allow(clippy::too_many_arguments)] mod child { async fn run_server_bootstrap(input: Input) {} }";
    assert!(
        audit_bootstrap_parameters(inherited)
            .allowed_functions
            .is_empty()
    );
    assert_eq!(
        rust_source_scan::lint_suppression_counts(inherited).too_many_arguments,
        1
    );
    for attribute in ["#![allow(warnings)]", "#![allow(clippy::all)]"] {
        assert_eq!(
            rust_source_scan::lint_suppression_counts(attribute).broad,
            1
        );
    }
}

#[test]
fn quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use() {
    let production = quinn_actor_production_source();
    let bootstrap = read_bootstrap_implementation();
    let inbound_module = production
        .split_once("mod inbound_ownership {")
        .and_then(|(_, source)| {
            source
                .split_once("\n}\n\npub(crate) use inbound_ownership::QuinnLegActorInbound;")
                .map(|(module, _)| module)
        })
        .expect("typed inbound ownership stays isolated in one private child module");
    let exact_private_inbound = r#"pub(crate) struct QuinnLegActorInbound {
        inbound: BoundInbound,
        arrived_at: SimTime,
        _byte_ownership: TrackedPermit,
        _composition_ownership: TrackedPermit,
    }"#;
    let exact_test_only_consumer = r#"#[cfg(test)]
        pub(super) fn consume_for_test(self, consume: impl FnOnce(BoundInbound)) {
            let Self {
                inbound,
                arrived_at: _,
                _byte_ownership,
                _composition_ownership,
            } = self;
            consume(inbound);
            drop((_byte_ownership, _composition_ownership));
        }"#;
    let exact_composition_consumer = r#"pub(super) fn consume_for_composition<T>(
            self,
            _gate: &QuinnLegCompositionGate,
            consume: impl FnOnce(BoundInbound, SimTime) -> T,
        ) -> T"#;
    assert!(inbound_module.contains(exact_private_inbound));
    assert!(inbound_module.contains(exact_test_only_consumer));
    assert!(inbound_module.contains(exact_composition_consumer));
    assert!(inbound_module.contains("pub(super) fn mint("));
    assert!(production.contains("pub(crate) use inbound_ownership::QuinnLegActorInbound;"));
    assert_eq!(
        source_occurrences(inbound_module, "QuinnLegActorInbound {"),
        3,
        "definition, sole inherent impl, and Debug impl stay inside the private child module",
    );
    assert_eq!(
        source_occurrences(production, "QuinnLegActorInbound::mint("),
        1,
        "the started actor is the sole parent-side mint caller",
    );
    assert_eq!(
        source_occurrences(inbound_module, "impl QuinnLegActorInbound {"),
        1,
        "no second inherent impl may add a production extractor",
    );
    assert_eq!(
        source_occurrences(inbound_module, "for QuinnLegActorInbound {"),
        1,
        "Debug is the sole trait impl; Deref/AsRef/Into cannot expose the payload",
    );
    assert!(inbound_module.contains("impl std::fmt::Debug for QuinnLegActorInbound {"));
    assert_eq!(
        source_occurrences(inbound_module, "pub(super)"),
        3,
        "mint, the composition-gated consumer, and the cfg(test) consumer are the complete parent-visible inbound API",
    );
    assert_eq!(
        source_occurrences(inbound_module, "pub(crate)"),
        1,
        "only the opaque envelope type is crate-visible",
    );
    assert!(!inbound_module.contains("pub(in "));
    assert_eq!(
        source_occurrences(inbound_module, ".inbound"),
        1,
        "only the redacted Debug implementation may borrow the private payload",
    );
    assert!(!inbound_module.contains("-> BoundInbound"));
    assert!(!inbound_module.contains("pub(crate) inbound: BoundInbound"));
    assert!(!inbound_module.contains("pub(super) inbound: BoundInbound"));
    assert!(!inbound_module.contains("pub inbound: BoundInbound"));
    for forbidden in [
        "into_bound_inbound",
        "into_inbound",
        "as_bound_inbound",
        "QuinnLegActorInbound) -> BoundInbound",
    ] {
        assert!(
            !production.contains(forbidden),
            "production naked inbound seam reappeared as {forbidden}",
        );
    }
    assert_eq!(
        source_occurrences(production, "read_record_or_cancel("),
        4,
        "one counted reader and the DATA-header, DATA-body, and DELIVERED-header calls stay exact",
    );
    assert!(production.contains("result = stream.read(&mut bytes[filled..]) => result"));
    assert!(!production.contains("stream.read_exact(bytes)"));
    assert!(production.contains("classify_record_read_end(context, filled, Some(error))"));

    assert!(production.contains("pub(crate) enum QuinnLegActorAdmission"));
    assert!(production.contains("pub(crate) async fn admit("));
    assert!(bootstrap.contains("pub(crate) async fn start(self)"));
    assert!(bootstrap.contains(
        "pub(crate) async fn start_with_clock(\n        mut self,\n        session_clock: SessionMonotonicClock,"
    ));
    assert!(bootstrap.contains("self.start_with_clock(SessionMonotonicClock::start()).await"));
    assert_eq!(
        source_occurrences(production, "fn accept("),
        1,
        "the combined accept/start helper must remain test-only",
    );
    assert!(production.contains("#[cfg(test)]\n    async fn accept("));
    assert!(!production.contains("pub(crate) async fn accept("));
    assert!(!production.contains("pub(super) async fn accept("));
    assert!(!production.contains("pub async fn accept("));
    assert!(production.contains("incoming.remote_address_validated()"));
    assert!(production.contains("incoming\n                .retry()"));
    assert_eq!(
        (
            source_occurrences(production, "tokio::time::timeout_at(deadline"),
            source_occurrences(&bootstrap, "tokio::time::timeout_at(deadline"),
        ),
        (1, 1),
        "both client and admitted server bootstrap must consume absolute deadlines",
    );
    assert_eq!(
        (
            source_occurrences(production, "ensure_bootstrap_deadline("),
            source_occurrences(&bootstrap, "ensure_bootstrap_deadline("),
        ),
        (3, 1),
        "the helper definition, both entry checks, and the publication check remain exact",
    );
    assert!(production.contains("ensure_bootstrap_deadline(publication_deadline)?;"));
    assert!(production.contains("client_crypto: Arc<dyn quinn::crypto::ClientConfig>"));
    assert!(production.contains("client_crypto: build_client_crypto(&ca_path)?"));
    assert!(production.contains("#[cfg(test)]\nfn build_server_quinn_config("));
    assert_eq!(
        (
            source_occurrences(production, ".take_drain_handle()"),
            source_occurrences(&bootstrap, ".take_drain_handle()"),
        ),
        (1, 2),
    );
    assert!(production.contains("connection_drain: quinn::ConnectionDrain"));
    assert!(production.contains("endpoint_driver_drain: quinn::EndpointDriverDrain"));
    assert_eq!(
        source_occurrences(production, "connection_drain.drained().await"),
        1,
        "a published actor root must consume the exact endpoint-and-driver witness once",
    );
    assert_eq!(
        (
            source_occurrences(production, "drain.abandon();"),
            source_occurrences(&bootstrap, "drain.abandon();"),
        ),
        (2, 2),
        "all unpublished connection cleanup paths retain explicit abandon authority",
    );
    assert_eq!(
        (
            source_occurrences(production, "if !drain.drained().await"),
            source_occurrences(&bootstrap, "if !drain.drained().await"),
        ),
        (0, 2),
        "both shared-listener server bootstrap cleanup paths consume their connection witnesses",
    );
    assert_eq!(
        source_occurrences(production, "Some(drain) => drain.drained().await"),
        1,
        "one shared client cleanup tail consumes an optional prepublication connection witness",
    );
    assert_eq!(
        source_occurrences(production, "endpoint_driver_drain.drained().await"),
        3,
        "client cleanup, published client actor, and listener shutdown each consume endpoint-driver completion",
    );
    assert_eq!(
        (
            source_occurrences(production, "QuinnLegActorError::EndpointReleaseLost"),
            source_occurrences(&bootstrap, "QuinnLegActorError::EndpointReleaseLost"),
        ),
        (3, 4),
        "missing or false release witnesses fail closed and dropped-bootstrap cleanup stays armed",
    );
    let actor_root = production
        .split_once("async fn actor_root(")
        .and_then(|(_, source)| source.split_once("async fn wait_cancel("))
        .map(|(source, _)| source)
        .expect("published actor root has one bounded source region");
    assert!(actor_root.contains("connection.close(CLOSE_CODE, b\"actor cancelled\");"));
    assert!(actor_root.contains("let connection_released = connection_drain.drained().await;"));
    assert!(actor_root.contains("let mut root_epilogue_error = None;"));
    let exact_drain = actor_root
        .find("connection_drain.drained().await")
        .expect("published actor consumes its exact dual witness");
    let delayed_root_error = actor_root
        .find("if let Some(error) = root_epilogue_error")
        .expect("published actor delays epilogue errors until after cleanup");
    let aggregate_endpoint_wait = actor_root
        .find("endpoint.wait_idle_or_driver_lost().await")
        .expect("a client actor always attempts its aggregate endpoint-driver cleanup");
    let final_endpoint_drop = actor_root
        .find("drop(endpoint);")
        .expect("a client actor releases its final endpoint handle before driver join");
    let endpoint_driver_drain = actor_root
        .find("endpoint_driver_drain.drained().await")
        .expect("a client actor consumes exact endpoint-driver completion");
    let release_error = actor_root
        .find("if !connection_released")
        .expect("lost exact connection release fails closed after cleanup attempts");
    assert!(
        exact_drain < delayed_root_error,
        "published actor errors cannot bypass the exact close-tail witness",
    );
    assert!(
        exact_drain < aggregate_endpoint_wait && aggregate_endpoint_wait < release_error,
        "a lost per-connection witness cannot skip the remaining client endpoint cleanup",
    );
    assert!(
        aggregate_endpoint_wait < final_endpoint_drop
            && final_endpoint_drop < endpoint_driver_drain
            && endpoint_driver_drain < release_error,
        "Joined requires releasing the final endpoint handle and consuming normal driver completion",
    );
    assert!(
        !actor_root.contains(".abandon()"),
        "a published connection must complete normal driver cleanup, never unpublished abandon",
    );
    assert!(production.contains("enum ListenerPhase {"));
    assert!(production.contains("ListenerPhase::Closing => {}"));
    assert!(production.contains("self.phase = ListenerPhase::Joined;"));
    assert!(production.contains("struct ClientBootstrapCleanup"));
    let client_failure_cleanup = production
        .split_once("async fn finish_failure(&mut self)")
        .and_then(|(_, source)| source.split_once("impl Drop for ClientBootstrapCleanup"))
        .map(|(source, _)| source)
        .expect("client failure cleanup has one bounded source region");
    assert!(client_failure_cleanup.contains("self.runtime.spawn(async move"));
    assert!(client_failure_cleanup.contains("cleanup.await"));
    assert!(
        client_failure_cleanup
            .contains("release_client_endpoint_tail(endpoint, drain, endpoint_driver_drain).await")
    );
    let client_release_tail = production
        .split_once("async fn release_client_endpoint_tail(")
        .and_then(|(_, source)| source.split_once("impl QuinnLegActorFactory {"))
        .map(|(source, _)| source)
        .expect("client endpoint release has one bounded source region");
    let client_endpoint_idle = client_release_tail
        .find("endpoint.wait_idle_or_driver_lost().await")
        .expect("client cleanup checks aggregate endpoint state");
    let client_endpoint_drop = client_release_tail
        .find("drop(endpoint);")
        .expect("client cleanup releases its final endpoint handle");
    let client_endpoint_driver = client_release_tail
        .find("endpoint_driver_drain.drained().await")
        .expect("client cleanup consumes exact driver completion");
    let client_release_error = client_release_tail
        .find("if !connection_released")
        .expect("client cleanup delays errors until all release attempts complete");
    assert!(
        client_endpoint_idle < client_endpoint_drop
            && client_endpoint_drop < client_endpoint_driver
            && client_endpoint_driver < client_release_error,
        "client failure cleanup cannot return before endpoint-driver/socket completion",
    );
    let listener_shutdown = production
        .split_once("pub(crate) async fn shutdown(")
        .and_then(|(_, source)| source.split_once("impl Drop for QuinnLegActorListener"))
        .map(|(source, _)| source)
        .expect("listener shutdown has one bounded source region");
    assert!(listener_shutdown.contains("endpoint_release_task"));
    assert!(listener_shutdown.contains("drop(endpoint);"));
    assert!(listener_shutdown.contains("endpoint_driver_drain.drained().await"));
    let listener_driver_verdict = listener_shutdown
        .find(".result()?;")
        .expect("listener consumes the cached driver cleanup verdict");
    let listener_unclean = listener_shutdown
        .find("self.registry.unclean.load(Ordering::Acquire)")
        .expect("listener checks sticky actor cleanup poison");
    let listener_joined = listener_shutdown
        .find("self.phase = ListenerPhase::Joined;")
        .expect("listener has one final Joined publication seam");
    assert!(
        listener_driver_verdict < listener_unclean && listener_unclean < listener_joined,
        "driver/socket cleanup and sticky unclean status both precede listener Joined",
    );
    assert!(bootstrap.contains("struct ServerBootstrapHandoff"));
    assert!(bootstrap.contains("slot: Option<Arc<ActorSlotLease>>"));
    let bootstrap_guard_drop = bootstrap
        .split("impl Drop for BootstrapTaskTerminationGuard")
        .nth(1)
        .expect("bootstrap termination guard has one bounded Drop implementation");
    let poison = bootstrap_guard_drop
        .find("self.registry.mark_unclean();")
        .expect("abnormal bootstrap cleanup publishes sticky poison");
    let slot_release = bootstrap_guard_drop
        .find("drop(self.slot.take());")
        .expect("bootstrap cleanup releases its retained slot");
    let task_release = bootstrap_guard_drop
        .find(".terminating_tasks\n            .fetch_sub(1, Ordering::AcqRel)")
        .expect("bootstrap cleanup retires its termination counter");
    assert!(
        poison < slot_release,
        "sticky listener poison must linearize before the last bootstrap slot can release",
    );
    assert!(
        task_release < slot_release,
        "a live slot cannot be reused while its old bootstrap termination guard is still counted",
    );
    assert!(production.contains("runtime: tokio::runtime::Handle"));
    assert_eq!(
        source_occurrences(production, ".wait_idle_or_driver_lost().await"),
        2,
        "client actor and shared listener shutdown must fail closed on driver loss",
    );

    assert!(QUINN_CONNECTION_SOURCE.contains("pub struct ConnectionDrain {"));
    assert!(QUINN_CONNECTION_SOURCE.contains("endpoint_released: oneshot::Receiver<()>,"));
    assert!(QUINN_CONNECTION_SOURCE.contains("driver_released: oneshot::Receiver<()>,"));
    assert!(
        QUINN_CONNECTION_SOURCE
            .contains("pub fn take_drain_handle(&mut self) -> Option<ConnectionDrain>")
    );
    assert_eq!(
        source_occurrences(QUINN_CONNECTION_SOURCE, "pub async fn drained(self)"),
        1,
        "the exact drain witness must be consuming and single-waiter",
    );
    assert!(!QUINN_CONNECTION_SOURCE.contains("pub async fn drained(&self)"));
    assert!(QUINN_CONNECTION_SOURCE.contains("conn.forward_endpoint_events();"));
    assert!(QUINN_CONNECTION_SOURCE.contains("let driver = ConnectionDriver(conn.clone());"));
    assert!(QUINN_CONNECTION_SOURCE.contains("drive_connection_and_report_release(driver"));
    assert!(QUINN_CONNECTION_SOURCE.contains("let result = (&mut driver).await;"));
    assert!(!QUINN_CONNECTION_SOURCE.contains("match driver.await {"));
    assert!(QUINN_CONNECTION_SOURCE.contains("let _ = driver_released_send.send(());"));
    let driver_drop = QUINN_CONNECTION_SOURCE
        .find("drop(driver);")
        .expect("normal release drops the exact connection driver first");
    let driver_release_ack = QUINN_CONNECTION_SOURCE
        .find("let _ = driver_released_send.send(());")
        .expect("normal driver completion mints one release acknowledgement");
    assert!(
        driver_drop < driver_release_ack,
        "the normal completion acknowledgement cannot overtake driver-held ownership",
    );
    assert!(!QUINN_CONNECTION_SOURCE.contains("impl Drop for ConnectionDriver"));
    assert!(
        QUINN_CONNECTION_SOURCE
            .contains("wait_for_connection_release(endpoint_released, driver_released).await")
    );
    assert!(QUINN_CONNECTION_SOURCE.contains("endpoint_released && driver_released"));
    assert!(
        QUINN_LIB_SOURCE.contains("endpoint_released: Option<tokio::sync::oneshot::Sender<()>>")
    );
    assert!(QUINN_ENDPOINT_SOURCE.contains("let event = self.inner.handle_event(ch, event);"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("let _ = endpoint_released.send(());"));
    let endpoint_handle = QUINN_ENDPOINT_SOURCE
        .find("let event = self.inner.handle_event(ch, event);")
        .expect("endpoint applies the exact protocol event");
    let endpoint_ack = QUINN_ENDPOINT_SOURCE
        .find("let _ = endpoint_released.send(());")
        .expect("endpoint mints the exact removal acknowledgement");
    assert!(endpoint_handle < endpoint_ack);
    assert!(QUINN_ENDPOINT_SOURCE.contains("endpoint.events.close();"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("while endpoint.events.try_recv().is_ok() {}"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("pub async fn wait_idle_or_driver_lost(&self) -> bool"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("if driver_lost {\n        Some(false)"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("pub struct EndpointDriverDrain {"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("driver_released: oneshot::Receiver<()>"));
    assert!(
        QUINN_ENDPOINT_SOURCE
            .contains("pub fn take_driver_drain_handle(&mut self) -> Option<EndpointDriverDrain>")
    );
    assert_eq!(
        source_occurrences(QUINN_ENDPOINT_SOURCE, "pub async fn drained(self) -> bool"),
        1,
        "the exact endpoint-driver witness is consuming and single-waiter",
    );
    assert!(!QUINN_ENDPOINT_SOURCE.contains("pub async fn drained(&self) -> bool"));
    assert!(QUINN_ENDPOINT_SOURCE.contains("let _ = driver_released_send.send(());"));
    assert!(QUINN_LIB_SOURCE.contains("EndpointDriverDrain"));
    assert!(QUINN_PROTO_CONNECTION_SOURCE.contains("pub fn abandon(&mut self)"));
    assert!(QUINN_PROTO_CONNECTION_SOURCE.contains("self.kill(ConnectionError::LocallyClosed);"));
}
