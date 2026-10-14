mod shared {
    pub use mini_vpn::shared::*;
}

#[path = "knife16_owned_leg_provenance/bootstrap.rs"]
mod bootstrap;

#[path = "knife16_owned_leg_provenance/continuation.rs"]
mod continuation;
#[path = "knife16_owned_leg_provenance/controller_inventory.rs"]
mod controller_inventory;

const QUINN_OWNER_INVENTORY_SOURCE: &str =
    include_str!("../src/owned_upstream/quinn_leg_composition/owner/inventory.rs");

#[path = "../src/resumable/mod.rs"]
mod resumable;

// `leg.rs` is compiled standalone here to prove its raw capabilities remain
// non-cloneable. Production obtains these gates from the sibling standby
// typestate; the uninhabited shim keeps this provenance crate unable to mint
// any of them while preserving the exact production signatures.
mod standby {
    use crate::leg::{LegSeal, LegTransportEndpoint};
    use crate::resumable::{AttachRequest, AttachTransportBinding};
    use std::sync::Weak;

    pub(super) enum RegisteredPendingAttachAuthority {}

    impl RegisteredPendingAttachAuthority {
        pub(super) fn into_parts(
            self,
        ) -> (
            LegSeal,
            Weak<LegTransportEndpoint>,
            AttachTransportBinding,
            AttachRequest,
        ) {
            match self {}
        }
    }

    pub(super) enum RegisteredAttachResponseGate {}
    pub(super) enum AcceptedRegisteredLossGate {}
    pub(super) enum RegisteredCatchUpBeginGate {}
    pub(super) enum RegisteredCatchUpAssemblyGate {}
    pub(super) enum RegisteredCatchUpResponseGate {}
}

// `leg.rs` may name this sibling gate but the standalone provenance crate
// must never be able to construct the controller's installation authority.
mod client_controller {
    pub(super) enum ClientControllerInstallGate {}
}

// Exact type-only mirror of the two client installation calls used by
// `leg.rs`. Every value involved is uninhabited here, so the provenance crate
// can type-check the production source without minting a supervisor, commit,
// or controller gate.
mod supervisor {
    use crate::resumable::{CommittedLeg, GenerationCatchUp, SessionEffect};

    pub(super) enum SessionSupervisor {}
    pub(super) enum SessionSupervisorError {}
    pub(super) enum ClientReplacementCommit {}

    impl SessionSupervisor {
        pub(super) fn install_prepared_client_replacement<T, E>(
            &mut self,
            _candidate: CommittedLeg,
            _prepare: impl FnOnce(Vec<SessionEffect>) -> Result<T, E>,
        ) -> Result<Result<(T, ClientReplacementCommit), E>, SessionSupervisorError> {
            match *self {}
        }

        pub(super) fn install_prepared_client_catch_up<T, E>(
            &mut self,
            _catch_up: GenerationCatchUp,
            _prepare: impl FnOnce(Vec<SessionEffect>) -> Result<T, E>,
        ) -> Result<Result<(T, ClientReplacementCommit), E>, SessionSupervisorError> {
            match *self {}
        }
    }
}

// `leg.rs` may name the production LegIo factory gate, but this standalone
// provenance crate receives only an uninhabited mirror and cannot construct
// authenticated production transport identity with it.
mod leg_io {
    pub(super) enum LegIoAuthenticatedTransportGate {}
}

#[path = "../src/owned_upstream/leg.rs"]
mod leg;

use bytes::Bytes;
use leg::{
    AttachResponse, AttachedLeg, CaughtUpAttachedLeg, EstablishedLeg, ExactLegTerminal,
    LegBoundFrame, LegGenerationStatus, LegProvenanceError, OwnerAttachTransaction, PendingAttach,
    PendingCatchUpAttach, PendingClientLegLoss, TerminalBoundRetiredClientLeg,
};
use resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
    FeatureSet, Frame, LegGeneration, OwnerIdentity, ReceiveBudgetLimits, Record,
    ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionEvent,
    SessionFlowId, SessionId, SessionModel, SessionRole, TcpWindowLimits, TlsExporterBinding,
    VersionRange,
};
use syn::visit::{self, Visit};
use syn::{Expr, ImplItem, Item, Member, Type};

// This inference becomes ambiguous and fails compilation if either type ever
// implements Clone.  It keeps the one-response/one-capability contract in the
// type system without adding a test-only dependency.
trait AmbiguousIfClone<Marker> {
    fn marker() {}
}

impl<T: ?Sized> AmbiguousIfClone<()> for T {}
impl<T: Clone> AmbiguousIfClone<u8> for T {}

const _: fn() = || {
    let _ = <PendingAttach as AmbiguousIfClone<_>>::marker;
    let _ = <LegBoundFrame as AmbiguousIfClone<_>>::marker;
    let _ = <AttachedLeg as AmbiguousIfClone<_>>::marker;
    let _ = <OwnerAttachTransaction as AmbiguousIfClone<_>>::marker;
    let _ = <LegGenerationStatus as AmbiguousIfClone<_>>::marker;
    let _ = <PendingCatchUpAttach as AmbiguousIfClone<_>>::marker;
    let _ = <CaughtUpAttachedLeg as AmbiguousIfClone<_>>::marker;
    let _ = <ExactLegTerminal as AmbiguousIfClone<_>>::marker;
    let _ = <TerminalBoundRetiredClientLeg as AmbiguousIfClone<_>>::marker;
    let _ = <PendingClientLegLoss as AmbiguousIfClone<_>>::marker;
    let _ = <quinn::ConnectionDrain as AmbiguousIfClone<_>>::marker;
    let _ = <quinn::EndpointDriverDrain as AmbiguousIfClone<_>>::marker;
};

const QUINN_ACTOR_SOURCE: &str = include_str!("../src/owned_upstream/quinn_leg_actor.rs");
const QUINN_ACTOR_TESTS_SOURCE: &str = concat!(
    include_str!("../src/owned_upstream/quinn_leg_actor/tests/mod.rs"),
    include_str!("../src/owned_upstream/quinn_leg_actor/tests/bootstrap_custody.rs"),
);
const QUINN_CLIENT_ROOT_SOURCE: &str = include_str!("../src/owned_upstream/quinn_client_root.rs");
const QUINN_CONNECTION_SOURCE: &str =
    include_str!("../third_party/quinn-0.11.11/src/connection.rs");
const QUINN_ENDPOINT_SOURCE: &str = include_str!("../third_party/quinn-0.11.11/src/endpoint.rs");
const QUINN_LIB_SOURCE: &str = include_str!("../third_party/quinn-0.11.11/src/lib.rs");
const QUINN_PROTO_CONNECTION_SOURCE: &str =
    include_str!("../third_party/quinn-proto-0.11.16/src/connection/mod.rs");
const LEG_IO_SOURCE: &str = include_str!("../src/owned_upstream/leg_io.rs");
const LEG_SOURCE: &str = include_str!("../src/owned_upstream/leg.rs");
const CLIENT_CONTROLLER_SOURCE: &str = include_str!("../src/owned_upstream/client_controller.rs");
const QUINN_COMPOSITION_SOURCE: &str =
    include_str!("../src/owned_upstream/quinn_leg_composition.rs");
const QUINN_OWNER_COMPOSITION_SOURCE: &str =
    include_str!("../src/owned_upstream/quinn_leg_composition/owner.rs");
const QUINN_OWNER_PHYSICAL_ROOT_SOURCE: &str =
    include_str!("../src/owned_upstream/quinn_leg_composition/owner/physical_root.rs");
const TWO_LEG_SOURCE: &str = include_str!("../src/owned_upstream/two_leg.rs");
const RESUMABLE_AUTH_SOURCE: &str = include_str!("../src/resumable/auth.rs");
const RESUMABLE_SESSION_SOURCE: &str = include_str!("../src/resumable/session.rs");
const OWNER_TARGET_SOURCE: &str = include_str!("../src/owned_upstream/owner_target.rs");
const OWNER_SESSION_CONTROLLER_SOURCE: &str =
    include_str!("../src/owned_upstream/owner_session_controller.rs");

fn source_occurrences(source: &str, needle: &str) -> usize {
    source.match_indices(needle).count()
}

const ABORTED_CONTROLLER_TURN: &str = "AbortedOwnerControllerTurn";

fn path_ends_with(path: &syn::Path, expected: &[&str]) -> bool {
    path.segments.len() >= expected.len()
        && path
            .segments
            .iter()
            .rev()
            .zip(expected.iter().rev())
            .all(|(actual, expected)| actual.ident == *expected)
}

fn peeled_expr(mut expression: &Expr) -> &Expr {
    loop {
        match expression {
            Expr::Group(group) => expression = &group.expr,
            Expr::Paren(paren) => expression = &paren.expr,
            _ => return expression,
        }
    }
}

fn is_aborted_controller_constructor(expression: &Expr) -> bool {
    let Expr::Call(call) = peeled_expr(expression) else {
        return false;
    };
    is_aborted_controller_constructor_call(call)
}

fn is_aborted_controller_constructor_call(call: &syn::ExprCall) -> bool {
    let Expr::Path(function) = peeled_expr(&call.func) else {
        return false;
    };
    path_ends_with(&function.path, &[ABORTED_CONTROLLER_TURN, "new"])
}

fn is_aborted_turn_slot(expression: &Expr) -> bool {
    let Expr::Field(field) = peeled_expr(expression) else {
        return false;
    };
    let Member::Named(member) = &field.member else {
        return false;
    };
    if member != "aborted_turn" {
        return false;
    }
    matches!(peeled_expr(&field.base), Expr::Path(base) if base.path.is_ident("self"))
}

fn is_some_aborted_controller_constructor(expression: &Expr) -> bool {
    let Expr::Call(call) = peeled_expr(expression) else {
        return false;
    };
    let Expr::Path(function) = peeled_expr(&call.func) else {
        return false;
    };
    function.path.is_ident("Some")
        && call.args.len() == 1
        && call
            .args
            .first()
            .is_some_and(is_aborted_controller_constructor)
}

#[derive(Default)]
struct AbortedControllerConstructionAudit {
    explicit_named_literals: usize,
    constructor_calls: usize,
    checked_slot_installations: usize,
}

impl<'ast> Visit<'ast> for AbortedControllerConstructionAudit {
    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if path_ends_with(&node.path, &[ABORTED_CONTROLLER_TURN]) {
            self.explicit_named_literals += 1;
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if is_aborted_controller_constructor_call(node) {
            self.constructor_calls += 1;
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        if is_aborted_turn_slot(&node.left) && is_some_aborted_controller_constructor(&node.right) {
            self.checked_slot_installations += 1;
        }
        visit::visit_expr_assign(self, node);
    }
}

#[derive(Default)]
struct SelfStructLiteralAudit {
    count: usize,
}

impl<'ast> Visit<'ast> for SelfStructLiteralAudit {
    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if node.path.is_ident("Self") {
            self.count += 1;
        }
        visit::visit_expr_struct(self, node);
    }
}

#[derive(Default)]
struct ControllerColdAuditCalls {
    independent_error_mapping: usize,
    stored_byte_audits: usize,
    page_origin_reads: usize,
}

struct NamedMethodCallAudit<'name> {
    name: &'name str,
    count: usize,
}

impl<'ast> Visit<'ast> for NamedMethodCallAudit<'_> {
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if node.method == self.name {
            self.count += 1;
        }
        visit::visit_expr_method_call(self, node);
    }
}

fn method_call_count(block: &syn::Block, name: &str) -> usize {
    let mut audit = NamedMethodCallAudit { name, count: 0 };
    audit.visit_block(block);
    audit.count
}

fn impl_methods_named<'syntax>(
    syntax: &'syntax syn::File,
    name: &str,
) -> Vec<&'syntax syn::ImplItemFn> {
    syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item_impl) => Some(item_impl),
            _ => None,
        })
        .flat_map(|item_impl| item_impl.items.iter())
        .filter_map(|item| match item {
            ImplItem::Fn(method) if method.sig.ident == name => Some(method),
            _ => None,
        })
        .collect()
}

fn type_is_named(ty: &Type, name: &str) -> bool {
    matches!(ty, Type::Path(path) if path_ends_with(&path.path, &[name]))
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let [syn::GenericArgument::Type(inner)] = arguments.args.iter().collect::<Vec<_>>().as_slice()
    else {
        return None;
    };
    Some(inner)
}

fn type_is_option_of(ty: &Type, name: &str) -> bool {
    option_inner_type(ty).is_some_and(|inner| type_is_named(inner, name))
}

fn mutable_reference_target(ty: &Type) -> Option<&Type> {
    let Type::Reference(reference) = ty else {
        return None;
    };
    reference.mutability.as_ref()?;
    Some(reference.elem.as_ref())
}

fn type_is_mutable_reference_to(ty: &Type, name: &str) -> bool {
    mutable_reference_target(ty).is_some_and(|target| type_is_named(target, name))
}

fn type_mentions_named(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Array(array) => type_mentions_named(&array.elem, name),
        Type::BareFn(function) => {
            function
                .inputs
                .iter()
                .any(|input| type_mentions_named(&input.ty, name))
                || matches!(
                    &function.output,
                    syn::ReturnType::Type(_, output) if type_mentions_named(output, name)
                )
        }
        Type::Group(group) => type_mentions_named(&group.elem, name),
        Type::Paren(paren) => type_mentions_named(&paren.elem, name),
        Type::Path(path) => {
            path_ends_with(&path.path, &[name])
                || path
                    .path
                    .segments
                    .iter()
                    .any(|segment| match &segment.arguments {
                        syn::PathArguments::None => false,
                        syn::PathArguments::AngleBracketed(arguments) => {
                            arguments.args.iter().any(|argument| {
                                matches!(
                                    argument,
                                    syn::GenericArgument::Type(argument)
                                        if type_mentions_named(argument, name)
                                )
                            })
                        }
                        syn::PathArguments::Parenthesized(arguments) => {
                            arguments
                                .inputs
                                .iter()
                                .any(|input| type_mentions_named(input, name))
                                || matches!(
                                    &arguments.output,
                                    syn::ReturnType::Type(_, output)
                                        if type_mentions_named(output, name)
                                )
                        }
                    })
        }
        Type::Ptr(pointer) => type_mentions_named(&pointer.elem, name),
        Type::Reference(reference) => type_mentions_named(&reference.elem, name),
        Type::Slice(slice) => type_mentions_named(&slice.elem, name),
        Type::Tuple(tuple) => tuple
            .elems
            .iter()
            .any(|element| type_mentions_named(element, name)),
        _ => false,
    }
}

fn signature_mentions_named(signature: &syn::Signature, name: &str) -> bool {
    signature.inputs.iter().any(|argument| {
        matches!(argument, syn::FnArg::Typed(argument) if type_mentions_named(&argument.ty, name))
    }) || matches!(
        &signature.output,
        syn::ReturnType::Type(_, output) if type_mentions_named(output, name)
    )
}

fn has_direct_cfg_test(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("cfg")
            && attribute
                .parse_args::<syn::Ident>()
                .is_ok_and(|predicate| predicate == "test")
    })
}

fn is_self_phase(expression: &Expr) -> bool {
    let Expr::Field(field) = peeled_expr(expression) else {
        return false;
    };
    matches!(&field.member, Member::Named(member) if member == "phase")
        && matches!(peeled_expr(&field.base), Expr::Path(base) if base.path.is_ident("self"))
}

fn is_running_composition_phase(expression: &Expr) -> bool {
    matches!(
        peeled_expr(expression),
        Expr::Path(path)
            if path_ends_with(
                &path.path,
                &["QuinnClientCompositionPhase", "Running"],
            )
    )
}

#[derive(Default)]
struct CurrentRunningPhaseGuardAudit {
    count: usize,
}

impl<'ast> Visit<'ast> for CurrentRunningPhaseGuardAudit {
    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        if matches!(node.op, syn::BinOp::Ne(_))
            && ((is_self_phase(&node.left) && is_running_composition_phase(&node.right))
                || (is_running_composition_phase(&node.left) && is_self_phase(&node.right)))
        {
            self.count += 1;
        }
        visit::visit_expr_binary(self, node);
    }
}

fn current_running_phase_guard_count(block: &syn::Block) -> usize {
    let mut audit = CurrentRunningPhaseGuardAudit::default();
    audit.visit_block(block);
    audit.count
}

impl<'ast> Visit<'ast> for ControllerColdAuditCalls {
    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if path_ends_with(
            &node.path,
            &[
                "OwnerControllerFrameByteAuditError",
                "into_cold_inventory_error",
            ],
        ) {
            self.independent_error_mapping += 1;
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if node.method == "cold_inventory_result" {
            self.stored_byte_audits += 1;
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_field(&mut self, node: &'ast syn::ExprField) {
        if matches!(&node.member, Member::Named(member) if member == "page_origin") {
            self.page_origin_reads += 1;
        }
        visit::visit_expr_field(self, node);
    }
}

fn rust_test_attribute_count(source: &str) -> usize {
    source
        .lines()
        .map(str::trim)
        .filter(|line| *line == "#[test]" || line.starts_with("#[tokio::test"))
        .count()
}

fn quinn_actor_production_source() -> &'static str {
    QUINN_ACTOR_SOURCE
        .strip_suffix("#[cfg(test)]\nmod tests;\n")
        .expect("fixed Quinn actor retains one terminal external cfg(test) module")
}

#[test]
fn owner_attach_materializes_recovery_before_the_generation_cas() {
    let transaction = RESUMABLE_AUTH_SOURCE
        .split_once("fn preflight_model_and_commit_with_hook(")
        .and_then(|(_, source)| source.split_once("\n    fn prepare("))
        .map(|(source, _)| source)
        .expect("owner ATTACH retains one closed model-preflight/CAS transaction");

    let recovery_preparation = transaction
        .find("model.prepare_owner_replacement(prepared.committed)")
        .expect("the owner seam returns only an already-materialized replacement");
    let race_hook = transaction
        .find("before_commit();")
        .expect("the deterministic race hook remains immediately before commit");
    let generation_cas = transaction
        .find("self.commit_prepared(prepared)")
        .expect("the transaction retains one generation commit");

    assert!(
        recovery_preparation < race_hook && race_hook < generation_cas,
        "all recovery inspection/allocation must finish before another contender can win the generation CAS",
    );
    assert!(
        !transaction.contains("model_preflight.install()"),
        "the post-CAS tail must consume only an already-materialized replacement",
    );

    let owner_preparation = RESUMABLE_SESSION_SOURCE
        .split_once("pub(super) fn prepare_owner_replacement(")
        .and_then(|(_, source)| source.split_once("\n    /// Read-only replacement preflight"))
        .map(|(source, _)| source)
        .expect("the session model retains one owner-only preparation seam");
    assert!(owner_preparation.contains("self.preflight_replacement(leg)?.prepare_recovery()"));
    assert!(
        !RESUMABLE_SESSION_SOURCE.contains("pub(super) struct PreflightedReplacement"),
        "raw replacement preflight authority must remain private to session.rs",
    );
    assert!(
        !RESUMABLE_SESSION_SOURCE.contains("pub(super) fn preflight_replacement("),
        "sibling modules must not obtain an unmaterialized replacement token",
    );
}

#[test]
fn initial_owner_retirement_requires_an_exact_zero_active_fifo() {
    let retirement = OWNER_TARGET_SOURCE
        .split_once("pub(super) fn prepare_initial_retirement")
        .and_then(|(_, source)| source.split_once("\n    /// Retired-only joined transaction."))
        .map(|(source, _)| source)
        .expect("initial-A retirement retains one closed precommit preparation");

    let inventory = retirement
        .find("a_queue.cold_inventory()")
        .expect("retirement independently inventories the exact A FIFO");
    let consume_binding = retirement
        .find("initial.physical_actor.take()")
        .expect("a successful preparation consumes the exact physical binding once");
    assert!(
        inventory < consume_binding,
        "all FIFO checks must finish before retirement consumes current-A ownership",
    );
    assert!(retirement.contains("a_inventory.is_exact_active_zero()"));
    assert!(retirement.contains("_a_queue: a_queue,"));

    let exact_zero = LEG_IO_SOURCE
        .split_once("pub(super) const fn is_exact_active_zero(&self) -> bool {")
        .and_then(|(_, source)| source.split_once("\n    }\n"))
        .map(|(source, _)| source)
        .expect("the cold FIFO inventory retains one exact-active-zero predicate");
    for required_check in [
        "self.was_active()",
        "self.queued_frames == 0",
        "self.summed_frame_bytes == 0",
        "self.stored_owned_bytes == 0",
        "!self.ordered_delivery_wait",
    ] {
        assert!(
            exact_zero.contains(required_check),
            "initial-A retirement is missing exact FIFO check {required_check}",
        );
    }
    assert!(OWNER_TARGET_SOURCE.contains(
        "pub(super) struct PreparedOwnerInitialRetirement<'queue> {\n    _a_queue: &'queue mut LegOutboundQueue,"
    ));
}

#[test]
fn owner_output_ledgers_are_one_structural_subroot() {
    let subroot = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("struct QuinnOwnerOutputSubroot {")
        .and_then(|(_, source)| source.split_once("\n}\n"))
        .map(|(source, _)| source)
        .expect("the owner composition retains one structural output subroot");
    let mut actual_fields = subroot
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    actual_fields.sort_unstable();
    let mut expected_fields = vec![
        "admission_failure: Option<OwnerOutputAdmissionFailure>,",
        "ledger: QuinnOwnerOutputLedger,",
        "lifecycle: OwnerOutputLifecyclePort,",
    ];
    expected_fields.sort_unstable();
    assert_eq!(
        actual_fields, expected_fields,
        "output subroot must own exactly one in-place ledger, rejection ledger, and lifecycle ledger",
    );

    let ledger = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("struct QuinnOwnerOutputLedger {")
        .and_then(|(_, source)| source.split_once("\n}\n\n/// One structural owner"))
        .map(|(source, _)| source)
        .expect("output drain and status remain inseparable in one in-place ledger");
    let mut actual_ledger_fields = ledger
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    actual_ledger_fields.sort_unstable();
    assert_eq!(
        actual_ledger_fields,
        vec![
            "drain: OwnerOutputDrain,",
            "status: QuinnOwnerOutputStatus,"
        ],
    );

    let status = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("enum QuinnOwnerOutputStatus {")
        .and_then(|(_, source)| source.split_once("\n}\n\n/// The exact drain never moves"))
        .map(|(source, _)| source)
        .expect("output ledger retains one closed status machine");
    for required in [
        "Live {\n        phase: QuinnOwnerOutputPhase,\n    },",
        "OrdinaryInFlight {\n        entered: bool,\n    },",
        "OrdinaryQuarantined {\n        first_stop_reason: Option<OwnerOutputDrainStopReason>,\n    },",
        "RejectedPage {\n        first_stop_reason: Option<OwnerOutputDrainStopReason>,\n    },",
        "ContinuationInFlight {\n        page: OwnerOutputPageDrained,\n        entered: bool,\n    },",
        "ContinuationQuarantined {\n        page: OwnerOutputPageDrained,\n        first_stop_reason: Option<OwnerOutputDrainStopReason>,\n    },",
        "Stopped {\n        reason: OwnerOutputDrainStopReason,\n        audited: OwnerOutputDrainInventory,\n    },",
        "Quarantined {\n        phase: QuinnOwnerOutputPhase,\n        first_stop_reason: Option<OwnerOutputDrainStopReason>,",
    ] {
        assert!(
            status.contains(required),
            "output status is missing {required}"
        );
    }
    for forbidden in [
        "Option<OwnerOutputDrain>",
        "Option<StoppedOwnerOutputDrain>",
        "Transitioning",
    ] {
        assert!(
            !status.contains(forbidden),
            "output status reintroduced movable or placeholder state {forbidden}",
        );
    }
    for forbidden in [
        "PreparedOwnerOutputDrainStop",
        "prepare_stop(",
        "commit_prepared_stop(",
    ] {
        assert!(
            !QUINN_OWNER_COMPOSITION_SOURCE.contains(forbidden),
            "output composition reintroduced portable stop authority {forbidden}",
        );
    }

    let inventory = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("pub(crate) struct QuinnOwnerOutputRootInventory {")
        .and_then(|(_, source)| source.split_once("\n}\n\nimpl QuinnOwnerOutputSubroot"))
        .map(|(source, _)| source)
        .expect("output root inventory remains structurally complete");
    for owned_field in [
        "drain: Option<OwnerOutputDrainInventory>,",
        "drain_ledgers: u8,",
        "drain_audit_failed: bool,",
        "phase_markers: u8,",
        "continuation_markers: u8,",
        "ordinary_transactions: u8,",
        "continuation_transactions: u8,",
        "phase_drain_consistent: bool,",
        "quarantined: bool,",
        "lifecycle: OwnerOutputLifecycleInventory,",
        "rejected_page: Option<OwnerOutputAdmissionInventory>,",
    ] {
        assert!(
            inventory.contains(owned_field),
            "output root inventory is missing {owned_field}",
        );
    }
    let exact_zero = QUINN_OWNER_INVENTORY_SOURCE
        .split_once("pub(crate) const fn is_output_exact_zero(self) -> bool {")
        .and_then(|(_, source)| source.split_once("\n    }"))
        .map(|(source, _)| source)
        .expect("output root keeps one exact-zero predicate");
    for required in [
        "self.drain_ledgers == 1",
        "!self.drain_audit_failed",
        "self.phase_markers == 0",
        "self.continuation_markers == 0",
        "self.ordinary_transactions == 0",
        "self.continuation_transactions == 0",
        "self.phase_drain_consistent",
        "!self.quarantined",
        "matches!(self.drain, Some(inventory) if inventory.is_exact_zero())",
        "self.lifecycle.is_exact_zero()",
        "self.rejected_page.is_none()",
    ] {
        assert!(exact_zero.contains(required), "exact zero omits {required}");
    }
    assert!(OWNER_SESSION_CONTROLLER_SOURCE.contains("_permit: QuinnOwnerContinuationPermit<'_>,"));
    assert!(!OWNER_SESSION_CONTROLLER_SOURCE.contains("pub(crate) fn continue_effects(&mut self)"));
    let recovery_root = QUINN_OWNER_PHYSICAL_ROOT_SOURCE
        .split_once("pub(crate) struct QuinnOwnerRecoverySessionComposition<T: TargetIo> {")
        .and_then(|(_, source)| source.split_once("\n}\n"))
        .map(|(source, _)| source)
        .expect("the test-only recovery adapter reuses one controller-output root");
    assert!(recovery_root.contains("controller_output: QuinnOwnerControllerOutputRoot<T>"));
    for forbidden_split_ledger in [
        "output_drain:",
        "output_phase:",
        "stopped_output_drain:",
        "output_stop_failure:",
        "output_admission_failure:",
        "lifecycle_output:",
    ] {
        assert!(
            !recovery_root.contains(forbidden_split_ledger),
            "recovery root still separately owns {forbidden_split_ledger}",
        );
    }
    let inbound_context = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("struct QuinnOwnerRecoveryInboundContext<'a, T: TargetIo> {")
        .and_then(|(_, source)| source.split_once("\n}\n"))
        .map(|(source, _)| source)
        .expect("owner B inbound retains one bounded transaction context");
    assert!(
        inbound_context.contains(
            "controller_output: Option<QuinnOwnerControllerOutputInboundTransaction<'a, T>>"
        ),
        "actor inbound must receive only the exact-root-minted admission transaction",
    );
    assert!(!inbound_context.contains("controller: &mut OwnerSessionController"));
    assert!(!inbound_context.contains("output: Option<QuinnOwnerOutputInboundTransaction"));
    assert!(!inbound_context.contains("output_drain:"));
    assert!(!inbound_context.contains("output_phase:"));
    assert!(!inbound_context.contains("output_admission_failure:"));
    assert_eq!(
        source_occurrences(
            QUINN_OWNER_PHYSICAL_ROOT_SOURCE,
            "#[cfg(test)]\npub(crate) struct QuinnOwnerRecoverySessionComposition<T: TargetIo> {",
        ),
        1,
        "the complete legacy B-only adapter must remain test-only",
    );
    assert_eq!(
        source_occurrences(
            QUINN_OWNER_PHYSICAL_ROOT_SOURCE,
            "#[cfg(test)]\nimpl<T: TargetIo> QuinnOwnerRecoverySessionComposition<T> {",
        ),
        1,
        "the legacy B-only implementation must remain test-only",
    );
    let test_reexports = QUINN_COMPOSITION_SOURCE
        .split_once("#[cfg(test)]\npub(crate) use owner::{")
        .and_then(|(_, source)| source.split_once("\n};"))
        .map(|(source, _)| source)
        .expect("legacy recovery adapter exports remain test-only");
    assert!(test_reexports.contains("QuinnOwnerRecoverySessionComposition"));
    assert_eq!(
        source_occurrences(
            QUINN_COMPOSITION_SOURCE,
            "QuinnOwnerRecoverySessionComposition",
        ),
        1,
    );
    assert!(!QUINN_COMPOSITION_SOURCE.contains("pub(crate) use owner::*;"));
}

#[test]
fn owner_recovery_service_is_one_guarded_closed_root_turn() {
    let continuation_source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/owned_upstream/quinn_leg_composition/owner/continuation.rs"
    ))
    .expect("the private controller-output leaf must exist");
    let syntax = syn::parse_file(&continuation_source)
        .expect("the private controller-output leaf remains valid Rust syntax");

    let roots = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerControllerOutputRoot" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [root] = roots.as_slice() else {
        panic!("recovery service must remain inside the unique controller-output root");
    };
    let syn::Fields::Named(root_fields) = &root.fields else {
        panic!("the controller-output root must name its recovery transaction state");
    };
    assert_eq!(
        root_fields
            .named
            .iter()
            .filter(|field| type_is_named(&field.ty, "QuinnOwnerRecoveryServiceState"))
            .count(),
        1,
        "the root must own exactly one recovery-service transaction state",
    );

    let transactions = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerRecoveryServiceTransaction" => {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [transaction] = transactions.as_slice() else {
        panic!("recovery service must retain one root-borrowing transaction type");
    };
    let syn::Fields::Named(transaction_fields) = &transaction.fields else {
        panic!("the recovery transaction must name its exact root borrow");
    };
    assert_eq!(transaction_fields.named.len(), 1);
    assert!(transaction_fields.named.first().is_some_and(|field| {
        type_is_mutable_reference_to(&field.ty, "QuinnOwnerControllerOutputRoot")
    }));

    let begin = continuation_source
        .split_once("fn begin_recovery_service(")
        .and_then(|(_, source)| source.split_once("    #[cfg(test)]\n    pub(super) fn snapshot"))
        .map(|(source, _)| source)
        .expect("the root retains one bounded recovery begin method");
    assert!(begin.contains("entered: false,\n                    work,"));

    let service = continuation_source
        .split_once("impl<T: TargetIo> QuinnOwnerRecoveryServiceTransaction<'_, T> {")
        .and_then(|(_, source)| {
            source.split_once("impl<T: TargetIo> Drop for QuinnOwnerRecoveryServiceTransaction")
        })
        .map(|(source, _)| source)
        .expect("the recovery transaction retains one closed service implementation");
    let enter = service
        .find("if !self.enter(QuinnOwnerServiceWork::Recovery)")
        .expect("the transaction must enter before controller access");
    let snapshot = service
        .find("let snapshot = self.root.controller.snapshot();")
        .expect("the transaction reads controller state only after entering");
    assert!(enter < snapshot);
    assert!(service.contains(
        "self.root.controller_failure = QuinnOwnerControllerFailureLedger::Failed(error);"
    ));
    assert!(service.contains("self.root.output.fail();"));
    assert!(service.contains("self.settle_idle();"));

    let drop_settlement = continuation_source
        .split_once("impl<T: TargetIo> Drop for QuinnOwnerRecoveryServiceTransaction<'_, T> {")
        .and_then(|(_, source)| {
            source
                .split_once("\n}\n\nimpl<T: TargetIo> QuinnOwnerControllerOutputInboundTransaction")
        })
        .map(|(source, _)| source)
        .expect("the recovery transaction retains one Drop settlement");
    assert!(drop_settlement.contains("InFlight { entered: false, .. }"));
    assert!(drop_settlement.contains("QuinnOwnerRecoveryServiceState::Idle"));
    assert!(drop_settlement.contains("InFlight { entered: true, .. }"));
    assert!(drop_settlement.contains("QuinnOwnerRecoveryServiceState::Quarantined"));

    let owner_service = QUINN_OWNER_PHYSICAL_ROOT_SOURCE
        .split_once("pub(crate) fn service_recovery_once(&mut self)")
        .and_then(|(_, source)| source.split_once("    pub(crate) fn try_flush("))
        .map(|(source, _)| source)
        .expect("the compatibility owner retains one bounded recovery caller");
    assert!(owner_service.contains("with_controller_parts_mut_result"));
    assert!(!owner_service.contains("record_controller_failure"));
    assert!(!owner_service.contains("request_controller_failure_if_running"));

    let inventory = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("pub(crate) struct QuinnOwnerOutputRootInventory {")
        .and_then(|(_, source)| source.split_once("\n}\n\nimpl QuinnOwnerOutputSubroot"))
        .map(|(source, _)| source)
        .expect("output inventory must expose recovery transaction ownership");
    assert!(inventory.contains("recovery_transactions: u8,"));
    assert!(inventory.contains("recovery_quarantined: bool,"));
}

#[test]
fn owner_ordinary_input_is_one_unwind_safe_root_transaction() {
    let continuation_source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/owned_upstream/quinn_leg_composition/owner/continuation.rs"
    ))
    .expect("the private controller-output leaf must exist");
    let syntax = syn::parse_file(&continuation_source)
        .expect("the private controller-output leaf remains valid Rust syntax");

    let transactions = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Struct(item) if item.ident == "QuinnOwnerControllerOutputInboundTransaction" => {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [transaction] = transactions.as_slice() else {
        panic!(
            "the exact root must mint one private ordinary transaction; found {}",
            transactions.len(),
        );
    };
    assert!(
        !matches!(transaction.vis, syn::Visibility::Public(_)),
        "ordinary transaction construction must remain inside the private leaf",
    );
    let syn::Fields::Named(fields) = &transaction.fields else {
        panic!("the ordinary transaction must name its exact root and phase");
    };
    assert_eq!(
        fields
            .named
            .iter()
            .filter(|field| {
                type_is_mutable_reference_to(&field.ty, "QuinnOwnerControllerOutputRoot")
            })
            .count(),
        1,
        "ordinary input must exclusively borrow one complete controller-output root",
    );
    for forbidden_split_authority in [
        "OwnerSessionController",
        "QuinnOwnerOutputInboundTransaction",
        "QuinnOwnerOutputSubroot",
        "OwnerSessionControllerError",
    ] {
        assert_eq!(
            fields
                .named
                .iter()
                .filter(|field| {
                    type_is_mutable_reference_to(&field.ty, forbidden_split_authority)
                        || type_is_named(&field.ty, forbidden_split_authority)
                        || type_is_option_of(&field.ty, forbidden_split_authority)
                })
                .count(),
            0,
            "ordinary input must not split out {forbidden_split_authority}",
        );
    }
    assert_eq!(
        fields.named.len(),
        1,
        "transaction phase belongs to the borrowed root, not a duplicated guard field",
    );

    let drop_impls = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Impl(item_impl) => Some(item_impl),
            _ => None,
        })
        .filter(|item_impl| {
            item_impl
                .trait_
                .as_ref()
                .is_some_and(|(_, path, _)| path_ends_with(path, &["Drop"]))
                && matches!(
                    item_impl.self_ty.as_ref(),
                    Type::Path(path)
                        if path_ends_with(
                            &path.path,
                            &["QuinnOwnerControllerOutputInboundTransaction"],
                        )
                )
        })
        .count();
    assert_eq!(
        drop_impls, 1,
        "ordinary input needs one closed Drop settlement for rollback/unwind",
    );
    assert!(
        !continuation_source.contains("fn record_first_failure("),
        "a split Option writer can drop a second ownership-carrying controller error",
    );

    let status = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("enum QuinnOwnerOutputStatus {")
        .and_then(|(_, source)| source.split_once("\n}\n\n/// The exact drain never moves"))
        .map(|(source, _)| source)
        .expect("output ledger retains one closed status machine");
    for required_state in [
        "OrdinaryInFlight {\n        entered: bool,\n    }",
        "OrdinaryQuarantined",
        "ContinuationInFlight {\n        page: OwnerOutputPageDrained,\n        entered: bool,\n    }",
    ] {
        assert!(
            status.contains(required_state),
            "output status is missing {required_state}",
        );
    }

    let inventory = QUINN_OWNER_COMPOSITION_SOURCE
        .split_once("pub(crate) struct QuinnOwnerOutputRootInventory {")
        .and_then(|(_, source)| source.split_once("\n}\n\nimpl QuinnOwnerOutputSubroot"))
        .map(|(source, _)| source)
        .expect("output root inventory remains structurally complete");
    assert!(inventory.contains("ordinary_transactions: u8,"));
    let exact_zero = QUINN_OWNER_INVENTORY_SOURCE
        .split_once("pub(crate) const fn is_output_exact_zero(self) -> bool {")
        .and_then(|(_, source)| source.split_once("\n    }"))
        .map(|(source, _)| source)
        .expect("output root keeps one exact-zero predicate");
    assert!(
        exact_zero.contains("self.ordinary_transactions == 0"),
        "an in-flight or forgotten ordinary transaction must prevent clean zero",
    );
}

#[test]
fn owner_inbound_root_settlement_is_pre_ack_and_has_no_raw_failure_bypass() {
    let continuation_source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/owned_upstream/quinn_leg_composition/owner/continuation.rs"
    ))
    .expect("the private controller-output leaf must exist");
    let continuation_syntax = syn::parse_file(&continuation_source)
        .expect("the private controller-output leaf remains valid Rust syntax");

    let owner_receive = QUINN_OWNER_PHYSICAL_ROOT_SOURCE
        .split_once("pub(crate) fn try_receive_owner_b(&mut self)")
        .and_then(|(_, source)| source.split_once("    /// Services exactly one ordinary output"))
        .map(|(source, _)| source)
        .expect("physical owner input retains one bounded receive transaction");
    let reserve_root = owner_receive
        .find("self.controller_output.begin_inbound_admission()")
        .expect("owner input reserves the complete root before actor mutation");
    let enter_actor = owner_receive
        .find("actor.try_transact_controller_inbound_with_parts(")
        .expect("owner input enters the actor first-failure transaction once");
    assert!(
        reserve_root < enter_actor,
        "the complete root guard must exist before the actor can dequeue or mutate a record",
    );
    assert_eq!(
        source_occurrences(owner_receive, "context.controller_output.take()"),
        2,
        "probe and session commits must each consume the exact root guard once",
    );
    assert!(
        !owner_receive.contains("context.controller_output.as_mut()"),
        "a mutable borrowed guard would let owner code reuse a settled transaction",
    );
    assert!(owner_receive.contains("controller_output.restore_unstarted();"));
    assert!(owner_receive.contains(".apply_actor_event_and_admit(input)"));
    assert!(owner_receive.contains(".accept_registered_owner_attach("));
    assert!(
        !owner_receive.contains("request_controller_failure_if_running()"),
        "owner input must not publish a post-hoc controller failure after actor settlement",
    );

    let actor_parts_transaction = quinn_actor_production_source()
        .split_once("pub(super) fn try_transact_controller_inbound_with_parts<")
        .and_then(|(_, source)| {
            source.split_once("pub(super) fn try_transact_inbound_before_probe_deadline<")
        })
        .map(|(source, _)| source)
        .expect("the actor retains one bounded controller/parts transaction");
    let first_failure = actor_parts_transaction
        .find("let committed = first_failure.commit(||")
        .expect("the actor commit is protected by its first-failure lock");
    let root_settlement = actor_parts_transaction
        .find("let value = commit(context, prepared, envelope, io, queue)?;")
        .expect("the actor delegates controller/output settlement before classification");
    let delivery_ack = actor_parts_transaction
        .find("record.commit_classified();")
        .expect("the actor publishes one final delivery acknowledgement");
    assert_eq!(
        source_occurrences(actor_parts_transaction, "record.commit_classified();"),
        1,
        "the physical-owner transaction must have one final ACK publication point",
    );
    assert!(
        first_failure < root_settlement && root_settlement < delivery_ack,
        "root settlement must succeed under the first-failure lock before the final actor ACK",
    );

    let mut raw_error_ports = Vec::new();
    let mut raw_authority_returns = Vec::new();
    for item in &continuation_syntax.items {
        let Item::Impl(item_impl) = item else {
            continue;
        };
        if !type_is_named(item_impl.self_ty.as_ref(), "QuinnOwnerControllerOutputRoot")
            || has_direct_cfg_test(&item_impl.attrs)
        {
            continue;
        }
        for item in &item_impl.items {
            let ImplItem::Fn(method) = item else {
                continue;
            };
            if matches!(method.vis, syn::Visibility::Inherited)
                || has_direct_cfg_test(&method.attrs)
            {
                continue;
            }
            if signature_mentions_named(&method.sig, "OwnerSessionControllerError") {
                raw_error_ports.push(method.sig.ident.to_string());
            }
            let returns_raw_authority = match &method.sig.output {
                syn::ReturnType::Default => false,
                syn::ReturnType::Type(_, output) => [
                    "OwnerSessionController",
                    "QuinnOwnerOutputSubroot",
                    "QuinnOwnerControllerFailureLedger",
                ]
                .iter()
                .any(|name| type_mentions_named(output, name)),
            };
            if returns_raw_authority {
                raw_authority_returns.push(method.sig.ident.to_string());
            }
        }
    }
    raw_error_ports.sort_unstable();
    raw_authority_returns.sort_unstable();
    assert!(
        raw_error_ports.is_empty(),
        "the unified root exposes production-callable raw error ports: {raw_error_ports:?}",
    );
    assert!(
        raw_authority_returns.is_empty(),
        "the unified root exposes production-callable split authorities: {raw_authority_returns:?}",
    );
    assert!(
        !QUINN_OWNER_COMPOSITION_SOURCE.contains(".record_controller_failure("),
        "owner code must receive only closed root drive values, never a raw failure writer",
    );
}

#[test]
fn quinn_actor_test_module_stays_external_and_private() {
    let _production = quinn_actor_production_source();
    assert_eq!(
        source_occurrences(QUINN_ACTOR_SOURCE, "#[cfg(test)]\nmod tests;"),
        1,
        "one terminal private child Module owns the actor test Implementation",
    );
    assert_eq!(
        source_occurrences(QUINN_ACTOR_SOURCE, "#[cfg(test)]\nmod tests {"),
        0,
        "the production source must not inline the actor test Implementation",
    );
    assert!(
        QUINN_ACTOR_SOURCE.ends_with("#[cfg(test)]\nmod tests;\n"),
        "the external private test Module stays the terminal production declaration",
    );
    assert_eq!(
        rust_test_attribute_count(QUINN_ACTOR_TESTS_SOURCE),
        110,
        "the external private Module retains the complete actor test inventory",
    );
    assert!(QUINN_ACTOR_TESTS_SOURCE.contains(
        "fn production_initial_attach_capacity_overflow_rejects_before_actor_creation()"
    ));
    assert!(QUINN_ACTOR_TESTS_SOURCE.contains(
        "async fn real_quinn_controller_only_discard_stays_consumed_failed_through_physical_join()"
    ));
    assert!(QUINN_ACTOR_TESTS_SOURCE.contains(
        "async fn controller_parts_transfer_rejects_mismatched_fifo_without_consuming_input()"
    ));
    assert_eq!(
        source_occurrences(QUINN_ACTOR_TESTS_SOURCE, "standby::tests::"),
        2,
        "the external Module retains the exact cross-sibling fixture use",
    );
}

fn quinn_client_root_production_source() -> &'static str {
    QUINN_CLIENT_ROOT_SOURCE
        .split_once("#[cfg(test)]\nmod tests {")
        .expect("Quinn client root retains one terminal cfg(test) module")
        .0
}

#[test]
fn production_root_lifecycle_cutoff_stays_transactional_and_input_preserving() {
    let production = quinn_client_root_production_source();
    let lifecycle = production
        .split_once("impl RootLifecycle {")
        .and_then(|(_, source)| source.split_once("\n}\n\n/// Linearizes shutdown"))
        .map(|(source, _)| source)
        .expect("root lifecycle implementation stays adjacent to shutdown arbitration");
    let run_root = production
        .split_once("async fn run_root(mut bundle: RootTaskBundle)")
        .and_then(|(_, source)| source.split_once("\nfn joined_witness("))
        .map(|(source, _)| source)
        .expect("production root loop stays bounded by its joined-witness helper");

    assert_eq!(
        source_occurrences(lifecycle, "fn transact_if_running<T>("),
        1,
        "one lifecycle transaction guards every bounded synchronous live turn",
    );
    assert_eq!(
        source_occurrences(lifecycle, "fn transact_if_running_with_input<I, T>("),
        1,
        "one input-preserving transaction guards asynchronously selected commands",
    );
    assert_eq!(
        source_occurrences(lifecycle, "let phase = self.phase.borrow();"),
        2,
        "both transactions hold the same watch read guard as the shutdown CAS",
    );
    assert!(!lifecycle.contains(".await"));
    assert!(!lifecycle.contains("spawn("));
    assert!(!lifecycle.contains("mpsc::"));

    assert!(run_root.contains("Ok(Some(RootReadyPublication::Published)) => (None, true),"));
    assert!(
        run_root.contains("Ok(Some(RootReadyPublication::ShutdownRequested)) => (None, false),")
    );
    assert!(
        !run_root
            .contains("RootReadyPublication::Published | RootReadyPublication::ShutdownRequested")
    );
    assert_eq!(
        source_occurrences(run_root, ".transact_if_running(||"),
        1,
        "clock read and one category drive share a single cutoff transaction",
    );
    assert_eq!(
        source_occurrences(
            run_root,
            ".transact_if_running_with_input(command, |command|"
        ),
        1,
        "the selected command crosses the cutoff exactly once with ownership intact",
    );
    assert_eq!(
        source_occurrences(run_root, "reject_root_command_after_lifecycle_cutoff("),
        2,
        "both shutdown and invalid-phase branches return the exact selected command",
    );
}

#[test]
fn quinn_factory_owns_the_transport_gate_and_exact_channel_binding_sources() {
    assert!(QUINN_ACTOR_SOURCE.contains("pub(super) struct QuinnLegActorTransportGate(());"));
    assert!(QUINN_ACTOR_SOURCE.contains("fn mint_for_fixed_factory() -> Self"));
    assert!(QUINN_ACTOR_SOURCE.contains("const ACTOR_ALPN: &[u8] = b\"mvpn-resumable-leg-v1\";"));
    assert!(
        QUINN_ACTOR_SOURCE
            .contains("const EXPORTER_LABEL: &[u8] = b\"EXPORTER-mini-vpn-resumable-leg-v1\";")
    );
    assert!(QUINN_ACTOR_SOURCE.contains("b\"mini-vpn/r6/quinn-leg/attach-binding/v1\\0\""));
    assert!(QUINN_ACTOR_SOURCE.contains(
        "const CONFIRM_PREFIX: &[u8] = b\"mini-vpn/r6/quinn-leg/binding-confirm/v1\\0\";"
    ));
    assert_eq!(
        source_occurrences(QUINN_ACTOR_SOURCE, ".export_keying_material("),
        1,
        "the fixed Quinn factory must remain the sole TLS exporter source",
    );
    assert_eq!(
        source_occurrences(
            QUINN_ACTOR_SOURCE,
            "QuinnLegActorTransportGate::mint_for_fixed_factory()",
        ),
        1,
    );
    let binding = "let binding = attach_binding(peer_policy, exporter)?;";
    let bootstrap = include_str!("../src/owned_upstream/quinn_leg_actor/bootstrap.rs");
    assert_eq!(source_occurrences(QUINN_ACTOR_SOURCE, binding), 1);
    assert_eq!(
        source_occurrences(bootstrap, binding),
        1,
        "the fixed server role must consume the live exporter-derived binding",
    );
    assert!(!LEG_IO_SOURCE.contains("struct QuinnLegActorTransportGate"));
}

#[test]
fn production_established_leg_and_joined_witnesses_have_single_gated_mints() {
    assert!(LEG_IO_SOURCE.contains("pub(super) struct LegIoAuthenticatedTransportGate(());"));
    assert!(LEG_IO_SOURCE.contains("fn mint_for_leg_io() -> Self"));
    assert_eq!(
        source_occurrences(
            LEG_IO_SOURCE,
            "LegIoAuthenticatedTransportGate::mint_for_leg_io()"
        ),
        1,
    );
    assert!(LEG_SOURCE.contains("_gate: LegIoAuthenticatedTransportGate"));
    assert_eq!(
        source_occurrences(QUINN_ACTOR_SOURCE, "Ok(QuinnLegActorJoined {"),
        1,
        "only a successful exact actor join may mint its Joined witness",
    );
    assert_eq!(
        source_occurrences(
            QUINN_ACTOR_SOURCE,
            "Ok(QuinnLegActorListenerJoined { _private: () })",
        ),
        1,
        "only a successful listener shutdown may mint its Joined witness",
    );
}

#[test]
fn physical_actor_incarnation_and_retired_clear_stay_closed_and_single_source() {
    assert!(LEG_IO_SOURCE.contains(
        "pub(super) struct PhysicalLegRuntimeIncarnation(Arc<PhysicalLegIncarnationIdentity>);"
    ));
    assert!(LEG_IO_SOURCE.contains(
        "pub(super) struct PhysicalLegPolicyIncarnation(Arc<PhysicalLegIncarnationIdentity>);"
    ));
    assert_eq!(
        source_occurrences(LEG_IO_SOURCE, "Arc::ptr_eq(&self.0, &policy.0)"),
        1,
        "physical actor agreement must use one opaque pointer-identity comparison",
    );
    assert_eq!(
        source_occurrences(
            QUINN_ACTOR_SOURCE,
            "PhysicalLegRuntimeIncarnation::mint_for_quinn_actor("
        ),
        1,
        "the fixed Quinn actor factory must be the sole physical-incarnation mint caller",
    );
    assert!(!LEG_IO_SOURCE.contains("impl Clone for PhysicalLegRuntimeIncarnation"));
    assert!(!LEG_IO_SOURCE.contains("impl Clone for PhysicalLegPolicyIncarnation"));
    assert!(!LEG_IO_SOURCE.contains("impl Copy for PhysicalLegRuntimeIncarnation"));
    assert!(!LEG_IO_SOURCE.contains("impl Copy for PhysicalLegPolicyIncarnation"));
    assert!(
        LEG_IO_SOURCE.contains("<PhysicalLegRuntimeIncarnation as AmbiguousIfClone<_>>::marker")
    );
    assert!(
        LEG_IO_SOURCE.contains("<PhysicalLegPolicyIncarnation as AmbiguousIfClone<_>>::marker")
    );

    assert!(QUINN_COMPOSITION_SOURCE.contains("pub(super) struct QuinnLegCompositionGate(())"));
    assert!(
        QUINN_COMPOSITION_SOURCE
            .contains("pub(crate) fn begin_retired_join(\n        &mut self,\n    )")
    );
    assert!(
        QUINN_COMPOSITION_SOURCE
            .contains("pub(crate) fn try_finish_retired_join(\n        &mut self,\n    )")
    );
    assert!(!QUINN_COMPOSITION_SOURCE.contains("fn join_and_clear_retired("));
    let retired_join_executor = QUINN_COMPOSITION_SOURCE
        .split_once("fn spawn_retired_join(")
        .and_then(|(_, source)| source.split_once("\n}\n\nimpl RetiredQuinnLegOwner"))
        .map(|(source, _)| source)
        .expect("the retired owner retains one closed join executor");
    let standby_join_executor = QUINN_COMPOSITION_SOURCE
        .split_once("fn spawn_standby_join(")
        .and_then(|(_, source)| source.split_once("\n}\n\nenum QuinnRegisteredStandbyState"))
        .map(|(source, _)| source)
        .expect("the standby owner retains one closed join executor");
    assert_eq!(
        source_occurrences(retired_join_executor, "runtime.spawn(async move"),
        1,
        "one bounded composition-owned task is the sole retired join executor",
    );
    assert_eq!(
        source_occurrences(standby_join_executor, "runtime.spawn(async move"),
        1,
        "one bounded composition-owned task is the sole standby join executor",
    );
    assert_eq!(
        source_occurrences(QUINN_COMPOSITION_SOURCE, "runtime.spawn(async move"),
        2,
        "only the disjoint retired and standby lifecycle owners may spawn join executors",
    );
    assert!(QUINN_COMPOSITION_SOURCE.contains(
        "enum QuinnClientCompositionPhase {\n    Running,\n    ShuttingDown,\n    Joined,\n}"
    ));
    assert!(QUINN_COMPOSITION_SOURCE.contains(
        "fn current_policy_is_active(&self) -> bool {\n        self.phase == QuinnClientCompositionPhase::Running\n            && self.current_terminal == QuinnCurrentTerminalState::Unbound\n            && self.current_terminal_stamp.is_none()\n    }"
    ));
    let composition_syntax = syn::parse_file(QUINN_COMPOSITION_SOURCE)
        .expect("Quinn composition remains valid Rust syntax");
    let mut production_current_policy_checks = Vec::new();
    let mut test_only_current_policy_checks = Vec::new();
    for item in &composition_syntax.items {
        let Item::Impl(item_impl) = item else {
            continue;
        };
        let impl_is_test_only = has_direct_cfg_test(&item_impl.attrs);
        for item in &item_impl.items {
            let ImplItem::Fn(method) = item else {
                continue;
            };
            let count = method_call_count(&method.block, "current_policy_is_active");
            if count == 0 {
                continue;
            }
            if impl_is_test_only || has_direct_cfg_test(&method.attrs) {
                test_only_current_policy_checks.push((method.sig.ident.to_string(), count));
            } else {
                production_current_policy_checks.push((method.sig.ident.to_string(), count));
            }
        }
    }
    production_current_policy_checks.sort_unstable();
    test_only_current_policy_checks.sort_unstable();
    let mut expected_production_checks = vec![
        ("apply_driver_input".to_owned(), 1),
        ("apply_driver_input_observed".to_owned(), 1),
        ("arm_current_probe".to_owned(), 1),
        ("begin_current_probe".to_owned(), 1),
        ("begin_owned_standby_registration".to_owned(), 1),
        ("begin_registered_replacement_core".to_owned(), 1),
        ("drive_owned_standby_physical_attach".to_owned(), 1),
        ("drive_owned_standby_registration".to_owned(), 1),
        ("drive_recovery_one".to_owned(), 1),
        ("drive_registered_standby_probe".to_owned(), 1),
        ("enqueue_current_frame".to_owned(), 1),
        ("enqueue_current_reset_frame".to_owned(), 1),
        ("mint_tcp_port_factory".to_owned(), 1),
        ("normal_replacement_blocker".to_owned(), 1),
        ("open_local".to_owned(), 1),
        ("open_prepared_tcp".to_owned(), 1),
        ("settle_completed_recovery_tail".to_owned(), 1),
        ("transact_root_ready_publication".to_owned(), 1),
        ("try_flush_current".to_owned(), 1),
        ("try_receive_current".to_owned(), 1),
        ("try_receive_current_before_probe_deadline".to_owned(), 1),
        ("try_receive_current_before_terminal_grace".to_owned(), 1),
    ];
    expected_production_checks.sort_unstable();
    assert_eq!(
        production_current_policy_checks, expected_production_checks,
        "all production active-current entries, including owned standby/probe lifecycle, typed factory/input ingress, root RESET admission, and semantic timers require the shutdown, exact-terminal, and terminal-stamp barriers",
    );
    let mut expected_test_only_checks = vec![
        ("begin_standby_registration".to_owned(), 1),
        (
            "force_current_raw_block_after_dequeue_for_test".to_owned(),
            1,
        ),
        (
            "force_current_wrong_route_after_dequeue_for_test".to_owned(),
            1,
        ),
        ("try_expire_current_probe".to_owned(), 1),
        ("enqueue_current_ordered_leg_control".to_owned(), 1),
    ];
    expected_test_only_checks.sort_unstable();
    assert_eq!(
        test_only_current_policy_checks, expected_test_only_checks,
        "test-only post-dequeue fault injection must remain explicit and separately inventoried",
    );
    assert_eq!(
        source_occurrences(QUINN_COMPOSITION_SOURCE, ".transact_controller_policy("),
        8,
        "all value-only current-policy entries must share the actor first-failure transaction",
    );
    let mut input_preserving_policy_transactions = Vec::new();
    for item in &composition_syntax.items {
        let Item::Impl(item_impl) = item else {
            continue;
        };
        for item in &item_impl.items {
            let ImplItem::Fn(method) = item else {
                continue;
            };
            let count = method_call_count(&method.block, "transact_controller_policy_with_input");
            if count > 0 {
                input_preserving_policy_transactions.push((method.sig.ident.to_string(), count));
            }
        }
    }
    input_preserving_policy_transactions.sort_unstable();
    assert_eq!(
        input_preserving_policy_transactions,
        vec![
            ("apply_driver_input".to_owned(), 1),
            ("apply_driver_input_observed".to_owned(), 1),
        ],
        "both typed driver entries must use one exact input-preserving first-failure transaction",
    );
    assert_eq!(
        source_occurrences(
            QUINN_ACTOR_SOURCE,
            "pub(super) fn transact_controller_policy<T>("
        ),
        1,
        "the fixed actor exposes one composition-gated current-policy linearization seam",
    );
    assert_eq!(
        source_occurrences(
            QUINN_ACTOR_SOURCE,
            "pub(super) fn transact_controller_policy_with_input<I, T>("
        ),
        1,
        "the fixed actor exposes one input-preserving composition-gated linearization seam",
    );
    assert!(QUINN_ACTOR_SOURCE.contains("match first_failure.run_if_clear(|| match self.phase"));
    let mut production_running_phase_guards = Vec::new();
    let mut test_only_running_phase_guards = Vec::new();
    for item in &composition_syntax.items {
        let Item::Impl(item_impl) = item else {
            continue;
        };
        let impl_is_test_only = has_direct_cfg_test(&item_impl.attrs);
        for item in &item_impl.items {
            let ImplItem::Fn(method) = item else {
                continue;
            };
            let count = current_running_phase_guard_count(&method.block);
            if count == 0 {
                continue;
            }
            let entry = (method.sig.ident.to_string(), count);
            if impl_is_test_only || has_direct_cfg_test(&method.attrs) {
                test_only_running_phase_guards.push(entry);
            } else {
                production_running_phase_guards.push(entry);
            }
        }
    }
    production_running_phase_guards.sort_unstable();
    test_only_running_phase_guards.sort_unstable();
    let mut expected_production_running_phase_guards = vec![
        ("admit_pending_physical_attach".to_owned(), 1),
        ("arm_registered_standby_probe".to_owned(), 1),
        ("begin_failed_standby_join".to_owned(), 1),
        ("begin_registered_post_loss_replacement_core".to_owned(), 1),
        ("begin_retired_join".to_owned(), 1),
        ("commit_owned_standby_physical_replacement".to_owned(), 1),
        ("commit_owned_standby_post_loss_replacement".to_owned(), 1),
        ("drive_owned_standby_physical_attach".to_owned(), 1),
        ("drive_owned_standby_post_loss_attach".to_owned(), 1),
        ("try_drive_current_terminal".to_owned(), 1),
        ("try_expire_registered_standby_probe".to_owned(), 1),
        ("try_finish_failed_standby_join".to_owned(), 1),
        ("try_finish_retired_join".to_owned(), 1),
        ("try_flush_registered_standby".to_owned(), 1),
        ("try_receive_registered_standby".to_owned(), 1),
        ("try_receive_retired".to_owned(), 1),
        ("try_transact_pending_switch_hint".to_owned(), 1),
    ];
    expected_production_running_phase_guards.sort_unstable();
    assert_eq!(
        production_running_phase_guards, expected_production_running_phase_guards,
        "every production physical ownership, standby, terminal, and retired entry must retain its outer Running barrier",
    );
    let mut expected_test_only_running_phase_guards = vec![
        (
            "bind_and_discard_current_terminal_controller_only_for_test".to_owned(),
            1,
        ),
        ("reject_current_terminal_before_bind_for_test".to_owned(), 1),
    ];
    expected_test_only_running_phase_guards.sort_unstable();
    assert_eq!(
        test_only_running_phase_guards, expected_test_only_running_phase_guards,
        "test-only terminal adapters must remain explicit and separately inventoried",
    );
    assert_eq!(
        source_occurrences(
            QUINN_COMPOSITION_SOURCE,
            "*phase = QuinnClientCompositionPhase::ShuttingDown;"
        ),
        2,
        "direct and root-facing Unbound shutdown each publish inside the actor first-failure transaction",
    );
    assert_eq!(
        source_occurrences(
            QUINN_COMPOSITION_SOURCE,
            "self.phase = QuinnClientCompositionPhase::ShuttingDown;"
        ),
        3,
        "direct terminal-led cleanup plus root terminal-led and fatal-fallback cleanup retain explicit publications",
    );
    assert_eq!(
        source_occurrences(
            QUINN_COMPOSITION_SOURCE,
            "self.phase = QuinnClientCompositionPhase::Joined;"
        ),
        2,
        "direct and root-facing completed joins each publish their final composition phase",
    );
    let shutdown_publication = QUINN_COMPOSITION_SOURCE
        .find("*phase = QuinnClientCompositionPhase::ShuttingDown;")
        .expect("composition publishes Unbound ShuttingDown under the actor gate");
    let current_cancel = QUINN_COMPOSITION_SOURCE
        .find("self.current.request_cancel();")
        .expect("composition synchronously closes current actor after shutdown publication");
    let current_join_await = QUINN_COMPOSITION_SOURCE
        .find("match self.current.join().await")
        .expect("composition awaits the exact current actor join");
    assert!(
        shutdown_publication < current_cancel && current_cancel < current_join_await,
        "ShuttingDown must publish under the fatal lock before synchronous cancellation and the current-join await",
    );
    assert_eq!(
        source_occurrences(
            QUINN_COMPOSITION_SOURCE,
            ".clear_retired_after_physical_join(runtime, terminal, &gate)"
        ),
        1,
        "only the closed composition may pair a joined actor with controller retirement",
    );
    assert_eq!(
        source_occurrences(
            CLIENT_CONTROLLER_SOURCE,
            "authority.discard_terminal(terminal).map_err(Some)"
        ),
        1,
        "normal retired cleanup must consume the exact unbound terminal once",
    );
    assert_eq!(
        source_occurrences(
            CLIENT_CONTROLLER_SOURCE,
            "(Self::TerminalBound(_), None) => Ok(())"
        ),
        1,
        "terminal-bound retired cleanup accepts exactly one no-second-terminal shape",
    );
    assert_eq!(
        source_occurrences(
            CLIENT_CONTROLLER_SOURCE,
            "RetiredClientClearErrorKind::ReplacementApplicationAckPending"
        ),
        1,
        "only the retired clear commit may expose the pending replacement-ACK barrier",
    );
    assert!(LEG_SOURCE.contains("pub(crate) struct TerminalBoundRetiredClientLeg"));
    assert!(LEG_SOURCE.contains("pub(crate) struct PendingClientLegLoss"));
    assert!(!LEG_SOURCE.contains("impl Clone for TerminalBoundRetiredClientLeg"));
    assert!(!LEG_SOURCE.contains("impl Clone for PendingClientLegLoss"));
    assert!(QUINN_COMPOSITION_SOURCE.contains("ConsumedFailed {"));
    assert!(
        QUINN_COMPOSITION_SOURCE
            .contains("if kind != ClientCurrentTerminalDiscardErrorKind::RecoveryCleanupRejected")
    );
    assert!(
        QUINN_COMPOSITION_SOURCE
            .contains("failure: QuinnCurrentTerminalConsumedFailure::ControllerDiscard(kind)")
    );
    assert_eq!(
        source_occurrences(QUINN_COMPOSITION_SOURCE, ".try_transact_reported_terminal("),
        2,
        "the composition owns one production actor-terminal controller transaction plus one test-only rollback probe",
    );
    assert_eq!(
        source_occurrences(
            QUINN_ACTOR_SOURCE,
            "pub(super) fn try_transact_reported_terminal<"
        ),
        1,
        "the fixed actor exposes one gated exact-terminal handoff",
    );
    assert!(
        QUINN_ACTOR_SOURCE
            .contains("let (reported_terminal_tx, reported_terminal_rx) = oneshot::channel();")
    );
    assert!(!QUINN_COMPOSITION_SOURCE.contains("actor_incarnation: u64"));
    assert!(!LEG_IO_SOURCE.contains("actor_incarnation: u64"));
}

#[test]
fn controller_parts_transfer_checks_the_complete_actor_port_chain_before_ownership_moves() {
    let transaction = quinn_actor_production_source()
        .split_once("pub(super) fn transact_controller_parts<I, T, E>(")
        .and_then(|(_, source)| source.split_once("    /// Lets only the composition root"))
        .map(|(source, _)| source)
        .expect("the actor retains one closed controller-parts transfer transaction");

    let runtime = transaction
        .find("let Some(runtime) = self.actor_incarnation.as_ref()")
        .expect("controller-parts transfer validates its runtime incarnation");
    let paired_ports = transaction
        .find("let (Some(io), Some(queue)) = (self.io.as_ref(), self.queue.as_ref())")
        .expect("controller-parts transfer borrows the exact endpoint/FIFO pair together");
    let io_runtime = transaction
        .find("io.belongs_to_physical_actor(runtime, gate)")
        .expect("controller-parts transfer binds LegIo to the exact runtime");
    let queue_io = transaction
        .find("queue.belongs_to_established_leg(io.established_leg())")
        .expect("controller-parts transfer binds the sole FIFO to the exact LegIo");
    let take_io = transaction
        .find("let Some(io) = self.io.take()")
        .expect("controller-parts transfer consumes LegIo once");
    let take_queue = transaction
        .find("let Some(queue) = self.queue.take()")
        .expect("controller-parts transfer consumes the FIFO once");
    let call = transaction
        .find("match transaction(input, io, queue)")
        .expect("controller-parts transfer has one caller transaction");

    assert!(
        runtime < paired_ports
            && paired_ports < io_runtime
            && io_runtime < queue_io
            && queue_io < take_io
            && take_io < take_queue
            && take_queue < call,
        "runtime -> LegIo -> FIFO identity must close before ownership moves or caller code runs",
    );
    assert!(QUINN_ACTOR_TESTS_SOURCE.contains(
        "async fn controller_parts_transfer_rejects_mismatched_fifo_without_consuming_input()"
    ));
    assert!(
        QUINN_ACTOR_TESTS_SOURCE.contains("std::mem::swap(&mut client.queue, &mut server.queue);")
    );
}

#[test]
fn quinn_actor_inbound_composition_bridge_stays_sync_bounded_and_gate_owned() {
    let production = quinn_actor_production_source();
    let actor_syntax =
        syn::parse_file(production).expect("production Quinn actor remains valid Rust syntax");
    let actor_transaction = production
        .split_once("pub(super) fn try_transact_controller_inbound<T>(")
        .and_then(|(_, source)| {
            source.split_once("pub(super) fn try_transact_controller_inbound_with_parts<")
        })
        .map(|(source, _)| source)
        .expect("the actor retains one bounded synchronous controller transaction");
    let actor_parts_transaction = production
        .split_once("pub(super) fn try_transact_controller_inbound_with_parts<")
        .and_then(|(_, source)| {
            source.split_once("pub(super) fn try_transact_inbound_before_probe_deadline<")
        })
        .map(|(source, _)| source)
        .expect("the actor retains one bounded synchronous controller/parts transaction");
    let timer_transaction = production
        .split_once("fn try_transact_inbound_before_timer<")
        .and_then(|(_, source)| {
            source.split_once("pub(super) fn try_transact_composition_inbound<T>(")
        })
        .map(|(source, _)| source)
        .expect("the actor retains one bounded receive-before-timer transaction");
    let composition_transactions =
        impl_methods_named(&actor_syntax, "try_transact_composition_inbound");
    let [composition_transaction] = composition_transactions.as_slice() else {
        panic!(
            "the actor must retain exactly one bounded synchronous composition transaction; found {}",
            composition_transactions.len(),
        );
    };
    assert!(!actor_transaction.contains(".await"));
    assert!(!actor_transaction.contains("tokio::spawn"));
    assert!(!actor_transaction.contains("mpsc::channel"));
    assert!(!actor_transaction.contains("bytes.clone"));
    assert!(!actor_transaction.contains("encoded.clone"));
    assert!(!actor_transaction.contains("to_vec"));
    assert_eq!(
        source_occurrences(actor_transaction, "receiver.try_recv()"),
        1,
        "one non-blocking dequeue is the complete per-drive actor work bound",
    );
    assert!(!actor_parts_transaction.contains(".await"));
    assert!(!actor_parts_transaction.contains("tokio::spawn"));
    assert!(!actor_parts_transaction.contains("mpsc::channel"));
    assert_eq!(
        source_occurrences(actor_parts_transaction, "receiver.try_recv()"),
        1,
        "the physical-owner preflight transaction independently dequeues at most one actor record",
    );
    assert_eq!(
        method_call_count(&composition_transaction.block, "try_recv"),
        1,
        "the typed composition drive independently consumes at most one actor record",
    );
    assert_eq!(
        source_occurrences(timer_transaction, "receiver.try_recv()"),
        1,
        "the shared receive-before-timer transaction consumes at most one actor record",
    );
    assert_eq!(
        source_occurrences(actor_transaction, "pending.take()"),
        1,
        "the actor-private retry slot is consumed at most once per drive",
    );
    assert_eq!(
        source_occurrences(actor_transaction, "pending.replace(record)"),
        1,
        "a blocked transaction restores exactly one record before returning",
    );
    assert_eq!(
        source_occurrences(timer_transaction, "pending.take()"),
        1,
        "the timer transaction consumes the actor-private retry slot at most once",
    );
    assert_eq!(
        source_occurrences(timer_transaction, "pending.replace(record)"),
        1,
        "the timer transaction restores at most one blocked record",
    );

    let cancellation = production
        .split_once("pub(crate) fn request_cancel(&mut self) {")
        .and_then(|(_, source)| source.split_once("pub(crate) async fn join(&mut self)"))
        .map(|(source, _)| source)
        .expect("the actor retains one cancellation epilogue");
    let pending_drop = cancellation
        .find("drop(self.pending_controller_inbound.take());")
        .expect("cancellation releases the private pending record");
    let cancel_publication = cancellation
        .find("self.cancel.send_replace(true);")
        .expect("cancellation publishes its monotonic signal");
    assert!(
        pending_drop < cancel_publication,
        "pending Ib/Cm/ACK ownership must release before actor children start joining",
    );

    let composition_bridge = QUINN_COMPOSITION_SOURCE
        .split_once("pub(crate) fn try_receive_current(")
        .and_then(|(_, source)| source.split_once("pub(crate) fn current_routes(&self)"))
        .map(|(source, _)| source)
        .expect("current and retired inbound drives stay in one closed composition region");
    assert!(!composition_bridge.contains("async fn"));
    assert!(!composition_bridge.contains(".await"));
    assert!(!composition_bridge.contains("spawn("));
    assert!(!composition_bridge.contains("mpsc::channel"));
    assert!(!composition_bridge.contains(".clone()"));
    assert!(composition_bridge.contains("self.controller.current_inbound_admission()"));
    assert!(composition_bridge.contains(".try_transact_controller_inbound(&gate"));
    assert!(composition_bridge.contains("receive_current_from_quinn(delivery, &gate, arrived_at)"));
    assert!(composition_bridge.contains("receive_retired_from_quinn(delivery, &gate)"));
    assert!(
        composition_bridge
            .contains("ClientInboundDisposition::Retired(_) => Err(QuinnLegActorError::Protocol(")
    );
    assert!(composition_bridge.contains("\"retired controller returned an active disposition\""));

    let actor_record_adapter = production
        .split_once("fn from_actor_record(")
        .and_then(|(_, source)| source.split_once("fn take_envelope(&mut self)"))
        .map(|(source, _)| source)
        .expect("the actor retains one raw-record adapter");
    assert!(
        actor_record_adapter
            .contains("let InboundRecord {\n            header,\n            bytes,")
    );
    assert!(actor_record_adapter.contains("            bytes,\n            header.ordinal,"));
    assert!(actor_record_adapter.contains("EncodedDelivery::from_quinn_actor_ingress("));
    assert!(actor_record_adapter.contains("            arrived_at,"));
    assert!(!actor_record_adapter.contains(".clone()"));
    assert!(!actor_record_adapter.contains("copy_from_slice"));
    assert!(!actor_record_adapter.contains("to_vec"));
    assert_eq!(
        source_occurrences(production, "EncodedDelivery::from_quinn_actor_ingress("),
        1,
        "the sole actor record adapter is the only gated constructor caller",
    );

    let gated_constructor = TWO_LEG_SOURCE
        .split_once("pub(super) fn from_quinn_actor_ingress(")
        .and_then(|(_, source)| source.split_once("pub(crate) const fn route(&self)"))
        .map(|(source, _)| source)
        .expect("the gated Quinn delivery constructor remains bounded");
    assert!(gated_constructor.contains("bytes: Bytes"));
    assert!(
        gated_constructor
            .contains("Self::from_memory_ingress(route, lane, bytes, send_ordinal, 0)")
    );
    assert!(!gated_constructor.contains(".clone()"));
    assert!(!gated_constructor.contains("copy_from_slice"));
    assert!(!gated_constructor.contains("to_vec"));
    assert!(TWO_LEG_SOURCE.contains(
        "#[cfg_attr(test, derive(Clone))]\n#[derive(Debug, PartialEq, Eq)]\npub(crate) struct EncodedDelivery"
    ));
    assert!(
        !TWO_LEG_SOURCE
            .contains("#[derive(Clone, Debug, PartialEq, Eq)]\npub(crate) struct EncodedDelivery")
    );

    assert!(CLIENT_CONTROLLER_SOURCE.contains("installed_features: FeatureSet"));
    assert_eq!(
        source_occurrences(
            CLIENT_CONTROLLER_SOURCE,
            "FeatureSet::new(attached.negotiated_features())",
        ),
        1,
        "the authenticated initial attach is the sole installed-feature source",
    );
    let current_quinn_receive = CLIENT_CONTROLLER_SOURCE
        .split_once("pub(super) fn receive_current_from_quinn(")
        .and_then(|(_, source)| source.split_once("fn receive_current_with_features("))
        .map(|(source, _)| source)
        .expect("controller retains one composed current receive adapter");
    let retired_quinn_receive = CLIENT_CONTROLLER_SOURCE
        .split_once("pub(super) fn receive_retired_from_quinn(")
        .and_then(|(_, source)| source.split_once("fn receive_retired_with_features("))
        .map(|(source, _)| source)
        .expect("controller retains one composed retired receive adapter");
    for adapter in [current_quinn_receive, retired_quinn_receive] {
        assert!(!adapter.contains("installed_features: FeatureSet"));
        assert!(adapter.contains("self.installed_features"));
    }
    assert_eq!(
        source_occurrences(
            production,
            "pending_controller_inbound: Option<ControllerInboundRecord>"
        ),
        1,
        "the actor owns exactly one defensive raw retry slot",
    );
}

fn binding(exporter_byte: u8) -> AttachTransportBinding {
    AttachTransportBinding::new(
        OwnerIdentity::new([0x31; 32]).unwrap(),
        AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
        TlsExporterBinding::new([exporter_byte; 32]).unwrap(),
        DevicePrincipal::new([0x53; 16]).unwrap(),
    )
}

fn request() -> AttachRequest {
    request_for(2, 0x22)
}

fn request_for(generation: u64, nonce_byte: u8) -> AttachRequest {
    AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(generation).unwrap(),
        AttachNonce::new([nonce_byte; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    )
}

fn credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0x71; 32]).unwrap(),
        ResumeSecret::new([0x72; 32]).unwrap(),
    )
    .unwrap()
}

fn authority(current_generation: u64) -> AttachAuthority {
    AttachAuthority::new(
        request().session_id(),
        LegGeneration::new(current_generation).unwrap(),
        credentials(),
        AttachPolicy::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
            SESSION_PROTOCOL_VERSION,
            0b1111,
        )
        .unwrap(),
    )
}

fn session_config() -> SessionConfig {
    SessionConfig::new(
        4,
        4,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReceiveBudgetLimits::new(128, 16).unwrap(),
        64,
    )
    .unwrap()
}

fn initial_owner_model() -> SessionModel {
    let authority = authority(1);
    let request = request();
    let transport = binding(0x41);
    let proof = credentials().prove(&request, &transport).unwrap();
    let initial = authority
        .verify_and_commit(&request, &transport, &proof)
        .unwrap();
    SessionModel::new(SessionRole::Owner, session_config(), initial)
}

fn attach(request: AttachRequest, transport: AttachTransportBinding) -> Frame {
    let proof = credentials().prove(&request, &transport).unwrap();
    request.to_attach_frame(proof)
}

fn accepted(request: AttachRequest) -> Frame {
    Frame::try_new(
        request.requested_generation(),
        Record::AttachAccepted {
            session_id: request.session_id(),
            nonce: request.nonce(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: FeatureSet::new(request.features().offered()),
        },
    )
    .unwrap()
}

fn generation_status(request: AttachRequest, current_generation: u64) -> Frame {
    Frame::try_new(
        LegGeneration::new(current_generation).unwrap(),
        Record::AttachGenerationStatus {
            session_id: request.session_id(),
            requested_generation: request.requested_generation(),
            nonce: request.nonce(),
        },
    )
    .unwrap()
}

#[test]
fn owner_commit_returns_same_leg_attached_capability_and_correlated_acceptance() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let established = EstablishedLeg::for_authenticated_transport(transport);
    let request = request_for(3, 0x33);
    let received = established.bind_received_frame(attach(request, transport));

    let OwnerAttachTransaction::Installed {
        attached,
        acceptance,
        recovery,
    } = established
        .transact_owner_attach_for_provenance_test(received, &authority, &mut model)
        .unwrap()
        .unwrap()
    else {
        panic!("exact next generation changed owner attach outcome");
    };

    assert_eq!(attached.generation(), request.requested_generation());
    assert_eq!(attached.nonce(), request.nonce());
    assert_eq!(attached.transport_binding(), transport);
    let SessionEvent::ReplacementAttached { leg } = attached.replacement_attached_event() else {
        panic!("owner must be able to install the attached leg before sending acceptance");
    };
    assert_eq!(leg.generation(), request.requested_generation());
    assert_eq!(acceptance, accepted(request));
    assert_eq!(authority.current_generation(), 3);
    assert_eq!(model.snapshot().generation().get(), 3);
    assert!(matches!(
        recovery.first(),
        Some(resumable::SessionEffect::LegActivated { generation }) if generation.get() == 3
    ));
}

#[test]
fn owner_attach_rejects_a_frame_bound_by_another_identical_leg_before_commit() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let leg_a = EstablishedLeg::for_authenticated_transport(transport);
    let leg_b = EstablishedLeg::for_authenticated_transport(transport);
    let request = request_for(3, 0x33);
    let received_on_a = leg_a.bind_received_frame(attach(request, transport));

    assert!(matches!(
        leg_b.transact_owner_attach_for_provenance_test(received_on_a, &authority, &mut model),
        Err(LegProvenanceError::WrongLeg)
    ));
    assert_eq!(authority.current_generation(), 2);
    assert_eq!(model.snapshot().generation().get(), 2);
}

#[test]
fn owner_attach_rejects_data_and_non_attach_records_without_committing() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let leg = EstablishedLeg::for_authenticated_transport(transport);

    for record in [
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"not-an-attach"),
        },
        Record::Ack {
            flow_id: SessionFlowId::new(8).unwrap(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        },
        Record::AttachAccepted {
            session_id: request().session_id(),
            nonce: request().nonce(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: FeatureSet::new(0b111),
        },
    ] {
        let received = leg
            .bind_received_frame(Frame::try_new(LegGeneration::new(3).unwrap(), record).unwrap());
        assert!(matches!(
            leg.transact_owner_attach_for_provenance_test(received, &authority, &mut model),
            Err(LegProvenanceError::Rejected)
        ));
        assert_eq!(authority.current_generation(), 2);
        assert_eq!(model.snapshot().generation().get(), 2);
    }
}

#[test]
fn owner_stale_attach_returns_only_correlated_generation_status() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let leg = EstablishedLeg::for_authenticated_transport(transport);
    let request = request();
    let received = leg.bind_received_frame(attach(request, transport));

    let OwnerAttachTransaction::Resynchronize { status } = leg
        .transact_owner_attach_for_provenance_test(received, &authority, &mut model)
        .unwrap()
        .unwrap()
    else {
        panic!("stale generation unexpectedly minted an attached capability");
    };

    assert_eq!(status, generation_status(request, 2));
    assert_eq!(authority.current_generation(), 2);
}

#[test]
fn simultaneous_next_generation_attaches_mint_one_capability_and_one_status() {
    let authority = authority(2);
    let mut model_a = initial_owner_model();
    let mut model_b = initial_owner_model();
    let request_a = request_for(3, 0x32);
    let request_b = request_for(3, 0x33);
    let binding_a = binding(0x42);
    let binding_b = binding(0x43);
    let leg_a = EstablishedLeg::for_authenticated_transport(binding_a);
    let leg_b = EstablishedLeg::for_authenticated_transport(binding_b);
    let received_a = leg_a.bind_received_frame(attach(request_a, binding_a));
    let received_b = leg_b.bind_received_frame(attach(request_b, binding_b));

    let (outcome_a, outcome_b) = std::thread::scope(|scope| {
        let attempt_a = scope.spawn(|| {
            leg_a
                .transact_owner_attach_for_provenance_test(received_a, &authority, &mut model_a)
                .unwrap()
                .unwrap()
        });
        let attempt_b = scope.spawn(|| {
            leg_b
                .transact_owner_attach_for_provenance_test(received_b, &authority, &mut model_b)
                .unwrap()
                .unwrap()
        });
        (attempt_a.join().unwrap(), attempt_b.join().unwrap())
    });

    let mut acceptance = None;
    let mut status = None;
    for outcome in [outcome_a, outcome_b] {
        match outcome {
            OwnerAttachTransaction::Installed {
                attached,
                acceptance: frame,
                recovery: _,
            } => {
                assert!(acceptance.replace(frame).is_none());
                assert_eq!(attached.generation(), LegGeneration::new(3).unwrap());
            }
            OwnerAttachTransaction::Resynchronize { status: frame } => {
                assert!(status.replace(frame).is_none());
            }
        }
    }

    let acceptance = acceptance.expect("one concurrent attach must commit");
    let status = status.expect("the concurrent loser must receive status only");
    assert!(
        (acceptance == accepted(request_a) && status == generation_status(request_b, 3))
            || (acceptance == accepted(request_b) && status == generation_status(request_a, 3))
    );
    assert_eq!(authority.current_generation(), 3);
    assert_eq!(
        [
            model_a.snapshot().generation().get(),
            model_b.snapshot().generation().get()
        ]
        .into_iter()
        .filter(|generation| *generation == 3)
        .count(),
        1
    );
}

#[test]
fn owner_attach_outcomes_redact_capability_and_response_state() {
    let transport = binding(0x42);

    let committed_authority = authority(2);
    let mut committed_model = initial_owner_model();
    let committed_leg = EstablishedLeg::for_authenticated_transport(transport);
    let committed_request = request_for(3, 0x33);
    let committed = committed_leg
        .transact_owner_attach_for_provenance_test(
            committed_leg.bind_received_frame(attach(committed_request, transport)),
            &committed_authority,
            &mut committed_model,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        format!("{committed:?}"),
        "OwnerAttachTransaction::Installed([REDACTED])"
    );

    let stale_authority = authority(2);
    let mut stale_model = initial_owner_model();
    let stale_leg = EstablishedLeg::for_authenticated_transport(transport);
    let stale = stale_leg
        .transact_owner_attach_for_provenance_test(
            stale_leg.bind_received_frame(attach(request(), transport)),
            &stale_authority,
            &mut stale_model,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        format!("{stale:?}"),
        "OwnerAttachTransaction::Resynchronize([REDACTED])"
    );
}

#[test]
fn same_authenticated_leg_accepts_its_correlated_response() {
    let leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();
    let pending = leg.begin_attach(request);
    let response = leg.bind_received_frame(accepted(request));

    let AttachResponse::Accepted(attached) = pending.validate_response(response).unwrap() else {
        panic!("correlated ATTACH_ACCEPTED changed response kind");
    };
    assert_eq!(attached.generation(), request.requested_generation());
    assert_eq!(attached.nonce(), request.nonce());
    assert_eq!(attached.transport_binding(), binding(0x42));
    assert_eq!(format!("{attached:?}"), "AttachedLeg([REDACTED])");

    let SessionEvent::ReplacementAttached { leg } = attached.replacement_attached_event() else {
        panic!("attached leg must mint its exact replacement event");
    };
    assert_eq!(leg.generation(), request.requested_generation());
}

#[test]
fn attached_data_plane_rejects_frames_bound_to_another_identical_transport() {
    let leg_a = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let leg_b = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();
    let AttachResponse::Accepted(attached_a) = leg_a
        .begin_attach(request)
        .validate_response(leg_a.bind_received_frame(accepted(request)))
        .unwrap()
    else {
        panic!("correlated ATTACH_ACCEPTED changed response kind");
    };

    for record in [
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"leg-bound-data"),
        },
        Record::Close {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            final_offset: ByteOffset::new(14),
        },
        Record::Reset {
            flow_id: SessionFlowId::new(7).unwrap(),
            reason: resumable::ResetReason::TargetFailure,
        },
    ] {
        let frame = Frame::try_new(request.requested_generation(), record).unwrap();
        let encoded = frame.encode().unwrap();
        let from_b = leg_b.bind_received_frame(Frame::decode_owned_exact(encoded.clone()).unwrap());
        assert!(matches!(
            attached_a.accept_frame(from_b),
            Err(LegProvenanceError::WrongLeg)
        ));

        let from_a = leg_a.bind_received_frame(Frame::decode_owned_exact(encoded).unwrap());
        let SessionEvent::PeerFrame { leg, frame } = attached_a.accept_frame(from_a).unwrap()
        else {
            panic!("attached leg must mint only a peer-frame event");
        };
        assert_eq!(leg.generation(), request.requested_generation());
        assert_eq!(frame.leg_generation(), request.requested_generation());
    }
}

#[test]
fn same_authenticated_leg_accepts_its_correlated_generation_status() {
    let leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();
    let pending = leg.begin_attach(request);
    let response = leg.bind_received_frame(generation_status(request, 4));

    let AttachResponse::GenerationStatus(status) = pending.validate_response(response).unwrap()
    else {
        panic!("correlated generation status changed response kind");
    };
    assert_eq!(status.current_generation(), LegGeneration::new(4).unwrap());
    assert_eq!(
        status.requested_generation(),
        request.requested_generation()
    );
    assert_eq!(status.transport_binding(), binding(0x42));
}

#[test]
fn status_and_followup_acceptance_retain_both_exact_leg_seals_for_catch_up() {
    let status_leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let followup_leg = EstablishedLeg::for_authenticated_transport(binding(0x43));
    let wrong_followup_leg = EstablishedLeg::for_authenticated_transport(binding(0x43));
    let original = request();
    let pending = status_leg.begin_attach(original);
    let status_response = status_leg.bind_received_frame(generation_status(original, 4));
    let AttachResponse::GenerationStatus(status) =
        pending.validate_response(status_response).unwrap()
    else {
        panic!("correlated generation status changed response kind");
    };

    let pending_catch_up = status
        .begin_catch_up(&followup_leg, AttachNonce::new([0x55; 16]).unwrap())
        .unwrap();
    let followup = pending_catch_up.request();
    assert_eq!(followup.requested_generation().get(), 5);
    let encoded = accepted(followup).encode().unwrap();
    let wrong_response =
        wrong_followup_leg.bind_received_frame(Frame::decode_owned_exact(encoded.clone()).unwrap());
    assert!(matches!(
        pending_catch_up.validate_response(wrong_response),
        Err(LegProvenanceError::WrongLeg)
    ));

    let AttachResponse::GenerationStatus(status) = status_leg
        .begin_attach(original)
        .validate_response(status_leg.bind_received_frame(generation_status(original, 4)))
        .unwrap()
    else {
        panic!("correlated generation status changed response kind");
    };
    let pending_catch_up = status
        .begin_catch_up(&followup_leg, AttachNonce::new([0x56; 16]).unwrap())
        .unwrap();
    let followup = pending_catch_up.request();
    let response = followup_leg.bind_received_frame(accepted(followup));
    let caught_up = pending_catch_up.validate_response(response).unwrap();
    assert_eq!(caught_up.generation().get(), 5);
    assert!(matches!(
        caught_up.replacement_caught_up_event(),
        SessionEvent::ReplacementCaughtUp { .. }
    ));

    let data = Frame::try_new(
        LegGeneration::new(5).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"catch-up-leg-bound"),
        },
    )
    .unwrap();
    let from_status_leg = status_leg.bind_received_frame(data.clone());
    assert!(matches!(
        caught_up.accept_frame(from_status_leg),
        Err(LegProvenanceError::WrongLeg)
    ));
    let from_followup_leg = followup_leg.bind_received_frame(data);
    assert!(matches!(
        caught_up.accept_frame(from_followup_leg),
        Ok(SessionEvent::PeerFrame { .. })
    ));
}

#[test]
fn byte_identical_responses_from_another_authenticated_leg_are_rejected_first() {
    let leg_a = EstablishedLeg::for_authenticated_transport(binding(0x42));
    // Use the same authenticated facts deliberately: binding equality must
    // not collapse two independent TLS connections into one provenance.
    let leg_b = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();

    for frame in [accepted(request), generation_status(request, 4)] {
        let encoded = frame.encode().unwrap();
        let decoded = Frame::decode_owned_exact(encoded.clone()).unwrap();
        assert_eq!(decoded.encode().unwrap(), encoded);

        let pending = leg_a.begin_attach(request);
        let response_from_b = leg_b.bind_received_frame(decoded);
        assert!(matches!(
            pending.validate_response(response_from_b),
            Err(LegProvenanceError::WrongLeg)
        ));
    }
}

#[test]
fn typed_leg_values_redact_transport_and_attach_state_from_debug() {
    let leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    assert_eq!(format!("{leg:?}"), "EstablishedLeg([REDACTED])");

    let request = request();
    let pending = leg.begin_attach(request);
    assert_eq!(format!("{pending:?}"), "PendingAttach([REDACTED])");

    let response = leg.bind_received_frame(accepted(request));
    assert_eq!(format!("{response:?}"), "LegBoundFrame([REDACTED])");
    let outcome = pending.validate_response(response).unwrap();
    assert_eq!(
        format!("{outcome:?}"),
        "AttachResponse::Accepted([REDACTED])"
    );
}
