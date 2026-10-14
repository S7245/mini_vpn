//! Test-only ACK gate for the real saved-loop physical receipt boundary.
//! Holds one exact identity, captures only actual minted receipts, and injects
//! one rejected copy on request. It cannot mint resources or clear quarantine.
use super::super::super::rearm::FlowPhysicalReceipt;
use super::super::*;

#[derive(Clone, Copy)]
enum Action {
    Hold,
    Release,
    WrongBytesOnce,
}

pub(in crate::client_tun::compatibility_runtime::flow_custody) struct ReceiptGate {
    id: FlowIdentity,
    action: Action,
    receipt: Option<FlowPhysicalReceipt>,
    calls: usize,
}

#[derive(Debug)]
pub(in crate::client_tun::compatibility_runtime) struct HeldReceiptView {
    pub(in crate::client_tun::compatibility_runtime) id: FlowIdentity,
    pub(in crate::client_tun::compatibility_runtime) disposition:
        (usize, usize, usize, usize, usize),
    pub(in crate::client_tun::compatibility_runtime) calls: usize,
    pub(in crate::client_tun::compatibility_runtime) row_held: bool,
    pub(in crate::client_tun::compatibility_runtime) quarantined: bool,
    pub(in crate::client_tun::compatibility_runtime) original_matches: bool,
    pub(in crate::client_tun::compatibility_runtime) role_joins: usize,
    pub(in crate::client_tun::compatibility_runtime) provider_settled: bool,
    pub(in crate::client_tun::compatibility_runtime) rearming: bool,
    pub(in crate::client_tun::compatibility_runtime) producer_stopped: bool,
    pub(in crate::client_tun::compatibility_runtime) other: Option<FlowIdentity>,
}

pub(in crate::client_tun::compatibility_runtime::flow_custody) fn intercept(
    opens: &mut OpenCustody,
    receipt: FlowPhysicalReceipt,
) -> Option<FlowPhysicalReceipt> {
    let Some(gate) = opens.flow.as_mut().and_then(|a| a.receipt_gate.as_mut()) else {
        return Some(receipt);
    };
    if gate.id != receipt.identity() {
        return Some(receipt);
    }
    if let Some(original) = gate.receipt {
        assert_eq!(receipt.data_disposition(), original.data_disposition());
    } else {
        gate.receipt = Some(receipt);
    }
    gate.calls = gate.calls.checked_add(1).unwrap();
    match gate.action {
        Action::Hold => None,
        Action::Release => Some(receipt),
        Action::WrongBytesOnce => {
            gate.action = Action::Release;
            Some(receipt.wrong_data_bytes())
        }
    }
}

fn sibling(opens: &OpenCustody, id: FlowIdentity) -> Option<FlowIdentity> {
    let mut other = opens
        .slots
        .iter()
        .flatten()
        .filter_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) if flow.id != id && flow.stage == FlowStage::Published => {
                Some(flow.id)
            }
            _ => None,
        });
    let found = other.next();
    assert!(
        other.next().is_none(),
        "only one healthy sibling in this fixture"
    );
    found
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn hold_published_receipt(
        &self,
        id: FlowIdentity,
    ) {
        let mut opens = self.opens();
        assert_eq!(
            opens.flow_mut(id).expect("actual selected donor").stage,
            FlowStage::Published
        );
        let accounting = opens.flow.as_mut().unwrap();
        assert!(accounting.receipt_gate.is_none());
        accounting.receipt_gate = Some(ReceiptGate {
            id,
            action: Action::Hold,
            receipt: None,
            calls: 0,
        });
    }

    pub(in crate::client_tun::compatibility_runtime) fn held_exact_receipt(
        &self,
    ) -> Option<HeldReceiptView> {
        self.receipt_observation(false)
    }

    pub(in crate::client_tun::compatibility_runtime) fn receipt_sibling(
        &self,
    ) -> Option<FlowIdentity> {
        let opens = self.opens();
        sibling(&opens, opens.flow.as_ref()?.receipt_gate.as_ref()?.id)
    }

    pub(in crate::client_tun::compatibility_runtime) fn hold_only_published_receipt(&self) {
        let mut opens = self.opens();
        let mut ids = opens
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) if flow.stage == FlowStage::Published => Some(flow.id),
                _ => None,
            });
        let id = ids.next().expect("one real published flow");
        assert!(ids.next().is_none(), "arm before the healthy second flow");
        let accounting = opens.flow.as_mut().unwrap();
        assert!(accounting.receipt_gate.is_none());
        accounting.receipt_gate = Some(ReceiptGate {
            id,
            action: Action::Hold,
            receipt: None,
            calls: 0,
        });
    }

    pub(in crate::client_tun::compatibility_runtime) fn release_held_receipt(&self) {
        if let Some(gate) = self
            .opens()
            .flow
            .as_mut()
            .and_then(|a| a.receipt_gate.as_mut())
        {
            gate.action = Action::Release;
        }
    }

    pub(in crate::client_tun::compatibility_runtime) fn corrupt_held_receipt(&self) {
        self.opens()
            .flow
            .as_mut()
            .unwrap()
            .receipt_gate
            .as_mut()
            .unwrap()
            .action = Action::WrongBytesOnce;
    }

    pub(in crate::client_tun::compatibility_runtime) fn held_receipt(
        &self,
    ) -> Option<HeldReceiptView> {
        self.receipt_observation(true)
    }

    fn receipt_observation(&self, include_sibling: bool) -> Option<HeldReceiptView> {
        let opens = self.opens();
        let gate = opens.flow.as_ref()?.receipt_gate.as_ref()?;
        let receipt = gate.receipt?;
        let flow = opens.slots[gate.id.index]
            .as_ref()
            .and_then(|slot| match &slot.phase {
                OpenPhase::Flow(flow) if flow.id == gate.id => Some(flow),
                _ => None,
            });
        let other_id = if include_sibling {
            sibling(&opens, gate.id)
        } else {
            None
        };
        Some(HeldReceiptView {
            id: gate.id,
            other: other_id,
            rearming: flow.is_some_and(|flow| flow.stage == FlowStage::Rearming),
            producer_stopped: flow.is_some_and(|flow| flow.producer_stopped),
            disposition: receipt.data_disposition(),
            calls: gate.calls,
            row_held: flow.is_some(),
            quarantined: flow.is_some_and(|flow| flow.stage == FlowStage::Quarantined),
            original_matches: flow.is_some_and(|flow| receipt.matches_flow(flow)),
            role_joins: flow
                .and_then(|flow| flow.roles.as_ref())
                .map_or(0, FlowRoles::join_count),
            provider_settled: flow
                .and_then(|flow| flow.attempt.as_ref())
                .is_some_and(|attempt| {
                    let view = attempt.view();
                    view.phase == TcpOpenPhase::Settled
                        && !view.lease_held
                        && !view.retirement_pending
                }),
        })
    }
}
