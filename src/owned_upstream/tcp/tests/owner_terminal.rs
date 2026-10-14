//! Owner-only terminal source refusal at the original TCP authority.
//! Uses real Supervisor mint/Open/terminal and actual port resources; only the
//! private admission mutex is fault-injected. No TUN/root retirement is claimed.

use super::*;
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::resumable::TerminalGrace;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct OwnerPort {
    supervisor: SessionSupervisor,
    leg: crate::resumable::CommittedLeg,
    factory: ResumableTcpPortFactory,
    source: ResumableTcpFlow,
    driver: ResumableTcpDriver,
}

impl OwnerPort {
    fn new(session_byte: u8) -> Self {
        let leg = test_leg_for_session(session_byte);
        let mut supervisor = SessionSupervisor::new(SessionModel::new(
            SessionRole::Owner,
            test_session_config(),
            leg,
        ));
        let factory = supervisor
            .mint_tcp_port_factory(port_config(64, 4, 4, 4))
            .unwrap();
        let effects = supervisor
            .apply_event(SessionEvent::PeerFrame {
                leg,
                frame: Frame::try_new(
                    leg.generation(),
                    Record::Open {
                        flow_id: SessionFlowId::new(1).unwrap(),
                        target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::LOCALHOST,
                            443,
                        ))),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        let (request, flow) = effects
            .iter()
            .find_map(|effect| match effect {
                SessionEffect::PeerOpenRequested { request, flow, .. } => Some((*request, *flow)),
                _ => None,
            })
            .unwrap();
        supervisor
            .apply_event(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::Opened,
            })
            .unwrap();
        let (source, driver) = factory.open_flow(flow).unwrap();
        Self {
            supervisor,
            leg,
            factory,
            source,
            driver,
        }
    }

    fn terminal(&mut self) -> TerminalGrace {
        self.supervisor
            .apply_event(SessionEvent::LocalReset {
                flow: self.driver.local_flow(),
                reason: ResetReason::LocalAbandon,
            })
            .unwrap()
            .into_iter()
            .find_map(|effect| match effect {
                SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                _ => None,
            })
            .unwrap()
    }

    fn revoke(&mut self, terminal: TerminalGrace) -> Result<(), FlowPortError> {
        self.driver
            .revoke_owner_terminal_source(&self.source, &self.factory, terminal)
    }
}

#[test]
fn poisoned_owner_admission_refuses_terminal_without_releasing_custody() {
    let mut port = OwnerPort::new(0x21);
    port.source
        .try_send_uplink_with(4, || Bytes::from_static(b"held"))
        .unwrap();
    let terminal = port.terminal();
    let state = Arc::clone(&port.source.state);
    assert!(Arc::ptr_eq(&state, &port.driver.state));
    assert!(
        state
            .origin
            .as_ref()
            .unwrap()
            .matches(port.factory.origin.as_ref().unwrap())
    );
    assert_eq!(state.snapshot().reserved_completion_slots(), 0);
    assert_eq!(state.snapshot().pending_downlink_actions(), 0);
    assert!(!state.source_reservation_active.load(Ordering::Acquire));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = state.admission_gate.lock().unwrap();
            panic!("poison actual Owner admission mutex");
        }))
        .is_err()
    );
    assert!(state.admission_gate.is_poisoned());
    let epoch = port.factory.progress_cursor().epoch;
    let result = port.revoke(terminal);
    assert_eq!(result, Err(FlowPortError::OwnerAdmissionPoisoned));
    assert!(!state.is_retired());
    assert!(!port.driver.reset_rx.is_closed());
    assert!(!port.driver.control_rx.is_closed());
    assert!(!port.driver.source_rx.is_closed());
    assert_eq!(port.driver.source_rx.len(), 1);
    assert_eq!(port.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(port.factory.session_uplink_owned_segments(), 1);
    assert_eq!(port.factory.progress_cursor().epoch, epoch);
    assert!(state.admission_gate.is_poisoned());
    assert_eq!(
        port.revoke(terminal),
        Err(FlowPortError::OwnerAdmissionPoisoned)
    );
    assert_eq!(port.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(port.driver.source_rx.len(), 1);
}

#[test]
fn owner_terminal_rejects_live_source_reservation_until_exact_return() {
    let mut port = OwnerPort::new(0x21);
    let reservation = port.source.try_reserve_uplink_source(4).unwrap();
    let terminal = port.terminal();
    let epoch = port.factory.progress_cursor().epoch;
    assert_eq!(
        port.revoke(terminal),
        Err(FlowPortError::UplinkSourceReservationActive)
    );
    assert_eq!(port.factory.progress_cursor().epoch, epoch);
    assert!(
        port.source
            .state
            .source_reservation_active
            .load(Ordering::Acquire)
    );
    assert!(!port.source.state.is_retired());
    assert!(!port.driver.source_rx.is_closed());
    assert_eq!(port.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(port.factory.session_uplink_owned_segments(), 1);
    drop(reservation);
    assert_eq!(port.factory.session_uplink_owned_bytes(), 0);
    assert_eq!(port.factory.session_uplink_owned_segments(), 0);
    assert_eq!(port.revoke(terminal), Ok(()));
    assert!(port.source.state.is_retired());
    assert!(port.driver.source_rx.is_closed());
}

#[test]
fn owner_terminal_rejects_exact_reserved_completion_until_return() {
    let mut port = OwnerPort::new(0x21);
    let completion = port.driver.reserve_completion().unwrap();
    let terminal = port.terminal();
    let epoch = port.factory.progress_cursor().epoch;
    assert_eq!(
        port.revoke(terminal),
        Err(FlowPortError::OwnerTerminalOwnershipPending {
            completions: 1,
            actions: 0,
        })
    );
    assert_eq!(port.factory.progress_cursor().epoch, epoch);
    assert!(!port.source.state.is_retired());
    assert!(!port.driver.control_rx.is_closed());
    assert_eq!(port.source.state.snapshot().reserved_completion_slots(), 1);
    assert!(completion.permit.is_some());
    assert!(Arc::ptr_eq(&completion.state, &port.driver.state));
    drop(completion);
    assert_eq!(port.source.state.snapshot().reserved_completion_slots(), 0);
    assert_eq!(port.revoke(terminal), Ok(()));
}

#[test]
fn owner_terminal_rejects_live_action_after_its_completion_is_published() {
    let mut port = OwnerPort::new(0x21);
    let completion = port
        .supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg: port.leg,
            frame: Frame::try_new(
                port.leg.generation(),
                Record::Close {
                    flow_id: port.driver.local_flow().flow_id(),
                    direction: Direction::ClientToTarget,
                    final_offset: ByteOffset::new(0),
                },
            )
            .unwrap(),
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::HalfCloseSink { completion } => Some(completion),
            _ => None,
        })
        .unwrap();
    port.driver.try_deliver_half_close(completion).unwrap();
    let terminal = port.terminal();
    let epoch = port.factory.progress_cursor().epoch;
    assert_eq!(
        port.revoke(terminal),
        Err(FlowPortError::OwnerTerminalOwnershipPending {
            completions: 1,
            actions: 1,
        })
    );
    assert_eq!(port.factory.progress_cursor().epoch, epoch);
    let Some(TunAction::HalfClose(mut delivery)) = port.source.try_recv_action().unwrap() else {
        panic!("retain original half-close delivery");
    };
    assert_eq!(delivery.capability(), completion);
    // Hold the original action across its real completion publication seam.
    // This is a TCP defensive preflight test, not a Bound TUN producer route.
    delivery.finish(FlowControl::SinkHalfClosed { completion });
    let epoch = port.factory.progress_cursor().epoch;
    assert_eq!(port.source.state.snapshot().reserved_completion_slots(), 0);
    assert_eq!(port.source.state.snapshot().pending_downlink_actions(), 1);
    assert_eq!(
        port.revoke(terminal),
        Err(FlowPortError::OwnerTerminalOwnershipPending {
            completions: 0,
            actions: 1,
        })
    );
    assert_eq!(port.factory.progress_cursor().epoch, epoch);
    assert!(delivery.is_live());
    assert!(!port.driver.control_rx.is_closed());
    assert_eq!(port.driver.control_rx.len(), 1);
    drop(delivery);
    assert_eq!(port.source.state.snapshot().pending_downlink_actions(), 0);
    assert_eq!(port.revoke(terminal), Ok(()));
    assert!(port.driver.control_rx.is_closed());
    assert_eq!(
        port.driver.control_rx.len(),
        1,
        "revoke is not completion drain"
    );
}

#[test]
fn owner_terminal_rejects_foreign_terminal_pair_and_factory_without_mutation() {
    let mut port = OwnerPort::new(0x21);
    let mut twin = OwnerPort::new(0x21);
    let mut foreign = OwnerPort::new(0x22);
    assert_eq!(port.driver.local_flow(), twin.driver.local_flow());
    assert!(
        !port
            .factory
            .origin
            .as_ref()
            .unwrap()
            .matches(twin.factory.origin.as_ref().unwrap())
    );
    port.source
        .try_send_uplink_with(4, || Bytes::from_static(b"held"))
        .unwrap();
    let terminal = port.terminal();
    let foreign_terminal = foreign.terminal();
    let epoch = port.factory.progress_cursor().epoch;
    let twin_epoch = twin.factory.progress_cursor().epoch;
    assert_eq!(
        port.revoke(foreign_terminal),
        Err(FlowPortError::CrossFlowTerminalEffect)
    );
    assert_eq!(
        port.driver
            .revoke_owner_terminal_source(&twin.source, &port.factory, terminal),
        Err(FlowPortError::CrossFlowTerminalEffect)
    );
    assert_eq!(
        port.driver
            .revoke_owner_terminal_source(&port.source, &twin.factory, terminal),
        Err(FlowPortError::CrossFlowTerminalEffect)
    );
    assert_eq!(port.factory.progress_cursor().epoch, epoch);
    assert_eq!(twin.factory.progress_cursor().epoch, twin_epoch);
    assert!(!port.source.state.is_retired());
    assert!(!port.driver.reset_rx.is_closed());
    assert!(!port.driver.control_rx.is_closed());
    assert!(!port.driver.source_rx.is_closed());
    assert_eq!(port.driver.source_rx.len(), 1);
    assert_eq!(port.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(port.factory.session_uplink_owned_segments(), 1);
    assert_eq!(port.revoke(terminal), Ok(()));
    assert_eq!(port.driver.source_rx.len(), 1);
    let twin_terminal = twin.terminal();
    assert_eq!(twin.revoke(twin_terminal), Ok(()));
    assert_eq!(foreign.revoke(foreign_terminal), Ok(()));
}

#[test]
fn poisoned_owner_does_not_prevent_another_owner_from_revoking_its_own_source() {
    let mut poisoned = OwnerPort::new(0x21);
    let mut healthy = OwnerPort::new(0x22);
    let poisoned_terminal = poisoned.terminal();
    let state = Arc::clone(&poisoned.source.state);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = state.admission_gate.lock().unwrap();
            panic!("poison only this Owner admission mutex");
        }))
        .is_err()
    );
    assert_eq!(
        poisoned.revoke(poisoned_terminal),
        Err(FlowPortError::OwnerAdmissionPoisoned)
    );
    healthy
        .source
        .try_send_uplink_with(4, || Bytes::from_static(b"live"))
        .unwrap();
    let healthy_terminal = healthy.terminal();
    assert_eq!(healthy.revoke(healthy_terminal), Ok(()));
    assert_eq!(healthy.driver.source_rx.len(), 1);
    assert_eq!(healthy.factory.session_uplink_owned_bytes(), 4);
    assert_eq!(healthy.factory.session_uplink_owned_segments(), 1);
    assert!(healthy.driver.source_rx.is_closed());
    assert!(state.admission_gate.is_poisoned());
    assert!(!state.is_retired());
    assert!(!poisoned.driver.source_rx.is_closed());
    assert_eq!(
        poisoned.revoke(poisoned_terminal),
        Err(FlowPortError::OwnerAdmissionPoisoned)
    );
}
