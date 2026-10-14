//! Real source/pair/selected-receipt primitive tests for the Bound service.
//!
//! Fixtures construct actual authenticated Owner state and original TCP ports.
//! These tests do not impersonate the checked Controller/physical Root or prove
//! its service/reducer wiring, runtime fairness or whole-session settlement.

use super::super::*;
use crate::owned_upstream::session::SessionMailboxLane;
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::owned_upstream::tcp::{
    DriverInput, FlowPortConfig, ResumableTcpFlow, ResumableTcpPortFactory,
};
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, ByteOffset, CommittedLeg, DevicePrincipal, DeviceSecret, Direction,
    FeatureOffer, Frame, LegGeneration, LocalFlow, OwnerIdentity, ReceiveBudgetLimits, Record,
    ReplayBudgetLimits, ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig,
    SessionEffect, SessionFlowId, SessionId, SessionModel, SessionRole, TcpWindowLimits,
    TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;
use bytes::Bytes;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

mod flow_lifecycle;
mod retirement;

struct SourceFixture {
    supervisor: SessionSupervisor,
    factory: ResumableTcpPortFactory,
    leg: CommittedLeg,
    local: LocalFlow,
    flow: ResumableTcpFlow,
    driver: ResumableTcpDriver,
}

fn source_leg() -> CommittedLeg {
    let sid = SessionId::new([0x91; 16]).unwrap();
    let owner = OwnerIdentity::new([0x92; 32]).unwrap();
    let principal = DevicePrincipal::new([0x93; 16]).unwrap();
    let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
    let binding = AttachTransportBinding::new(
        owner,
        alpn,
        TlsExporterBinding::new([0x94; 32]).unwrap(),
        principal,
    );
    let credentials = AttachCredentials::new(
        DeviceSecret::new([0x95; 32]).unwrap(),
        ResumeSecret::new([0x96; 32]).unwrap(),
    )
    .unwrap();
    let request = AttachRequest::new(
        sid,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x97; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b11, 0b01).unwrap(),
    );
    let proof = credentials.prove(&request, &binding).unwrap();
    AttachAuthority::new(
        sid,
        LegGeneration::new(1).unwrap(),
        credentials,
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b11).unwrap(),
    )
    .verify_and_commit(&request, &binding, &proof)
    .unwrap()
}

fn source_fixture() -> SourceFixture {
    let leg = source_leg();
    let config = SessionConfig::new(
        4,
        8,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(128, 16).unwrap(),
        64,
    )
    .unwrap();
    let mut supervisor = SessionSupervisor::new(SessionModel::new(SessionRole::Owner, config, leg));
    let factory = supervisor
        .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
        .unwrap();
    let effects = supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (request, local) = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, flow, .. } => Some((request, flow)),
            _ => None,
        })
        .unwrap();
    supervisor
        .apply_event(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::Opened,
        })
        .unwrap();
    let (flow, driver) = factory.open_flow(local).unwrap();
    SourceFixture {
        supervisor,
        factory,
        leg,
        local,
        flow,
        driver,
    }
}

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn selected_inbox_keeps_other_lane_and_original_driver_permits() {
    let mut source = source_fixture();
    let PreparedSessionMailbox {
        mut commands,
        mut inbox,
    } = SessionCommands::prepare_pair(1, 1).unwrap();
    let ordered_wake = Arc::new(CountWake::default());
    let control_wake = Arc::new(CountWake::default());
    let ordered_waker = Waker::from(Arc::clone(&ordered_wake));
    let control_waker = Waker::from(Arc::clone(&control_wake));
    let mut ordered_cx = Context::from_waker(&ordered_waker);
    let mut control_cx = Context::from_waker(&control_waker);
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut ordered_cx)
            .is_pending()
    );
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::Control, &mut control_cx)
            .is_pending()
    );
    let mut cx = Context::from_waker(Waker::noop());
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    assert_eq!(
        commands.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut ordered_cx)
            .is_pending()
    );
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);
    let transferred = commands
        .visit_selected_lane(SendLane::OrderedSource, &mut cx)
        .unwrap();
    assert_eq!(
        transferred.work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    assert_eq!(ordered_wake.0.load(Ordering::SeqCst), 1);
    assert_eq!(control_wake.0.load(Ordering::SeqCst), 0);
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    assert_eq!(
        commands.poll_driver_lane(&mut source.driver, SendLane::Control, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(
        commands
            .visit_selected_lane(SendLane::Control, &mut cx)
            .unwrap()
            .work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    let Poll::Ready(Some(received)) =
        inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut ordered_cx)
    else {
        panic!("selected real DATA must be retained");
    };
    let SessionOwnerCommand::Driver(DriverInput::Data(data)) = &received else {
        panic!("Control must not replace selected DATA");
    };
    assert_eq!(data.flow(), source.local);
    assert_eq!(data.payload(), b"data");
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut ordered_cx)
            .is_pending()
    );
    let effects = source.supervisor.apply_command(received).unwrap();
    assert!(effects.iter().any(|effect| matches!(effect, SessionEffect::Transmit(frame) if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"data"))));
    source
        .supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg: source.leg,
            frame: Frame::try_new(
                source.leg.generation(),
                Record::Ack {
                    flow_id: source.local.flow_id(),
                    direction: Direction::TargetToClient,
                    next_accepted: ByteOffset::new(4),
                    final_accepted: false,
                },
            )
            .unwrap(),
        })
        .unwrap();
    assert_eq!(source.factory.session_uplink_owned_bytes(), 0);
    assert_eq!(source.factory.session_uplink_owned_segments(), 0);
    let Poll::Ready(Some(control)) =
        inbox.poll_selected(SessionMailboxLane::Control, &mut control_cx)
    else {
        panic!("other lane still owns the original RESET");
    };
    assert!(
        matches!(&control, SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { flow, reason: ResetReason::TargetFailure })) if *flow == source.local)
    );
    source.supervisor.apply_command(control).unwrap();
    assert_eq!(control_wake.0.load(Ordering::SeqCst), 1);
    // Only source/replay permits settled here, not a physical-root lifecycle.
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        panic!("capacity observer interrupted after prefetch commit");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn selected_receipt_survives_capacity_unwind_without_second_epoch() {
    let mut source = source_fixture();
    let PreparedSessionMailbox {
        mut commands,
        mut inbox,
    } = SessionCommands::prepare_pair(1, 1).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for payload in [b"one!", b"two!"] {
        source
            .flow
            .try_send_uplink_with(4, || Bytes::from_static(payload))
            .unwrap();
        assert_eq!(
            commands.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut cx),
            Poll::Ready(OwnerLaneAdmission::Admitted)
        );
        let work = commands
            .visit_selected_lane(SendLane::OrderedSource, &mut cx)
            .unwrap()
            .work;
        assert_eq!(
            work,
            OwnerReadyUnit::Transfer(if payload == b"one!" {
                OwnerReadyDrive::MailboxOwned
            } else {
                OwnerReadyDrive::Full
            })
        );
    }
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    assert_eq!(
        commands.poll_driver_lane(&mut source.driver, SendLane::Control, &mut cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(
        commands
            .visit_selected_lane(SendLane::Control, &mut cx)
            .unwrap()
            .work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut panic_cx = Context::from_waker(&panic_waker);
    {
        let mut wait = std::pin::pin!(commands.rearm_full_commands());
        assert!(wait.as_mut().poll(&mut panic_cx).is_pending());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _received = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx);
            }))
            .is_err()
        );
    }
    assert_eq!(source.factory.session_uplink_owned_bytes(), 8);
    assert_eq!(source.factory.session_uplink_owned_segments(), 2);
    assert_eq!(
        commands.rearm_full_commands().await,
        OwnerCapacityRearm::Rearmed
    );
    // Select the other lane while the interrupted Ordered prefetch still
    // exists. A mixed "any prefetch first" implementation must fail here.
    let Poll::Ready(Some(control)) = inbox.poll_selected(SessionMailboxLane::Control, &mut cx)
    else {
        panic!("the other lane remains owned independently");
    };
    assert!(
        matches!(&control, SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { flow, .. })) if *flow == source.local)
    );
    assert_eq!(
        commands.capacity.changed().await,
        SessionMailboxCapacityChange::Control
    );
    let Poll::Ready(Some(first)) = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("the original prefetch survives capacity notification unwind");
    };
    assert!(
        matches!(&first, SessionOwnerCommand::Driver(DriverInput::Data(data)) if data.flow() == source.local && data.payload() == b"one!")
    );
    {
        let mut no_second_epoch = std::pin::pin!(commands.capacity.changed());
        assert!(no_second_epoch.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(
        commands
            .visit_selected_lane(SendLane::OrderedSource, &mut cx)
            .unwrap()
            .work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    let Poll::Ready(Some(second)) = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("the same returned Full command transfers after the real capacity edge");
    };
    assert!(
        matches!(&second, SessionOwnerCommand::Driver(DriverInput::Data(data)) if data.flow() == source.local && data.payload() == b"two!")
    );
    source.supervisor.apply_command(first).unwrap();
    source.supervisor.apply_command(second).unwrap();
    source
        .supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg: source.leg,
            frame: Frame::try_new(
                source.leg.generation(),
                Record::Ack {
                    flow_id: source.local.flow_id(),
                    direction: Direction::TargetToClient,
                    next_accepted: ByteOffset::new(8),
                    final_accepted: false,
                },
            )
            .unwrap(),
        })
        .unwrap();
    assert_eq!(source.factory.session_uplink_owned_bytes(), 0);
    assert_eq!(source.factory.session_uplink_owned_segments(), 0);
    source.supervisor.apply_command(control).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn selected_primitives_register_and_receive_after_cooperative_budget_exhaustion() {
    let mut source = source_fixture();
    let PreparedSessionMailbox {
        mut commands,
        mut inbox,
    } = SessionCommands::prepare_pair(1, 1).unwrap();
    let source_wake = Arc::new(CountWake::default());
    let inbox_wake = Arc::new(CountWake::default());
    let source_waker = Waker::from(Arc::clone(&source_wake));
    let inbox_waker = Waker::from(Arc::clone(&inbox_wake));
    let mut source_cx = Context::from_waker(&source_waker);
    let mut inbox_cx = Context::from_waker(&inbox_waker);
    // Exhaust the real current task's allowance, without yielding the final
    // exhausted poll. Root fairness is unchanged; only raw primitives opt out.
    for _ in 0..256 {
        if !tokio::task::coop::has_budget_remaining() {
            break;
        }
        tokio::task::consume_budget().await;
    }
    assert!(!tokio::task::coop::has_budget_remaining());
    assert!(
        source
            .driver
            .poll_owner_ordered_source(&mut source_cx)
            .is_pending()
    );
    assert!(
        source
            .driver
            .poll_owner_control(&mut source_cx)
            .is_pending()
    );
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut inbox_cx)
            .is_pending()
    );
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::Control, &mut inbox_cx)
            .is_pending()
    );
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    source
        .flow
        .try_send_reset(ResetReason::TargetFailure)
        .unwrap();
    assert_eq!(source_wake.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        commands.poll_driver_lane(&mut source.driver, SendLane::OrderedSource, &mut source_cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(
        commands.poll_driver_lane(&mut source.driver, SendLane::Control, &mut source_cx),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    for lane in [SendLane::OrderedSource, SendLane::Control] {
        assert_eq!(
            commands
                .visit_selected_lane(lane, &mut source_cx)
                .unwrap()
                .work,
            OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
        );
    }
    assert_eq!(inbox_wake.0.load(Ordering::SeqCst), 2);
    let Poll::Ready(Some(data)) =
        inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut inbox_cx)
    else {
        panic!("real DATA must not become cooperative Pending");
    };
    let Poll::Ready(Some(reset)) = inbox.poll_selected(SessionMailboxLane::Control, &mut inbox_cx)
    else {
        panic!("real Control must not become cooperative Pending");
    };
    assert!(
        matches!(&data, SessionOwnerCommand::Driver(DriverInput::Data(data)) if data.flow() == source.local && data.payload() == b"data")
    );
    assert!(
        matches!(&reset, SessionOwnerCommand::Driver(DriverInput::Control(SessionEvent::LocalReset { flow, .. })) if *flow == source.local)
    );
    assert!(!tokio::task::coop::has_budget_remaining());
    // Keep real dispositions through this primitive observation; this test
    // does not call them a completed Controller or physical-root transaction.
    source.supervisor.apply_command(data).unwrap();
    source.supervisor.apply_command(reset).unwrap();
}

#[tokio::test]
async fn bound_driver_pull_preflights_transit_and_keeps_exact_command_until_transfer() {
    let mut source = source_fixture();
    let PreparedSessionMailbox {
        mut commands,
        mut inbox,
    } = SessionCommands::prepare_pair(1, 1).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    let mut transit = usize::MAX;
    assert_eq!(
        commands.pull_bound_driver_lane(
            &mut source.driver,
            SendLane::OrderedSource,
            &mut transit,
            &mut cx
        ),
        Poll::Ready(OwnerLaneAdmission::TransitExhausted)
    );
    assert_eq!(transit, usize::MAX);
    assert!(matches!(commands.cells[1], SendCell::Vacant));
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
            .is_pending()
    );
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);

    transit = 3;
    assert_eq!(
        commands.pull_bound_driver_lane(
            &mut source.driver,
            SendLane::OrderedSource,
            &mut transit,
            &mut cx
        ),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    assert_eq!(transit, 4);
    assert!(matches!(&commands.cells[1], SendCell::Held { packet, .. }
        if matches!(&packet.storage, CommandStorage::Fresh(SessionOwnerCommand::Driver(DriverInput::Data(data)))
            if data.flow() == source.local && data.payload() == b"data")));
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
            .is_pending()
    );
    assert_eq!(
        commands.pull_bound_driver_lane(
            &mut source.driver,
            SendLane::OrderedSource,
            &mut transit,
            &mut cx
        ),
        Poll::Ready(OwnerLaneAdmission::Occupied)
    );
    assert_eq!(transit, 4);
    assert_eq!(
        commands
            .visit_selected_lane(SendLane::OrderedSource, &mut cx)
            .unwrap()
            .work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    assert_eq!(
        transit, 4,
        "mailbox transfer is not Controller receipt settlement"
    );
    let Poll::Ready(Some(command)) =
        inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("same real DATA must reach the paired inbox");
    };
    assert!(
        matches!(&command, SessionOwnerCommand::Driver(DriverInput::Data(data))
        if data.flow() == source.local && data.payload() == b"data")
    );
    source.supervisor.apply_command(command).unwrap();
    assert!(
        commands
            .pull_bound_driver_lane(
                &mut source.driver,
                SendLane::OrderedSource,
                &mut transit,
                &mut cx
            )
            .is_pending()
    );
    assert_eq!(transit, 4, "genuine Pending never adds a transit charge");
}

#[tokio::test]
async fn bound_held_data_rejects_equal_ids_with_foreign_local_authority() {
    let mut source = source_fixture();
    let PreparedSessionMailbox {
        mut commands,
        mut inbox,
    } = SessionCommands::prepare_pair(1, 1).unwrap();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    let mut transit = 0;
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        commands.pull_bound_driver_lane(
            &mut source.driver,
            SendLane::OrderedSource,
            &mut transit,
            &mut cx
        ),
        Poll::Ready(OwnerLaneAdmission::Admitted)
    );
    let SendCell::Held { packet, .. } = &mut commands.cells[1] else {
        panic!("actual DATA custody");
    };
    // Adversarial metadata mutation only; the actual DATA and its permits are
    // untouched. Equal numeric IDs are not an exact LocalFlow association.
    packet.origin = CommandOrigin::Driver(
        SendLane::OrderedSource,
        LocalFlow::for_adapter_test(
            source.local.session_id(),
            source.local.flow_id(),
            source.local.direction(),
            source.local.authority_id().checked_add(1).unwrap(),
        ),
    );
    assert!(
        commands
            .visit_selected_lane(SendLane::OrderedSource, &mut cx)
            .is_err()
    );
    assert_eq!(transit, 1);
    assert_eq!(source.factory.session_uplink_owned_bytes(), 4);
    assert!(matches!(&commands.cells[1], SendCell::Held { packet, .. }
        if matches!(&packet.storage, CommandStorage::Fresh(SessionOwnerCommand::Driver(DriverInput::Data(data)))
            if data.flow() == source.local && data.payload() == b"data")));
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
            .is_pending()
    );
    assert!(
        commands.quarantined,
        "corruption retains command, never a false empty cell"
    );
}

#[test]
fn selected_driver_metadata_preserves_wake_during_pending_clear() {
    use crate::owned_upstream::owner_ready::{
        OwnerReadyFlowAssociation, OwnerReadyFlowImport, OwnerReadySessionLimits, ReadyGeometry,
        ReadyIndex,
    };
    use std::num::NonZeroUsize;
    let source = source_fixture();
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index
        .reserve_session(source.local.session_id(), limits)
        .unwrap();
    let imported = OwnerReadyFlowImport {
        association: OwnerReadyFlowAssociation::Source(source.local),
        target_present: false,
        read_interest: false,
        write_interest: false,
        join_interest: false,
        completion: false,
        pending_open: false,
        retiring: false,
    };
    let prepared = reservation.prepare_flow_backing(1, &[imported]).unwrap();
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("original metadata binding"),
    };
    installed
        .binding
        .activate(&mut index, 0, &installed.session, 0b10)
        .unwrap();
    let waker = installed.tickets[0]
        .driver_waker(SendLane::OrderedSource)
        .unwrap();
    for dirty in [true, false] {
        let claim = index.claim(0).unwrap();
        let turn = installed.binding.begin_publication().unwrap();
        let selected = installed
            .binding
            .select_driver_source(SendLane::OrderedSource, true)
            .unwrap();
        let candidate = selected.candidate.as_ref().expect("same due actual Flow");
        assert_eq!(candidate.position, 0);
        assert_eq!(
            candidate.association,
            OwnerReadyFlowAssociation::Source(source.local)
        );
        if dirty {
            waker.wake_by_ref();
        }
        // This is only metadata clear/recheck characterization, not a fake
        // physical Driver result or proof of the outer Bound running path.
        let runnable = installed
            .binding
            .finish_driver_source(&selected, false, 0b10)
            .unwrap();
        assert_eq!(runnable, if dirty { 0b10 } else { 0 });
        let completed = SessionCommandVisit {
            work: OwnerReadyUnit::Waiting,
            runnable,
            blocked: Some(0),
        };
        assert!(turn.finish(&completed).is_ok());
        index
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
    }
    assert_eq!(index.select(3).unwrap(), None);
}
