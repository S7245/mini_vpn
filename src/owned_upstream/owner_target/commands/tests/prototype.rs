//! Fixed outer-test operations over the same command owner; no Root policy.
//! Captures happen before the original outer operation. Cold-return capture
//! deliberately also accepts a rearmed Held command, unlike Full capture.

use super::super::*;
use crate::owned_upstream::owner_target::OwnerCommandPrototype;

impl OwnerCommandPrototype {
    pub(in crate::owned_upstream) fn from_open_completion(
        ingress: SessionOwnerIngress,
        request: PeerOpenRequest,
        result: OpenResultCode,
    ) -> Result<Self, SessionCommandsBuildFailure> {
        SessionCommands::from_open_completion(ingress, request, result).map(Self)
    }

    pub(in crate::owned_upstream) fn check_open_completion_admission(
        &self,
    ) -> Option<(crate::resumable::SessionId, [usize; 2])> {
        let commands = &self.0;
        if commands.quarantined
            || !commands.custody_is_known_and_valid()
            || !matches!(commands.cells[1], SendCell::Vacant)
        {
            return None;
        }
        match &commands.cells[0] {
            SendCell::Held {
                packet:
                    HeldCommand {
                        origin: CommandOrigin::OpenCompletion(request),
                        ..
                    },
                ..
            } => Some((
                request.session_id(),
                [1, usize::from(commands.cells[0].is_capacity_blocked())],
            )),
            _ => None,
        }
    }

    pub(in crate::owned_upstream) fn visit_selected_lane(
        &mut self,
        lane: SendLane,
        cx: &mut Context<'_>,
    ) -> Result<SessionCommandVisit, ()> {
        self.0.visit_selected_lane(lane, cx)
    }

    pub(in crate::owned_upstream) fn quarantine_source(&mut self) {
        self.0.quarantined = true;
    }

    pub(in crate::owned_upstream) fn drive_one_unit(&mut self) -> OwnerReadyDrive {
        self.0.drive_one_unit()
    }

    pub(in crate::owned_upstream) async fn rearm_full_commands(&mut self) -> OwnerCapacityRearm {
        self.0.rearm_full_commands().await
    }

    pub(in crate::owned_upstream) fn capture_full_control(&self) -> *const SessionOwnerCommand {
        let SendCell::Held {
            packet:
                HeldCommand {
                    storage: CommandStorage::Returned(command),
                    ..
                },
            eligibility: SendEligibility::CapacityBlocked,
        } = &self.0.cells[0]
        else {
            panic!("exact Full command must remain blocked");
        };
        command.as_ref()
    }

    pub(in crate::owned_upstream) fn capture_returned_cold_control(
        &self,
        request: PeerOpenRequest,
    ) -> *const SessionOwnerCommand {
        let SendCell::Held {
            packet:
                HeldCommand {
                    origin: CommandOrigin::OpenCompletion(origin),
                    storage: CommandStorage::Returned(command),
                },
            ..
        } = &self.0.cells[0]
        else {
            panic!("cold rejection must retain the original Full Box");
        };
        assert_eq!(*origin, request);
        assert!(matches!(command.as_ref(),
            SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
                request: actual, result: OpenResultCode::TargetRefused,
            }) if *actual == request));
        self.ordered_vacant();
        self.command_quarantined(false);
        command.as_ref()
    }

    pub(in crate::owned_upstream) fn held_open_completion(&self, request: PeerOpenRequest) {
        let SendCell::Held { packet, .. } = &self.0.cells[0] else {
            panic!("original Held open completion");
        };
        assert!(
            matches!(packet.origin, CommandOrigin::OpenCompletion(origin) if origin == request)
        );
        let command = match &packet.storage {
            CommandStorage::Fresh(command) => command,
            CommandStorage::Returned(command) => command.as_ref(),
        };
        assert!(matches!(command, SessionOwnerCommand::Event(event)
        if *event == SessionEvent::PeerOpenResolved {
            request, result: OpenResultCode::TargetRefused
        }));
    }

    pub(in crate::owned_upstream) fn fresh_ready_control(&self, request: PeerOpenRequest) {
        assert!(matches!(&self.0.cells[0], SendCell::Held {
            packet: HeldCommand {
                origin: CommandOrigin::OpenCompletion(origin),
                storage: CommandStorage::Fresh(SessionOwnerCommand::Event(
                    SessionEvent::PeerOpenResolved { request: actual, result: OpenResultCode::TargetRefused }
                )),
            }, eligibility: SendEligibility::Ready,
        } if *origin == request && *actual == request));
    }

    pub(in crate::owned_upstream) fn control_ready(&self) {
        assert!(self.0.cells[0].is_ready());
    }

    pub(in crate::owned_upstream) fn control_capacity_blocked(&self) {
        assert!(self.0.cells[0].is_capacity_blocked());
    }

    pub(in crate::owned_upstream) fn unknown_control(&self, request: PeerOpenRequest) {
        assert!(matches!(&self.0.cells[0],
            SendCell::TransferringUnknown { origin: CommandOrigin::OpenCompletion(origin) }
                if *origin == request));
    }

    pub(in crate::owned_upstream) fn control_vacant(&self) {
        assert!(matches!(self.0.cells[0], SendCell::Vacant));
    }

    pub(in crate::owned_upstream) fn ordered_vacant(&self) {
        assert!(matches!(self.0.cells[1], SendCell::Vacant));
    }

    pub(in crate::owned_upstream) fn command_quarantined(&self, expected: bool) {
        assert_eq!(self.0.quarantined, expected);
    }
}
