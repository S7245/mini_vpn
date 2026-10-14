//! Fixed inspection tail for the actual single Bound resident.
//!
//! No scheduler, reducer, output drain or resource getter lives here. Each
//! operation borrows the same stored whole root and returns no authority.

use super::super::*;
use crate::resumable::SessionId;

impl OwnerServerRoot<crate::owned_upstream::target::MemoryTarget> {
    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn assert_only_bound_fifo_for_test(
        &mut self,
        session: SessionId,
        frames: usize,
        bytes: usize,
    ) {
        assert_eq!(self.residents.len(), 1);
        let resident = &mut self.residents[0];
        assert_eq!(resident.session_id, session);
        match &mut resident.owner {
            OwnerResident::Current(current) => current.assert_fifo_for_bound_test(frames, bytes),
            OwnerResident::Prototype(_) => panic!("a Prototype cannot prove real FIFO admission"),
        }
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) fn flush_only_bound_frame_for_test(
        &mut self,
        session: SessionId,
    ) {
        assert_eq!(self.residents.len(), 1);
        let resident = &mut self.residents[0];
        assert_eq!(resident.session_id, session);
        match &mut resident.owner {
            OwnerResident::Current(current) => current.flush_one_for_bound_test(),
            OwnerResident::Prototype(_) => panic!("a Prototype has no real physical output"),
        }
    }

    pub(in crate::owned_upstream::quinn_leg_composition::owner) async fn join_only_bound_current_for_test(
        &mut self,
        session: SessionId,
    ) {
        assert_eq!(self.residents.len(), 1);
        let resident = &mut self.residents[0];
        assert_eq!(resident.session_id, session);
        match &mut resident.owner {
            OwnerResident::Current(current) => current.join_for_bound_test().await,
            OwnerResident::Prototype(_) => panic!("a Prototype cannot settle a physical actor"),
        }
    }
}
