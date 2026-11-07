//! Borrows the original Connection for a synchronous local stream transaction.
//! The caller supplies its protocol clock; stream/packet state, first error and
//! close authority remain in that Connection. There is no secondary fatal flag,
//! allocation or production detached owner. Chunks reborrows this same owner.
//! Pure stream-state tests/fuzzing retain their existing detached algorithms.
//! Entry: Connection::{send_stream,recv_stream}. Tests: crate::tests/admission.
use super::{Retransmits, StreamsState};
use crate::{
    connection::{Connection, State},
    packet::SpaceId,
    Instant,
};

pub(in crate::connection) enum StreamOwner<'a> {
    Connection {
        connection: &'a mut Connection,
        now: Instant,
    },
    #[cfg(any(test, fuzzing))]
    Detached {
        streams: &'a mut StreamsState,
        pending: &'a mut Retransmits,
        conn_state: &'a State,
    },
}

impl StreamOwner<'_> {
    pub(super) fn fail(&mut self, reason: &'static str) {
        match self {
            Self::Connection { connection, now } => connection.fail_control_inventory(*now, reason),
            #[cfg(any(test, fuzzing))]
            Self::Detached { .. } => {}
        }
    }

    /// Unknown physical inventory requires the original Connection authority,
    /// even when the component's own meter field is the missing inventory.
    /// Known control/data capacity failures continue through `fail` unchanged.
    pub(super) fn fail_receive_inventory(&mut self, reason: &'static str) {
        if let Some(meter) = self.streams().native_meter() { meter.quarantine(); }
        self.fail(reason);
    }

    pub(super) fn admit(&mut self) -> bool {
        match self {
            Self::Connection { connection, now } => {
                if connection
                    .endpoint_config
                    .receive_backing_meter
                    .as_ref()
                    .is_some_and(|meter| meter.admit_precharged().is_err())
                {
                    connection
                        .fail_control_inventory(*now, "stream backing authority is quarantined");
                    return false;
                }
                true
            }
            #[cfg(any(test, fuzzing))]
            Self::Detached { .. } => true,
        }
    }

    pub(super) fn streams(&self) -> &StreamsState {
        match self {
            Self::Connection { connection, .. } => &connection.streams,
            #[cfg(any(test, fuzzing))]
            Self::Detached { streams, .. } => streams,
        }
    }

    pub(super) fn parts(&mut self) -> (&mut StreamsState, &mut Retransmits, &State) {
        match self {
            Self::Connection { connection, .. } => (
                &mut connection.streams,
                &mut connection.spaces[SpaceId::Data].pending,
                &connection.state,
            ),
            #[cfg(any(test, fuzzing))]
            Self::Detached {
                streams,
                pending,
                conn_state,
            } => (streams, pending, conn_state),
        }
    }

    pub(super) fn reborrow(&mut self) -> StreamOwner<'_> {
        match self {
            Self::Connection { connection, now } => StreamOwner::Connection {
                connection,
                now: *now,
            },
            #[cfg(any(test, fuzzing))]
            Self::Detached {
                streams,
                pending,
                conn_state,
            } => StreamOwner::Detached {
                streams,
                pending,
                conn_state,
            },
        }
    }
}
