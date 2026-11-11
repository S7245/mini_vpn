//! D152 (A2) slot transport storage: a slot's current connection is either live
//! quinn storage or, once its closed-slot demand settled into Waiting, a
//! storage-free tombstone of the same identity and close evidence.
//!
//! Owns: the `Live`/`Released` representation and the one-way `release` of a
//! closed connection. Does not own: when a slot is released (Node recovery,
//! `recovery::waiting::release_parked`) or the physical attempt record (Node).
//! Invariants: only a closed connection is released; the tombstone keeps the
//! stable id, close reason, last stats and path generation, so every reader
//! sees the same dead slot; nothing reconstructs live storage from it.
//! Entry: `SlotConnection::{live, release, released}`. Tests:
//! tuic/node/tests/waiting_release.rs.
use quinn::{Connection, ConnectionError, ConnectionStats, VarInt};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(in crate::tuic) enum SlotConnection {
    Live(Connection),
    Released(Arc<Tombstone>),
}

#[derive(Debug)]
pub(in crate::tuic) struct Tombstone {
    stable_id: usize,
    reason: ConnectionError,
    stats: ConnectionStats,
    path_generation: u64,
}

impl From<Connection> for SlotConnection {
    fn from(connection: Connection) -> Self {
        Self::Live(connection)
    }
}

impl SlotConnection {
    pub(in crate::tuic) fn live(&self) -> Option<&Connection> {
        match self {
            Self::Live(connection) => Some(connection),
            Self::Released(_) => None,
        }
    }

    /// A live clone for I/O; a released slot is a closed slot.
    pub(in crate::tuic) fn connection(&self) -> Result<Connection, crate::shared::ClientError> {
        self.live().cloned().ok_or_else(|| {
            crate::shared::ClientError::Io(std::io::Error::other("tuic slot connection released"))
        })
    }

    pub(in crate::tuic) fn released(&self) -> bool {
        matches!(self, Self::Released(_))
    }

    /// Replace closed live storage by its tombstone and return the storage so
    /// the caller drops it outside the slot lock. A live connection stays.
    pub(in crate::tuic) fn release(&mut self) -> Option<Connection> {
        let Self::Live(connection) = self else {
            return None;
        };
        let reason = connection.close_reason()?;
        let tombstone = Tombstone {
            stable_id: connection.stable_id(),
            reason,
            stats: connection.stats(),
            path_generation: connection.current_path_generation(),
        };
        match std::mem::replace(self, Self::Released(Arc::new(tombstone))) {
            Self::Live(connection) => Some(connection),
            Self::Released(_) => None,
        }
    }

    pub(in crate::tuic) fn stable_id(&self) -> usize {
        match self {
            Self::Live(connection) => connection.stable_id(),
            Self::Released(dead) => dead.stable_id,
        }
    }

    pub(in crate::tuic) fn close_reason(&self) -> Option<ConnectionError> {
        match self {
            Self::Live(connection) => connection.close_reason(),
            Self::Released(dead) => Some(dead.reason.clone()),
        }
    }

    pub(in crate::tuic) fn stats(&self) -> ConnectionStats {
        match self {
            Self::Live(connection) => connection.stats(),
            Self::Released(dead) => dead.stats,
        }
    }

    pub(in crate::tuic) fn current_path_generation(&self) -> u64 {
        match self {
            Self::Live(connection) => connection.current_path_generation(),
            Self::Released(dead) => dead.path_generation,
        }
    }

    pub(in crate::tuic) fn close(&self, code: VarInt, reason: &[u8]) {
        if let Self::Live(connection) = self {
            connection.close(code, reason);
        }
    }
}
