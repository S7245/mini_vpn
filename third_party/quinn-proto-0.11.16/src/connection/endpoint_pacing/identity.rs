//! Pacing identity owned by the Endpoint service, separate from resource accounting.
//! Entry: key construction and path transition; tests live in identity/tests.
use crate::ConnectionHandle;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EndpointPacingConnectionKey {
    pub(super) connection_handle: ConnectionHandle,
    registration_generation: u64,
    pub(super) path_generation: u64,
}

impl EndpointPacingConnectionKey {
    pub(crate) const fn new(connection_handle: ConnectionHandle, path_generation: u64) -> Self {
        Self::for_registration(connection_handle, 0, path_generation)
    }
    pub(crate) const fn for_registration(
        connection_handle: ConnectionHandle,
        registration_generation: u64,
        path_generation: u64,
    ) -> Self {
        Self {
            connection_handle,
            registration_generation,
            path_generation,
        }
    }
    pub(super) const fn with_path(self, path_generation: u64) -> Self {
        Self {
            path_generation,
            ..self
        }
    }
}

#[cfg(test)]
mod tests;
