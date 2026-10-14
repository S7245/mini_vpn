//! Bounded failure values captured at the original resource owner's control boundary.
//! Owns typed facts and at most 1024 bytes of known UTF-8 text, never source objects.
//! Does not own role slots, first-error policy, notification IDs, or completed rings.
//! Entry points consume I/O, client, task-join, and caught-panic values; tests live below.

use super::ClientError;
use std::any::Any;
use std::io;
use tokio::task::JoinError;

const TEXT_CAPACITY: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailureClass {
    Io,
    InvalidTarget,
    Utf8,
    Reality,
    Panic,
    Cancelled,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailurePhase {
    DeviceRead,
    DeviceWrite,
    DeviceJoin,
    Startup,
    SessionLoop,
    SessionLoopDrop,
    DeviceStop,
    DeviceShutdown,
    DeviceDrop,
    TcpOpen,
    TcpOpenDrop,
    TcpOpenJoin,
    UdpJoin,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailureResource {
    TunPump,
    UdpDriver,
    TuicNode,
    Session,
    TcpOpen,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FailureOrigin {
    pub(crate) phase: FailurePhase,
    pub(crate) resource: FailureResource,
    pub(crate) resource_id: u64,
    pub(crate) attempt_id: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FailureFacts {
    pub(crate) origin: FailureOrigin,
    pub(crate) class: FailureClass,
    pub(crate) io_kind: Option<io::ErrorKind>,
    pub(crate) raw_os_code: Option<i32>,
    /// Byte length of known source text or the rejected UTF-8 input.
    pub(crate) source_len: Option<usize>,
    pub(crate) utf8_valid_up_to: Option<usize>,
    pub(crate) utf8_error_len: Option<usize>,
    pub(crate) truncated: bool,
    pub(crate) opaque: bool,
    /// Source content was discarded, including invalid UTF-8 input bytes.
    pub(crate) source_content_omitted: bool,
}

pub(crate) struct RetainedFailure {
    facts: FailureFacts,
    text: [u8; TEXT_CAPACITY],
    text_len: u16,
}

const _: () = assert!(std::mem::size_of::<RetainedFailure>() <= 1152);

impl RetainedFailure {
    pub(crate) fn from_io(origin: FailureOrigin, error: io::Error) -> Self {
        Self::from_io_ref(origin, &error)
    }

    pub(crate) fn from_io_ref(origin: FailureOrigin, error: &io::Error) -> Self {
        let mut retained = Self::empty(origin, FailureClass::Io);
        retained.facts.io_kind = Some(error.kind());
        retained.facts.raw_os_code = error.raw_os_error();
        retained.facts.opaque = error.get_ref().is_some();
        retained.facts.source_content_omitted = retained.facts.opaque;
        retained
    }

    pub(crate) fn from_client(origin: FailureOrigin, error: ClientError) -> Self {
        Self::from_client_ref(origin, &error)
    }

    pub(crate) fn from_client_ref(origin: FailureOrigin, error: &ClientError) -> Self {
        match error {
            ClientError::Io(error) => Self::from_io_ref(origin, error),
            ClientError::InvalidTarget(text) => {
                Self::from_known_text(origin, FailureClass::InvalidTarget, &text)
            }
            ClientError::Reality(text) => {
                Self::from_known_text(origin, FailureClass::Reality, &text)
            }
            ClientError::Utf8(error) => {
                let mut retained = Self::empty(origin, FailureClass::Utf8);
                let utf8 = error.utf8_error();
                retained.facts.source_len = Some(error.as_bytes().len());
                retained.facts.utf8_valid_up_to = Some(utf8.valid_up_to());
                retained.facts.utf8_error_len = utf8.error_len();
                retained.facts.source_content_omitted = true;
                retained
            }
        }
    }

    pub(crate) fn from_join(origin: FailureOrigin, error: JoinError) -> Self {
        match error.try_into_panic() {
            Ok(payload) => Self::from_panic(origin, payload),
            Err(cancelled) => {
                drop(cancelled);
                Self::empty(origin, FailureClass::Cancelled)
            }
        }
    }

    pub(crate) fn from_panic(origin: FailureOrigin, payload: Box<dyn Any + Send>) -> Self {
        let retained = Self::opaque_panic(origin);
        drop(payload);
        retained
    }

    /// Record facts before an unknown panic payload is destroyed by its owner.
    pub(crate) fn opaque_panic(origin: FailureOrigin) -> Self {
        let mut retained = Self::empty(origin, FailureClass::Panic);
        retained.facts.opaque = true;
        retained.facts.source_content_omitted = true;
        retained
    }

    pub(crate) fn facts(&self) -> &FailureFacts {
        &self.facts
    }

    pub(crate) fn text_bytes(&self) -> &[u8] {
        &self.text[..usize::from(self.text_len)]
    }

    fn from_known_text(origin: FailureOrigin, class: FailureClass, text: &str) -> Self {
        let mut retained = Self::empty(origin, class);
        let mut len = text.len().min(TEXT_CAPACITY);
        while !text.is_char_boundary(len) {
            len -= 1;
        }
        retained.text[..len].copy_from_slice(&text.as_bytes()[..len]);
        retained.text_len = len as u16;
        retained.facts.source_len = Some(text.len());
        retained.facts.truncated = len < text.len();
        retained.facts.source_content_omitted = retained.facts.truncated;
        retained
    }

    fn empty(origin: FailureOrigin, class: FailureClass) -> Self {
        Self {
            facts: FailureFacts {
                origin,
                class,
                io_kind: None,
                raw_os_code: None,
                source_len: None,
                utf8_valid_up_to: None,
                utf8_error_len: None,
                truncated: false,
                opaque: false,
                source_content_omitted: false,
            },
            text: [0; TEXT_CAPACITY],
            text_len: 0,
        }
    }
}

#[cfg(test)]
mod tests;
