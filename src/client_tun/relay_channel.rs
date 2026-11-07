//! Original event seam, backed by the one shared bounded channel implementation.
//! This module owns only RelayEvent validation/disposition, never another queue,
//! callback runner or refund authority. Its static implementation is in value.
mod value;
pub(super) use crate::shared::bounded_channel::{
    Fault, Observer, Permit, Receiver, ReserveError, SendError, Sender, Value, WeakSender, channel,
    native_channel, native_layout_bytes,
};

#[cfg(test)]
mod tests;
