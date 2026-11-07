//! Existing UDP callback path; the complete physical owner lives in shared.
pub(crate) use crate::shared::actor_callbacks::{
    CallbackError, CallbackFailure, Callbacks, Custody, Field, Guard, Lease, Role,
};
