//! Unpublished cache insertion resources, owned by one synchronous packet.
//! No lock survives preparation. Commit rebases onto the original fixed slots
//! under their original mutex, so concurrent take/eviction remains valid.
//! No cache authority or eviction is published by prepare or cancellation.
//! Tests: token_memory_cache/tests/prepared.rs.
use super::*;
use crate::token::{PreparedTokenInsert, TokenInsert};
use bytes::BackingMeter;
struct Prepared {
    cache: Arc<TokenMemoryCache>,
    name: Bytes,
    candidate: Option<Cold>,
    meter: BackingMeter,
}
pub(super) fn prepare(
    cache: Arc<TokenMemoryCache>,
    name: &str,
    meter: &BackingMeter,
) -> Result<PreparedTokenInsert, TokenStoreError> {
    meter
        .admit_precharged()
        .map_err(TokenStoreError::Capacity)?;
    let state = cache.lock()?;
    let candidate = match &*state {
        State::Cold(cold) => {
            cold.check_authority(meter)?;
            None
        }
        State::Empty {
            max_server_names,
            max_tokens_per_server,
        } => Some(Cold::prepare(
            *max_server_names as usize,
            *max_tokens_per_server,
            meter,
        )?),
        State::Legacy(legacy) if legacy.lookup.is_empty() && legacy.lru.len() == 0 => {
            Some(Cold::prepare(
                legacy.max_server_names as usize,
                legacy.max_tokens_per_server,
                meter,
            )?)
        }
        State::Legacy(_) => return Err(TokenStoreError::OccupiedLegacy),
    };
    let prepared_name = match &*state {
        State::Cold(cold) => cold.prepare_name(name)?,
        _ => candidate
            .as_ref()
            .ok_or(TokenStoreError::Unsupported)?
            .prepare_name(name)?,
    };
    drop(state);
    PreparedTokenInsert::new(
        Prepared {
            cache,
            name: prepared_name,
            candidate,
            meter: meter.clone(),
        },
        meter,
    )
}
impl TokenInsert for Prepared {
    fn insert(&mut self, token: Bytes) -> Result<(), TokenStoreError> {
        Cold::check_token(&token, &self.meter)?;
        let mut state = self.cache.lock()?;
        // A legacy user may have populated the cache since preparation. Never
        // normalize that unknown ownership or silently overwrite its tokens.
        state.release_empty_legacy()?;
        match &mut *state {
            State::Cold(cold) => {
                cold.check_authority(&self.meter)?;
                cold.insert_prepared(self.name.clone(), token)
            }
            State::Empty { .. } => {
                let mut candidate = self
                    .candidate
                    .take()
                    .ok_or(TokenStoreError::ForeignAuthority)?;
                candidate.insert_prepared(self.name.clone(), token)?;
                *state = State::Cold(candidate);
                Ok(())
            }
            State::Legacy(_) => Err(TokenStoreError::OccupiedLegacy),
        }
    }
}
