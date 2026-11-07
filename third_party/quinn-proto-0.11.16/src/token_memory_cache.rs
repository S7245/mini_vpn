//! Storing tokens sent from servers in NEW_TOKEN frames and using them in subsequent connections

use std::{
    collections::{hash_map, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use lru_slab::LruSlab;
use tracing::trace;

use crate::token::{TokenStore, TokenStoreError};

mod cold;
mod prepared;
use cold::Cold;

/// `TokenStore` implementation that stores up to `N` tokens per server name for up to a
/// limited number of server names, in-memory
#[derive(Debug)]
pub struct TokenMemoryCache(Mutex<State>);

impl TokenMemoryCache {
    /// Construct empty
    pub fn new(max_server_names: u32, max_tokens_per_server: usize) -> Self {
        Self(Mutex::new(State::new(
            max_server_names,
            max_tokens_per_server,
        )))
    }
}

impl TokenMemoryCache {
    /// Exact fixed hard-cache slab layouts at these configured limits.
    /// Excludes dynamic name/token backings and bounded instance/platform heads.
    /// Planning does not bind an authority or change the cache representation.
    #[doc(hidden)]
    pub fn fixed_layout_bytes(&self) -> Result<usize, TokenStoreError> {
        match &*self.lock()? {
            State::Empty {
                max_server_names,
                max_tokens_per_server,
            } => Cold::layout_bytes(*max_server_names as usize, *max_tokens_per_server),
            State::Legacy(legacy) => Cold::layout_bytes(
                legacy.max_server_names as usize,
                legacy.max_tokens_per_server,
            ),
            State::Cold(cold) => cold.fixed_layout_bytes(),
        }
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, TokenStoreError> {
        self.0.lock().map_err(|poison| {
            if let State::Cold(cold) = &**poison.get_ref() {
                cold.quarantine();
            }
            TokenStoreError::Capacity(bytes::MeteredCopyError::Quarantined)
        })
    }
}

impl TokenStore for TokenMemoryCache {
    fn prepare_insert_metered(
        self: Arc<Self>, name: &str, meter: &bytes::BackingMeter,
    ) -> Result<crate::token::PreparedTokenInsert, TokenStoreError> {
        prepared::prepare(self, name, meter)
    }

    fn insert(&self, server_name: &str, token: Bytes) {
        // Legacy public callers may carry a charged token too. Never send it
        // through uncharged legacy containers. Connection uses the typed path.
        if let Some(meter) = token.backing_meter().filter(|m| m.budget_limit().is_some()) {
            if self.insert_metered(server_name, token, &meter).is_err() {
                meter.quarantine();
            }
            return;
        }
        if let Ok(mut state) = self.lock() {
            if let State::Empty {
                max_server_names,
                max_tokens_per_server,
            } = *state
            {
                *state = State::Legacy(Legacy::new(max_server_names, max_tokens_per_server));
            }
            match &mut *state {
                State::Cold(cold) => cold.quarantine(),
                State::Legacy(legacy) => {
                    trace!(%server_name, "storing token");
                    legacy.store(server_name, token);
                }
                State::Empty { .. } => {}
            }
        }
    }

    fn take(&self, server_name: &str) -> Option<Bytes> {
        let mut state = self.lock().ok()?;
        let token = match &mut *state {
            State::Cold(cold) => cold.take(server_name).ok().flatten(),
            State::Legacy(legacy) => legacy.take(server_name),
            State::Empty { .. } => None,
        };
        trace!(%server_name, found=%token.is_some(), "taking token");
        token
    }

    fn insert_metered(
        &self,
        name: &str,
        token: Bytes,
        meter: &bytes::BackingMeter,
    ) -> Result<(), TokenStoreError> {
        let mut state = self.lock()?;
        Cold::check_token(&token, meter)?;
        state.release_empty_legacy()?;
        match &mut *state {
            State::Cold(cold) => {
                cold.check_authority(meter)?;
                cold.insert(name, token)
            }
            State::Empty {
                max_server_names,
                max_tokens_per_server,
            } => {
                // The entire candidate stays private through slab/name admission.
                // An error drops its exact layouts; publication is infallible.
                let mut cold =
                    Cold::prepare(*max_server_names as usize, *max_tokens_per_server, meter)?;
                cold.insert(name, token)?;
                *state = State::Cold(cold);
                Ok(())
            }
            State::Legacy(_) => Err(TokenStoreError::OccupiedLegacy),
        }
    }

    fn take_metered(
        &self,
        name: &str,
        meter: &bytes::BackingMeter,
    ) -> Result<Option<Bytes>, TokenStoreError> {
        let mut state = self.lock()?;
        meter
            .admit_precharged()
            .map_err(TokenStoreError::Capacity)?;
        state.release_empty_legacy()?;
        match &mut *state {
            State::Cold(cold) => {
                cold.check_authority(meter)?;
                cold.take(name)
            }
            State::Empty {
                max_server_names,
                max_tokens_per_server,
            } if *max_server_names != 0
                && *max_tokens_per_server != 0
                && meter.budget_limit().is_some() =>
            {
                Ok(None)
            }
            _ => Err(TokenStoreError::Unsupported),
        }
    }
}

/// Defaults to a maximum of 256 servers and 2 tokens per server
impl Default for TokenMemoryCache {
    fn default() -> Self {
        Self::new(256, 2)
    }
}

/// Exclusive representations: a hard cache can never retain legacy backing.
#[derive(Debug)]
enum State {
    Empty {
        max_server_names: u32,
        max_tokens_per_server: usize,
    },
    Legacy(Legacy),
    Cold(Cold),
}
impl State {
    fn new(max_server_names: u32, max_tokens_per_server: usize) -> Self {
        Self::Empty {
            max_server_names,
            max_tokens_per_server,
        }
    }
    fn release_empty_legacy(&mut self) -> Result<(), TokenStoreError> {
        if let Self::Legacy(legacy) = self {
            if !legacy.lookup.is_empty() || legacy.lru.len() != 0 {
                return Err(TokenStoreError::OccupiedLegacy);
            }
            // Assigning destroys both real retained allocations, not just len.
            // No token is consumed and the replacement has no allocation.
            *self = Self::new(legacy.max_server_names, legacy.max_tokens_per_server);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Legacy {
    max_server_names: u32,
    max_tokens_per_server: usize,
    lookup: HashMap<Arc<str>, u32>,
    lru: LruSlab<CacheEntry>,
}
impl Legacy {
    fn new(max_server_names: u32, max_tokens_per_server: usize) -> Self {
        Self {
            max_server_names,
            max_tokens_per_server,
            lookup: HashMap::new(),
            lru: LruSlab::default(),
        }
    }

    fn store(&mut self, server_name: &str, token: Bytes) {
        if self.max_server_names == 0 {
            // the rest of this method assumes that we can always insert a new entry so long as
            // we're willing to evict a pre-existing entry. thus, an entry limit of 0 is an edge
            // case we must short-circuit on now.
            return;
        }
        if self.max_tokens_per_server == 0 {
            // similarly to above, the rest of this method assumes that we can always push a new
            // token to a queue so long as we're willing to evict a pre-existing token, so we
            // short-circuit on the edge case of a token limit of 0.
            return;
        }

        let server_name = Arc::<str>::from(server_name);
        match self.lookup.entry(server_name.clone()) {
            hash_map::Entry::Occupied(hmap_entry) => {
                // key already exists, push the new token to its token queue
                let tokens = &mut self.lru.get_mut(*hmap_entry.get()).tokens;
                if tokens.len() >= self.max_tokens_per_server {
                    debug_assert!(tokens.len() == self.max_tokens_per_server);
                    tokens.pop_front().unwrap();
                }
                tokens.push_back(token);
            }
            hash_map::Entry::Vacant(hmap_entry) => {
                // key does not yet exist, create a new one, evicting the oldest if necessary
                let removed_key = if self.lru.len() >= self.max_server_names {
                    // unwrap safety: max_server_names is > 0, so there's at least one entry, so
                    //                lru() is some
                    Some(self.lru.remove(self.lru.lru().unwrap()).server_name)
                } else {
                    None
                };

                hmap_entry.insert(self.lru.insert(CacheEntry::new(server_name, token)));

                // for borrowing reasons, we must defer removing the evicted hmap entry to here
                if let Some(removed_slot) = removed_key {
                    let removed = self.lookup.remove(&removed_slot);
                    debug_assert!(removed.is_some());
                }
            }
        };
    }

    fn take(&mut self, server_name: &str) -> Option<Bytes> {
        let slab_key = *self.lookup.get(server_name)?;

        // pop from entry's token queue
        let entry = self.lru.get_mut(slab_key);
        // unwrap safety: we never leave tokens empty
        let token = entry.tokens.pop_front().unwrap();

        if entry.tokens.is_empty() {
            // token stack emptied, remove entry
            self.lru.remove(slab_key);
            self.lookup.remove(server_name);
        }

        Some(token)
    }
}

/// Cache entry within `TokenMemoryCache`'s LRU slab
#[derive(Debug)]
struct CacheEntry {
    server_name: Arc<str>,
    // invariant: tokens is never empty
    tokens: VecDeque<Bytes>,
}

impl CacheEntry {
    /// Construct with a single token
    fn new(server_name: Arc<str>, token: Bytes) -> Self {
        let mut tokens = VecDeque::new();
        tokens.push_back(token);
        Self {
            server_name,
            tokens,
        }
    }
}

#[cfg(test)]
mod tests;
