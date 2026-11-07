//! Owns fixed hard-cache slots, LRU/FIFO and their exact physical allowances.
//! Preparation includes replacement names before eviction; commit never allocates.
//! Token Bytes move once to the next handshake. Connection owns first/Close,
//! cache last reference owns slab disposal. No DATA path or second refund owner.
//! Entry: prepare/insert/take; tests: token_memory_cache/tests/native.
use super::TokenStoreError;
use bytes::{BackingBudgetReservation, BackingMeter, Bytes, BytesMut, MeteredCopyError};
use std::alloc::{alloc, Layout};

#[derive(Debug, Default)]
struct Server {
    name: Option<Bytes>,
    first: usize,
    count: usize,
    previous: Option<usize>,
    next: Option<usize>,
}
#[derive(Debug)]
struct Slab<T> {
    values: Box<[T]>,
    // Declaration order drops the physical allocation before its allowance.
    _allowance: BackingBudgetReservation,
}
impl<T: Default> Slab<T> {
    fn layout(count: usize) -> Result<Layout, TokenStoreError> {
        let layout = Layout::array::<T>(count)
            .map_err(|_| TokenStoreError::Capacity(MeteredCopyError::TooLarge))?;
        if layout.size() == 0 {
            return Err(TokenStoreError::Unsupported);
        }
        Ok(layout)
    }
    fn prepare(count: usize, meter: &BackingMeter) -> Result<Self, TokenStoreError> {
        let layout = Self::layout(count)?;
        let allowance = meter
            .reserve_cold_layout(layout.size())
            .map_err(TokenStoreError::Capacity)?
            .ok_or(TokenStoreError::Unsupported)?;
        let ptr = unsafe { alloc(layout).cast::<T>() };
        if ptr.is_null() {
            return Err(TokenStoreError::Capacity(MeteredCopyError::Allocation));
        }
        // Server and Option<Bytes> have infallible, allocation-free defaults.
        for index in 0..count {
            unsafe {
                ptr.add(index).write(T::default());
            }
        }
        let values = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, count)) };
        Ok(Self {
            values,
            _allowance: allowance,
        })
    }
}
#[derive(Debug)]
pub(super) struct Cold {
    servers: Slab<Server>,
    tokens: Slab<Option<Bytes>>,
    per_server: usize,
    oldest: Option<usize>,
    newest: Option<usize>,
    meter: BackingMeter,
}
impl Cold {
    fn token_capacity(names: usize, per_server: usize) -> Result<usize, TokenStoreError> {
        if names == 0 || per_server == 0 {
            return Err(TokenStoreError::Unsupported);
        }
        names
            .checked_mul(per_server)
            .ok_or(TokenStoreError::Capacity(MeteredCopyError::TooLarge))
    }
    pub(super) fn layout_bytes(names: usize, per_server: usize) -> Result<usize, TokenStoreError> {
        let tokens = Self::token_capacity(names, per_server)?;
        Slab::<Server>::layout(names)?
            .size()
            .checked_add(Slab::<Option<Bytes>>::layout(tokens)?.size())
            .ok_or(TokenStoreError::Capacity(MeteredCopyError::TooLarge))
    }
    pub(super) fn fixed_layout_bytes(&self) -> Result<usize, TokenStoreError> {
        Self::layout_bytes(self.servers.values.len(), self.per_server)
    }
    pub(super) fn prepare(
        names: usize,
        per_server: usize,
        meter: &BackingMeter,
    ) -> Result<Self, TokenStoreError> {
        let tokens = Self::token_capacity(names, per_server)?;
        // Both exact layouts are admitted before their respective allocations.
        // A later refusal drops only the still-private candidate.
        Ok(Self {
            servers: Slab::prepare(names, meter)?,
            tokens: Slab::prepare(tokens, meter)?,
            per_server,
            oldest: None,
            newest: None,
            meter: meter.clone(),
        })
    }
    pub(super) fn quarantine(&self) {
        self.meter.quarantine();
    }
    pub(super) fn check_authority(&self, meter: &BackingMeter) -> Result<(), TokenStoreError> {
        if !self.meter.same_authority(meter) {
            return Err(TokenStoreError::ForeignAuthority);
        }
        meter.admit_precharged().map_err(TokenStoreError::Capacity)
    }
    pub(super) fn check_token(token: &Bytes, meter: &BackingMeter) -> Result<(), TokenStoreError> {
        if !token
            .backing_meter()
            .is_some_and(|m| m.same_authority(meter))
        {
            return Err(TokenStoreError::ForeignAuthority);
        }
        meter.admit_precharged().map_err(TokenStoreError::Capacity)
    }
    fn find(&self, name: &str) -> Option<usize> {
        // At most the configured number of names, on this cold control path.
        self.servers
            .values
            .iter()
            .position(|s| s.name.as_deref() == Some(name.as_bytes()))
    }
    pub(super) fn prepare_name(&self, name: &str) -> Result<Bytes, TokenStoreError> {
        if let Some(index) = self.find(name) {
            return self.servers.values[index].name.clone().ok_or(TokenStoreError::ForeignAuthority);
        }
        if name.is_empty() { return Ok(Bytes::new()); }
        Ok(BytesMut::try_copy_from_slice_metered(name.as_bytes(), &self.meter)
            .map_err(TokenStoreError::Capacity)?.freeze())
    }
    pub(super) fn insert(&mut self, name: &str, token: Bytes) -> Result<(), TokenStoreError> {
        Self::check_token(&token, &self.meter)?;
        let name = self.prepare_name(name)?;
        self.insert_prepared(name, token)
    }
    pub(super) fn insert_prepared(&mut self, name: Bytes, token: Bytes) -> Result<(), TokenStoreError> {
        Self::check_token(&token, &self.meter)?;
        if !name.is_empty() { Self::check_token(&name, &self.meter)?; }
        let existing = self.servers.values.iter().position(|s| s.name.as_deref() == Some(name.as_ref()));
        let index = existing
            .or_else(|| self.servers.values.iter().position(|s| s.name.is_none()))
            .or(self.oldest)
            .ok_or(TokenStoreError::Unsupported)?;
        self.meter
            .admit_precharged()
            .map_err(TokenStoreError::Capacity)?;
        let mutation = Mutation::begin(&self.meter);
        if existing.is_some() {
            self.unlink(index);
        } else {
            if self.servers.values[index].name.is_some() {
                self.unlink(index);
                self.clear(index);
            }
            self.servers.values[index].name = Some(name);
        }
        let server = &mut self.servers.values[index];
        let base = index * self.per_server;
        if server.count == self.per_server {
            self.tokens.values[base + server.first] = None;
            server.first = (server.first + 1) % self.per_server;
            server.count -= 1;
        }
        let position = (server.first + server.count) % self.per_server;
        self.tokens.values[base + position] = Some(token);
        server.count += 1;
        self.link_newest(index);
        mutation.commit();
        Ok(())
    }
    pub(super) fn take(&mut self, name: &str) -> Result<Option<Bytes>, TokenStoreError> {
        self.meter
            .admit_precharged()
            .map_err(TokenStoreError::Capacity)?;
        let Some(index) = self.find(name) else {
            return Ok(None);
        };
        let server = &self.servers.values[index];
        let position = index * self.per_server + server.first;
        // Validate the physical value before detaching anything.
        let token = self.tokens.values[position].as_ref().ok_or_else(|| {
            self.meter.quarantine();
            TokenStoreError::Capacity(MeteredCopyError::Quarantined)
        })?;
        Self::check_token(token, &self.meter)?;
        let mutation = Mutation::begin(&self.meter);
        self.unlink(index);
        let server = &mut self.servers.values[index];
        let token = self.tokens.values[position].take();
        server.count -= 1;
        server.first = (server.first + 1) % self.per_server;
        if server.count == 0 {
            self.clear(index);
        } else {
            self.link_newest(index);
        }
        mutation.commit();
        Ok(token)
    }
    fn clear(&mut self, index: usize) {
        let start = index * self.per_server;
        for token in &mut self.tokens.values[start..start + self.per_server] {
            *token = None;
        }
        self.servers.values[index] = Server::default();
    }
    fn unlink(&mut self, index: usize) {
        let server = &self.servers.values[index];
        let (previous, next) = (server.previous, server.next);
        if let Some(p) = previous {
            self.servers.values[p].next = next;
        } else {
            self.oldest = next;
        }
        if let Some(n) = next {
            self.servers.values[n].previous = previous;
        } else {
            self.newest = previous;
        }
    }
    fn link_newest(&mut self, index: usize) {
        self.servers.values[index].previous = self.newest;
        self.servers.values[index].next = None;
        if let Some(p) = self.newest {
            self.servers.values[p].next = Some(index);
        } else {
            self.oldest = Some(index);
        }
        self.newest = Some(index);
    }
}
// A panic after mutation starts leaves the same physical authority sticky Q.
struct Mutation {
    meter: BackingMeter,
    dirty: bool,
}
impl Mutation {
    fn begin(meter: &BackingMeter) -> Self {
        Self {
            meter: meter.clone(),
            dirty: true,
        }
    }
    fn commit(mut self) {
        self.dirty = false;
    }
}
impl Drop for Mutation {
    fn drop(&mut self) {
        if self.dirty {
            self.meter.quarantine();
        }
    }
}

#[cfg(test)]
mod tests;
