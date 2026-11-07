//! Public Quinn consumers can implement typed hard-cache custody without proto.
use bytes::{BackingBudget, BackingMeter, Bytes, BytesMut};
use quinn::{TokenMemoryCache, TokenStore, TokenStoreError};

struct Delegating(TokenMemoryCache);
impl TokenStore for Delegating {
    fn insert(&self, name: &str, token: Bytes) {
        self.0.insert(name, token);
    }
    fn take(&self, name: &str) -> Option<Bytes> {
        self.0.take(name)
    }
    fn insert_metered(
        &self,
        name: &str,
        token: Bytes,
        meter: &BackingMeter,
    ) -> Result<(), TokenStoreError> {
        self.0.insert_metered(name, token, meter)
    }
    fn take_metered(
        &self,
        name: &str,
        meter: &BackingMeter,
    ) -> Result<Option<Bytes>, TokenStoreError> {
        self.0.take_metered(name, meter)
    }
}
struct LegacyOnly;
impl TokenStore for LegacyOnly {
    fn insert(&self, _: &str, _: Bytes) {
        panic!("typed refusal cannot call legacy insert")
    }
    fn take(&self, _: &str) -> Option<Bytes> {
        panic!("typed refusal cannot call legacy take")
    }
}

#[test]
fn external_typed_store_can_name_error_and_preserve_one_time_backing() {
    let budget = BackingBudget::new(1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let store: Box<dyn TokenStore> = Box::new(Delegating(TokenMemoryCache::default()));
    assert!(store.take_metered("localhost", &meter).unwrap().is_none());
    let token = BytesMut::try_copy_from_slice_metered(b"opaque", &meter)
        .unwrap()
        .freeze();
    let ptr = token.as_ptr();
    store.insert_metered("localhost", token, &meter).unwrap();
    let taken = store.take_metered("localhost", &meter).unwrap().unwrap();
    assert_eq!(taken.as_ptr(), ptr);
    assert!(store.take_metered("localhost", &meter).unwrap().is_none());
    drop(store);
    assert!(
        budget.used() > 0,
        "moved token still owns its original backing"
    );
    drop(taken);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn unsupported_typed_store_rejects_without_invoking_legacy_methods() {
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let token = BytesMut::try_copy_from_slice_metered(b"opaque", &meter)
        .unwrap()
        .freeze();
    let store = LegacyOnly;
    assert_eq!(
        store.take_metered("localhost", &meter),
        Err(TokenStoreError::Unsupported)
    );
    assert_eq!(
        store.insert_metered("localhost", token, &meter),
        Err(TokenStoreError::Unsupported)
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
