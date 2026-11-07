//! Actual Endpoint Meta/route/reset inventory observation before replacement.
use super::*;

impl Endpoint {
    pub(crate) fn assert_registration_inventory_for_probe(&self) -> (usize, usize, usize) {
        self.registrations.store.assert_inventory()
    }
}

#[test]
fn native_registry_layout_is_precharged_before_any_connection() {
    let budget = bytes::BackingBudget::new(8 * 1024 * 1024).unwrap();
    let mut config = EndpointConfig::default();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let endpoint = Endpoint::try_new(Arc::new(config), None, true, Some([0x62; 32])).unwrap();
    assert_eq!(endpoint.open_connections(), 0);
    assert!(budget.used() > RegistrationObserver::layout_bytes(), "Native Meta/routes/reset physical storage must be admitted cold, in addition to the settlement ledger");
    assert_eq!(budget.used(), Endpoint::native_registration_layout_bytes());
    assert!(!budget.quarantined());
    drop(endpoint);
    assert_eq!(budget.used(), 0);
}

impl Endpoint {
    pub(crate) fn hold_identifier_nodes_for_probe(
        &self,
        ch: ConnectionHandle,
    ) -> Vec<crate::connection::cid_frames::Prepared> {
        let pool = self
            .registrations
            .store
            .get(ch.0)
            .unwrap()
            .cid_frames
            .as_ref()
            .unwrap();
        let mut held = Vec::new();
        while let Some(node) = pool.prepare(1) {
            held.push(node);
        }
        held
    }
    pub(crate) fn repeat_current_cid_for_probe(
        &mut self,
        ch: ConnectionHandle,
    ) -> Arc<std::sync::atomic::AtomicUsize> {
        struct Repeat {
            inner: Box<dyn ConnectionIdGenerator>,
            cid: ConnectionId,
            calls: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl ConnectionIdGenerator for Repeat {
            fn generate_cid(&mut self) -> ConnectionId {
                self.calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.cid
            }
            fn validate(&self, cid: &ConnectionId) -> Result<(), crate::cid_generator::InvalidCid> {
                self.inner.validate(cid)
            }
            fn cid_len(&self) -> usize {
                self.inner.cid_len()
            }
            fn cid_lifetime(&self) -> Option<Duration> {
                self.inner.cid_lifetime()
            }
        }
        let cid = *self
            .registrations
            .store
            .get(ch.0)
            .unwrap()
            .loc_cids
            .values()
            .next()
            .unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inner = std::mem::replace(
            &mut self.local_cid_generator,
            Box::<crate::HashedConnectionIdGenerator>::default(),
        );
        self.local_cid_generator = Box::new(Repeat {
            inner,
            cid,
            calls: calls.clone(),
        });
        calls
    }
}

#[test]
fn native_stateless_reset_uses_same_precharged_signature_scratch() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let mut config = EndpointConfig::default();
    let key = config.reset_key.clone();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
    let cid = endpoint.local_cid_generator.generate_cid();
    let expected = ResetToken::new(&*key, cid);
    let mut response = ResponseScratch::prepare(
        1472,
        endpoint.config.receive_backing_meter.as_ref().unwrap(),
    )
    .unwrap();
    let mut transmit = None;
    let allocations = crate::tests::allocations_in(|| {
        transmit = endpoint.stateless_reset(
            Instant::now(),
            80,
            FourTuple {
                remote: "[::1]:4433".parse().unwrap(),
                local_ip: None,
            },
            cid,
            &mut ResponseBuffer::from(&mut response),
        )
    });
    assert_eq!(allocations, 0);
    assert_eq!(transmit.unwrap().size, response.as_slice().len());
    assert!(response.as_slice().ends_with(&expected));
    assert!(!budget.quarantined());
    drop((endpoint, response));
    assert_eq!(budget.used(), 0);
}

impl Endpoint {
    pub(crate) fn poison_prepared_pacing_for_probe(&self) {
        self.endpoint_pacing_service
            .as_ref()
            .unwrap()
            .poison_prepared_for_probe();
    }
}

impl Endpoint {
    pub(crate) fn replace_identifier_pool_for_probe(
        &mut self,
        target: ConnectionHandle,
        source: ConnectionHandle,
    ) -> Option<crate::connection::cid_frames::Pool> {
        let pool = self
            .registrations
            .store
            .get(source.0)
            .unwrap()
            .cid_frames
            .clone();
        std::mem::replace(
            &mut self
                .registrations
                .store
                .get_mut(target.0)
                .unwrap()
                .cid_frames,
            pool,
        )
    }
    pub(crate) fn next_identifier_sequence_for_probe(&self, ch: ConnectionHandle) -> u64 {
        self.registrations.store.get(ch.0).unwrap().cids_issued
    }
}
