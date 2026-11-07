use super::*;
fn fixture() -> (Endpoint, BackingMeter, bytes::BackingBudget) {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut config = EndpointConfig::default();
    config.receive_backing_meter(meter.clone());
    (
        Endpoint::try_new(Arc::new(config), None, true, Some([0x51; 32])).unwrap(),
        meter,
        budget,
    )
}
fn packet(endpoint: &mut Endpoint, meter: &BackingMeter, len: usize) -> BytesMut {
    let cid = endpoint.local_cid_generator.generate_cid();
    let mut data = vec![0; len];
    data[0] = 0x40;
    data[1..1 + cid.len()].copy_from_slice(&cid);
    BytesMut::try_copy_from_slice_metered(&data, meter).unwrap()
}
#[test]
fn naked_native_vec_cannot_respond_but_same_clock_typed_input_does_without_allocating() {
    let (mut endpoint, meter, budget) = fixture();
    let now = Instant::now();
    let mut raw = Vec::new();
    let data = packet(&mut endpoint, &meter, 80);
    assert!(endpoint
        .handle(
            now,
            "[::1]:4433".parse().unwrap(),
            None,
            None,
            data,
            &mut raw
        )
        .is_none());
    assert_eq!(raw.capacity(), 0);
    assert_eq!(endpoint.last_stateless_reset, None);
    let baseline = budget.used();
    let mut scratch = ResponseScratch::prepare(1472, &meter).unwrap();
    assert_eq!(budget.used(), baseline + 1470);
    let data = packet(&mut endpoint, &meter, 80);
    let expected = ResetToken::new(
        &*endpoint.config.reset_key,
        ConnectionId::new(&data[1..1 + endpoint.local_cid_generator.cid_len()]),
    );
    let mut result = None;
    let allocated = crate::tests::allocations_in(|| {
        result = endpoint.handle_with_admission(
            now,
            "[::1]:4433".parse().unwrap(),
            None,
            None,
            data,
            &mut scratch,
            |_| Some(()),
        );
    });
    let Some(AdmittedDatagram::Other(DatagramEvent::Response(transmit))) = result else {
        panic!("real reset required");
    };
    assert_eq!(allocated, 0);
    assert_eq!(transmit.size, scratch.as_slice().len());
    assert!(scratch.as_slice().ends_with(&expected));
    assert_eq!(endpoint.last_stateless_reset, Some(now));
    drop((endpoint, scratch));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn foreign_or_undersized_typed_input_preserves_scratch_and_endpoint_clock() {
    let (mut endpoint, meter, budget) = fixture();
    let foreign = BackingMeter::with_budget(&budget);
    for authority in [&foreign, &meter] {
        let mut scratch = ResponseScratch::prepare(43, authority).unwrap();
        scratch.buffer.extend_from_slice(&[7, 8, 9]);
        let data = packet(&mut endpoint, &meter, 80);
        assert!(endpoint
            .handle_with_admission(
                Instant::now(),
                "[::1]:4433".parse().unwrap(),
                None,
                None,
                data,
                &mut scratch,
                |_| Some(())
            )
            .is_none());
        assert_eq!(scratch.as_slice(), &[7, 8, 9]);
        assert_eq!(endpoint.last_stateless_reset, None);
    }
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn minimum_and_maximum_reset_inputs_stay_in_the_original_allocation() {
    for len in [22, 42, 43, 1472] {
        let (mut endpoint, meter, budget) = fixture();
        let mut scratch = ResponseScratch::prepare(1472, &meter).unwrap();
        let original = scratch.buffer.as_ptr();
        let data = packet(&mut endpoint, &meter, len);
        let result = endpoint.handle_with_admission(
            Instant::now(),
            "[::1]:4433".parse().unwrap(),
            None,
            None,
            data,
            &mut scratch,
            |_| Some(()),
        );
        let Some(AdmittedDatagram::Other(DatagramEvent::Response(transmit))) = result else {
            panic!("reset {len}");
        };
        assert_eq!(scratch.buffer.as_ptr(), original);
        assert!(transmit.size <= if len <= 42 { len - 1 } else { len - 2 });
        if len <= 42 {
            assert_eq!(transmit.size, len - 1);
        }
        drop((endpoint, scratch));
        assert_eq!(budget.used(), 0);
    }
}
#[test]
fn cold_full_and_cancel_return_only_the_exact_scratch_layout() {
    let budget = bytes::BackingBudget::new(1469).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    assert!(ResponseScratch::prepare(1472, &meter).is_err());
    assert_eq!(budget.used(), 0);
    let budget = bytes::BackingBudget::new(1470).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let scratch = ResponseScratch::prepare(1472, &meter).unwrap();
    assert_eq!(budget.used(), 1470);
    drop(scratch);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
