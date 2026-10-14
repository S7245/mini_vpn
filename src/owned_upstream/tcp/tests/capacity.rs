use super::*;

#[test]
fn factory_capacity_witness_preserves_primitive_layout_across_equal_aggregate_collisions() {
    let factory_a =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(128, 2, 3, 4), 256, 4, 8)
            .unwrap();
    let factory_b =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(128, 3, 2, 4), 256, 5, 7)
            .unwrap();

    let a = factory_a.capacity_witness();
    let b = factory_b.capacity_witness();

    assert_eq!(
        a.flow_port().uplink_data_messages()
            + a.flow_port().control_messages()
            + a.flow_port().downlink_actions(),
        b.flow_port().uplink_data_messages()
            + b.flow_port().control_messages()
            + b.flow_port().downlink_actions(),
    );
    assert_eq!(
        a.per_flow_uplink_segment_capacity() + a.session_uplink_segment_capacity(),
        b.per_flow_uplink_segment_capacity() + b.session_uplink_segment_capacity(),
    );
    assert_eq!(a.per_flow_channel_slots(), b.per_flow_channel_slots());
    assert_ne!(a, b);

    assert_eq!(a.flow_port(), port_config(128, 2, 3, 4));
    assert_eq!(a.bound_direction(), None);
    assert_eq!(a.session_uplink_byte_capacity(), 256);
    assert_eq!(a.per_flow_uplink_segment_capacity(), 4);
    assert_eq!(a.session_uplink_segment_capacity(), 8);
    assert_eq!(a.fixed_reset_slots(), 1);
    assert_eq!(a.fixed_terminal_slots(), 1);
    assert_eq!(a.per_flow_channel_slots(), 11);
    assert_eq!(a.max_single_data_payload_bytes(), 128);
}

#[test]
fn factory_capacity_direction_is_observed_from_the_single_admission_binding() {
    let fixture = fixture();
    let (factory, mut admission) = ResumableTcpPortFactory::new_unbound_with_admission_for_test(
        port_config(128, 2, 3, 4),
        256,
        4,
        8,
    )
    .unwrap();
    assert_eq!(factory.capacity_witness().bound_direction(), None);

    let (_flow, _driver) = factory.open_flow(fixture.flow).unwrap();
    let bound = factory.capacity_witness();
    assert_eq!(bound.bound_direction(), Some(fixture.flow.direction()));

    let independent = factory.fork_with_independent_ledgers_for_test().unwrap();
    assert_eq!(independent.capacity_witness(), bound);

    assert_eq!(admission.close(), TcpFactoryAdmissionClose::ClosedNow);
    assert_eq!(factory.capacity_witness(), bound);
}

#[test]
fn byte_capacity_ceiling_covers_both_wire_and_tokio_semaphore_domains() {
    let expected = (u32::MAX as usize).min(Semaphore::MAX_PERMITS);
    assert_eq!(FlowPortConfig::max_exact_uplink_byte_capacity(), expected);
    assert!(FlowPortConfig::new(expected, 1, 1, 1).is_ok());

    let above = expected
        .checked_add(1)
        .expect("the exact byte ceiling is below usize::MAX on every supported target");
    let expected_error = FlowPortConfigError::UplinkByteCapacityTooLarge {
        value: above,
        max: expected,
    };
    assert_eq!(FlowPortConfig::new(above, 1, 1, 1), Err(expected_error));
    assert!(matches!(
        ResumableTcpPortFactory::new_unbound_for_test(
            port_config(1, 1, 1, 1),
            above,
            1,
            1,
        ),
        Err(error) if error == expected_error
    ));
}
