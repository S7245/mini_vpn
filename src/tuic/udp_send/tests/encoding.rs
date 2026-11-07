//! The original Packet wire contract with real output allocation ownership.
use super::super::encoding::{encode_packet, native_packet};
use crate::shared::TargetAddr;
use bytes::{BackingBudget, BackingMeter, BytesMut, MeteredCopyError};

#[test]
fn native_packet_keeps_wire_bytes_and_charge_until_actual_last_alias_drop() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let targets = [
        TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
        TargetAddr::IpPort("[2001:db8::1]:9000".parse().unwrap()),
        TargetAddr::DomainPort {
            host: "x".repeat(300),
            port: 53,
        },
    ];
    for target in targets {
        let expected = encode_packet(17, &target, b"original UDP payload");
        let data = native_packet(17, &target, b"original UDP payload", &meter).unwrap();
        assert_eq!(data.as_ref(), expected);
        assert!(data.backing_meter().unwrap().same_budget(&meter));
        let bill = BytesMut::metered_copy_layout_for_len(data.len()).unwrap();
        assert_eq!(budget.used(), bill);
        let alias = data.clone();
        let identity = data.allocation_identity();
        drop(data);
        assert_eq!(alias.allocation_identity(), identity);
        assert_eq!(budget.used(), bill);
        drop(alias);
        assert_eq!(budget.used(), 0);
    }
    assert!(!budget.quarantined());
}

#[test]
fn native_packet_pressure_size_and_q_are_distinct_and_leave_input_intact() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
    let input = b"original";
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = BytesMut::try_zeroed_metered(budget.limit() - header, &meter).unwrap();
    assert_eq!(
        native_packet(1, &target, input, &meter),
        Err(MeteredCopyError::BudgetFull)
    );
    assert_eq!(budget.used(), budget.limit());
    assert!(!budget.quarantined());
    drop(pressure);
    assert_eq!(
        native_packet(1, &target, &vec![0; 65536], &meter),
        Err(MeteredCopyError::TooLarge)
    );
    assert_eq!(budget.used(), 0);
    meter.quarantine();
    assert_eq!(
        native_packet(1, &target, input, &meter),
        Err(MeteredCopyError::Quarantined)
    );
    assert_eq!(input, b"original");
}
