use super::*;

mod amplification;
mod instances;
pub(crate) mod counting_alloc;
use crate::quic::{CcChoice, MtuPolicy, QuicGsoPolicy, QuicPacingPolicy};

const ONE: Ratio = Ratio::ONE;

fn unit_factors() -> MeasuredFactors {
    MeasuredFactors {
        rx: ONE,
        tx: ONE,
        datagram_rx: ONE,
        per_connection: 0,
        per_endpoint: 0,
    }
}

/// The shape must be the exact values the actual Native/TUIC TransportConfig
/// installs, or the bound would describe a different stack.
#[test]
fn shape_matches_actual_transport_config() {
    for mtu in [MtuPolicy::Default, MtuPolicy::Safe1200] {
        let shape = TransportShape::tuic(mtu);
        let cfg = super::super::quic_transport_config(
            CcChoice::Bbr,
            mtu,
            QuicGsoPolicy::Enabled,
            QuicPacingPolicy::QuinnDefault,
        );
        let debug = format!("{cfg:?}");
        for needle in [
            format!("max_concurrent_bidi_streams: {}", shape.bidi_streams),
            format!("max_concurrent_uni_streams: {}", shape.uni_streams),
            format!("stream_receive_window: {}", shape.stream_window),
            format!("receive_window: {}", shape.connection_window),
            format!("send_window: {}", shape.send_window),
            format!("crypto_buffer_size: {}", shape.crypto_buffer),
            format!("datagram_receive_buffer_size: Some({})", shape.datagram_rx),
            format!("datagram_send_buffer_size: {}", shape.datagram_tx),
        ] {
            assert!(debug.contains(&needle), "missing `{needle}` in {debug}");
        }
    }
}

#[test]
fn connection_bound_sums_every_configured_term() {
    let shape = TransportShape::tuic(MtuPolicy::Default);
    let per_conn = u64::from(shape.connection_window)
        + shape.send_window
        + shape.datagram_rx as u64
        + shape.datagram_tx as u64
        + 3 * shape.crypto_buffer as u64;
    assert_eq!(node_bound(&shape, &unit_factors(), 1), Ok(per_conn));
    assert_eq!(node_bound(&shape, &unit_factors(), 2), Ok(2 * per_conn));
}

/// Local bidi streams are not counted by the peer stream limits, so a small
/// peer stream count must not shrink the receive term below the conn window.
#[test]
fn receive_term_is_the_connection_window_even_with_few_peer_streams() {
    let shape = TransportShape {
        bidi_streams: 1,
        uni_streams: 1,
        stream_window: 10,
        connection_window: 1_000,
        send_window: 0,
        datagram_rx: 0,
        datagram_tx: 0,
        crypto_buffer: 0,
    };
    assert_eq!(node_bound(&shape, &unit_factors(), 1), Ok(1_000));
}

#[test]
fn factors_round_up_and_constants_are_added() {
    let shape = TransportShape {
        bidi_streams: 1,
        uni_streams: 0,
        stream_window: 10,
        connection_window: 10,
        send_window: 3,
        datagram_rx: 0,
        datagram_tx: 0,
        crypto_buffer: 0,
    };
    let factors = MeasuredFactors {
        rx: Ratio::new(3, 2).unwrap(),
        tx: Ratio::new(1, 2).unwrap(),
        datagram_rx: ONE,
        per_connection: 7,
        per_endpoint: 100,
    };
    // rx ceil(10*3/2)=15, tx ceil(3/2)=2, +7 per conn; 2 conns + 100.
    assert_eq!(node_bound(&shape, &factors, 2), Ok(2 * (15 + 2 + 7) + 100));
}

#[test]
fn overflow_is_typed_not_wrapped() {
    let shape = TransportShape {
        bidi_streams: 0,
        uni_streams: 0,
        stream_window: 0,
        connection_window: 0,
        send_window: u64::MAX,
        datagram_rx: 0,
        datagram_tx: 0,
        crypto_buffer: 0,
    };
    assert_eq!(node_bound(&shape, &unit_factors(), 2), Err(BoundError::Overflow));
    assert_eq!(Ratio::new(1, 0), None);
}

#[test]
fn preflight_rejects_typed_when_required_exceeds_hard() {
    assert_eq!(preflight(100, 40, &[60]), Ok(100));
    assert_eq!(
        preflight(100, 40, &[30, 31]),
        Err(PreflightRejection::OverBudget {
            required: 101,
            hard: 100
        })
    );
    assert_eq!(
        preflight(u64::MAX, 1, &[u64::MAX]),
        Err(PreflightRejection::Overflow)
    );
}

/// Current-config characterization: one honest generation already exceeds
/// the 32MiB hard budget the Native Service tests use (A2 stop-line finding).
#[test]
fn current_config_exceeds_service_test_budget_even_at_unit_factors() {
    let shape = TransportShape::tuic(MtuPolicy::Default);
    let node = node_bound(&shape, &unit_factors(), 1).unwrap();
    assert!(matches!(
        preflight(32 << 20, 0, &[node]),
        Err(PreflightRejection::OverBudget { .. })
    ));
}

#[test]
fn reserve_is_twice_all_generations_and_checked() {
    assert_eq!(protocol_reserve(10, 2), Ok(40));
    assert_eq!(protocol_reserve(u64::MAX, 1), Err(BoundError::Overflow));
}

/// A2 step 3 recorded observations (counting allocator, real Node endpoint,
/// `amplification::measure_amplification`, 2026-11-05): honest whole-window
/// fills on the Service-test shape and on the production shape. The unit
/// preflight omitted c_conn and understated the small-shape fill; the
/// measured factors must cover every honest observation.
#[test]
fn recorded_honest_peaks_fit_the_measured_bound() {
    let small = TransportShape {
        bidi_streams: 8,
        uni_streams: 64,
        stream_window: 256 * 1024,
        connection_window: 1024 * 1024,
        send_window: 1024 * 1024,
        datagram_rx: 64 * 1024,
        datagram_tx: 64 * 1024,
        crypto_buffer: 16 * 1024,
    };
    let production = TransportShape::tuic(MtuPolicy::Default);
    // (shape, observed peak bytes of one connection incl. c_conn)
    let observed = [
        (small, 2_907_534u64),  // Bulk receive fill
        (small, 2_749_706),     // Duplicate
        (small, 2_636_934),     // TxStormBulk send fill
        (small, 1_688_997),     // Datagrams
        (production, 39_893_202), // Rebind (max honest receive)
        (production, 35_409_161), // TxStormBulk
        (production, 3_238_759),  // Datagrams
    ];
    let unit = node_bound(&small, &MeasuredFactors::UNMEASURED_UNIT, 1).unwrap();
    assert!(observed[0].1 > unit, "unit bound {unit} must be shown insufficient");
    for (shape, peak) in observed {
        let bound = node_bound(&shape, &MeasuredFactors::A2_MEASURED, 1).unwrap();
        assert!(peak <= bound, "observed {peak} > measured bound {bound}");
    }
}
