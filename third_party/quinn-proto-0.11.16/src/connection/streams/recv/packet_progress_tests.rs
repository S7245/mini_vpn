use super::*;

#[test]
fn packet_fin_cannot_shrink_the_preceding_frames_final_size() {
    let recv = Recv::new(100);
    let mut progress = recv.packet_progress();
    assert_eq!(progress.validate(10, 10, false, 0, 100).unwrap(), (20, 20));
    let error = progress.validate(15, 0, true, 20, 100).unwrap_err();
    assert_eq!(error.code, crate::TransportErrorCode::FINAL_SIZE_ERROR);
    assert_eq!(recv.end, 0);
    assert!(recv.final_offset().is_none());
}

#[test]
fn packet_progress_accounts_duplicates_fin_and_aggregate_credit() {
    let recv = Recv::new(100);
    let mut first = recv.packet_progress();
    let mut second = recv.packet_progress();
    assert_eq!(first.validate(10, 10, false, 0, 30).unwrap(), (20, 20));
    assert_eq!(first.validate(0, 15, false, 20, 30).unwrap(), (15, 0));
    assert_eq!(second.validate(0, 10, true, 20, 30).unwrap(), (10, 10));
    assert_eq!(
        first.validate(20, 1, false, 30, 30).unwrap_err().code,
        crate::TransportErrorCode::FLOW_CONTROL_ERROR
    );
    assert_eq!(first.validate(20, 0, true, 30, 30).unwrap(), (20, 0));
    assert_eq!(first.validate(20, 0, true, 30, 30).unwrap(), (20, 0));
    assert_eq!(
        first.validate(19, 0, true, 30, 30).unwrap_err().code,
        crate::TransportErrorCode::FINAL_SIZE_ERROR
    );
    assert_eq!(
        second.validate(10, 1, false, 30, 100).unwrap_err().code,
        crate::TransportErrorCode::FINAL_SIZE_ERROR
    );
    assert_eq!(recv.end, 0);
}
