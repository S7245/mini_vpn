//! Original SessionEgress first-source/deadline boundary, using the real Native
//! Egress owner in an explicit backend-less shell; not a full Service acceptance.
use super::*;
#[tokio::test(start_paused = true)]
async fn native_encoder_error_and_q_keep_original_first_and_absolute_deadline() {
    let (budget, meter, mut raw) = crate::device::VirtualTunDevice::test_native_egress_only_shell();
    let owner = Arc::new(SessionControl::default());
    let original_deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    {
        let mut device = SessionEgress::new(&mut raw, owner.clone());
        let result = device
            .inject_ip_packet_with(60, |_| {
                meter.quarantine();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "original typed encoder IO",
                ))
            })
            .unwrap_err();
        let first = original_source(result);
        assert_eq!(first.kind(), io::ErrorKind::InvalidData);
        assert_eq!(first.to_string(), "original typed encoder IO");
        assert!(Arc::ptr_eq(
            &first,
            owner.ledger().first_egress_io.as_ref().unwrap()
        ));
        let later = device.inject_ip_packet_with(60, |_| panic!("Q must prevent another encoder"));
        assert_eq!(later.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        let ledger = owner.ledger();
        assert!(Arc::ptr_eq(
            &first,
            ledger.first_egress_io.as_ref().unwrap()
        ));
        assert_eq!(ledger.shutdown_deadline, Some(original_deadline));
        assert_eq!(
            ledger.first_failure.as_ref().unwrap().facts().io_kind,
            Some(io::ErrorKind::InvalidData)
        );
        assert_eq!(ledger.egress_failures, 2);
    }
    assert!(raw.queued_tx_bytes() >= 60);
    std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    drop(raw);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
    assert_eq!(owner.ledger().shutdown_deadline, Some(original_deadline));
}

#[cfg(feature = "harness")]
mod native_link;

#[cfg(feature = "harness")]
mod capture;

#[cfg(feature = "harness")]
mod deadline;
