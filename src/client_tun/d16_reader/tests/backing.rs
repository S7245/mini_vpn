//! Source meter mismatch retains the original physical backing in the joined reader outcome.
use super::*;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn root_reader_retains_foreign_backing_and_quarantines_both_authorities() {
    let expected = bytes::BackingMeter::new();
    let foreign = bytes::BackingMeter::new();
    let mut packet = bytes::BytesMut::with_capacity(16);
    packet.extend_from_slice(b"foreign!");
    let chunk = packet.split_to(8);
    assert!(chunk.charge_shared_backing(&foreign));
    drop(packet);
    let bytes = chunk.freeze();
    let backing = bytes.as_ptr();
    let (queue, budget) = queue();
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(4);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let (_issuer, mut accounting) = paired_accounting(std::sync::Arc::new(crate::shared::errors::FailureOrder::default()));
    accounting.backing_meter = Some(expected.clone());
    gate_tx.send(()).unwrap();
    let outcome = run_d16_native_reader(
        handle(),
        7,
        Box::new(GatedReader {
            gate: gate_rx,
            chunk: Some(NativeTcpChunk { offset: 0, bytes }),
        }),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    )
    .await;
    assert_eq!(outcome.reason, ("root", "d16_backing_mismatch"));
    assert_eq!(outcome.retained.as_ref().unwrap().as_ptr(), backing);
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
    assert_eq!(budget.snapshot().total_bytes(), 0);
    drop(outcome);
    assert_eq!(foreign.snapshot().physical_bytes, 0);
}
