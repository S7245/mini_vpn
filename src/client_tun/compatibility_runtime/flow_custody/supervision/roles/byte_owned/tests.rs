use super::*;
use crate::tcp_downlink_pump::DownstreamPermitReleaseMode;

#[test]
fn consumed_earlier_failure_does_not_hide_later_d16_errors() {
    let mut order = [Some(3), Some(4), Some(2)];
    assert_eq!(next_error(order), Some(2));
    order[2] = None;
    assert_eq!(next_error(order), Some(0));
    order[0] = None;
    assert_eq!(next_error(order), Some(1));
    order[1] = None;
    assert_eq!(next_error(order), None);
}

#[tokio::test]
async fn writer_close_before_reader_done_keeps_original_clean_eof() {
    let budget = D16GlobalByteBudget::new(16).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget,
    )
    .unwrap();
    assert!(queue.close_for_remote_eof_and_mark_ready().await);
    let (writer_tx, writer_rx) = mpsc::channel(1);
    let (_activity_tx, activity_rx) = mpsc::channel(1);
    let (reader_done_tx, reader_done_rx) = oneshot::channel();
    writer_tx
        .send(RelayWriterSignal::Closed {
            direction: "local_to_remote",
            reason: "local_channel_closed",
        })
        .await
        .unwrap();
    let mut monitor = Monitor {
        writer_rx,
        activity_rx,
        reader_done_rx,
        queue,
        timer: RelayCloseTimer::new(),
        writer_done: false,
        reader_done: false,
        observed_reader: None,
        reader_done_failed: false,
        activity_open: true,
        half_closed_local_progress: None,
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), monitor.run())
            .await
            .is_err()
    );
    reader_done_tx
        .send(ReaderDone {
            reason: ("remote_to_local", "remote_eof"),
            clean: true,
        })
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), monitor.run())
            .await
            .unwrap(),
        ("remote_to_local", "remote_eof")
    );
    assert!(monitor.observed_reader.is_some_and(|done| done.clean));
}
