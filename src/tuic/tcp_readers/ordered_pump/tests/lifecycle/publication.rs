//! The actual ordered source publishes facts before its reader notification,
//! while its original IO remains in the original pump through the real JOIN.
use super::*;
use crate::shared::errors::{FailureOrder, QuietFailureSink, RetainedFailure};

#[derive(Default)]
struct Sink(std::sync::Mutex<Option<RetainedFailure>>);
impl QuietFailureSink for Sink {
    fn record_quiet(&self, failure: RetainedFailure) {
        *self.0.lock().unwrap() = Some(failure);
    }
}

#[tokio::test]
async fn source_publishes_before_notification_without_consuming_original_io() {
    let drops = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
        OriginalFailureSource {
            io: Some(io::Error::new(
                io::ErrorKind::ConnectionReset,
                OriginalIo(drops.clone()),
            )),
            panic: None,
        },
        lease,
        None,
        None,
    );
    let sink = Arc::new(Sink::default());
    let erased: Arc<dyn QuietFailureSink> = sink.clone();
    let clock = Arc::new(FailureOrder::default());
    assert!(
        clock
            .bind(
                Arc::downgrade(&erased),
                crate::shared::errors::FailureOrigin {
                    phase: crate::shared::errors::FailurePhase::TcpOpen,
                    resource: crate::shared::errors::FailureResource::TcpOpen,
                    resource_id: 2,
                    attempt_id: Some(7),
                },
                None
            )
            .is_ok()
    );
    let mut pump = prepared.commit(Some(clock.clone()));
    let notice = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 8)),
    )
    .await
    .unwrap();
    assert!(notice.is_err());
    assert!(!pump.joined(), "notification is not JOIN");
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        sink.0.lock().unwrap().as_ref().unwrap().facts().io_kind,
        Some(io::ErrorKind::ConnectionReset)
    );
    assert_eq!(pump.error_order(), Some(0));
    assert!(clock.reported(0));
    drop(reader);
    pump.settle().await.unwrap();
    assert!(pump.joined());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "facts never dispose the original object"
    );
    let failure = pump.take_failure().unwrap();
    assert!(clock.reported(failure.order.unwrap()));
    let crate::upstream::NativePumpFailureValue::Io(error) = failure.into_value() else {
        panic!("original IO")
    };
    assert_eq!(error.to_string(), "original Native IO");
    drop(error);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
