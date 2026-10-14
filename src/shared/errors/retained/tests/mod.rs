use super::super::ClientError;
use super::*;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct OpaqueError {
    dropped: Arc<AtomicUsize>,
    _payload: Vec<u8>,
}

impl fmt::Debug for OpaqueError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("opaque Debug must not be called during capture");
    }
}

impl fmt::Display for OpaqueError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("opaque Display must not be called during capture");
    }
}

impl std::error::Error for OpaqueError {}

impl Drop for OpaqueError {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn captures_opaque_io_facts_without_formatting_or_retaining_payload() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let error = io::Error::new(
        io::ErrorKind::ConnectionReset,
        OpaqueError {
            dropped: Arc::clone(&dropped),
            _payload: vec![0x5a; 64 * 1024],
        },
    );
    let retained = RetainedFailure::from_io(
        FailureOrigin {
            phase: FailurePhase::DeviceRead,
            resource: FailureResource::TunPump,
            resource_id: 17,
            attempt_id: Some(3),
        },
        error,
    );

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(retained.facts().class, FailureClass::Io);
    assert_eq!(
        retained.facts().io_kind,
        Some(io::ErrorKind::ConnectionReset)
    );
    assert_eq!(retained.facts().origin.phase, FailurePhase::DeviceRead);
    assert_eq!(retained.facts().origin.resource, FailureResource::TunPump);
    assert_eq!(retained.facts().origin.resource_id, 17);
    assert_eq!(retained.facts().origin.attempt_id, Some(3));
    assert_eq!(retained.facts().raw_os_code, None);
    assert_eq!(retained.facts().source_len, None);
    assert!(retained.facts().opaque);
    assert!(retained.facts().source_content_omitted);
    assert!(!retained.facts().truncated);
    assert!(retained.text_bytes().is_empty());
    assert!(std::mem::size_of::<RetainedFailure>() <= 1152);
    println!(
        "RetainedFailure size: {} bytes",
        std::mem::size_of::<RetainedFailure>()
    );
}

#[test]
fn known_text_retains_utf8_prefix_and_marks_loss() {
    for text in [format!("{}界tail", "a".repeat(1023)), "界".repeat(342)] {
        let original_len = text.len();
        let expected_prefix = text.as_bytes()[..1023].to_vec();
        let retained = RetainedFailure::from_client(
            FailureOrigin {
                phase: FailurePhase::Startup,
                resource: FailureResource::Session,
                resource_id: 8,
                attempt_id: None,
            },
            ClientError::InvalidTarget(text),
        );

        assert_eq!(retained.facts().class, FailureClass::InvalidTarget);
        assert_eq!(retained.facts().source_len, Some(original_len));
        assert_eq!(retained.text_bytes(), expected_prefix);
        assert!(std::str::from_utf8(retained.text_bytes()).is_ok());
        assert!(retained.facts().truncated);
        assert!(retained.facts().source_content_omitted);
        assert!(!retained.facts().opaque);
    }
}

#[test]
fn retains_raw_os_code_and_kind_without_synthesizing_text() {
    let error = io::Error::from_raw_os_error(13);
    let kind = error.kind();
    let retained = RetainedFailure::from_io(
        FailureOrigin {
            phase: FailurePhase::DeviceRead,
            resource: FailureResource::TunPump,
            resource_id: 4,
            attempt_id: None,
        },
        error,
    );

    assert_eq!(retained.facts().raw_os_code, Some(13));
    assert_eq!(retained.facts().io_kind, Some(kind));
    assert!(retained.text_bytes().is_empty());
    assert!(!retained.facts().opaque);
    assert!(!retained.facts().source_content_omitted);
    assert!(!retained.facts().truncated);
}

#[test]
fn client_variants_preserve_known_text_and_delegate_io_facts() {
    let at_capacity = "x".repeat(1024);
    for (error, class, expected) in [
        (
            ClientError::InvalidTarget(String::new()),
            FailureClass::InvalidTarget,
            "",
        ),
        (
            ClientError::Reality("TLS refused".into()),
            FailureClass::Reality,
            "TLS refused",
        ),
        (
            ClientError::InvalidTarget(at_capacity.clone()),
            FailureClass::InvalidTarget,
            at_capacity.as_str(),
        ),
    ] {
        let retained = RetainedFailure::from_client(
            FailureOrigin {
                phase: FailurePhase::Startup,
                resource: FailureResource::Session,
                resource_id: 8,
                attempt_id: None,
            },
            error,
        );
        assert_eq!(retained.facts().class, class);
        assert_eq!(retained.facts().source_len, Some(expected.len()));
        assert_eq!(retained.text_bytes(), expected.as_bytes());
        assert!(!retained.facts().truncated);
        assert!(!retained.facts().opaque);
        assert!(!retained.facts().source_content_omitted);
    }
    let retained = RetainedFailure::from_client(
        FailureOrigin {
            phase: FailurePhase::DeviceRead,
            resource: FailureResource::TunPump,
            resource_id: 5,
            attempt_id: None,
        },
        ClientError::Io(io::Error::from_raw_os_error(2)),
    );
    assert_eq!(retained.facts().class, FailureClass::Io);
    assert_eq!(retained.facts().raw_os_code, Some(2));
}

#[test]
fn invalid_utf8_keeps_position_and_discards_original_bytes() {
    for (bytes, error_len) in [(vec![b'a', 0xff], Some(1)), (vec![b'a', 0xe2, 0x82], None)] {
        let original_len = bytes.len();
        let error = String::from_utf8(bytes).expect_err("fixture is invalid UTF-8");
        let retained = RetainedFailure::from_client(
            FailureOrigin {
                phase: FailurePhase::Startup,
                resource: FailureResource::Session,
                resource_id: 8,
                attempt_id: None,
            },
            ClientError::Utf8(error),
        );

        assert_eq!(retained.facts().class, FailureClass::Utf8);
        assert_eq!(retained.facts().source_len, Some(original_len));
        assert_eq!(retained.facts().utf8_valid_up_to, Some(1));
        assert_eq!(retained.facts().utf8_error_len, error_len);
        assert!(retained.text_bytes().is_empty());
        assert!(retained.facts().source_content_omitted);
        assert!(!retained.facts().opaque);
        assert!(!retained.facts().truncated);
    }
}

#[tokio::test]
async fn real_join_panic_releases_opaque_payload_and_keeps_origin() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let task_dropped = Arc::clone(&dropped);
    let error = tokio::spawn(async move {
        std::panic::panic_any(OpaqueError {
            dropped: task_dropped,
            _payload: vec![0x5a; 64 * 1024],
        });
    })
    .await
    .expect_err("fixture task panics");
    assert!(error.is_panic());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    let retained = RetainedFailure::from_join(
        FailureOrigin {
            phase: FailurePhase::DeviceJoin,
            resource: FailureResource::TunPump,
            resource_id: 19,
            attempt_id: None,
        },
        error,
    );

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(retained.facts().class, FailureClass::Panic);
    assert_eq!(retained.facts().origin.resource_id, 19);
    assert!(retained.facts().opaque);
    assert!(retained.facts().source_content_omitted);
    assert!(retained.text_bytes().is_empty());
}

#[tokio::test]
async fn real_join_cancel_distinguishes_cancellation_after_payload_drop() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let task_dropped = Arc::clone(&dropped);
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let guard = OpaqueError {
            dropped: task_dropped,
            _payload: vec![0x5a; 64 * 1024],
        };
        started.send(()).expect("start receiver remains alive");
        std::future::pending::<()>().await;
        drop(guard);
    });
    ready.await.expect("task must own the payload before abort");
    task.abort();
    let error = task.await.expect_err("fixture task is cancelled");
    assert!(error.is_cancelled());
    let retained = RetainedFailure::from_join(
        FailureOrigin {
            phase: FailurePhase::DeviceJoin,
            resource: FailureResource::TunPump,
            resource_id: 21,
            attempt_id: None,
        },
        error,
    );

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(retained.facts().class, FailureClass::Cancelled);
    assert_eq!(retained.facts().origin.resource_id, 21);
    assert!(!retained.facts().opaque);
    assert!(!retained.facts().source_content_omitted);
    assert!(retained.text_bytes().is_empty());
}

#[test]
fn caught_panic_payload_is_consumed_at_the_conversion_boundary() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let payload = std::panic::catch_unwind(|| {
        std::panic::panic_any(OpaqueError {
            dropped: Arc::clone(&dropped),
            _payload: vec![0x5a; 64 * 1024],
        });
    })
    .expect_err("fixture unwinds into the original boundary");
    let retained = RetainedFailure::from_panic(
        FailureOrigin {
            phase: FailurePhase::DeviceRead,
            resource: FailureResource::TunPump,
            resource_id: 27,
            attempt_id: Some(1),
        },
        payload,
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(retained.facts().class, FailureClass::Panic);
    assert_eq!(retained.facts().origin.phase, FailurePhase::DeviceRead);
    assert!(retained.facts().opaque);
    assert!(retained.facts().source_content_omitted);
    assert!(retained.text_bytes().is_empty());
}
