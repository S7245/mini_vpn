//! Dependency counterexample, not Native acceptance. The process boundary keeps
//! both original panic diagnostics when Tokio consumes a waking last Arc.
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

struct WakeAndDrop;
impl Wake for WakeAndDrop {
    fn wake(self: Arc<Self>) {
        panic!("N1 global credit original wake failure");
    }
}
impl Drop for WakeAndDrop {
    fn drop(&mut self) {
        panic!("N1 global credit original Waker Drop failure");
    }
}

#[test]
fn proposed_tokio_credit_close_exposes_double_unwind() {
    const CHILD: &str = "N1_GLOBAL_CREDIT_CLOSE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let semaphore = tokio::sync::Semaphore::new(0);
        let mut acquire = std::pin::pin!(semaphore.acquire());
        let waker = Waker::from(Arc::new(WakeAndDrop));
        assert!(
            std::future::Future::poll(acquire.as_mut(), &mut Context::from_waker(&waker),)
                .is_pending()
        );
        // The semaphore now retains the sole original Waker Arc.
        drop(waker);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| semaphore.close()));
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "shared::bounded_channel::tests::close_probe::proposed_tokio_credit_close_exposes_double_unwind", "--nocapture"])
        .env(CHILD, "1")
        .env("RUST_BACKTRACE", "0")
        .output().unwrap();
    eprintln!(
        "original credit close child: {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            output.status.signal(),
            Some(6),
            "counterexample must reach SIGABRT, not just a test assertion"
        );
    }
    #[cfg(not(unix))]
    assert!(!output.status.success());
}
