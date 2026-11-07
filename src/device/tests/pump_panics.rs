//! Actual source poll plus source Drop failure, isolated to preserve abort evidence.
use super::*;

struct DualPanicReader;
impl AsyncRead for DualPanicReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        panic!("original Device read panic");
    }
}
impl Drop for DualPanicReader {
    fn drop(&mut self) {
        panic!("later Device source Drop panic");
    }
}

#[test]
fn original_read_and_source_drop_panics_keep_first_and_actual_join() {
    const CHILD: &str = "N1_DEVICE_DUAL_PANIC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("device::tests::pump_panics::original_read_and_source_drop_panics_keep_first_and_actual_join")
            .arg("--nocapture").env(CHILD, "1").output().unwrap();
        eprintln!(
            "child status: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "actual original reader double panic must be settled without process abort"
        );
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut pump = spawn_tun_ingress_pump(DualPanicReader, 1200, 1);
            let shutdown = tokio::time::timeout(Duration::from_secs(1), pump.observe_join())
                .await
                .unwrap();
            assert_eq!(shutdown, PumpExit::Failed(PumpFailureId));
            assert!(matches!(pump.task, PumpTask::Observed(_)));
            let failure = pump.take_failure().unwrap();
            assert_eq!(failure.facts().origin.phase, FailurePhase::DeviceRead);
            assert_eq!(
                failure.facts().class,
                crate::shared::errors::FailureClass::Panic
            );
            assert_eq!(pump.control.additional_failures.load(Ordering::Relaxed), 1);
            assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), 0);
            assert_eq!(pump.control.stats.closed.load(Ordering::Relaxed), 1);
        });
}

struct ConstructorPanicSource;
impl IngressPacketSource for ConstructorPanicSource {
    fn read_packet(
        &mut self,
        _: &mut PumpPacket,
    ) -> std::future::Ready<std::io::Result<SourceRead>> {
        panic!("original synchronous read construction");
    }
}
impl Drop for ConstructorPanicSource {
    fn drop(&mut self) {
        panic!("later source destruction");
    }
}
#[tokio::test]
async fn synchronous_read_construction_keeps_first_before_source_drop() {
    let mut pump = TunIngressPump::spawn_source(ConstructorPanicSource, 1);
    let exit = tokio::time::timeout(Duration::from_secs(1), pump.observe_join())
        .await
        .unwrap();
    let failure = pump.take_failure().unwrap();
    assert_eq!(exit, PumpExit::Failed(PumpFailureId));
    assert_eq!(failure.facts().origin.phase, FailurePhase::DeviceRead);
    assert_eq!(pump.control.additional_failures.load(Ordering::Relaxed), 1);
}
