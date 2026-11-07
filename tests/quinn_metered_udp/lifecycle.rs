//! Real Endpoint/Connection driver lifetime evidence for budgeted terminal slots.
//! Unpublished attempts may abandon; these tests do not certify Native DATA.
use super::*;
use std::sync::{atomic::AtomicBool, Mutex};

#[derive(Debug)]
struct FailingReceive {
    inner: Arc<dyn quinn::AsyncUdpSocket>,
    fail: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

impl quinn::AsyncUdpSocket for FailingReceive {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        self.inner.clone().create_io_poller()
    }
    fn try_send(&self, transmit: &quinn::udp::Transmit) -> io::Result<()> {
        self.inner.try_send(transmit)
    }
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut],
        metas: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.fail.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::other("terminal driver-loss stimulus")))
        } else {
            self.inner.poll_recv(cx, bufs, metas)
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

fn client(
    socket: Arc<dyn quinn::AsyncUdpSocket>,
    runtime: Arc<dyn quinn::Runtime>,
    meter: BackingMeter,
) -> quinn::Endpoint {
    client_with_seed(socket, runtime, meter, None)
}

fn client_with_seed(
    socket: Arc<dyn quinn::AsyncUdpSocket>,
    runtime: Arc<dyn quinn::Runtime>,
    meter: BackingMeter,
    seed: Option<[u8; 32]>,
) -> quinn::Endpoint {
    let mut config = quinn::EndpointConfig::default();
    config.rng_seed(seed);
    let mut endpoint =
        quinn::Endpoint::new_with_metered_abstract_socket(config, None, socket, runtime, meter)
            .unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    endpoint.set_default_client_config(local_client_config(
        rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec()),
    ));
    endpoint
}

#[tokio::test]
async fn terminal_slots_reuse_across_twenty_four_actual_unpublished_drains() {
    let runtime = quinn::default_runtime().unwrap();
    let socket = runtime
        .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut endpoint = client(socket, runtime, meter);
    let baseline = budget.used();
    for round in 0..24 {
        let mut connecting = endpoint
            .connect(remote.local_addr().unwrap(), "localhost")
            .unwrap();
        let drain = connecting.take_drain_handle().unwrap();
        assert!(
            budget.used() > baseline,
            "round {round} did not reserve event layouts"
        );
        drain.abandon();
        drop(connecting);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), drain.drained())
                .await
                .unwrap(),
            "round {round} lost terminal receipt"
        );
        assert_eq!(
            endpoint.open_connections(),
            0,
            "round {round} retained Proto map"
        );
        assert_eq!(budget.used(), baseline, "round {round} retained slots");
        assert!(!budget.quarantined());
    }
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
}

#[derive(Debug, PartialEq, Eq)]
struct TerminalTrace {
    deltas: Vec<usize>,
    peaks: [usize; 2],
}

// Calibration and acceptance execute the same configured sequence, including
// original facade rejection before RNG/TLS and actual two-batch driver drains.
async fn terminal_batches(
    endpoint: &quinn::Endpoint,
    remote: SocketAddr,
    budget: &BackingBudget,
) -> TerminalTrace {
    let baseline = budget.used();
    let mut trace = TerminalTrace {
        deltas: Vec::new(),
        peaks: [0; 2],
    };
    for batch in 0..2 {
        let mut drains = Vec::new();
        for _ in 0..19 {
            let before = budget.used();
            let mut connecting = endpoint.connect(remote, "localhost").unwrap();
            trace.deltas.push(budget.used() - before);
            drains.push(connecting.take_drain_handle().unwrap());
            drop(connecting);
        }
        assert_eq!(endpoint.open_connections(), 19);
        trace.peaks[batch] = budget.used();
        let before = budget.used();
        assert!(matches!(
            endpoint.connect(remote, "localhost"),
            Err(quinn::ConnectError::EventCapacityUnavailable)
        ));
        assert_eq!(endpoint.open_connections(), 19);
        assert_eq!(
            budget.used(),
            before,
            "20th facade refusal cannot consume physical inventory"
        );
        for drain in &drains {
            drain.abandon();
        }
        // All original 19 terminal slots are occupied before the driver runs.
        for drain in drains {
            assert!(
                tokio::time::timeout(Duration::from_secs(2), drain.drained())
                    .await
                    .unwrap()
            );
        }
        assert_eq!(endpoint.open_connections(), 0, "batch {batch}");
        assert_eq!(budget.used(), baseline, "batch {batch}");
        assert!(!budget.quarantined());
    }
    trace
}

#[tokio::test]
async fn all_nineteen_terminal_slots_settle_and_reuse_in_two_batches() {
    const SEED: [u8; 32] = [0x91; 32];
    let runtime = quinn::default_runtime().unwrap();
    let remote_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let remote = remote_socket.local_addr().unwrap();
    let calibration_socket = runtime
        .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    // A wide test allowance admits the full known 19-instance sequence. Its
    // observed peaks bound this fixed seed/fixture only, never production TLS.
    let calibration_budget = BackingBudget::new(32 * 1024 * 1024).unwrap();
    let mut calibration = client_with_seed(
        calibration_socket,
        runtime.clone(),
        BackingMeter::with_budget(&calibration_budget),
        Some(SEED),
    );
    let expected = terminal_batches(&calibration, remote, &calibration_budget).await;
    let capacity = *expected.peaks.iter().max().unwrap();
    let driver = calibration.take_driver_drain_handle().unwrap();
    drop(calibration);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap()
    );
    assert_eq!(calibration_budget.used(), 0);
    let socket = runtime
        .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let budget = BackingBudget::new(capacity).unwrap();
    let mut endpoint = client_with_seed(
        socket,
        runtime,
        BackingMeter::with_budget(&budget),
        Some(SEED),
    );
    let actual = terminal_batches(&endpoint, remote, &budget).await;
    assert_eq!(
        actual, expected,
        "same configured RNG sequence must reproduce each actual layout"
    );
    eprintln!("terminal19 fixed-seed calibration/actual={actual:?} budget={capacity}");
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn queued_terminal_lost_with_endpoint_driver_keeps_quarantine() {
    let runtime = quinn::default_runtime().unwrap();
    let socket = Arc::new(FailingReceive {
        inner: runtime
            .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
            .unwrap(),
        fail: AtomicBool::new(false),
        waker: Mutex::new(None),
    });
    let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut endpoint = client(socket.clone(), runtime, meter.clone());
    let mut connecting = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let connection = connecting.take_drain_handle().unwrap();
    let driver = endpoint.take_driver_drain_handle().unwrap();
    // These synchronous operations enqueue the real Drained witness before
    // the Endpoint driver can first poll. Its receive stage then fails before
    // handle_events can acknowledge that witness.
    connection.abandon();
    drop(connecting);
    socket.fail.store(true, Ordering::Release);
    assert!(
        !tokio::time::timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap()
    );
    assert!(
        !tokio::time::timeout(Duration::from_secs(2), connection.drained())
            .await
            .unwrap()
    );
    assert!(
        meter.snapshot().quarantined,
        "discarded queued terminal cannot certify clean"
    );
    assert!(budget.quarantined());
    drop(endpoint);
    assert!(budget.quarantined());
}

#[tokio::test]
async fn actual_endpoint_driver_io_loss_rejects_connection_release_and_keeps_q() {
    let runtime = quinn::default_runtime().unwrap();
    let socket = Arc::new(FailingReceive {
        inner: runtime
            .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
            .unwrap(),
        fail: AtomicBool::new(false),
        waker: Mutex::new(None),
    });
    let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut endpoint = client(socket.clone(), runtime, meter.clone());
    let mut connecting = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let connection = connecting.take_drain_handle().unwrap();
    let driver = endpoint.take_driver_drain_handle().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while socket.waker.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    socket.fail.store(true, Ordering::Release);
    socket.waker.lock().unwrap().take().unwrap().wake();
    assert!(
        !tokio::time::timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap()
    );
    assert!(!endpoint.wait_idle_or_driver_lost().await);
    drop(connecting);
    assert!(
        !tokio::time::timeout(Duration::from_secs(2), connection.drained())
            .await
            .unwrap()
    );
    assert!(meter.snapshot().quarantined);
    assert!(budget.quarantined());
    drop(endpoint);
    assert!(
        budget.quarantined(),
        "actual reference destruction cannot erase sticky Q"
    );
}

// Calibrate the actual cold owners before the two-batch slot test. This is a
// fixture sizing observation, not a production RSS or whole-connection bound.
pub(super) async fn measure_unpublished_cold_layout(
    runtime: Arc<dyn quinn::Runtime>,
    remote: SocketAddr,
) -> (usize, usize) {
    let socket = runtime
        .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let mut endpoint = client(socket, runtime, BackingMeter::with_budget(&budget));
    let baseline = budget.used();
    let mut connecting = endpoint.connect(remote, "localhost").unwrap();
    let per_connection = budget.used() - baseline;
    let drain = connecting.take_drain_handle().unwrap();
    drain.abandon();
    drop(connecting);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap()
    );
    assert_eq!(endpoint.open_connections(), 0);
    assert_eq!(budget.used(), baseline);
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    (baseline, per_connection)
}
