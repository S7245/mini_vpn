//! Actual constructor and Runtime spawn boundary, before any connection exists.
use super::*;

#[derive(Debug)]
struct CountSpawn {
    inner: Arc<dyn quinn::Runtime>,
    count: Arc<AtomicUsize>,
}
impl quinn::Runtime for CountSpawn {
    fn new_timer(&self, at: std::time::Instant) -> Pin<Box<dyn quinn::AsyncTimer>> {
        self.inner.new_timer(at)
    }
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.count.fetch_add(1, Ordering::AcqRel);
        self.inner.spawn(future);
    }
    fn wrap_udp_socket(&self, socket: UdpSocket) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        self.inner.wrap_udp_socket(socket)
    }
}

#[tokio::test]
async fn embedded_hard_meter_cannot_bypass_quinn_constructor_authority() {
    for explicit in [false, true] {
        let inner = quinn::default_runtime().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let runtime = Arc::new(CountSpawn {
            inner: inner.clone(),
            count: count.clone(),
        });
        let socket = inner
            .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
            .unwrap();
        let original = BackingBudget::new(4 * 1024 * 1024).unwrap();
        let conflicting = BackingBudget::new(4 * 1024 * 1024).unwrap();
        let mut config = quinn::EndpointConfig::default();
        config.receive_backing_meter(BackingMeter::with_budget(&original));
        let result = if explicit {
            quinn::Endpoint::new_with_metered_abstract_socket(
                config,
                None,
                socket,
                runtime,
                BackingMeter::with_budget(&conflicting),
            )
        } else {
            quinn::Endpoint::new_with_abstract_socket(config, None, socket, runtime)
        };
        let accepted = result.is_ok();
        let rejected_kind = result.as_ref().err().map(io::Error::kind);
        if let Ok(mut endpoint) = result {
            let drain = endpoint.take_driver_drain_handle().unwrap();
            drop(endpoint);
            assert!(
                tokio::time::timeout(Duration::from_secs(2), drain.drained())
                    .await
                    .unwrap()
            );
        }
        assert!(!accepted, "explicit={explicit}: a second constructor authority must not bypass or overwrite the embedded owner");
        assert_eq!(rejected_kind, Some(io::ErrorKind::InvalidInput));
        assert_eq!(
            count.load(Ordering::Acquire),
            0,
            "rejection must precede driver spawn"
        );
        assert_eq!(original.used(), 0);
        assert_eq!(conflicting.used(), 0);
        assert!(!original.quarantined() && !conflicting.quarantined());
    }
}

#[tokio::test]
async fn matching_embedded_authority_installs_before_one_real_driver_spawn() {
    let inner = quinn::default_runtime().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let runtime = Arc::new(CountSpawn {
        inner: inner.clone(),
        count: count.clone(),
    });
    let socket = inner
        .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut config = quinn::EndpointConfig::default();
    config.receive_backing_meter(meter.clone());
    let mut endpoint = quinn::Endpoint::new_with_metered_abstract_socket(
        config,
        None,
        socket,
        runtime,
        meter.clone(),
    )
    .unwrap();
    assert!(endpoint
        .receive_backing_meter()
        .unwrap()
        .same_authority(&meter));
    assert_eq!(count.load(Ordering::Acquire), 1);
    assert!(budget.used() > 0);
    let drain = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
