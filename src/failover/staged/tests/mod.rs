//! Cold unsupported-contract fixtures only; real success/failure lifecycle paths
//! are tested through actual Node/QUIC/REALITY in tuic/node/tests/failover_*.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct Unsupported(AtomicU64);
#[async_trait::async_trait]
impl ProxyUpstream for Unsupported {
    async fn open_tcp(
        &self,
        _: &crate::shared::TargetAddr,
    ) -> Result<crate::upstream::RelayStream, ClientError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(ClientError::Io(io::Error::other("unexpected legacy open")))
    }
}
#[async_trait::async_trait]
impl HealthProbe for Unsupported {
    async fn probe(&self) -> bool {
        false
    }
    async fn is_dead(&self) -> bool {
        true
    }
    async fn rx_datagrams(&self) -> Option<u64> {
        None
    }
}
#[tokio::test]
async fn unsupported_selected_factory_is_cold_and_never_tries_other_leg() {
    let tuic = Arc::new(Unsupported::default());
    let reality = Arc::new(Unsupported::default());
    let factory = Arc::new(FailoverUpstream::new(tuic.clone(), reality.clone()));
    for backup in [false, true] {
        if backup {
            assert!(factory.state().record_blackhole(0));
        }
        let result = factory.clone().staged_tcp_open(TcpOpenRequest {
            target: crate::shared::TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
            deadline: Instant::now() + Duration::from_secs(20),
        });
        assert!(matches!(result, Err(TcpOpenUnavailable::Unsupported)));
        assert_eq!(tuic.0.load(Ordering::Relaxed), 0);
        assert_eq!(reality.0.load(Ordering::Relaxed), 0);
        assert_eq!(Arc::strong_count(&factory), 1);
    }
}
