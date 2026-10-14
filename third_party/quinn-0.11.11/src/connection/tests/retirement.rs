use super::super::*;
use crate::{ClientConfig, Endpoint, ServerConfig};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::time::timeout;

pub(super) struct Loopback {
    pub(super) endpoint: Endpoint,
    pub(super) connection: Connection,
    pub(super) peer: Connection,
}

impl Loopback {
    pub(super) async fn new() -> Self {
        Self::with_bi_limit(None).await
    }

    pub(super) async fn with_bi_limit(limit: Option<u32>) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
        let mut server =
            ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();
        if let Some(limit) = limit {
            let mut transport = proto::TransportConfig::default();
            transport.max_concurrent_bidi_streams(limit.into());
            server.transport = Arc::new(transport);
        }
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let mut endpoint =
            Endpoint::server(server, SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        endpoint.set_default_client_config(
            ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );
        let (connection, peer) = tokio::join!(
            async {
                endpoint
                    .connect(endpoint.local_addr().unwrap(), "localhost")
                    .unwrap()
                    .await
                    .unwrap()
            },
            async { endpoint.accept().await.unwrap().await.unwrap() },
        );
        Self {
            endpoint,
            connection,
            peer,
        }
    }

    pub(super) async fn close(self) {
        self.connection.close(0u32.into(), b"test complete");
        self.peer.close(0u32.into(), b"test complete");
        self.endpoint.close(0u32.into(), b"test complete");
        timeout(Duration::from_secs(2), self.endpoint.wait_idle())
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn atomic_admission_rejects_real_zero_rtt_without_allocating_a_stream() {
    let pair = Loopback::new().await;
    // A received 1-RTT server stream ensures its preceding NewSessionTicket was processed.
    let mut marker = pair.peer.open_uni().await.unwrap();
    marker.write_all(b"ticket").await.unwrap();
    marker.finish().unwrap();
    assert_eq!(
        pair.connection
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(6)
            .await
            .unwrap(),
        b"ticket"
    );
    drop(marker);
    let (early, accepted) = pair
        .endpoint
        .connect(pair.endpoint.local_addr().unwrap(), "localhost")
        .unwrap()
        .into_0rtt()
        .expect("the real previous handshake must have supplied a resumption ticket");
    assert!(early
        .0
        .state
        .lock("test early state")
        .inner
        .is_handshaking());
    for _ in 0..257 {
        assert!(early.try_open_uni_1rtt_with_retirement().is_none());
    }
    let mut probe = early.open_uni().await.unwrap();
    assert_eq!(
        probe.id().index(),
        0,
        "atomic early-data rejection must consume no stream IDs"
    );
    probe.reset(0u32.into()).unwrap();
    drop(probe);
    let (was_accepted, next_peer) = tokio::join!(accepted, async {
        pair.endpoint.accept().await.unwrap().await.unwrap()
    },);
    assert!(was_accepted);
    assert!(!early
        .0
        .state
        .lock("test established state")
        .inner
        .is_handshaking());
    let (mut established, ticket) = early.try_open_uni_1rtt_with_retirement().unwrap();
    assert_eq!(established.id().index(), 1);
    assert_eq!(ticket.stream, established.id());
    assert!(Arc::ptr_eq(
        &ticket.connection.upgrade().unwrap(),
        &early.0 .0
    ));
    assert!(!ticket.is_retired());
    assert!(early
        .0
        .state
        .lock("test no notification registry")
        .stopped
        .is_empty());
    established.reset(0u32.into()).unwrap();
    drop(established);
    early.close(0u32.into(), b"test complete");
    assert!(early.try_open_uni_1rtt_with_retirement().is_none());
    next_peer.close(0u32.into(), b"test complete");
    drop((early, next_peer));
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn weak_upgrade_failure_does_not_retire_before_the_final_destructor_token() {
    let pair = Loopback::new().await;
    let (mut stream, ticket) = pair.connection.try_open_uni_1rtt_with_retirement().unwrap();
    stream
        .write_all(b"retained until reset acknowledgement or destruction")
        .await
        .unwrap();
    stream.reset(0u32.into()).unwrap();
    drop(stream);
    assert!(!ticket.is_retired());
    // Retain only a bare Arc outside the sender: no extra public handle or self-retaining ticket.
    let retained = ticket.connection.upgrade().unwrap();
    pair.close().await;
    timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&retained) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the endpoint and driver must release every other strong reference");
    assert!(
        !ticket.is_retired(),
        "close and driver teardown do not erase retained send state"
    );
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    *retained
        .retirement_barrier
        .lock("install destruction window") = Some(super::DestructionBarrier {
        entered: entered.clone(),
        release: release.clone(),
    });
    let destroy = std::thread::spawn(move || drop(retained));
    entered.wait(); // State and Shared have dropped; the final sentinel has not.
    let weak_missing = ticket.connection.upgrade().is_none();
    let token_in_window = ticket.destroyed.load(Ordering::Acquire);
    let retired_in_window = ticket.is_retired();
    release.wait();
    destroy.join().unwrap();
    assert!(
        weak_missing,
        "the final Arc has entered the real ConnectionInner destructor"
    );
    assert!(!token_in_window);
    assert!(
        !retired_in_window,
        "Weak::upgrade(None) must not grant premature reuse"
    );
    assert!(ticket.destroyed.load(Ordering::Acquire));
    assert!(
        ticket.is_retired(),
        "the post-storage Release token finally grants reuse"
    );
}
