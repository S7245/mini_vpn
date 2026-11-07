//! A real encrypted RETIRE interrupts Native reply settlement under both locks.
use super::super::*;
use crate::{AsyncTimer, ClientConfig, Endpoint, EndpointConfig, ServerConfig, TokioRuntime};
use std::{any::Any, panic::AssertUnwindSafe, sync::atomic::AtomicUsize};
use tokio::time::timeout;

async fn catch_poll<F: Future + ?Sized>(mut future: Pin<Box<F>>) -> Result<F::Output, Box<dyn Any + Send>> {
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }).await
}

#[derive(Debug)]
struct CaptureRuntime {
    next: AtomicUsize,
    panics: Arc<std::sync::Mutex<Vec<(usize, Box<dyn Any + Send>)>>>,
}
impl Runtime for CaptureRuntime {
    fn new_timer(&self, i: Instant) -> Pin<Box<dyn AsyncTimer>> {
        TokioRuntime.new_timer(i)
    }
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        let ordinal = self.next.fetch_add(1, Ordering::Relaxed);
        let panics = self.panics.clone();
        TokioRuntime.spawn(Box::pin(async move {
            if let Err(payload) = catch_poll(future).await {
                panics.lock().unwrap().push((ordinal, payload));
            }
        }));
    }
    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        TokioRuntime.wrap_udp_socket(socket)
    }
}
struct PanicHmac {
    armed: Arc<AtomicBool>,
    original: Arc<()>,
}
impl proto::crypto::HmacKey for PanicHmac {
    fn sign(&self, data: &[u8], out: &mut [u8]) {
        if self.armed.load(Ordering::Relaxed) {
            std::panic::panic_any(self.original.clone());
        }
        let tag = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13().unwrap().hkdf_provider
            .hmac_sign(&rustls::crypto::tls13::OkmBlock::new(&[0x53; 32]), data);
        out.copy_from_slice(tag.as_ref());
    }
    fn signature_len(&self) -> usize { 32 }
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<(), proto::crypto::CryptoError> {
        let mut expected = [0; 32];
        self.sign(data, &mut expected);
        if expected.as_slice() == signature { Ok(()) } else { Err(proto::crypto::CryptoError) }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn encrypted_retire_panic_keeps_original_payload_and_poisoned_state_fails_closed() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let server_config = ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();
    let server = Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let budget = bytes::BackingBudget::new(8 * 1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let armed = Arc::new(AtomicBool::new(false));
    let original = Arc::new(());
    let panics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let runtime = Arc::new(CaptureRuntime { next: AtomicUsize::new(0), panics: panics.clone() });
    let mut config = EndpointConfig::default();
    config.reset_key(Arc::new(PanicHmac { armed: armed.clone(), original: original.clone() }));
    let socket = runtime.wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
    let mut client = Endpoint::new_with_metered_abstract_socket(config, None, socket, runtime, meter.clone()).unwrap();
    let endpoint_drain = client.take_driver_drain_handle().unwrap();
    client.set_default_client_config(ClientConfig::with_root_certificates(Arc::new(roots)).unwrap());
    let mut connecting = client.connect(server.local_addr().unwrap(), "localhost").unwrap();
    let drain = connecting.take_drain_handle().unwrap();
    let (connection, peer) = tokio::join!(async { connecting.await.unwrap() }, async { server.accept().await.unwrap().await.unwrap() });
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    send.write_all(b"before").await.unwrap();
    let (mut peer_send, mut peer_recv) = peer.accept_bi().await.unwrap();
    peer_recv.read_exact(&mut [0; 6]).await.unwrap();
    peer_send.write_all(b"ready").await.unwrap();
    recv.read_exact(&mut [0; 5]).await.unwrap();
    timeout(Duration::from_secs(3), async {
        while send.progress_handle().sample().unwrap().acknowledged_bytes != 6 {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    armed.store(true, Ordering::Relaxed);
    {
        // Invoke the real peer's public protocol migration operation, keeping
        // this fault test on the original loopback path. Its driver encrypts
        // and transmits the resulting RETIRE; no client event is fabricated.
        let mut state = peer.0.state.lock("peer real CID rotation");
        let now = state.runtime.now();
        assert!(state.inner.try_local_address_changed(now));
        state.wake();
    }
    let observed = timeout(Duration::from_secs(3), async {
        while panics.lock().unwrap().is_empty() { tokio::task::yield_now().await; }
    }).await;
    if observed.is_err() {
        eprintln!("reply-panic stimulus: client={:?} peer={:?} budgetQ={} clientstats={:?} peerstats={:?}", connection.close_reason(), peer.close_reason(), budget.quarantined(), connection.stats(), peer.stats());
    }
    observed.unwrap();
    let (ordinal, payload) = panics.lock().unwrap().pop().unwrap();
    let original_preserved = payload.downcast_ref::<Arc<()>>().is_some_and(|p| Arc::ptr_eq(p, &original));
    // Invoke public operations after the Runtime caught the original provider panic.
    let mut output = [0x91; 32];
    let export_after_panic = connection.export_keying_material(&mut output, b"quarantine-probe", b"same-session");
    let output_untouched = output == [0x91; 32];
    let after = catch_poll(Box::pin(async {
        let failure = connection.open_uni().await.unwrap_err();
        connection.close(0u32.into(), b"after poison");
        assert_eq!(connection.close_reason(), Some(failure));
    })).await;
    let metadata = connection.0.state.lock("retained original stream layouts")
        .inner.retained_stream_metadata_bytes().expect("actual layouts and original tokens must agree");
    assert!(metadata > 1, "unknown interruption retains real stream metadata, not only a Q sentinel");
    drop((send, recv, peer_send, peer_recv));
    peer.close(0u32.into(), b"peer cleanup");
    server.close(0u32.into(), b"peer cleanup");
    drop((connection, peer, client, server));
    let drained = timeout(Duration::from_secs(3), drain.drained()).await.unwrap();
    let endpoint_drained = timeout(Duration::from_secs(3), endpoint_drain.drained()).await.unwrap();
    assert!(original_preserved && ordinal == 1, "panic must originate in the actual Connection driver, not Endpoint receive");
    assert!(after.is_ok(), "poison observation/close must not panic a second time");
    assert!(export_after_panic.is_err() && output_untouched, "poison must reject TLS mutation without writing output");
    assert!(!drained && !endpoint_drained);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), metadata);
    let backing = meter.snapshot();
    assert_eq!((backing.allocations, backing.payload_bytes, backing.physical_bytes), (0, 0, 0));
}
