//! Actual saved Service cold policy admission and original Device settlement.
//! The peer witnesses forbidden transport creation; no fake endpoint/role.
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_d16_policy_refuses_before_node() { refusal(true, false).await; }
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_generic_node_with_direct_runtime_refuses_before_node() { refusal(false, false).await; }

#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_direct_node_with_d16_runtime_refuses_before_node() { refusal(false, true).await; }

async fn refusal(d16: bool, runtime_d16: bool) {
    let identity = PublicCa::new();
    let server = quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let actual_policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        d16_byte_owned: d16.then_some("1"), tuic_native_chunk: runtime_d16.then_some("1"), ..Default::default()
    }));
    // Deliberately valid-looking Runtime cannot qualify a different Node carrier.
    let runtime_policy = if runtime_d16 {
        Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            d16_byte_owned: Some("1"), ..Default::default()
        }))
    } else if d16 { actual_policy.clone() } else {
        Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            tuic_native_chunk: Some("1"), ..Default::default()
        }))
    };
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(actual_policy);
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap()
        .with_relay_policy(runtime_policy).with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(32 << 20), 64).unwrap();
    let gate = Arc::new(device::Gate::default()); gate.open();
    let actual = gate.clone();
    let mut service = prepare_factory(move |meter: Option<bytes::BackingMeter>| async move {
        Ok::<_, ClientError>(device::GatedDevice {
            inner: LoopbackTunDevice::native_with_factory(64, 1500, &meter.unwrap(), || {
                Ok((PacketLink::bounded(64), PacketLink::bounded(64)))
            })?, gate: actual,
        })
    }, Configuration { tuic: cfg, composition: () }, runtime, Arc::new(Metrics::default()), NoopSink);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let mut peer = None;
    let observed = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::select! {
            report = service.run() => Some(report),
            remote = async { server.accept().await.unwrap().await.unwrap() } => {
                peer = Some(remote); None
            },
        }
    }).await;
    let autonomous = matches!(&observed, Ok(Some(_)));
    let report = match observed {
        Ok(Some(report)) => report,
        _ => {
            handle.request_stop();
            let deadline = saved.deadline.lock().unwrap().unwrap();
            tokio::time::timeout_at(deadline, service.run()).await.unwrap()
        }
    };
    server.close(0u32.into(), b"hard policy acceptance finished");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle()).await.unwrap();
    // Complete physical cleanup before asserting the forbidden startup witness.
    assert!(report.device_settled());
    assert!(autonomous, "known cold rejection must settle without external stop");
    assert!(report.device_failed());
    assert_eq!(gate.joins.load(Ordering::SeqCst), 1);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(peer.is_none(), "hard unqualified Node actually completed QUIC handshake: d16={d16}");
    assert!(saved.native_node_sample.lock().unwrap().is_none(), "no Node owner before rejection");
    assert_eq!(report.node, None);
    let owner = &service.completion.as_ref().unwrap().control;
    let flow = owner.flow_observation().unwrap();
    assert_eq!((flow.factory_started, flow.published, flow.joined, flow.f), (0, 0, 0, 0));
    let ledger = owner.ledger();
    assert_eq!(ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::InvalidTarget);
    assert_eq!(ledger.first_failure.as_ref().unwrap().text_bytes(),
        b"hard Native Service requires a qualified Native TCP carrier");
    assert_eq!(ledger.additional_failures, 0);
}
