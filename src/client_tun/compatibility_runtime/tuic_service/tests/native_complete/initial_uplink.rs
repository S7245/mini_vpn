//! Original Service/QUIC and two actual App sockets. Only the second initial
//! allocation is refused; the first published Flow must keep carrying DATA.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn hard_initial_allocation_refusal_keeps_other_published_flow_running() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 19).await;
}

pub(super) async fn reject_and_continue(
    app: &mut Apps, stream: &mut (quinn::SendStream, quinn::RecvStream),
    owner: &Arc<SessionControl>,
) {
    let original = owner.flow_contracts()[0].id;
    app.enable_second_tcp();
    loop {
        app.turn().await;
        assert!(!owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire));
        let flow = owner.flow_observation().unwrap();
        if owner.initial_uplink_refusal().is_some() && flow.released == 1 {
            assert_eq!((flow.published, flow.running, flow.factory_started), (1, 5, 1));
            break;
        }
        tokio::task::yield_now().await;
    }
    owner.assert_initial_uplink_refusal();
    assert_eq!(owner.flow_contracts().len(), 1);
    assert_eq!(owner.flow_contracts()[0].id, original);
    assert!(app.second_reply.is_empty());
    let marker = b"original-flow-after-neighbour-allocation-failure";
    assert_eq!(app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).send_slice(marker).unwrap(), marker.len());
    let mut actual = vec![0; marker.len()];
    tokio::select! {
        result = stream.1.read_exact(&mut actual) => result.unwrap(),
        _ = async { loop { app.turn().await; tokio::task::yield_now().await; } } => unreachable!(),
    }
    assert_eq!(actual, marker);
    stream.0.write_all(marker).await.unwrap();
    while app.tcp_reply.len() < 8192 + marker.len() {
        app.turn().await;
        tokio::task::yield_now().await;
    }
    assert_eq!(&app.tcp_reply[8192..], marker);
    assert!(!owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire));
    owner.assert_initial_uplink_refusal();
}
