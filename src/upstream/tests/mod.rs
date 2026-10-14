use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 一个把流接到本地 echo 的假上游,验证 trait + RelayStream 能正常双向读写。
struct EchoUpstream;

#[async_trait::async_trait]
impl ProxyUpstream for EchoUpstream {
    async fn open_tcp(&self, _target: &TargetAddr) -> Result<RelayStream, ClientError> {
        let (near, far) = tokio::io::duplex(64);
        tokio::spawn(async move {
            let mut far = far;
            let mut buf = [0u8; 32];
            while let Ok(n) = far.read(&mut buf).await {
                if n == 0 || far.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        });
        Ok(Box::new(near))
    }
}

#[tokio::test]
async fn relay_stream_roundtrips_through_trait_object() {
    let up: Box<dyn ProxyUpstream> = Box::new(EchoUpstream);
    let mut s = up
        .open_tcp(&TargetAddr::IpPort("1.2.3.4:80".parse().unwrap()))
        .await
        .unwrap();
    s.write_all(b"hello-tuic").await.unwrap();
    let mut buf = [0u8; 10];
    s.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello-tuic");
}

#[tokio::test]
async fn open_tcp_relay_defaults_to_generic_stream() {
    let up: Box<dyn ProxyUpstream> = Box::new(EchoUpstream);
    let relay = up
        .open_tcp_relay(&TargetAddr::IpPort("1.2.3.4:80".parse().unwrap()))
        .await
        .unwrap();

    assert!(matches!(relay, OpenedTcpRelay::Generic(_)));
}

#[tokio::test]
async fn tcp_relay_open_diag_context_is_task_scoped() {
    assert!(current_tcp_relay_open_diag().is_none());

    let diag = TcpRelayOpenDiag {
        handle: "SocketHandle(7)".to_string(),
        epoch: 41,
    };
    let observed =
        with_tcp_relay_open_diag(diag.clone(), async { current_tcp_relay_open_diag() }).await;

    assert_eq!(observed, Some(diag));
    assert!(current_tcp_relay_open_diag().is_none());
}

/// 一个把上行 datagram 捕获进 Vec 的假上游，验证 DatagramUpstream trait 可被 mock 替代。
/// 中文要点：这是 knife1 压测 mock 上游的最小形态——send_udp 不走网络，只记账。
#[derive(Default)]
struct CapturingDatagramUpstream {
    sent: std::sync::Mutex<Vec<Vec<u8>>>,
}

#[async_trait::async_trait]
impl DatagramUpstream for CapturingDatagramUpstream {
    async fn send_udp(&self, datagram: Vec<u8>) {
        self.sent.lock().unwrap().push(datagram);
    }
}

#[tokio::test]
async fn datagram_upstream_captures_sent_datagrams() {
    let up = CapturingDatagramUpstream::default();
    up.send_udp(vec![1, 2, 3]).await;
    up.send_udp(vec![4, 5]).await;
    let sent = up.sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0], vec![1, 2, 3]);
    assert_eq!(sent[1], vec![4, 5]);
}
