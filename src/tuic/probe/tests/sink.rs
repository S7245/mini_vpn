use super::super::*;
use crate::upstream::RelayStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn original_statistics_loop_reads_actual_tcp_to_eof() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.write_all(b"original-statistics").await.unwrap();
        stream.shutdown().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert!(request.is_empty());
    };
    let client = async {
        let mut stream: RelayStream =
            Box::new(tokio::net::TcpStream::connect(address).await.unwrap());
        let report = sink::read(&mut stream, "192.0.2.1:443", 1).await.unwrap();
        assert_eq!(report.bytes, 19);
        assert!(report.reads > 0);
        assert!(report.first_rx_ms.is_some());
        assert!(report.eof);
        assert_eq!(report.target, "192.0.2.1:443");
        assert_eq!(report.requested_duration_secs, 1);
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(client, peer);
    })
    .await
    .unwrap();
}
