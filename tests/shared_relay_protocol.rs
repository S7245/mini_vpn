use mini_vpn::shared::{
    FAKE_HTTP_HEADER,
    RelayRequest,
    TargetAddr,
    read_relay_request,
    write_relay_request,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

#[test]
fn parses_ipv4_target() {
    let target = TargetAddr::parse("127.0.0.1:7897").expect("target should parse");
    assert_eq!(target.to_wire_string(), "127.0.0.1:7897");
}

#[test]
fn parses_domain_target() {
    let target = TargetAddr::parse("www.figma.com:443").expect("target should parse");
    assert_eq!(target.to_wire_string(), "www.figma.com:443");
}

#[test]
fn rejects_missing_port() {
    let err = TargetAddr::parse("www.figma.com").expect_err("port is required");
    assert!(err.to_string().contains("invalid target address"));
}

#[tokio::test]
async fn tcp_request_round_trip() {
    let request = RelayRequest::Tcp {
        target: TargetAddr::parse("34.107.238.235:443").expect("target should parse"),
    };
    let expected = request.clone();
    let (client, server) = duplex(256);

    let writer = tokio::spawn(async move {
        let mut client = client;
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
    });

    let reader = tokio::spawn(async move {
        let mut server = server;
        read_relay_request(&mut server)
            .await
            .expect("read should succeed")
    });

    writer.await.expect("writer task should join");
    let received = reader.await.expect("reader task should join");
    assert_eq!(received, expected);
}

#[tokio::test]
async fn udp_request_round_trip() {
    let request = RelayRequest::Udp { target: None };
    let expected = request.clone();
    let (client, server) = duplex(256);

    let writer = tokio::spawn(async move {
        let mut client = client;
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
    });

    let reader = tokio::spawn(async move {
        let mut server = server;
        read_relay_request(&mut server)
            .await
            .expect("read should succeed")
    });

    writer.await.expect("writer task should join");
    let received = reader.await.expect("reader task should join");
    assert_eq!(received, expected);
}

#[tokio::test]
async fn write_relay_request_starts_with_fake_header() {
    let request = RelayRequest::Tcp {
        target: TargetAddr::parse("127.0.0.1:7897").expect("target should parse"),
    };
    let (client, mut server) = duplex(256);

    let writer = tokio::spawn(async move {
        let mut client = client;
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
    });

    let mut magic = [0u8; 38];
    server
        .read_exact(&mut magic)
        .await
        .expect("server should read fake header");

    writer.await.expect("writer task should join");
    assert_eq!(&magic, FAKE_HTTP_HEADER);
}

#[tokio::test]
async fn read_request_preserves_following_payload() {
    let request = RelayRequest::Tcp {
        target: TargetAddr::parse("127.0.0.1:7897").expect("target should parse"),
    };
    let (mut client, mut server) = duplex(256);

    let writer = tokio::spawn(async move {
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
        client
            .write_all(b"payload-after-request")
            .await
            .expect("payload write should succeed");
    });

    let received = read_relay_request(&mut server)
        .await
        .expect("read should succeed");
    assert_eq!(
        received,
        RelayRequest::Tcp {
            target: TargetAddr::parse("127.0.0.1:7897").expect("target should parse"),
        }
    );

    let mut trailing = [0u8; 21];
    server
        .read_exact(&mut trailing)
        .await
        .expect("trailing payload should remain readable");
    writer.await.expect("writer task should join");
    assert_eq!(&trailing, b"payload-after-request");
}
