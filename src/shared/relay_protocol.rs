use crate::shared::{ClientError, TargetAddr};
use tokio::io::{
    AsyncBufReadExt,
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
    BufReader,
};

pub const FAKE_HTTP_HEADER: &[u8; 38] = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";

/// Relay request exchanged over a Yamux substream after the fake HTTP header.
/// 中文要点：阶段一沿用文本协议，后续可升级为二进制帧，但外层调用接口保持不变。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayRequest {
    Tcp { target: TargetAddr },
    Udp { target: Option<TargetAddr> },
}

/// Write the shared relay request payload.
/// 中文要点：先写伪装头，再写一行文本请求。
pub async fn write_relay_request<W>(
    writer: &mut W,
    request: &RelayRequest,
) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(FAKE_HTTP_HEADER).await?;

    let line = match request {
        RelayRequest::Tcp { target } => format!("TCP {}\n", target.to_wire_string()),
        RelayRequest::Udp {
            target: Some(target),
        } => format!("UDP {}\n", target.to_wire_string()),
        RelayRequest::Udp { target: None } => "UDP\n".to_string(),
    };

    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Read and validate a relay request.
/// 中文要点：校验伪装头后，再解析一行请求文本。
pub async fn read_relay_request<R>(reader: &mut R) -> Result<RelayRequest, ClientError>
where
    R: AsyncRead + Unpin,
{
    let mut magic_buf = [0u8; 38];
    reader.read_exact(&mut magic_buf).await?;

    if &magic_buf != FAKE_HTTP_HEADER {
        return Err(ClientError::InvalidRelayRequest(
            "fake header mismatch".to_string(),
        ));
    }

    let mut buffered = BufReader::new(reader);
    let mut line = String::new();
    let bytes_read = buffered.read_line(&mut line).await?;

    if bytes_read == 0 {
        return Err(ClientError::InvalidRelayRequest(
            "empty relay request".to_string(),
        ));
    }

    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    if line == "UDP" {
        return Ok(RelayRequest::Udp { target: None });
    }

    if let Some(target) = line.strip_prefix("TCP ") {
        return Ok(RelayRequest::Tcp {
            target: TargetAddr::parse(target)?,
        });
    }

    if let Some(target) = line.strip_prefix("UDP ") {
        return Ok(RelayRequest::Udp {
            target: Some(TargetAddr::parse(target)?),
        });
    }

    Err(ClientError::InvalidRelayRequest(line.to_string()))
}
