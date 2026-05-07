use crate::shared::{ClientError, RelayRequest, write_relay_request};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

/// Open a Yamux substream and send the shared relay handshake.
/// 中文要点：统一负责开子流、接转换器、写入共享握手，外层不再重复发暗号和目标。
pub async fn open_remote_session(
    ctrl: &mut yamux::Control,
    request: &RelayRequest,
) -> Result<Compat<yamux::Stream>, ClientError> {
    let stream = ctrl.open_stream().await.map_err(ClientError::YamuxOpen)?;
    let mut stream = stream.compat();
    write_relay_request(&mut stream, request).await?;
    Ok(stream)
}
