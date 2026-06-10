//! Proxy upstream abstraction (Stage 13a).
//!
//! 中文要点：把「代理出口」收成一个 trait —— 给「目标」开一条中继流。TUIC 是第一个实现;
//! 将来 VLESS+REALITY 等只需新增一个 impl(见 ADR-0004 的两层扩展模型)。legacy(yamux)路径在
//! 退役前仍走原内联逻辑(零回归),不强行套进 trait。

use crate::shared::{ClientError, TargetAddr};
use tokio::io::{AsyncRead, AsyncWrite};

/// 统一的中继流类型:legacy(yamux compat)与 tuic(QUIC 双向流 compat)都收成它,喂给同一套双向泵。
/// 中文要点:用 boxed trait object 收口,避免给 enum 手写 AsyncRead/Write 的易错样板(系统稳定优先)。
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// 一条到出口的中继流(双向字节)。
pub type RelayStream = Box<dyn AsyncStream>;

/// 代理上游:给一个 Target 开一条到出口的 TCP 中继流。
/// 中文要点:async fn 要在 `Box<dyn ProxyUpstream>` 上分发,用成熟的 `async-trait`。
#[async_trait::async_trait]
pub trait ProxyUpstream: Send + Sync {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError>;
}

#[cfg(test)]
mod tests {
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
}
