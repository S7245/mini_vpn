use thiserror::Error;

/// Shared client-side error type for relay setup and protocol parsing.
/// 中文要点：统一承载共享层错误，避免连接生命周期热路径里直接 panic。
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid target address: {0}")]
    InvalidTarget(String),

    #[error("invalid utf-8 payload: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}
