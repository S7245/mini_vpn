use crate::shared::errors::ClientError;

/// Structured target address for direct proxy and TUN relay requests.
/// 中文要点：统一承载 IP:port 与域名:port，避免热路径里拼接裸字符串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAddr {
    IpPort(std::net::SocketAddr),
    DomainPort { host: String, port: u16 },
}

impl TargetAddr {
    /// Parse a target string into a structured target model.
    /// 中文要点：优先解析为 `SocketAddr`，失败后退化为域名加端口解析。
    pub fn parse(input: &str) -> Result<Self, ClientError> {
        if let Ok(addr) = input.parse::<std::net::SocketAddr>() {
            return Ok(Self::IpPort(addr));
        }

        let (host, port_str) = input
            .rsplit_once(':')
            .ok_or_else(|| ClientError::InvalidTarget(input.to_string()))?;
        let port = port_str
            .parse::<u16>()
            .map_err(|_| ClientError::InvalidTarget(input.to_string()))?;

        if host.is_empty() {
            return Err(ClientError::InvalidTarget(input.to_string()));
        }

        Ok(Self::DomainPort {
            host: host.to_string(),
            port,
        })
    }

    /// Render the target in the current wire format.
    /// 中文要点：阶段一沿用现有换行分隔协议，输出 `host:port` 文本。
    pub fn to_wire_string(&self) -> String {
        match self {
            Self::IpPort(addr) => addr.to_string(),
            Self::DomainPort { host, port } => format!("{host}:{port}"),
        }
    }
}
