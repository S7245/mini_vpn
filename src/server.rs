use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use mini_vpn::shared::{RelayRequest, read_relay_request};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};

use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode};

const DEFAULT_SERVER_BIND_ADDR: &str = "127.0.0.1:8081";
const DEFAULT_SERVER_CERT_PATH: &str = "cert.pem";
const DEFAULT_SERVER_KEY_PATH: &str = "key.pem";

/// Startup configuration for the relay server listener.
/// 中文要点：这一层只负责“服务端监听在哪个地址”，不参与 TLS/Yamux 业务逻辑。
#[derive(Debug, Clone)]
struct ServerRuntimeConfig {
    /// TCP bind address used by the relay server listener.
    /// 中文要点：这是服务端真正对外监听的地址，客户端的 upstream 地址要和它对齐。
    bind_addr: String,
}

impl ServerRuntimeConfig {
    /// Build the server runtime config from optional sources.
    /// 中文要点：当前先保持最小配置面，只开放监听地址一个入口。
    fn from_sources(bind_addr: Option<&str>) -> Result<Self, String> {
        let bind_addr = bind_addr.unwrap_or(DEFAULT_SERVER_BIND_ADDR).to_string();
        bind_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|_| format!("invalid server bind addr: {bind_addr}"))?;

        Ok(Self { bind_addr })
    }

    /// Read the bind address from process environment.
    /// 中文要点：默认值继续兼容 127.0.0.1:8081，只有显式传值时才覆盖。
    fn from_env() -> Result<Self, String> {
        let bind_addr = std::env::var("MINI_VPN_SERVER_BIND_ADDR").ok();
        Self::from_sources(bind_addr.as_deref())
    }
}

/// Startup configuration for server-side TLS material files.
/// 中文要点：这一层只描述服务端要加载哪张证书、哪把私钥，不负责监听地址。
#[derive(Debug, Clone)]
struct ServerTlsConfig {
    /// PEM certificate chain path used by the TLS acceptor.
    /// 中文要点：服务端握手时发给客户端的证书链文件路径。
    cert_path: String,
    /// PKCS8 private key path paired with the certificate chain.
    /// 中文要点：和证书配套的私钥文件路径。
    key_path: String,
}

impl ServerTlsConfig {
    /// Build TLS material config from optional string sources.
    /// 中文要点：本阶段先做最小路径配置，空字符串直接视为非法输入。
    fn from_sources(cert_path: Option<&str>, key_path: Option<&str>) -> Result<Self, String> {
        let cert_path = cert_path.unwrap_or(DEFAULT_SERVER_CERT_PATH).to_string();
        let key_path = key_path.unwrap_or(DEFAULT_SERVER_KEY_PATH).to_string();

        if cert_path.trim().is_empty() {
            return Err("invalid server cert path: empty".to_string());
        }

        if key_path.trim().is_empty() {
            return Err("invalid server key path: empty".to_string());
        }

        Ok(Self {
            cert_path,
            key_path,
        })
    }

    /// Read TLS material paths from process environment.
    /// 中文要点：默认继续兼容 `cert.pem` / `key.pem`，只有显式传值时才覆盖。
    fn from_env() -> Result<Self, String> {
        let cert_path = std::env::var("MINI_VPN_SERVER_CERT_PATH").ok();
        let key_path = std::env::var("MINI_VPN_SERVER_KEY_PATH").ok();
        Self::from_sources(cert_path.as_deref(), key_path.as_deref())
    }
}

pub async fn run() {
    let runtime_config = match ServerRuntimeConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            println!("加载服务端运行时配置失败: {e}");
            return;
        }
    };

    let tls_config = match ServerTlsConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            println!("加载服务端 TLS 配置失败: {e}");
            return;
        }
    };

    println!(
        "运行服务器端，监听地址: {}, cert_path: {}, key_path: {}",
        runtime_config.bind_addr, tls_config.cert_path, tls_config.key_path
    );

    // 1. 读取证书和私钥
    let cert_file = match File::open(tls_config.cert_path.as_str()) {
        Ok(file) => file,
        Err(e) => {
            println!("打开服务端证书失败 {}: {e}", tls_config.cert_path);
            return;
        }
    };
    let key_file = match File::open(tls_config.key_path.as_str()) {
        Ok(file) => file,
        Err(e) => {
            println!("打开服务端私钥失败 {}: {e}", tls_config.key_path);
            return;
        }
    };
    let cert_file = &mut BufReader::new(cert_file);
    let key_file = &mut BufReader::new(key_file);

    let cert_chain = rustls_pemfile::certs(cert_file)
        .unwrap()
        .into_iter()
        .map(Certificate)
        .collect();
    // 读取 PKCS8 格式的私钥
    let mut keys = rustls_pemfile::pkcs8_private_keys(key_file).unwrap();
    let key = PrivateKey(keys.remove(0));

    // 2. 构建 TLS 配置
    let config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .expect("TLS 证书配置失败");

    let acceptor = TlsAcceptor::from(Arc::new(config));

    // 服务端监听地址来自显式配置，这样可以和 client-tun 的 upstream 覆盖值对齐。
    let listener = match TcpListener::bind(runtime_config.bind_addr.as_str()).await {
        Ok(listener) => listener,
        Err(e) => {
            println!("绑定失败: {e:?}");
            return;
        }
    };

    loop {
        // 【外层套娃】：接收真实的底层 TCP 连接
        let (stream, _addr) = match listener.accept().await {
            Ok((stream, addr)) => (stream, addr),
            Err(e) => {
                println!("接受连接失败: {e:?}");
                return;
            }
        };

        let acceptor = acceptor.clone();

        tokio::spawn(async move {
            // 1. 进行 TLS 握手，建立安全的加密隧道
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    println!("TLS 握手失败: {e:?}");
                    return;
                }
            };
            // 2. 【关键魔法】：接上转换插头，启动 Yamux 引擎 (Server 模式)
            let compat_tls_stream = tls_stream.compat();
            let mut yamux_conn =
                Connection::new(compat_tls_stream, Config::default(), Mode::Server);

            // 3. 【内层套娃】：Yamux 内部路由器开始工作！
            // 只要这条 TLS 长连接没断，它就会不断从里面吐出新的“车厢 (yamux_stream)”
            while let Ok(Some(yamux_stream)) = yamux_conn.next_stream().await {
                // 为每一个浏览器发来的网页请求，单独开一个微线程处理
                tokio::spawn(async move {
                    let mut tokio_yamux_stream = yamux_stream.compat();
                    let request = match read_relay_request(&mut tokio_yamux_stream).await {
                        Ok(request) => request,
                        Err(e) => {
                            println!("读取共享中继请求失败: {e}");
                            return;
                        }
                    };

                    match request {
                        RelayRequest::Udp { .. } => {
                        println!("🌟 收到 UDP 代理指令，切换为 UDP 中继模式！");
                        // 准备一个服务端的 UDP 端口，用来和真实的互联网（如 8.8.8.8）通信
                        let server_udp = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();

                        let mut len_buf = [0u8; 2]; // 用来读 UDP 数据包长度的缓冲区
                        let mut internet_buf = [0u8; 65536]; // 用来读 UDP 数据包的缓冲区

                        loop {
                            tokio::select! {
                                // ================= 分支 1：从隧道读 -> 发往真实互联网 =================
                                res = tokio_yamux_stream.read_exact(&mut len_buf) => {
                                    if res.is_err() { break; }

                                    let payload_len = u16::from_be_bytes(len_buf);
                                    let mut payload_buf = vec![0u8; payload_len as usize];
                                    if tokio_yamux_stream.read_exact(&mut payload_buf).await.is_err() { break; }

                                    if payload_buf.len() < 4 || payload_buf[0] != 0 || payload_buf[1] != 0 {
                                        println!("非法的 SOCKS5 UDP 数据包");
                                        continue; // 直接处理下一个包
                                    }

                                    let atyp = payload_buf[3];
                                    let header_len; // 用来记录“导航头”一共占了多少字节
                                    let target_addr = match atyp {
                                        1 => {
                                            if payload_buf.len() < 10 {
                                                continue;
                                            } // 4(头) + 4(IP) + 2(端口) = 10
                                            let ip = &payload_buf[4..8];
                                            let port =
                                                u16::from_be_bytes(payload_buf[8..10].try_into().unwrap());
                                            // let port = u16::from_be_bytes(&payload_buf[8..10]);
                                            header_len = 10;
                                            format!("{}.{}.{}.{}:{}", ip[0], ip[1], ip[2], ip[3], port)
                                        }
                                        3 => {
                                            // 提取域名长度，提取域名字符串，提取端口
                                            // header_len = 4 + 1(长度) + 域名长度 + 2(端口);
                                            let domain_len = payload_buf[4] as usize;
                                            let domain =
                                                String::from_utf8_lossy(&payload_buf[5..5 + domain_len]);
                                            let port = u16::from_be_bytes(
                                                payload_buf[5 + domain_len..7 + domain_len]
                                                    .try_into()
                                                    .unwrap(),
                                            );
                                            header_len = 7 + domain_len;
                                            format!("{}:{}", domain, port)
                                        }
                                        _ => {
                                            println!("不支持的 UDP 地址类型");
                                            continue;
                                        }
                                    };
                                    // 切割出真正的用户数据！
                                    let real_data = &payload_buf[header_len..];
                                    // 解析完成后，直接发射给目标！
                                    if let Err(e) = server_udp.send_to(real_data, &target_addr).await {
                                        println!("代发 UDP 数据失败: {e}");
                                    }
                                    println!("已发送 {} 字节数据到 {}", real_data.len(), target_addr);
                                }
                                // ================= 分支 2：从真实互联网接收响应 -> (稍后)发回隧道 =================
                                res = server_udp.recv_from(&mut internet_buf) => {
                                    let (len, remote_addr) = match res {
                                        Ok(r) => r,
                                        Err(_) => break,
                                    };
                                    println!("🎉 收到来自互联网 {} 的 {} 字节响应！", remote_addr, len);
                                    // (下一步，我们将在这里把数据重新打包回 SOCKS5 格式，发进 Yamux 隧道)

                                    // 3. 使用 match 智能分流，并提取对应的 IP 和 端口
                                    // 【你的任务】：
                                    // 1. 创建一个新的动态数组 `let mut response_payload = Vec::new();`
                                    let mut response_payload = Vec::new();
                                    // 2. 依次向里面 extend 或 push 以下内容：
                                    // - SOCKS5 头部: &[0, 0, 0, 1]  (1 代表 IPv4)
                                    // response_payload.extend_from_slice(&[0, 0, 0, 1]);

                                    // 2. 先塞入固定的 3 个字节：[0 (保留), 0 (保留), 0 (分片号)]
                                    response_payload.extend_from_slice(&[0, 0, 0]);

                                    match remote_addr {
                                        std::net::SocketAddr::V4(addr_v4) => {
                                            let ip_bytes = addr_v4.ip().octets(); // 拿到 [8, 8, 8, 8]
                                            let port_bytes = addr_v4.port().to_be_bytes(); // 拿到端口的 2 字节数组
                                            response_payload.push(1); // 1 代表 IPv4
                                            response_payload.extend_from_slice(&ip_bytes);
                                            response_payload.extend_from_slice(&port_bytes);
                                        }
                                        std::net::SocketAddr::V6(addr_v6) => {
                                            let ip_bytes = addr_v6.ip().octets(); // 拿到 [16 字节]
                                            let port_bytes = addr_v6.port().to_be_bytes(); // 拿到端口的 2 字节数组
                                            response_payload.push(4); // 4 代表 IPv6
                                            response_payload.extend_from_slice(&ip_bytes);
                                            response_payload.extend_from_slice(&port_bytes);
                                        }
                                    }
                                    // - 真正的响应数据: &internet_buf[..len]
                                    response_payload.extend_from_slice(&internet_buf[..len]);
                                    // 3. 算出 response_payload 的总长度 (as u16)，并转换为 2 字节的大端序数组。
                                    let payload_len = response_payload.len() as u16;
                                    let payload_len_be = payload_len.to_be_bytes();
                                    // 4. 使用 tokio_yamux_stream 依次将 [2字节长度] 和 [response_payload] write_all 发送进隧道！
                                    match tokio_yamux_stream.write_all(&payload_len_be).await {
                                        Ok(_) => {},
                                        Err(e) => {
                                            println!("UDP发送长度字节失败: {e}");
                                            continue;
                                        }
                                    }
                                    match tokio_yamux_stream.write_all(&response_payload).await {
                                        Ok(_) => {},
                                        Err(e) => {
                                            println!("UDP发送响应数据失败: {e}");
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        }
                        RelayRequest::Tcp { target } => {
                        let target_addr = target.to_wire_string();
                        println!("解析出的目标地址是: {target_addr}");
                        // 🌐 原本的 TCP 处理逻辑保持不变
                        // 7. 最后，像之前一样，连接 target_addr，并开启 copy_bidirectional。
                        let mut target_stream = match TcpStream::connect(&target_addr).await {
                            Ok(s) => s,
                            Err(e) => {
                                println!("无法连接到目标地址 {target_addr}: {e}");
                                return;
                            }
                        };
                        let _ = tokio::io::copy_bidirectional(
                            &mut tokio_yamux_stream,
                            &mut target_stream,
                        )
                        .await;
                        }
                    }
                });
            }
            println!("一条 Yamux 多路复用长连接已断开");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SERVER_BIND_ADDR, ServerRuntimeConfig, ServerTlsConfig};

    #[test]
    fn server_runtime_config_defaults_match_existing_behavior() {
        let config = ServerRuntimeConfig::from_sources(None).expect("config should load");
        assert_eq!(config.bind_addr, DEFAULT_SERVER_BIND_ADDR);
    }

    #[test]
    fn server_runtime_config_accepts_valid_bind_addr() {
        let config = ServerRuntimeConfig::from_sources(Some("127.0.0.1:9000"))
            .expect("config should load");
        assert_eq!(config.bind_addr, "127.0.0.1:9000");
    }

    #[test]
    fn server_runtime_config_rejects_invalid_bind_addr() {
        let err = ServerRuntimeConfig::from_sources(Some("bad-addr"))
            .expect_err("invalid bind addr should fail");
        assert!(err.contains("invalid server bind addr"));
    }

    #[test]
    fn server_tls_config_defaults_match_existing_behavior() {
        let config = ServerTlsConfig::from_sources(None, None).expect("config should load");
        assert_eq!(config.cert_path, "cert.pem");
        assert_eq!(config.key_path, "key.pem");
    }

    #[test]
    fn server_tls_config_accepts_override_paths() {
        let config = ServerTlsConfig::from_sources(
            Some("certs/dev/server-cert.pem"),
            Some("certs/dev/server-key.pem"),
        )
        .expect("config should load");
        assert_eq!(config.cert_path, "certs/dev/server-cert.pem");
        assert_eq!(config.key_path, "certs/dev/server-key.pem");
    }
}
