use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};

use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode};

pub async fn run() {
    println!("运行服务器端");

    // 1. 读取证书和私钥
    let cert_file = &mut BufReader::new(File::open("cert.pem").unwrap());
    let key_file = &mut BufReader::new(File::open("key.pem").unwrap());

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

    // 1. 在 server 分支里，让 TcpListener 监听 "127.0.0.1:8081"，并加上我们熟悉的 loop 和 tokio::spawn 结构。
    let listener = match TcpListener::bind("127.0.0.1:8081").await {
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
                    // ==========================================
                    // 【你的任务】：把你之前写的逻辑全部搬到这里面来！
                    //
                    // 注意：由于 yamux_stream 使用的是 futures 标准，
                    // 我们需要给它也接上转换插头，转回 tokio 标准，才能使用我们熟悉的 read_exact：
                    // let mut tokio_yamux_stream = yamux_stream.compat();
                    //
                    // 1. 从 tokio_yamux_stream 验证 38 字节的 HTTP 暗号门神
                    // 2. 从 tokio_yamux_stream 读取 \n 结尾的目标地址
                    // 3. 连接目标网站 target_stream
                    // 4. copy_bidirectional(&mut tokio_yamux_stream, &mut target_stream).await;
                    // ==========================================

                    let mut tokio_yamux_stream = yamux_stream.compat();

                    // ================= 六、服务器 (Server) 的暗号验证 =========================
                    // 6.1. 准备一个正好 40 字节的数组作为“篮子”
                    let mut magic_buf = [0u8; 38];
                    // 6.2. 尝试从 stream 中严格读取 40 个字节 (提示: 使用 read_exact)
                    // 如果这里读取失败 (比如 GFW 只发了 10 个字节探测包)，使用 match 处理 Result，遇到 Err 直接 return;
                    match tokio_yamux_stream.read_exact(&mut magic_buf).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("读取暗号失败: {e:?}");
                            return;
                        }
                    };

                    // 6.3. 校验暗号：如果读到的 magic_buf 不是我们的 fake_header
                    if &magic_buf != b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n" {
                        println!("遭遇主动探测或未知连接，静默断开！");
                        // 提示：直接结束当前的 spawn 任务，装死
                        return;
                    }

                    // 6.4. 暗号正确！继续执行原本读取目标地址的代码...
                    // ==========================================

                    // 2. 在 spawn 内部，准备一个空的动态数组 let mut addr_bytes = Vec::new();，用来存放读到的地址字节。
                    let mut addr_bytes = Vec::new();
                    // 3. 写一个无限循环 loop，每次准备一个 1 字节的缓冲区 let mut byte = [0u8; 1];。
                    // 4. 使用 stream.read_exact(&mut byte).await.unwrap(); 读取这 1 个字节。
                    // 5. 判断：如果 byte[0] == b'\n'，就可以 break; 跳出读取循环了。否则，把读到的字节推进数组：addr_bytes.push(byte[0]);。
                    loop {
                        let mut byte = [0u8; 1];
                        match tokio_yamux_stream.read_exact(&mut byte).await {
                            Ok(_) => {}
                            Err(e) => {
                                println!("读取目标地址失败: {e:?}");
                                return;
                            }
                        };
                        if byte[0] == b'\n' {
                            break;
                        }
                        addr_bytes.push(byte[0]);
                    }
                    // 6. 循环结束后，将读到的字节转换为字符串：let target_addr = String::from_utf8(addr_bytes).unwrap();。
                    let target_addr = match String::from_utf8(addr_bytes) {
                        Ok(addr) => addr,
                        Err(e) => {
                            println!("解析目标地址失败: {e:?}");
                            return;
                        }
                    };
                    println!("解析出的目标地址是: {target_addr}");

                    if target_addr == "UDP" {
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
                    } else {
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
                });
            }
            println!("一条 Yamux 多路复用长连接已断开");
        });
    }
}
