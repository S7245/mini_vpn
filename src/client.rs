use bytes::buf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::rustls::Certificate;

use std::convert::TryFrom;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode};

pub struct VpnFrame {
    pub len: u16,
    pub data: Vec<u8>,
}

pub async fn run() {
    println!("Client 模式启动！");

    let mut root_cert_store = RootCertStore::empty();
    let cert_file = &mut BufReader::new(File::open("cert.pem").unwrap());
    let certs = rustls_pemfile::certs(cert_file).unwrap();
    for cert in certs {
        root_cert_store.add(&Certificate(cert)).unwrap();
    }
    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    // 1. 【发车前准备】在启动 SOCKS5 监听之前，先建立唯一的 TLS 隧道并挂载 Yamux 多路复用
    let server_stream = match TcpStream::connect("127.0.0.1:8081").await {
        Ok(stream) => stream,
        Err(e) => {
            println!("连接代理服务端失败: {e}");
            return;
        }
    };
    println!("成功连接到代理服务端: 127.0.0.1:8081");
    let domain = match ServerName::try_from("localhost") {
        Ok(domain) => domain,
        Err(e) => {
            println!("解析 SNI 域名失败: {e:?}");
            return;
        }
    };
    let tls_stream = match connector.clone().connect(domain, server_stream).await {
        Ok(s) => s,
        Err(e) => {
            println!("与代理服务端 TLS 握手失败: {:?}", e);
            return;
        }
    };
    // 使用 Mode::Client 创建 Yamux 引擎
    let mut yamux_conn = Connection::new(tls_stream.compat(), Config::default(), Mode::Client);

    // 2. 【获取遥控器】拿到一个可以无限克隆的控制柄，用来随时开启新车厢
    let ctrl = yamux_conn.control();

    // 3. 【启动引擎】Yamux 必须在后台不断运转，才能处理收发的数据包
    tokio::spawn(async move {
        // 不断 poll connection 驱动底层数据流
        while let Ok(Some(_)) = yamux_conn.next_stream().await {}
        println!("与服务端的 Yamux 长连接已断开，请重启 Client");
    });

    let listener = match TcpListener::bind("127.0.0.1:1080").await {
        Ok(listener) => listener,
        Err(e) => {
            println!("绑定端口失败: {e:?}");
            return;
        }
    };
    println!("Client 模式启动！SOCKS5 监听中...");

    // 4. 【本地 SOCKS5 监听循环】
    loop {
        let (mut stream, _addr) = match listener.accept().await {
            Ok((stream, addr)) => (stream, addr),
            Err(e) => {
                println!("接受连接失败: {e:?}");
                continue;
            }
        };
        // 给每一个本地浏览器请求，发一个“遥控器”
        let mut ctrl = ctrl.clone();
        let _connector = connector.clone();

        tokio::spawn(async move {
            // 1. 先只读 2 个字节，获取版本号和方法数量。
            let mut version_and_methods = [0u8; 2];
            match stream.read_exact(&mut version_and_methods).await {
                Ok(_) => {}
                Err(e) => {
                    println!("读取版本号和方法数量失败: {e}");
                    return;
                }
            };

            // 2. 从第二个字节提取出方法数量。在 Rust 中，数组长度需要是 usize 类型，所以我们要转换一下：
            let nmethods = version_and_methods[1] as usize;

            // 3. 根据这个数量，创建一个动态数组（Vec），并把剩下的认证方法字节读完，清空管道：
            let mut methods = vec![0u8; nmethods];
            match stream.read_exact(&mut methods).await {
                Ok(_) => {}
                Err(e) => {
                    println!("读取认证方法失败: {e}");
                    return;
                }
            };

            println!("收到认证方法: {methods:?}");

            if version_and_methods[0] == 5 {
                match stream.write_all(&[5, 0]).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("写入成功响应失败: {e}");
                        return;
                    }
                };
            }
            // return;

            let mut req_header = [0u8; 4];
            match stream.read_exact(&mut req_header).await {
                Ok(_) => {}
                Err(e) => {
                    println!("读取请求头失败: {e}");
                    return;
                }
            };
            println!("收到请求头: {req_header:?}");

            // 在 SOCKS5 协议中，req_header[1] == 1 代表浏览器请求建立 TCP 代理（CONNECT）。
            // 如果浏览器想打游戏或者解析 DNS，它会发来 req_header[1] == 3，也就是请求 UDP ASSOCIATE (UDP 关联)。
            match req_header[1] {
                1 => {
                    // TCP 连接
                    let target_addr = match req_header[3] {
                        1 => {
                            // 1. 准备一个 4 字节的数组读取 IP (stream.read_exact)
                            let mut addr = [0u8; 4];
                            match stream.read_exact(&mut addr).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("读取目标地址失败: {e}");
                                    return;
                                }
                            };
                            // 2. 准备一个 2 字节的数组读取端口，并用 u16::from_be_bytes 转换
                            let mut port_buf = [0u8; 2];
                            match stream.read_exact(&mut port_buf).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("读取目标端口失败: {e}");
                                    return;
                                }
                            };
                            let port = u16::from_be_bytes(port_buf);
                            // 3. 使用 format!("{}.{}.{}.{}:{}", ip[0], ip[1], ip[2], ip[3], port) 返回字符串
                            let ipv4_addr =
                                format!("{}.{}.{}.{}:{}", addr[0], addr[1], addr[2], addr[3], port);

                            ipv4_addr
                        }
                        3 => {
                            // 解析域名 (Domain)
                            let mut len_buf = [0u8; 1];
                            match stream.read_exact(&mut len_buf).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("读取域名长度失败: {e}");
                                    return;
                                }
                            };
                            let len = len_buf[0] as usize;

                            let mut domain_buf = vec![0u8; len];
                            match stream.read_exact(&mut domain_buf).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("读取域名失败: {e}");
                                    return;
                                }
                            };
                            let domain = String::from_utf8_lossy(&domain_buf);

                            let mut port_buf = [0u8; 2];
                            match stream.read_exact(&mut port_buf).await {
                                Ok(_) => {}
                                Err(e) => {
                                    println!("读取目标端口失败: {e}");
                                    return;
                                }
                            };
                            let port = u16::from_be_bytes(port_buf);

                            let domain = format!("{domain}:{port}");
                            domain
                        }
                        _ => {
                            println!("暂不支持的地址类型");
                            return;
                        } // _ => return,
                    };
                    println!("解析出的目标地址是: {target_addr}");

                    // 当你拿到 target_addr 后，不要再去连 TcpStream 了！
                    // 而是用遥控器申请打开一节新车厢 (Stream)：
                    let yamux_stream = match ctrl.open_stream().await {
                        Ok(s) => s,
                        Err(e) => {
                            println!("打开多路复用流失败: {:?}", e);
                            return;
                        }
                    };
                    let mut tokio_yamux_stream = yamux_stream.compat();

                    // 发送成功响应
                    match stream.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("写入成功响应失败: {e}");
                            return;
                        }
                    };

                    // 1.1 发送 faker header 到服务端
                    let fake_header = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";
                    if tokio_yamux_stream.write_all(fake_header).await.is_err() {
                        return;
                    };

                    // 2. 此时的服务端还不知道我们要去哪。我们需要设计一个极其简单的自定义通信协议：将目标地址拼接上一个换行符 `\n`，发送给服务端。
                    match tokio_yamux_stream.write_all(format!("{target_addr}\n").as_bytes()).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("写入目标地址失败: {e}");
                            return;
                        }
                    };

                    // 将浏览器的明文流，和这节多路复用的车厢对接起来！
                    let _ =
                        tokio::io::copy_bidirectional(&mut stream, &mut tokio_yamux_stream).await;
                }
                3 => {
                    // UDP 关联
                    // ================= SOCKS5 的 UDP 握手过程 =================
                    let udp_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
                        .await
                        .expect("绑定 UDP 端口失败");
                    let local_addr = udp_socket.local_addr().expect("获取 UDP 端口失败");
                    println!("本地 UDP 端口: {:?}", local_addr);
                    // 使用 local_addr.port() 获取端口号（这是一个 u16 整数）。
                    let port = local_addr.port();
                    // 使用 .to_be_bytes() 将这个端口号转换为 2 个字节的网络大端序数组。
                    let port_be = port.to_be_bytes();
                    // 组装一个长度为 10 的数组，格式为：[5 (版本), 0 (成功), 0 (保留), 1 (IPv4), 127, 0, 0, 1, 端口高位, 端口低位]
                    let udp_resp: [u8; 10] = [5, 0, 0, 1, 127, 0, 0, 1, port_be[0], port_be[1]];
                    // 把这个数组发给浏览器
                    match stream.write_all(&udp_resp).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("写入 UDP 响应失败: {e}");
                            return;
                        }
                    };

                    // 把申请车厢、发送门神暗号，以及发送 "UDP\n" 指令的代码补上
                    let yamux_stream = match ctrl.open_stream().await {
                        Ok(s) => s,
                        Err(e) => {
                            println!("打开多路复用流失败: {:?}", e);
                            return;
                        }
                    };
                    let mut tokio_yamux_stream = yamux_stream.compat();
                    // 1.1 发送 faker header 到服务端
                    let fake_header = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";
                    if tokio_yamux_stream.write_all(fake_header).await.is_err() {
                        return;
                    };
                    // 2. 此时的服务端还不知道我们要去哪。我们需要设计一个极其简单的自定义通信协议：将目标地址拼接上一个换行符 `\n`，发送给服务端。
                    match tokio_yamux_stream.write_all(format!("UDP\n").as_bytes()).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("写入目标地址失败: {e}");
                            return;
                        }
                    };

                    // 准备一个小本子，记住是哪个本地端口在和我们通信
                    let mut client_addr = None;
                    
                    // 准备两个常驻缓冲区
                    let mut udp_buf = [0u8; 65536];
                    let mut len_buf = [0u8; 2];

                    // 把这些射过来的“水滴”（UDP 包）收集起来，准备装进我们设计好的“带有 2 字节长度前缀的包装盒”里。
                    loop {
                        tokio::select! {
                            // ================= 分支 1：从本地 UDP 读 -> 发进隧道 =================
                            res = udp_socket.recv_from(&mut udp_buf) => {
                                let (len, src_addr) = match res {
                                    Ok(res) => res,
                                    Err(e) => {
                                        println!("读取本地 UDP 失败: {e}");
                                        break;
                                    }
                                };
                                println!("接收到 {} 字节数据，来自 {:?}", len, src_addr);
                                // 记下浏览器的地址，方便等下回包
                                client_addr = Some(src_addr);

                                // 【你的任务 1】：
                                // 1. 将 len (usize类型) 转换为 u16，然后再转成 2 字节的大端序数组。
                                let len = len as u16;
                                let len_be = len.to_be_bytes();
                                // 2. 先把这 2 个字节通过 tokio_yamux_stream.write_all 发送出去 (这就是前面说的“包装盒长度”)。
                                tokio_yamux_stream.write_all(&len_be).await.unwrap();
                                // 3. 再把真正的 UDP 数据 (udp_buf 里的前 len 个字节：&udp_buf[..len]) 发送出去。
                                tokio_yamux_stream.write_all(&udp_buf[..len as usize]).await.unwrap();
                            }
                            // ================= 分支 2：从隧道读 -> 发回本地 UDP =================
                            res = tokio_yamux_stream.read_exact(&mut len_buf) => {
                                if res.is_err() {
                                    println!("隧道断开");
                                    break;
                                }
                                // 【你的任务 2】：
                                // 1. 用 u16::from_be_bytes 解析 len_buf，得到接下来的真实数据长度 payload_len。
                                let payload_len = u16::from_be_bytes(len_buf);
                                // 2. 创建一个大小为 payload_len 的动态数组：vec![0u8; payload_len]。
                                let mut payload_buf = vec![0u8; payload_len as usize];
                                // 3. 再次使用 tokio_yamux_stream.read_exact 把真实数据读满。
                                tokio_yamux_stream.read_exact(&mut payload_buf).await.unwrap();
                                // 4. 判断 client_addr 是否有值 (if let Some(addr) = client_addr)。
                                // 5. 如果有值，使用 udp_socket.send_to 把读到的数据发还给这个 addr。
                                if let Some(addr) = client_addr {
                                    udp_socket.send_to(&payload_buf, addr).await.unwrap();
                                }
                            }
                        }
                    }
                }
                _ => {
                    return;
                }
            }
        });
    }
}
