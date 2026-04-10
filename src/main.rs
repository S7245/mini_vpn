use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};

use std::convert::TryFrom;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode};

#[tokio::main]
async fn main() {
    let mode = std::env::args().nth(1).expect("请指定运行模式: client 或 server");

    if mode == "server" {
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
                    });
                }
                println!("一条 Yamux 多路复用长连接已断开");
            });
        }
    } else if mode == "client" {
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
                if req_header[1] != 1 {
                    return;
                }

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
                if tokio_yamux_stream.write_all(fake_header).await.is_err() { return; };

                // 2. 此时的服务端还不知道我们要去哪。我们需要设计一个极其简单的自定义通信协议：将目标地址拼接上一个换行符 `\n`，发送给服务端。
                match tokio_yamux_stream.write_all(format!("{target_addr}\n").as_bytes()).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("写入目标地址失败: {e}");
                        return;
                    }
                };

                // 将浏览器的明文流，和这节多路复用的车厢对接起来！
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut tokio_yamux_stream).await;
            });
        }
    }
}
