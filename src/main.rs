use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use bytes::{Buf, BytesMut};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use std::io;
use tokio_util::codec::Decoder;

use bytes::BufMut; // 记得引入这个特型
use tokio_util::codec::Encoder;

use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed; // 必须引入这两个特型才能使用 send 和 next

// 代表我们在网络中传输的一个完整数据帧
pub struct VpnFrame {
    pub data: Vec<u8>,
}

// 我们的编解码器（暂时里面不需要存状态，是个空结构体）
// pub struct VpnCodec;
pub struct VpnCodec {
    cipher: ChaCha20Poly1305,
    send_counter: u64, // 发送计数器
    recv_counter: u64, // 接收计数器
}

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("请指定运行模式: client 或 server");

    if mode == "server" {
        println!("运行服务器端");
        // 1. 在 server 分支里，让 TcpListener 监听 "127.0.0.1:8081"，并加上我们熟悉的 loop 和 tokio::spawn 结构。
        let listener = match TcpListener::bind("127.0.0.1:8081").await {
            Ok(listener) => listener,
            Err(e) => {
                println!("绑定失败: {e:?}");
                return;
            }
        };
        loop {
            let (mut stream, _addr) = match listener.accept().await {
                Ok((stream, addr)) => (stream, addr),
                Err(e) => {
                    println!("接受连接失败: {e:?}");
                    return;
                }
            };
            tokio::spawn(async move {
                // ================= 六、服务器 (Server) 的暗号验证 =========================
                // 6.1. 准备一个正好 40 字节的数组作为“篮子”
                let mut magic_buf = [0u8; 38];
                // 6.2. 尝试从 stream 中严格读取 40 个字节 (提示: 使用 read_exact)
                // 如果这里读取失败 (比如 GFW 只发了 10 个字节探测包)，使用 match 处理 Result，遇到 Err 直接 return;
                match stream.read_exact(&mut magic_buf).await {
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
                    match stream.read_exact(&mut byte).await {
                        Ok(_) => {},
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
                    Ok(stream) => stream,
                    Err(e) => {
                        println!("无法连接到目标地址 {target_addr}: {e}");
                        return;
                    }
                };
                // copy_bidirectional(&mut stream, &mut target_stream).await.unwrap();

                // 2. 给 Server 换上新引擎
                // 2.1. 准备一个统一的 32 字节密钥 (两边必须一致！)
                let secret_key = b"an example very very secret key.";
                // 2.2. 将来自客户端的 stream 包装成加密流
                let mut encrypted_client = Framed::new(stream, VpnCodec::new(secret_key));
                // (注意：与目标网站 target_stream 的通信依然是明文的，所以不需要包装)
                let mut buf_from_target = [0u8; 8192];
                loop {
                    tokio::select! {
                        // 分支 A：从客户端接收加密数据，解密后发给目标网站
                        result = encrypted_client.next() => {
                            let frame_opt = match result {
                               Some(Ok(f)) => f,
                               _ => break, // 如果没数据了，或者解密失败报错了，直接退出循环断开连接
                            };
                            // frame_opt 现在是一个 VpnFrame。
                            // 请把它里面的 data (明文) 通过 target_stream.write_all 写入目标网站。
                            match target_stream.write_all(&frame_opt.data).await {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("写入失败: {e}");
                                    break;
                                }
                            };
                        }
                        // 分支 B：从目标网站接收明文数据，加密后发给客户端
                        result = target_stream.read(&mut buf_from_target) => {
                            let n = match result {
                                Ok(n) if n > 0 => n,
                                _ => break,
                            };
                            // 我们将读到的明文 buf_from_target[..n] 包装成 VpnFrame。
                            match encrypted_client.send(VpnFrame { data: buf_from_target[..n].to_vec() }).await {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("发送失败: {e}");
                                    break;
                                }
                            };
                        }

                    }
                }
            });
        }
    } else if mode == "client" {
        println!("Client 模式启动！");

        let listener = match TcpListener::bind("127.0.0.1:1080").await {
            Ok(listener) => listener,
            Err(e) => {
                println!("绑定端口失败: {e:?}");
                return;
            }
        };

        loop {
            let (mut stream, _addr) = match listener.accept().await {
                Ok((stream, addr)) => (stream, addr),
                Err(e) => {
                    println!("接受连接失败: {e:?}");
                    return;
                }
            };
            tokio::spawn(async move {
                // let mut buf = [0u8; 3];
                // stream.read_exact(&mut buf).await.unwrap();
                // println!("收到数据: {:?}", buf);

                // 1. 先只读 2 个字节，获取版本号和方法数量。
                let mut version_and_methods = [0u8; 2];
                match stream.read_exact(&mut version_and_methods).await {
                    Ok(_) => {},
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
                    Ok(_) => {},
                    Err(e) => {
                        println!("读取认证方法失败: {e}");
                        return;
                    }
                };

                println!("收到认证方法: {methods:?}");

                if version_and_methods[0] == 5 {
                    match stream.write_all(&[5, 0]).await {
                        Ok(_) => {},
                        Err(e) => {
                            println!("写入成功响应失败: {e}");
                            return;
                        }
                    };
                }
                // return;

                let mut req_header = [0u8; 4];
                match stream.read_exact(&mut req_header).await {
                    Ok(_) => {},
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
                            Ok(_) => {},
                            Err(e) => {
                                println!("读取目标地址失败: {e}");
                                return;
                            }
                        };
                        // 2. 准备一个 2 字节的数组读取端口，并用 u16::from_be_bytes 转换
                        let mut port_buf = [0u8; 2];
                        match stream.read_exact(&mut port_buf).await {
                            Ok(_) => {},
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
                            Ok(_) => {},
                            Err(e) => {
                                println!("读取域名长度失败: {e}");
                                return;
                            }
                        };
                        let len = len_buf[0] as usize;

                        let mut domain_buf = vec![0u8; len];
                        match stream.read_exact(&mut domain_buf).await {
                            Ok(_) => {},
                            Err(e) => {
                                println!("读取域名失败: {e}");
                                return;
                            }
                        };
                        let domain = String::from_utf8_lossy(&domain_buf);

                        let mut port_buf = [0u8; 2];
                        match stream.read_exact(&mut port_buf).await {
                            Ok(_) => {},
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

                // 发送成功响应
                match stream.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await {
                    Ok(_) => {},
                    Err(e) => {
                        println!("写入成功响应失败: {e}");
                        return;
                    }
                };

                // 1. 在给客户端发送成功响应之后，不要去连接 target_addr 了。改为连接我们的代理服务端：
                let mut server_stream = match TcpStream::connect("127.0.0.1:8081").await {
                    Ok(stream) => stream,
                    Err(e) => {
                        println!("连接代理服务端失败: {e}");
                        return;
                    }
                };
                println!("成功连接到代理服务端: 127.0.0.1:8081");

                // 1.1 发送 faker header 到服务端
                let fake_header = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";
                match server_stream.write_all(fake_header).await {
                    Ok(_) => {},
                    Err(e) => {
                        println!("写入 faker header 失败: {e}");
                        return;
                    }
                };

                // 2. 此时的服务端还不知道我们要去哪。我们需要设计一个极其简单的自定义通信协议：将目标地址拼接上一个换行符 `\n`，发送给服务端。
                match server_stream.write_all(format!("{target_addr}\n").as_bytes()).await {
                    Ok(_) => {},
                    Err(e) => {
                        println!("写入目标地址失败: {e}");
                        return;
                    }
                };

                // 2. 给 Server 换上新引擎
                // 2.1. 准备一个统一的 32 字节密钥 (两边必须一致！)
                let secret_key = b"an example very very secret key.";
                let mut encrypted_server = Framed::new(server_stream, VpnCodec::new(secret_key));
                // 2.2. 准备本地明文缓冲区
                let mut buf_from_local = [0u8; 8192];

                // 编写对称的 tokio::select! 循环：
                loop {
                    tokio::select! {
                        // 分支 A (本地发往远端)：从本地 stream 读取明文到 buf_from_local，打包成 VpnFrame，然后使用 encrypted_server.send(...).await 发送出去。
                        result = stream.read(&mut buf_from_local) => {
                            let n = match result {
                                Ok(n) => n,
                                _ => break,
                            };
                            if n == 0 { break; }
                            // 把 buf_from_local[..n] 通过 encrypted_server.send(...).await 发送出去。
                            match encrypted_server.send(VpnFrame {
                                data: buf_from_local[..n].to_vec(),
                            }).await {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("发送 VpnFrame 失败: {e}");
                                    return;
                                }
                            };
                        }

                        // 分支 B (远端收回本地)：调用 encrypted_server.next().await 接收解密好的 VpnFrame，把里面的 data 使用 stream.write_all(...).await 写回给本地浏览器/curl。
                        result = encrypted_server.next() => {
                            let frame = match result {
                                Some(Ok(f)) => f,
                                _ => break,
                            };
                            match stream.write_all(&frame.data).await {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("写入 VpnFrame 失败: {e}");
                                    return;
                                }
                            };
                        }
                    }
                }
            });
        }
    }
}

impl VpnCodec {
    // 初始化时传入 32 字节的密钥
    pub fn new(secret_key: &[u8; 32]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(Key::from_slice(secret_key)),
            send_counter: 0,
            recv_counter: 0,
        }
    }
    // 辅助魔法：把 u64 计数器变成 12 字节的 Nonce
    fn nonce_from_counter(counter: u64) -> chacha20poly1305::Nonce {
        let mut nonce_bytes = [0u8; 12];
        let counter_bytes = counter.to_be_bytes(); // u64 是 8 字节
        nonce_bytes[4..12].copy_from_slice(&counter_bytes); // 放到最后 8 个字节
        *Nonce::from_slice(&nonce_bytes)
    }
}

impl Decoder for VpnCodec {
    type Item = VpnFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 1. 检查缓冲区里有没有至少 2 个字节（因为我们约定用 2 字节来存长度 u16）
        // 数据不够 2 字节，连头都读不出来，直接告诉 Tokio 继续等
        if src.len() < 2 {
            return Ok(None);
        }

        // 2. 偷看前 2 个字节，计算出真正的 payload (加密数据) 的长度
        // 注意：这里只是偷看 (src[0], src[1])，并没有把这 2 个字节从缓冲区里吃掉
        let payload_len = u16::from_be_bytes([src[0], src[1]]) as usize;

        // 3. 检查当前缓冲区的总长度 src.len() 是否大于或等于【完整的包长度】。
        //    完整的包长度 = 2 (头部) + payload_len。
        //    如果不够，说明虽然知道了长度，但后面的数据还没在网线上全传过来，这里应该返回什么？
        // 数据不够，直接返回 None
        if src.len() < 2 + payload_len {
            return Ok(None);
        }

        // 4. 如果长度足够了（凑齐了一个完整的包）：
        //    - 第一步：使用 src.advance(2); 把头部的 2 个字节彻底从缓冲区里消耗掉。
        //    - 第二步：使用 src.split_to(payload_len); 把接下来的有效数据切下来，它会返回一个 Bytes 对象。
        //    - 第三步：把切下来的数据转换成 Vec<u8> (比如调用 .to_vec())，装进 VpnFrame 结构体中。
        //    - 第四步：返回 Ok(Some(装好的 VpnFrame))。
        src.advance(2);
        let ciphertext = src.split_to(payload_len);

        // 完成防篡改解密逻辑
        // 1. 获取当前的接收 Nonce（使用 self.recv_counter）
        // 2. 立刻递增 self.recv_counter
        let nonce = Self::nonce_from_counter(self.recv_counter);
        self.recv_counter += 1;

        // 3. 调用 self.cipher.decrypt(&nonce, src.split_to(payload_len).as_ref()) 尝试解密。
        //    如果成功，得到明文 plaintext (是一个 Vec<u8>)。
        //    如果失败 (Err)，说明遭遇了篡改或探测！请直接通过 return Err(...) 返回一个 io::Error。
        //    (提示：可以使用 std::io::Error::new(std::io::ErrorKind::InvalidData, "解密失败/遭探测") )
        let plaintext = match self.cipher.decrypt(&nonce, ciphertext.as_ref()) {
            Ok(plaintext) => plaintext,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "解密失败/遭探测",
                ));
            }
        };

        // 4. 将成功解密的 plaintext 包装成 VpnFrame，通过 Ok(Some(VpnFrame { data: plaintext })) 返回。
        Ok(Some(VpnFrame { data: plaintext }))
    }
}

impl Encoder<VpnFrame> for VpnCodec {
    type Error = io::Error;

    fn encode(&mut self, item: VpnFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // 2.1. 获取当前发送计数器对应的 Nonce：调用 Self::nonce_from_counter(self.send_counter)。
        let nonce = Self::nonce_from_counter(self.send_counter);
        // 2.2. 立刻将 self.send_counter += 1;（用完马上加 1，绝不重复）。
        self.send_counter += 1;

        // 2.3. 调用 self.cipher.encrypt(...) 对 item.data 进行加密，得到 ciphertext。
        // 提示：这里可能会发生加密错误，encrypt 会返回 Result。如果失败，可以把它转换为 io::Error，或者简单点直接 .expect("加密失败")。
        let ciphertext = match self.cipher.encrypt(&nonce, item.data.as_ref()) {
            Ok(ciphertext) => ciphertext,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "加密失败",
                ));
            }
        };

        // 1.1. 安全起见，先要求底层缓冲区为我们准备好足够的空间
        dst.reserve(2 + ciphertext.len());

        // 2.4. 此时，你的数据长度变长了！（因为多了 16 字节的 Poly1305 MAC）。你需要把 ciphertext.len() 强转为 u16，作为长度写入缓冲区 dst.put_u16(...)。
        dst.put_u16(ciphertext.len() as u16);
        // 2.5. 最后把加密后的 ciphertext 写入缓冲区 dst.put_slice(...)。
        dst.put_slice(&ciphertext);
        // 2.6. 返回 Ok(())
        Ok(())
    }
}
