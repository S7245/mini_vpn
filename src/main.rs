// use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("请指定运行模式: client 或 server");

    if mode == "server" {
        println!("运行服务器端");
        // 1. 在 server 分支里，让 TcpListener 监听 "127.0.0.1:8081"，并加上我们熟悉的 loop 和 tokio::spawn 结构。
        let listener = TcpListener::bind("127.0.0.1:8081").await.unwrap();
        loop {
            let (mut stream, _addr) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                // 2. 在 spawn 内部，准备一个空的动态数组 let mut addr_bytes = Vec::new();，用来存放读到的地址字节。
                let mut addr_bytes = Vec::new();
                // 3. 写一个无限循环 loop，每次准备一个 1 字节的缓冲区 let mut byte = [0u8; 1];。
                // 4. 使用 stream.read_exact(&mut byte).await.unwrap(); 读取这 1 个字节。
                // 5. 判断：如果 byte[0] == b'\n'，就可以 break; 跳出读取循环了。否则，把读到的字节推进数组：addr_bytes.push(byte[0]);。
                loop {
                    let mut byte = [0u8; 1];
                    stream.read_exact(&mut byte).await.unwrap();
                    if byte[0] == b'\n' {
                        break;
                    }
                    addr_bytes.push(byte[0]);
                }
                // 6. 循环结束后，将读到的字节转换为字符串：let target_addr = String::from_utf8(addr_bytes).unwrap();。
                let target_addr = String::from_utf8(addr_bytes).unwrap();
                println!("解析出的目标地址是: {}", target_addr);

                // 7. 最后，像之前一样，连接 target_addr，并开启 copy_bidirectional。
                let mut target_stream = TcpStream::connect(&target_addr).await.unwrap();
                // copy_bidirectional(&mut stream, &mut target_stream).await.unwrap();

                let mut buf1 = [0u8; 8192]; // 从 Client (stream) 读取的数据缓冲区
                let mut buf2 = [0u8; 8192]; // 从目标网站 (target_stream) 读取的数据缓冲区
                loop {
                    tokio::select! {
                        // 从 Client (stream) 读到的数据是密文。在发给真正的目标网站 (target_stream) 之前，需要先解密。
                        result = stream.read(&mut buf1) => {
                            
                            let n = match result {
                                Ok(n) => n,
                                Err(e) => {
                                    println!("读取失败: {}", e);
                                    break;
                                }
                            };

                            if n == 0 {break;}

                            for i in 0..n {
                                buf1[i] ^= 0x55;
                            }
                            target_stream.write_all(&buf1[..n]).await.unwrap();
                        }
                        // Server 从目标网站 (target_stream) 读回来的数据是明文。在发回给 Client (stream) 之前，需要先加密。
                        result = target_stream.read(&mut buf2) => {
                            let n = match result {
                                Ok(n) => n,
                                Err(e) => {
                                    println!("读取失败: {}", e);
                                    break;
                                }
                            };
                            if n == 0 {break;}
                            for i in 0..n {
                                buf2[i] ^= 0x55;
                            }
                            stream.write_all(&buf2[..n]).await.unwrap();
                        }
                    }
                }

            });
        }
    } else if mode == "client" {
        println!("Client 模式启动！");

        let listener = TcpListener::bind("127.0.0.1:1080").await.unwrap();

        loop {
            let (mut stream, _addr) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                // let mut buf = [0u8; 3];
                // stream.read_exact(&mut buf).await.unwrap();
                // println!("收到数据: {:?}", buf);

                // 1. 先只读 2 个字节，获取版本号和方法数量。
                let mut version_and_methods = [0u8; 2];
                stream.read_exact(&mut version_and_methods).await.unwrap();

                // 2. 从第二个字节提取出方法数量。在 Rust 中，数组长度需要是 usize 类型，所以我们要转换一下：
                let nmethods = version_and_methods[1] as usize;

                // 3. 根据这个数量，创建一个动态数组（Vec），并把剩下的认证方法字节读完，清空管道：
                let mut methods = vec![0u8; nmethods];
                stream.read_exact(&mut methods).await.unwrap();

                println!("收到认证方法: {:?}", methods);

                if version_and_methods[0] == 5 {
                    stream.write_all(&[5, 0]).await.unwrap();
                }
                // return;

                let mut req_header = [0u8; 4];
                stream.read_exact(&mut req_header).await.unwrap();
                println!("收到请求头: {:?}", req_header);
                if req_header[1] != 1 {
                    return;
                }

                let target_addr = match req_header[3] {
                    1 => {
                        // 1. 准备一个 4 字节的数组读取 IP (stream.read_exact)
                        let mut addr = [0u8; 4];
                        stream.read_exact(&mut addr).await.unwrap();
                        // 2. 准备一个 2 字节的数组读取端口，并用 u16::from_be_bytes 转换
                        let mut port_buf = [0u8; 2];
                        stream.read_exact(&mut port_buf).await.unwrap();
                        let port = u16::from_be_bytes(port_buf);
                        // 3. 使用 format!("{}.{}.{}.{}:{}", ip[0], ip[1], ip[2], ip[3], port) 返回字符串
                        let ipv4_addr =
                            format!("{}.{}.{}.{}:{}", addr[0], addr[1], addr[2], addr[3], port);

                        ipv4_addr
                    }
                    3 => {
                        // 解析域名 (Domain)
                        let mut len_buf = [0u8; 1];
                        stream.read_exact(&mut len_buf).await.unwrap();
                        let len = len_buf[0] as usize;

                        let mut domain_buf = vec![0u8; len];
                        stream.read_exact(&mut domain_buf).await.unwrap();
                        let domain = String::from_utf8_lossy(&domain_buf);

                        let mut port_buf = [0u8; 2];
                        stream.read_exact(&mut port_buf).await.unwrap();
                        let port = u16::from_be_bytes(port_buf);

                        let domain = format!("{}:{}", domain, port);
                        domain
                    }
                    _ => {
                        println!("暂不支持的地址类型");
                        return;
                    } // _ => return,
                };
                println!("解析出的目标地址是: {}", target_addr);

                // 发送成功响应
                stream
                    .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();

                // 1. 在给客户端发送成功响应之后，不要去连接 target_addr 了。改为连接我们的代理服务端：
                let mut server_stream = TcpStream::connect("127.0.0.1:8081").await.unwrap();
                println!("成功连接到代理服务端: 127.0.0.1:8081");

                // 2. 此时的服务端还不知道我们要去哪。我们需要设计一个极其简单的自定义通信协议：将目标地址拼接上一个换行符 `\n`，发送给服务端。
                server_stream
                    .write_all(format!("{}\n", target_addr).as_bytes())
                    .await
                    .unwrap();

                // 3. 使用 copy_bidirectional 把本地的 stream（比如 curl）和 server_stream（代理服务端）对接起来。
                // 删除：copy_bidirectional(&mut stream, &mut server_stream).await.unwrap();

                // 1. 在进入循环之前，我们需要为双向数据流分别准备存放数据的“篮子”（因为不能边读边写同一个数组）
                let mut buf1 = [0u8; 8192]; // 用来装从本地浏览器/curl 读到的数据
                let mut buf2 = [0u8; 8192]; // 用来装从远端 Server 读到的数据
                loop {
                    tokio::select! {
                        // 2. 构建分支 A（加密发出 🔒）:这个分支负责处理客户端到服务端的方向：把本地发来的明文请求加密，然后交给 Server。
                        result = stream.read(&mut buf1) => {
                            let n = result.unwrap();
                            if n == 0 { break; }
                            // 对 buf1 进行 异或(XOR) 加密
                            for i in 0..n {
                                buf1[i] ^= 0x55;
                            }
                            // 把加密后的切片 &buf1[..n] 通过 server_stream.write_all(...).await 发送出去。
                            server_stream.write_all(&buf1[..n]).await.unwrap();
                        }

                        // 构建分支 B（解密收回 🔓）:这个分支负责处理服务端到客户端的方向：把 Server 发回来的加密响应解密，然后交回给本地。
                        result = server_stream.read(&mut buf2) => {
                            let n = result.unwrap();
                            if n == 0 { break; }
                            // 对 buf2 进行 异或(XOR) 解密
                            for i in 0..n {
                               buf2[i] ^= 0x55;
                            }
                            // 把解密后的切片 &buf2[..n] 通过 stream.write_all(...).await 发送出去。
                            stream.write_all(&buf2[..n]).await.unwrap();
                        }
                    }
                }
            });
        }
    }
}
