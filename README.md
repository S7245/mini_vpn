- 输出结果：

```rust
if mode == "client" {
    println!("Client 模式启动！");
    let listener = TcpListener::bind("127.0.0.1:1080").await.unwrap();
    loop {
        let (mut stream, _addr) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            // 省略代码...
            // 1. 在给客户端发送成功响应之后，不要去连接 target_addr 了。改为连接我们的代理服务端：
            let mut server_stream = TcpStream::connect("127.0.0.1:8081").await.unwrap();
            println!("成功连接到代理服务端: 127.0.0.1:8081");
            // 2. 此时的服务端还不知道我们要去哪。我们需要设计一个极其简单的自定义通信协议：将目标地址拼接上一个换行符 `\n`，发送给服务端。
            server_stream.write_all(format!("{}\n", target_addr).as_bytes()).await.unwrap();
            
            // 2. 给 Server 换上新引擎
            // 2.1. 准备一个统一的 32 字节密钥 (两边必须一致！)
            let secret_key = b"an example very very secret key.";
            let mut encrypted_server = Framed::new(server_stream,VpnCodec::new(secret_key));
            // 2.2. 准备本地明文缓冲区
            let mut buf_from_local = [0u8; 8192];
            // 编写对称的 tokio::select! 循环：
            loop {
                tokio::select! {
                    // 分支 A (本地发往远端)：从本地 stream 读取明文到 buf_from_local，打包成 VpnFrame，然后使用 encrypted_server.send(...).await 发送出去。
                    result = stream.read(&mut buf_from_local) => {
                        let n = result.unwrap();
                        if n == 0 { break; }
                        // 把 buf_from_local[..n] 通过 encrypted_server.send(...).await 发送出去。
                        encrypted_server.send(VpnFrame {
                            data: buf_from_local[..n].to_vec(),
                        }).await.unwrap();
                    }
                    
                    // 分支 B (远端收回本地)：调用 encrypted_server.next().await 接收解密好的 VpnFrame，把里面的 data 使用 stream.write_all(...).await 写回给本地浏览器/curl。
                    result = encrypted_server.next() => {
                        let frame = result.unwrap();
                        stream.write_all(&frame.data).await.unwrap();
                    }
                }
            }
        });
    }
}
```

- 执行：`curl --socks5 127.0.0.1:8080 www.reddit.com`

- Client输出：
```ini
Client 模式启动！
到认证方法: [0, 1]
收到请求头: [5, 1, 0, 1]
解析出的目标地址是: 151.101.193.140:80
成功连接到代理服务端: 127.0.0.1:8081
```

- Server输出：
```ini
运行服务器端
解析出的目标地址是: 151.101.193.140:80
```

