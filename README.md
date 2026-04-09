- 输出结果：

```rust
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
                let mut magic_buf = [0u8; 40];
                match stream.read_exact(&mut magic_buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("读取暗号失败: {:?}", e);
                        return;
                    }
                };

                if &magic_buf[..38] != b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n" {
                    println!("遭遇主动探测或未知连接，静默断开！");
                    // 提示：直接结束当前的 spawn 任务，装死
                    return;
                }

                let mut addr_bytes = Vec::new();
                loop {
                    let mut byte = [0u8; 1];
                    stream.read_exact(&mut byte).await.unwrap();
                    if byte[0] == b'\n' {
                        break;
                    }
                    addr_bytes.push(byte[0]);
                }
                // 省略代码
            });
        }
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

