📄 src/main.rs: 指挥中心。只负责解析命令行参数，然后按需启动 Client 或 Server。
📄 src/server.rs: 服务端基站。封装 Server 模式下的监听、TLS 握手和 Yamux 逻辑。
📄 src/client.rs: 客户端引擎。封装 SOCKS5 解析、长连接维护和多路复用逻辑。

- 关键代码片段：

```rust
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
```

- 执行：`curl --socks5-hostname 127.0.0.1:1080 ipinfo.io`

- Client输出：

```ini
与代理服务端 TLS 握手失败: Custom { kind: InvalidData, error: InvalidCertificate(Other(CaUsedAsEndEntity)) }
```

- Server输出：

```ini
TLS 握手失败: Custom { kind: InvalidData, error: AlertReceived(CertificateUnknown) }
```

```sh
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"

# req -x509: 指定创建自签名证书。
# -newkey rsa:2048: 生成一个 2048 位的 RSA 私钥。
# -keyout key.pem: 指定输出的私钥文件名。
# -out cert.pem: 指定输出的证书文件名。
# -days 365: 证书有效期为 365 天。
# -nodes (No DES): 生成不加密的私钥（本地测试用），无需输入密码。
# -subj "/CN=localhost": 直接填写证书申请信息，跳过交互式问答。
```
