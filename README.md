📄 src/main.rs: 指挥中心。只负责解析命令行参数，然后按需启动 Client 或 Server。
📄 src/server.rs: 服务端基站。封装 Server 模式下的监听、TLS 握手和 Yamux 逻辑。
📄 src/client.rs: 客户端引擎。封装 SOCKS5 解析、长连接维护和多路复用逻辑。


- 关键代码片段：

```rust
if payload_buf.len() < 4 || payload_buf[0] != 0 || payload_buf[1] != 0 {
    println!("非法的 SOCKS5 UDP 数据包");
    continue; // 直接处理下一个包
}
let atyp = payload_buf[3];
let mut header_len = 0; // 用来记录“导航头”一共占了多少字节
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
        let domain = String::from_utf8_lossy(&payload_buf[5..5 + domain_len]);
        let port = u16::from_be_bytes(payload_buf[5 + domain_len..7 + domain_len].try_into().unwrap());
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
println!("准备将 {} 字节真实数据发往 {}", real_data.len(), target_addr);
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