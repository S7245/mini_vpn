# 07 Cert Path And SNI Alignment

## 背景

Stage 6 已经把 TUN 客户端的上游地址和 `tls_sni` 做成了运行时配置，但当我们把：

- `MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000`
- `MINI_VPN_TUN_TLS_SNI=example.com`

传入后，仍然可能握手失败。

根因不是地址没生效，而是 TLS 还有另一层输入：

- server 用哪张证书
- server 用哪把私钥
- client 信任哪份 CA/证书

如果这些材料还硬编码，`server_addr` 与 `tls_sni` 再可配，也无法真正把链路切到另一套证书身份。

中文要点：地址、SNI、证书材料三者必须一起对齐，TLS 才会稳定通过。

## 本阶段新增配置

### Server

```text
MINI_VPN_SERVER_BIND_ADDR
MINI_VPN_SERVER_CERT_PATH
MINI_VPN_SERVER_KEY_PATH
```

默认值：

```text
bind_addr = 127.0.0.1:8081
cert_path = cert.pem
key_path = key.pem
```

### Client TUN

```text
MINI_VPN_TUN_SERVER_ADDR
MINI_VPN_TUN_TLS_SNI
MINI_VPN_TUN_CA_PATH
```

默认值：

```text
server_addr = 127.0.0.1:8081
tls_sni = localhost
ca_path = cert.pem
```

## 为什么要拆出 CA / cert / key

职责拆分后更容易排障：

- `server_addr` 决定“连到哪个 socket”
- `tls_sni` 决定“客户端期望服务端是谁”
- `cert_path` / `key_path` 决定“服务端拿什么身份证明自己”
- `ca_path` 决定“客户端信任哪份身份证”

中文要点：TCP 打通不代表 TLS 能过，TLS 能过也不代表证书名字一定对。

## 开发证书脚本

新增脚本：

```bash
./scripts/gen-test-certs.sh
```

作用：

- 生成开发用 SAN 证书
- SAN 至少包含：
  - `localhost`
  - `example.com`
  - `127.0.0.1`

默认输出：

```text
certs/dev/server-cert.pem
certs/dev/server-key.pem
certs/dev/ca-cert.pem
```

中文要点：这些是开发联调用材料，不需要手工改源码，也不应该替换仓库根目录下的默认证书文件。

## 默认链路验证

先构建：

```bash
cargo build
```

终端 1 启动 server：

```bash
cargo run -- server
```

终端 2 启动 client-tun：

```bash
sudo ./target/debug/mini_vpn client-tun
```

预期：

- server 打印默认监听地址和默认证书路径
- client 打印默认 `server_addr/tls_sni/ca_path`
- TLS 握手成功

## example.com 覆盖链路验证

先生成开发证书：

```bash
./scripts/gen-test-certs.sh
```

终端 1：

```bash
export MINI_VPN_SERVER_BIND_ADDR=127.0.0.1:9000
export MINI_VPN_SERVER_CERT_PATH=certs/dev/server-cert.pem
export MINI_VPN_SERVER_KEY_PATH=certs/dev/server-key.pem
cargo run -- server
```

终端 2：

```bash
export MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000
export MINI_VPN_TUN_TLS_SNI=example.com
export MINI_VPN_TUN_CA_PATH=certs/dev/ca-cert.pem
sudo -E ./target/debug/mini_vpn client-tun
```

预期：

- server 打印 `127.0.0.1:9000` 和 `certs/dev/...`
- client 打印 `server_addr=127.0.0.1:9000`
- client 打印 `tls_sni=example.com`
- client 打印 `ca_path=certs/dev/ca-cert.pem`
- TLS 握手成功，不再出现 `NotValidForName`

## 常见失败模式

### 1. TCP 连接拒绝

```text
Connection refused
```

含义：

- 服务端没启动
- 或 `server_addr` 和 `bind_addr` 没对齐

### 2. 证书名不匹配

```text
InvalidCertificate(NotValidForName)
```

含义：

- `tls_sni` 生效了
- 但服务端证书的 SAN 不包含该名字

### 3. CA 不信任

可能表现为握手失败，但不是 `NotValidForName`，而是证书链不受信任。

含义：

- `ca_path` 指向了错误文件
- 或服务端证书不是由这份 CA/证书导出的

中文要点：先分清是“连不上”，还是“连上了但不信”，还是“信了但名字不对”。

## 工程建议

- `cargo build` / `cargo run -- server` 继续用普通用户执行
- 只有最终 `client-tun` 二进制运行才使用 `sudo`
- 不要再用 `sudo cargo run ...`，避免把 `target/` 重新写成 `root`
- 开发证书脚本生成的是临时联调材料，不建议直接提交到仓库
