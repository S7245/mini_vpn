# 07 Server Bind Config And Client Connect Errors

## 背景

Stage 6 已经让 `client-tun` 支持这两个上游覆盖项：

- `MINI_VPN_TUN_SERVER_ADDR`
- `MINI_VPN_TUN_TLS_SNI`

但如果服务端仍然硬编码监听 `127.0.0.1:8081`，而客户端被切到 `127.0.0.1:9000`，那么覆盖值虽然生效，最终仍然会在 TCP 建连时失败。

中文要点：客户端“会不会按你说的去连”和服务端“有没有真的在那个端口等着”，是两件事。

## 这次修复做了什么

### 1. 服务端监听地址可配置

`server.rs` 新增了最小运行时配置：

- `MINI_VPN_SERVER_BIND_ADDR`

默认值仍然保持：

```text
127.0.0.1:8081
```

所以旧用法不受影响，只有显式传值时才覆盖。

### 2. client-tun 不再因为上游不可达直接 panic

之前 `client_tun.rs` 在上游 TCP 建连失败时使用了 `expect(...)`。

这意味着：

- 若服务端没启动
- 或端口不匹配

程序会直接 panic。

现在改成了显式日志后返回，例如：

```text
连接代理服务端失败 127.0.0.1:9000: Connection refused
```

中文要点：这类错误属于运行条件不满足，不应该用 panic 表达。

## 默认联调脚本

先启动服务端：

```bash
cargo run -- server
```

再启动 TUN 客户端：

```bash
sudo cargo run -- client-tun
```

这条路径沿用默认配置：

- server bind addr = `127.0.0.1:8081`
- client upstream server addr = `127.0.0.1:8081`
- client tls sni = `localhost`

## 覆盖到 9000 的联调脚本

先启动服务端：

```bash
export MINI_VPN_SERVER_BIND_ADDR=127.0.0.1:9000
cargo run -- server
```

再启动客户端：

```bash
export MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000
export MINI_VPN_TUN_TLS_SNI=example.com
sudo -E cargo run -- client-tun
```

如果覆盖成功，你应该在客户端看到：

```text
TUN runtime started with local_port=80, pool_size=4, target=httpbin.org:80, server_addr=127.0.0.1:9000, tls_sni=example.com
```

## 如果仍然失败，先看哪一层

### 场景 1：日志还是 `8081 / localhost`

说明环境变量没有传进 `sudo` 子进程。

优先检查：

```bash
sudo -E cargo run -- client-tun
```

### 场景 2：日志已经是 `9000 / example.com`，但报 `Connection refused`

说明客户端配置已经生效，但服务端没有监听在 `127.0.0.1:9000`。

可以先确认端口是否监听：

```bash
lsof -nP -iTCP:9000 -sTCP:LISTEN
```

### 场景 3：TCP 建连成功，但 TLS 握手失败

说明端口打通了，但证书/SNI 与服务端 TLS 配置不一致。

中文要点：先看“有没有监听”，再看“有没有握手成功”，不要把两层问题混在一起。

## 这一步的收益

- 服务端和客户端都支持最小端口对齐
- 默认值行为保持不变
- 上游不可达时不再 panic
- 本机联调脚本更清楚，适合后续继续扩到 `cert_path`
