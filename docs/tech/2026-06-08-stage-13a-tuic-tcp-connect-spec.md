# Stage 13a — TUIC client: TCP relay via Connect (spec)

> grill 产出(见 ADR-0004 + 2026-06-08 TUIC evaluation)。第一刀:让 client-tun 把拦截到的 **TCP**
> 经 **TUIC `Connect`** 转发到一个成熟 **sing-box TUIC 服务端**;**双轨**(配置开关切 legacy/tuic),
> legacy(yamux+自研 server)零改动。UDP 仍走 legacy(13b 再迁)。

## 背景 / 定位

ADR-0004:数据面改用 **TUIC v5 协议**(成熟、QUIC、0-RTT),**client-only**,出口用 sing-box。13a 是
其中最小闭环:**TCP 经 sing-box 通**。复用现有 quinn(0.10,已含 `export_keying_material`,TUIC 认证用)。

## 目标

1. 新增 `tuic` 上游:QUIC 连 sing-box → `Authenticate` → 每条拦截 TCP 开一条 `Connect` 双向流中继。
2. 引入 **`ProxyUpstream` trait**(代理上游抽象),`legacy`(yamux)与 `tuic` 各一个实现;主循环按配置择一。
3. **双轨配置开关**(`MINI_VPN_UPSTREAM=legacy|tuic`,默认 `legacy` → 零回归)。
4. 验收:`MINI_VPN_UPSTREAM=tuic` 下 `curl https://1.1.1.1/` 经真 sing-box TUIC server 通。

## 非目标(后续刀)

- TUIC UDP(`Packet` native+quic 模式)→ **13b**(13a 的 tuic 模式下 UDP 暂不支持)。
- 连接迁移 / 0-RTT / heartbeat 调优 → **13c**。
- 退役 legacy + 自研 server → **13d**。
- VLESS+REALITY 回退、协议选择器、移动端 I/O backend → roadmap。

## 术语(见 CONTEXT.md)

- **Upstream**:客户端穿过的代理出口。13a 起它可以是**外部 sing-box TUIC server**(不再必须是自研 server)。
- **ProxyUpstream**(新):代理上游的抽象——给「目标 + 字节流」,把它中继到出口。`legacy`/`tuic` 两实现。

## TUIC v5 线格式(13a 用到的部分)

命令头 `[VER=0x05][TYPE][OPT…]`。地址 `[ATYP][ADDR][PORT:u16 BE]`(0x00=域名`[len][bytes]` / 0x01=IPv4 /
0x02=IPv6 —— 以 sing-box 实现为准,实现时核对)。

- **Authenticate(0x00)**(每连接一次,走**单向流**):`[VER][0x00][UUID:16][TOKEN:32]`。
  `TOKEN = conn.export_keying_material(out=32, label=UUID(16B), context=password)` —— **字节级对齐 sing-box**。
- **Connect(0x01)**(每条 TCP 一条**双向流**):`[VER][0x01][ADDR]`,**写完头立即开始双向搬字节**(0-RTT,
  不等服务端响应)。fake-IP→域名 ⇒ ATYP=域名;IpPort ⇒ ATYP=IPv4/IPv6。
- Heartbeat/Dissociate/Packet → 13b/13c。

## 架构 / 数据流(tuic 模式,TCP)

```text
启动:quinn connect sing-box(TLS: SNI + CA + ALPN)→ 开 uni-stream 发 Authenticate(UUID+token)
每条拦截 TCP(沿用 Stage 8/9/11 的 SYN→listener→首包→resolve_target 得 Target):
  upstream.open_tcp(Target) → 开 QUIC 双向流 → 写 Connect(Target) → 返回该流
  → 主循环像现在一样把 smoltcp socket 与该流双向泵(spawn_remote_relay 逻辑复用)
```

## 模块改动

### 新增 `src/upstream.rs`（lib）— 上游抽象
- `trait ProxyUpstream`(13a 只含 TCP):
  ```rust
  async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError>;
  ```
  `RelayStream` = 统一的 `AsyncRead+AsyncWrite+Unpin+Send`(legacy 包 `Compat<yamux::Stream>`;tuic 包
  QUIC 双向流的 compat)。用 enum 或 `Box<dyn …>` 收口(系统稳定优先,取简单可靠者)。
- `LegacyYamuxUpstream`:包现有 `open_remote_session(ctrl, RelayRequest::Tcp)`。
- `TuicUpstream`:见下。

### 新增 `src/tuic.rs`（lib）— TUIC 客户端
- `TuicClientConfig`(结构体,单一事实源):`server(UDP SocketAddr) / uuid / password / sni / ca_path /
  alpn / congestion_control / udp_relay_mode`。
- `connect(cfg) -> TuicUpstream`:建 quinn 连接(复用 `quic.rs` 的 config 思路:rustls 0.21 + ALPN;
  **ALPN/SNI/CA 必须和 sing-box 对齐**)→ 发 Authenticate(uni-stream)。
- `encode_connect(target) -> Vec<u8>` / `encode_authenticate(uuid, token) -> Vec<u8>` / `encode_address`
  —— **纯函数,TDD 主战场**(各 ATYP round-trip / 越界不 panic)。
- `impl ProxyUpstream for TuicUpstream { open_tcp }`:开双向流、写 Connect 头、返回流。

### `src/client_tun.rs`
- 启动按 `MINI_VPN_UPSTREAM` 构造 `Box<dyn ProxyUpstream>`(legacy 或 tuic),放进主循环。
- `handle_local_payload` 的「开远端」从 `open_remote_session(ctrl, …)` 改为 `upstream.open_tcp(target)`;
  其余(uplink_tx / spawn_remote_relay / 下行 global channel)**不变**。
- legacy 分支行为**逐字保持**(零回归)。

### `Cargo.toml`
- 无新增(quinn/rustls/bytes 已在;TUIC 协议自实现)。

## 配置变化(新增,默认不改变现状)

| env | 默认 | 说明 |
|---|---|---|
| `MINI_VPN_UPSTREAM` | `legacy` | `legacy`\|`tuic`;默认 legacy → 零回归 |
| `MINI_VPN_TUIC_SERVER` | — | sing-box TUIC 的 UDP 地址(tuic 模式必填) |
| `MINI_VPN_TUIC_UUID` / `_PASSWORD` | — | 凭据(**不入日志**) |
| `MINI_VPN_TUIC_SNI` / `_CA_PATH` | 复用现有 | QUIC TLS 校验,需与 sing-box 证书一致 |
| `MINI_VPN_TUIC_ALPN` | `h3` | **必须与 sing-box `tls.alpn` 一致** |

## 验收 recipe

### 层 1 — TDD 纯函数(CI)
`tuic.rs` 单测:`encode_address`(IPv4/IPv6/域名,越界 None)、`encode_connect`、`encode_authenticate`
字节布局;config 解析(缺 server/uuid/password 报错;默认 legacy)。

### 层 2 — 互通 e2e(手动,对真 sing-box)
**起一个最小 sing-box TUIC server**(VPS 或本地;证书复用 dev 证书):
```jsonc
// sing-box config.json
{
  "inbounds": [{
    "type": "tuic", "listen": "::", "listen_port": 8443,
    "users": [{ "uuid": "<UUID>", "password": "<PASS>" }],
    "congestion_control": "bbr",
    "tls": { "enabled": true, "alpn": ["h3"],
             "certificate_path": "certs/dev/server-cert.pem",
             "key_path": "certs/dev/server-key.pem" }
  }],
  "outbounds": [{ "type": "direct" }]
}
```
客户端(tuic 模式):
```bash
sudo MINI_VPN_UPSTREAM=tuic \
  MINI_VPN_TUIC_SERVER=<SINGBOX_IP>:8443 \
  MINI_VPN_TUIC_UUID=<UUID> MINI_VPN_TUIC_PASSWORD=<PASS> \
  MINI_VPN_TUIC_SNI=example.com MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem \
  ./target/debug/mini_vpn client-tun
# 路由靶子进 TUN（同 Stage 9），TCP：
curl -v -k -m 15 https://1.1.1.1/
```
**期望**:curl 拿到响应;sing-box 日志显示来自我们 UUID 的 `Connect` 到 1.1.1.1:443;切回
`MINI_VPN_UPSTREAM=legacy` 仍正常(零回归)。**这同时证明:认证 token 字节级对齐 ✅、Connect 编码正确 ✅、
我们确实在说真 TUIC 协议(而非自造)✅。**

> server 真实 IP / sing-box IP 不可路由进 TUN(沿用既有拓扑约束,避免回环)。

## 风险 / 注记
- **认证字节级对齐**是 interop 命门:token 的 label/context/长度、Connect/Address 的 ATYP 取值,**实现时对着
  sing-box 源码/抓包逐字节核**(单测覆盖编码,真值由 sing-box 握手验)。
- `RelayStream` 统一类型:legacy 与 tuic 两种底层流要收成同一类型喂给现有泵逻辑——用 enum 最稳。
- ALPN/SNI/证书三者任一与 sing-box 不一致 → 握手失败;recipe 里固定一组对齐值。
