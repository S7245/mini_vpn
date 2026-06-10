# 13a TUIC client — TCP relay via Connect (setup + acceptance)

> Stage 13a:client-tun 把拦截到的 **TCP** 经 **TUIC `Connect`** 转发到一个成熟 **sing-box TUIC 服务端**。
> 双轨(`MINI_VPN_UPSTREAM=legacy|tuic`,默认 legacy 零回归)。设计见 ADR-0004 + 13a spec/plan。
> ⚠️ **凭据(UUID/password)不要提交进 git**;本文用占位符。

## TUIC v5 线格式(本阶段用到)

命令头 `[VER=0x05][TYPE]`;地址 `[ATYP][ADDR][PORT:u16 BE]`,ATYP **0x00=域名`[len][bytes]` / 0x01=IPv4 /
0x02=IPv6**(注意与 Stage-12 自定义 ATYP 不同)。

- **Authenticate(0x00)**[单向流]:`[0x05][0x00][UUID:16][TOKEN:32]`;
  `TOKEN = export_keying_material(out=32, label=UUID(16B), context=password)` —— 字节级对齐 sing-box。
- **Connect(0x01)**[双向流]:`[0x05][0x01][ADDR]`,写完头**立即双向搬字节**(0-RTT)。

## 出口:最小 sing-box TUIC server

```bash
curl -fsSL https://sing-box.app/install.sh | sh        # sing-box ≥ 1.8
mkdir -p /etc/sing-box
cp certs/dev/server-cert.pem certs/dev/server-key.pem /etc/sing-box/   # 复用 dev 证书(SAN 含 example.com)
UUID=$(uuidgen | tr 'A-Z' 'a-z')   # 记下;PASS 自定
```

`/etc/sing-box/config.json`(`<UUID>`/`<PASS>` 替换;**勿入库**):
```jsonc
{
  "log": { "level": "info" },
  "inbounds": [{
    "type": "tuic", "tag": "tuic-in", "listen": "::", "listen_port": 8443,
    "users": [{ "name": "u1", "uuid": "<UUID>", "password": "<PASS>" }],
    "congestion_control": "bbr",
    "tls": { "enabled": true, "server_name": "example.com", "alpn": ["h3"],
             "certificate_path": "/etc/sing-box/server-cert.pem",
             "key_path": "/etc/sing-box/server-key.pem" }
  }],
  "outbounds": [{ "type": "direct" }]
}
```
```bash
# 云安全组/防火墙放行 UDP 8443
sing-box run -c /etc/sing-box/config.json
ss -ulnp | grep 8443        # 确认在听 UDP
```
> 可与自研 server(TCP 8081)同机共存(不同端口/协议)。

## 客户端参数(tuic 模式;凭据从环境/文件注入,勿入库)

| env | 值 | 说明 |
|---|---|---|
| `MINI_VPN_UPSTREAM` | `tuic` | 切到 TUIC 上游 |
| `MINI_VPN_TUIC_SERVER` | `<VPS_IP>:8443` | sing-box 的 UDP 地址 |
| `MINI_VPN_TUIC_UUID` / `_PASSWORD` | `<UUID>` / `<PASS>` | 必须与 sing-box 一致 |
| `MINI_VPN_TUIC_SNI` | `example.com` | 与证书 SAN 一致 |
| `MINI_VPN_TUIC_CA_PATH` | `certs/dev/ca-cert.pem` | 信任 dev CA |
| `MINI_VPN_TUIC_ALPN` | `h3` | 必须与 sing-box `tls.alpn` 一致 |

## 验收 recipe(Task 5 接线完成后)

```bash
sudo MINI_VPN_UPSTREAM=tuic \
  MINI_VPN_TUIC_SERVER=<VPS_IP>:8443 \
  MINI_VPN_TUIC_UUID=<UUID> MINI_VPN_TUIC_PASSWORD=<PASS> \
  MINI_VPN_TUIC_SNI=example.com MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem MINI_VPN_TUIC_ALPN=h3 \
  ./target/debug/mini_vpn client-tun
# 路由靶子进 TUN(同 Stage 9),TCP:
curl -v -k -m 15 https://1.1.1.1/
```
**期望**:curl 拿到响应;sing-box 日志显示来自该 UUID 的 `Connect` 到 1.1.1.1:443。切回
`MINI_VPN_UPSTREAM=legacy` 仍正常(零回归)。**握手通即证明:token 字节对齐 ✅、Connect 编码正确 ✅、说的是真 TUIC ✅。**

> 拓扑:server/sing-box 的真实 IP 不可路由进 TUN(沿用既有约束,防回环)。
