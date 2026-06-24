# sing-box VLESS+REALITY inbound 准备清单（刀8 真出口 acceptance 服务端）

> 配合 mini_vpn 刀8 REALITY 客户端（**空 flow + cipher 0x1301**，见 ADR-0008/0009/0010 + 研究 brief）。
> 凭据**勿入库**——本文件只含占位符；实际 UUID/key 经 env 注入（见 §5 / `scripts/knife8-reality-acceptance.sh`）。

## 1. 在 VPS 上生成凭据

```bash
sing-box generate reality-keypair    # PrivateKey / PublicKey（均 base64url RawURL，43 字符）
# PrivateKey: <server reality.private_key - NOT IN GIT>
# PublicKey:  <client MINI_VPN_REALITY_PBK - NOT IN GIT>
sing-box generate uuid               # 或任意 RFC4122 UUID
# UUID: <both ends identical - NOT IN GIT>
openssl rand -hex 8                   # short_id（8 字节=16 hex；1-8 字节都行）
# short_id: <both ends identical - NOT IN GIT>
```

| 产物 | 用途 |
|---|---|
| **PrivateKey** | 服务端 `reality.private_key` |
| **PublicKey** | 客户端 `MINI_VPN_REALITY_PBK`（base64url 43 字符，解码须恰 32B） |
| UUID | 两端逐字一致 |
| short_id (hex) | 两端逐字一致；≤ 8 字节 |

## 2. 选借用站（handshake.server / decoy）

要求：① 从 **VPS 出口**可达 `:443`；② 支持 TLS 1.3。客户端只 offer 0x1301，RFC 8446 §9.1 强制所有合规 TLS1.3 服务端必须支持 0x1301 → 几乎任何站都行（含 microsoft/apple）。仍建议**从 VPS 上**预检：

```bash
echo | openssl s_client -connect <DECOY>:443 -servername <DECOY> \
      -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256 2>&1 | grep "Cipher is"
# 期望：New, TLSv1.3, Cipher is TLS_AES_128_GCM_SHA256

# echo | openssl s_client -connect gateway.icloud.com:443 -servername gateway.icloud.com -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256 2>&1 | grep "Cipher is"
# echo | openssl s_client -connect dl.google.com:443 -servername dl.google.com -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256 2>&1 | grep "Cipher is"
# echo | openssl s_client -connect www.cloudflare.com:443 -servername www.cloudflare.com -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256 2>&1 | grep "Cipher is"
```

候选（靠近 VPS 更稳）：`gateway.icloud.com` / `dl.google.com` / `www.cloudflare.com`。

```ini
[你的客户端] ──REALITY(伪装成访问 DECOY)──> [你的 1 台 VPS: sing-box]
                                                  │
                              探针没认证时 ──io.Copy──> [DECOY 真站(别人的, 如 gateway.icloud.com)]
```

## 3. inbound 配置（最小，空 flow）

```jsonc
{
  "inbounds": [{
    "type": "vless",
    "tag": "vless-reality-in",
    "listen": "::",
    "listen_port": 443,                       // ← 客户端 MINI_VPN_REALITY_SERVER 的端口
    "users": [
      { "uuid": "<UUID>", "flow": "" }         // ← flow 必须空（Vision 已 defer）
    ],
    "tls": {
      "enabled": true,
      "server_name": "<DECOY>",                // ← 客户端 SNI 必须 == 此值
      "reality": {
        "enabled": true,
        "handshake": { "server": "<DECOY>", "server_port": 443 },  // 借用站，建议与 server_name 同域
        "private_key": "<PrivateKey>",
        "short_id": [ "<SHORT_ID_HEX>" ]       // 数组；含客户端用的那个
      }
    }
  }]
}
```

**三处域名一致**（建议）：客户端 `SNI` == `tls.server_name` == `handshake.server` 域名。客户端 SNI 不在服务端 serverNames 集合 → 服务端静默转发 decoy、**不报错**（最难排查）。

## 4. 不变量核对（对不上 = 静默失败）

| 项 | 约束 |
|---|---|
| `flow` | **空**（两端）。Vision 未实现 |
| cipher | 借用站需协商 **0x1301**（客户端只 offer 这个；0x1302/0x1303 会 loud-fail，ADR-0009） |
| SNI | 客户端 SNI == 服务端 `server_name` |
| UUID / short_id | 两端逐字一致；short_id ≤ 8 字节 hex |
| public/private key | 同一对；客户端用 **public**，服务端用 **private** |
| 防火墙 | VPS `:443` 对客户端放行；VPS → 借用站 `:443` 出站放行 |

## 5. 启服务端 + 客户端 env

```bash
sing-box run -c config.json
```
```bash
export MINI_VPN_REALITY_SERVER=<VPS_IP>:443
export MINI_VPN_REALITY_UUID=<UUID>
export MINI_VPN_REALITY_PBK=<PublicKey>        # base64url 43 字符
export MINI_VPN_REALITY_SHORT_ID=<SHORT_ID_HEX>
export MINI_VPN_REALITY_SNI=<DECOY>            # == server_name
```

## 6. 验收

```bash
DECOY=<DECOY> bash scripts/knife8-reality-acceptance.sh preflight
sudo -E bash scripts/knife8-reality-acceptance.sh soak
bash scripts/knife8-reality-acceptance.sh smoke
sudo -E bash scripts/knife8-reality-acceptance.sh soak-stop
```
**通过判据**：client 日志见 `🔐 REALITY 握手成功（证书 HMAC 校验通过）`（非 echo 充数）+ curl 200/301 三端闭环。
**注意**：REALITY=TCP-only，HTTP/3(QUIC/UDP) 被 no-op 丢 → curl 自动回落 TCP/HTTP2（符合预期）。

## 7. 排错速查

| 症状 | 多半原因 |
|---|---|
| client 无 `🔐` 行、curl 失败 | SNI ∉ server_name / pbk 错 / short_id 不符 → 被静默回落 decoy（HMAC 必败） |
| client 日志 `cipher_suite != 0x1301` loud-fail | 借用站协商了 0x1302（换借用站，§2 预检） |
| client 日志 `REALITY 握手超时` | VPS 不可达 / `:443` 被墙 / 借用站从 VPS 出口不可达 |
| 握手成功但无数据 | VLESS 帧问题（已 KAT 覆盖；抓包看 sing-box vless 日志） |
| 长连接随机断 | 借用站发了 KeyUpdate（本刀 loud-fail，留刀9，见 ADR-0010 gap） |