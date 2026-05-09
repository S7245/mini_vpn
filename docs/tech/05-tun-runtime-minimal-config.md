# 05 TUN Runtime Minimal Config

## 背景

Stage 4 已经把 TUN 监听池真正激活了，但运行时还有 3 个关键值是写死的：

- local port
- target address
- pool size

这会带来两个现实问题：

- 每次切换测试目标都需要改代码
- 运行时行为不够透明，不方便联调和复现

所以 Stage 5 的目标不是做一套“大而全”的配置系统，而是先把这 3 个硬编码拔掉。

## 这一步为什么只做最小配置版

如果一上来就把下面这些也一起拉进来：

- `server_addr`
- `tls_sni`
- `client-direct`

改动面会明显变大，排障边界也会被打散。

因此 Stage 5 先只做：

- `MINI_VPN_TUN_LOCAL_PORT`
- `MINI_VPN_TUN_TARGET_ADDR`
- `MINI_VPN_TUN_POOL_SIZE`

这一步的原则是：

- keep Stage 4 behavior by default
- move configuration decisions to startup
- do not change the shared relay protocol

中文要点：先把最痛的 3 个硬编码拔掉，而不是一口气做成统一配置中心。

## 关键结构

本阶段新增了 `TunRuntimeConfig`。

它负责：

- 读取环境变量
- 应用默认值
- 校验非法输入
- 派生 `ListenerSpec`

它不负责：

- 创建 TUN 设备
- 管理 smoltcp 生命周期
- 管理 Yamux 子流

中文要点：`TunRuntimeConfig` 只做“启动参数归一化”，不碰热路径业务逻辑。

## 默认值

当环境变量缺失时，系统保持 Stage 4 的默认行为：

- `local_port = 80`
- `target_addr = httpbin.org:80`
- `pool_size = 4`

这保证了已有联调路径不需要任何额外改动。

## 环境变量

Stage 5 支持以下配置入口：

```bash
MINI_VPN_TUN_LOCAL_PORT
MINI_VPN_TUN_TARGET_ADDR
MINI_VPN_TUN_POOL_SIZE
```

校验规则如下：

- `MINI_VPN_TUN_LOCAL_PORT` 必须能解析为 `u16`
- `MINI_VPN_TUN_TARGET_ADDR` 必须能被 `TargetAddr::parse()` 正确解析
- `MINI_VPN_TUN_POOL_SIZE` 必须能解析为 `usize`，且至少为 `1`

如果用户显式传入非法值，启动会直接失败，而不是偷偷回退到默认值。

中文要点：缺省才走默认值，显式错误输入必须尽早失败。

## 运行流变化

### Stage 4 之前

```text
start_tun_proxy()
-> read hardcoded constants
-> build ListenerSpec
-> parse hardcoded target
-> enter runtime loop
```

### Stage 5 之后

```text
start_tun_proxy()
-> TunRuntimeConfig::from_env()
-> derive ListenerSpec
-> clone TargetAddr
-> enter Stage 4 runtime loop
```

中文要点：Stage 5 改的是启动前半段，不改 Stage 4 已经验证通过的主循环骨架。

## 使用示例

默认启动：

```bash
./target/debug/mini_vpn client-tun
```

覆盖启动参数：

```bash
MINI_VPN_TUN_LOCAL_PORT=8080 \
MINI_VPN_TUN_TARGET_ADDR=127.0.0.1:7897 \
MINI_VPN_TUN_POOL_SIZE=2 \
./target/debug/mini_vpn client-tun
```

如果配置生效，启动日志会显示类似内容：

```text
TUN runtime started with local_port=8080, pool_size=2, target=127.0.0.1:7897
```

## 测试策略

本阶段重点测试 4 类情况：

- 默认值是否保持 Stage 4 行为
- 自定义值是否能正确派生 `ListenerSpec`
- 非法端口是否被拒绝
- 非法目标地址或非法 `pool_size` 是否被拒绝

这样做的目的不是测试环境变量 API 本身，而是测试“启动配置是否稳定可预测”。

## 这一步带来的收益

- 不再需要为每次联调改源码
- 默认行为与 Stage 4 保持一致
- 错误配置能在启动时尽早暴露
- 为后续扩展 `server_addr`、`tls_sni`、`client-direct` 统一配置留下清晰入口

## 下一步

Stage 5 之后，一个自然的下一阶段是：

- add `server_addr`
- add `tls_sni`
- consider shared config entry for `client-direct`

但这应该在保持当前最小配置版稳定之后再做。

中文要点：先把最小配置入口跑稳，再扩成统一配置层，别把多个风险点绑在同一个阶段里。
