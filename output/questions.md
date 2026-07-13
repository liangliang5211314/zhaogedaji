# 生产上线待决清单

## 已确认

- 生产 SSH：本机别名 `xiaotudou-prod`，目标服务器 `39.103.57.77`。
- 项目目录：`/www/wwwroot/zhaogedaji`。
- 服务管理：`systemd` 的 `zhaojishi.service`。
- 正式域名：C 端 `https://app.xingjiawu.cn`；后台 `https://admin.xingjiawu.cn`。
- 代码来源：GitHub `liangliang5211314/zhaogedaji`；服务器网络异常时使用经过 `git bundle verify` 的快进 bundle，仍由 `deploy.sh` 执行备份、更新、重启与健康检查。
- 生产 `API_SECRET`、`ADMIN_KEY`、`JWT_SECRET` 已换为非默认随机值；旧管理密钥兼容关闭。
- 高德 Web 服务 Key 已在生产 `app_settings.amap_ws_key` 中配置，不进入 Git。

## 仍需外部操作

1. 高德控制台创建或提供“Web 端（JS API）”Key 与安全密钥 `securityJsCode`，并把 `app.xingjiawu.cn` 加入域名白名单。它们与 Web 服务 Key 不是同一种凭据；当前 `/api/app-config/map` 明确返回 `configured=false`，地图页使用降级提示。
2. 使用真实手机完成最后一项硬件验收：打开首页 → 授权定位 → 附近列表 → 详情 → 导航跳转高德。桌面浏览器无法替代系统定位权限和地图 App 唤起证明。

以上两项完成前，不把“真实地图 + 手机实机”标为通过；其它生产接口、数据安全、后台权限与页面运行态继续独立验收。
