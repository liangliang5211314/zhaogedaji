# 生产上线待决清单

## 已确认

- 生产 SSH：本机别名 `xiaotudou-prod`，目标服务器 `39.103.57.77`。
- 项目目录：`/www/wwwroot/zhaogedaji`。
- 服务管理：`systemd` 的 `zhaojishi.service`。
- 正式域名：C 端 `https://app.xingjiawu.cn`；后台 `https://admin.xingjiawu.cn`。
- 代码来源：GitHub `liangliang5211314/zhaogedaji`；服务器网络异常时使用经过 `git bundle verify` 的快进 bundle，仍由 `deploy.sh` 执行备份、更新、重启与健康检查。
- 生产 `API_SECRET`、`ADMIN_KEY`、`JWT_SECRET` 已换为非默认随机值；旧管理密钥兼容关闭。
- 高德 Web 服务 Key 已在生产 `app_settings.amap_ws_key` 中配置，不进入 Git。
- 高德 Web 端 JS API Key 与安全密钥已写入生产数据库，不进入 Git；配置前备份位于 `/www/wwwroot/zhaogedaji/data/backups/amap_js_config_20260714_100038/`。
- `/api/app-config/map` 已返回 `configured=true`；线上地图默认态、列表拖起态均加载真实高德底图，出现 AutoNavi 版权标识与 `[AMap] SDK ready`，控制台无 error。

## 仍需外部操作

1. 使用真实手机完成最后一项硬件验收：打开首页 → 授权定位 → 附近列表 → 详情 → 导航跳转高德。桌面浏览器无法替代系统定位权限和地图 App 唤起证明。

手机实机完成前，不把总目标标记为全部完成；真实地图、其它生产接口、数据安全、后台权限与页面运行态均已独立验收通过。
