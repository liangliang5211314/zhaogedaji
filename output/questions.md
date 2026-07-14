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
- 小米 M2007J1SC 已完成真实系统定位与附近列表验收，返回 12 条附近集市；下拉刷新指示条 618ms 内收起。

## 仍需外部操作

1. 使用真实手机完成最后一项硬件验收：从已通过的首页附近列表进入详情 → 点击导航 → 确认系统成功唤起高德 App。

详情到高德 App 的实机链路完成前，不把总目标标记为全部完成；首页定位、附近推荐、下拉刷新、真实地图与其它生产能力均已独立验收通过。
