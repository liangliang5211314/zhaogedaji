# 生产视觉冒烟记录

- `home-icons-local-fixed.png`：C 端首页切换为本地 Material Symbols 子集字体后的 1440×1024 浏览器截图，移动画板宽 375px。
- `admin-icons-local-fixed.png`：后台待办中心切换为本地字体后的截图；侧栏、待办图标和箭头均为线性图标，不再显示英文 ligature 名称。
- 字体来源：Google Material Symbols Outlined 官方可变字体，按本项目实际使用的图标子集压缩为约 39KB WOFF2。
- `map-default-live.png`：生产域名地图默认态，真实高德底图、POI、AutoNavi 版权标识与默认半屏列表均正常。
- `map-raised-live.png`：生产域名地图列表拖起态，真实高德底图、四张列表卡片、650 m 距离案例与“沂州古城庙会”示例名称均正常。
- `real-device-20260715/pull-refresh-fixed.png`：小米 M2007J1SC 真机、小米浏览器、真实定位下的首页刷新完成态；12 条附近推荐与县区补全名称正常，刷新指示条已完全收起。
- 线上地图复验：`/api/app-config/map` 返回 `configured=true`；默认态与拖起态均记录 `[AMap] SDK ready`，控制台无 error。Web 端凭据只保存在生产数据库，不写入代码或本归档。

## 生产功能冒烟（2026-07-14）

- 当前提交：`a0dc1ce`；生产服务器、本机、GitHub、NAS 一致。
- 写入型生产冒烟已覆盖注册、密码登录、收藏、提醒、后台两级角色、待办/复核队列、open_time v2 保存与阴历月历；合成数据全部清理，业务表计数恢复到测试前。
- 李官大集生产临时用例返回 2026-07 的 5、10、14、19、24、29 日及对应阴历小字。
- 真实待办总数 676：坐标 521、名称 84、集期 71，其余当前为 0；OCR 名称已能从待办中心直达真实处理队列。
- PWA manifest、favicon、logo、10 个 icon 与本地 Material Symbols 字体均为 HTTP 200。
- 后台 HTML 已追加 `no-cache, no-store, must-revalidate`，避免部署后继续显示旧待办数。
- 高德 Web 端配置写入前已备份生产数据库；写入后完整性检查通过，生产地图两态截图见本目录。
- 真实手机首页定位、附近列表与下拉刷新已完成硬件验收；当前仅剩详情页到高德 App 的唤起链路。
- 2026-07-15 真机补充：下拉刷新不再全量下载 3030 条集市，改为附近推荐单请求；实测指示条 618ms 收起。首页定位与附近列表已通过，只剩详情到高德 App 唤起。
- 完整提交、数据 diff、冒烟矩阵与回滚路径见 `output/deployment-report.md`。
