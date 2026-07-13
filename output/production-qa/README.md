# 生产视觉冒烟记录

- `home-icons-local-fixed.png`：C 端首页切换为本地 Material Symbols 子集字体后的 1440×1024 浏览器截图，移动画板宽 375px。
- `admin-icons-local-fixed.png`：后台待办中心切换为本地字体后的截图；侧栏、待办图标和箭头均为线性图标，不再显示英文 ligature 名称。
- 字体来源：Google Material Symbols Outlined 官方可变字体，按本项目实际使用的图标子集压缩为约 39KB WOFF2。
- 线上地图观察：高德 JS API Key 与安全密钥为空，地图容器降级为灰底；不影响列表、附近接口和导航动作，待补凭据后复验。

## 生产功能冒烟（2026-07-14）

- 当前提交：`a0dc1ce`；生产服务器、本机、GitHub、NAS 一致。
- 写入型生产冒烟已覆盖注册、密码登录、收藏、提醒、后台两级角色、待办/复核队列、open_time v2 保存与阴历月历；合成数据全部清理，业务表计数恢复到测试前。
- 李官大集生产临时用例返回 2026-07 的 5、10、14、19、24、29 日及对应阴历小字。
- 真实待办总数 676：坐标 521、名称 84、集期 71，其余当前为 0；OCR 名称已能从待办中心直达真实处理队列。
- PWA manifest、favicon、logo、10 个 icon 与本地 Material Symbols 字体均为 HTTP 200。
- 后台 HTML 已追加 `no-cache, no-store, must-revalidate`，避免部署后继续显示旧待办数。
- 完整提交、数据 diff、冒烟矩阵与回滚路径见 `output/deployment-report.md`。
