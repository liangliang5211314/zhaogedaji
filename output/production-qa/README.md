# 生产视觉冒烟记录

- `home-icons-local-fixed.png`：C 端首页切换为本地 Material Symbols 子集字体后的 1440×1024 浏览器截图，移动画板宽 375px。
- `admin-icons-local-fixed.png`：后台待办中心切换为本地字体后的截图；侧栏、待办图标和箭头均为线性图标，不再显示英文 ligature 名称。
- 字体来源：Google Material Symbols Outlined 官方可变字体，按本项目实际使用的图标子集压缩为约 39KB WOFF2。
- 线上地图观察：高德 JS API Key 与安全密钥为空，地图容器降级为灰底；不影响列表、附近接口和导航动作，待补凭据后复验。
