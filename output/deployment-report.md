# 找个大集生产上线报告

生成时间：2026-07-14（Asia/Shanghai）

## 当前生产版本

- C 端：`https://app.xingjiawu.cn`
- 管理后台：`https://admin.xingjiawu.cn`
- 代码提交：`a0dc1ce`（生产服务器、本机、GitHub、NAS 一致）
- 上线标签：`production-20260714`
- 上一版回滚标签：`production-pre-rollout-20260714` → `3c6477d`
- 服务：`zhaojishi.service=active`，`nginx=active`
- 数据库：`PRAGMA integrity_check=ok`，集市 3030 条

## 提交列表

### 数据与推荐

- `619ef02` 实现 open_time v2 迁移与历法服务
- `d72381b` 完善集市坐标精准化与断点报告
- `d3dabc3` 新增时间感知附近集市推荐
- `43b921d` 补救坐标编码并建立数据复核队列
- `6373806` 补严 OCR 分隔符脏数据识别

### C 端

- `e426546` 首页四态与品牌闪屏
- `82192f5` 地图页 C 双态与拖拽列表
- `5998606` 详情页与阴历日历
- `e6d3aa9` 地区搜索与三个触发式弹层
- `e4a74c9` 收藏提醒管理与我的设置
- `b997843` 无网络状态与全局反馈
- `5259ce7` 删除旧引导、金刚区与四级地区选择遗留
- `2133097` 统一示例名称并复核详情历法
- `d1ab449` C 端阶段一全局回归
- `e5e3c83` 统一前台集期历法展示口径

### 后台

- `f6bac12` 评价审核、提醒任务与集期校验
- `d97e901` open_time v2 编辑审核与提醒队列
- `1af3e49`、`0e10f7b` 旅行热门地区接口与标签约束
- `5835102` 两级角色权限与操作人留痕
- `6ec50ed` B1/B2/B3 三种核心样板落地
- `43ca7c1` 运营、设置、采集与日志页面闭环
- `6c52ce8` 补齐 OCR 名称复核真实队列
- `a0dc1ce` 审核徽标统一使用真实待办数量

### 部署与安全

- `a035ccb` 幂等集市同步及本地演练
- `0f4e3d1` 生产备份、快进部署与回滚保护
- `8dea1d3` JWT 管理认证并关闭公开旧密钥
- `d3afd35` 幂等补齐数据复核队列
- `3c6477d` 修正服务器虚拟环境依赖安装
- `93f1391` 自托管 Material Symbols 图标子集

## 页面 QA 档案

- C 端：首页四态 `output/frontend-home/`；地图双态 `output/frontend-map/`；生产高德地图两态 `output/production-qa/map-default-live.png`、`map-raised-live.png`；详情两态 `output/frontend-detail/`；地区与弹层 `output/frontend-trigger-sheets/`；收藏/我的 `output/frontend-favorites-my/`；无网络 `output/frontend-feedback/`。
- 后台：B1/B2/B3 对比图在 `output/admin-b1/`、`output/admin-b2/`、`output/admin-b3/`；其余七页在 `output/admin-pages/`。
- OCR 名称复核运行态：`output/admin-pages/name-review-1280.png`。
- 逐页结论与修正历史：`design-qa.md`，当前无遗留 P0/P1/P2。
- 自动化：全量 pytest `57 passed`；`app.py` 编译通过。

## 生产数据同步摘要

生产执行档案：`/www/wwwroot/zhaogedaji/data/backups/deploy_20260714_022327/`

- 来源集市 3030，目标同步前 3030；新增 0，按 ID 更新 2994，保持不变 36。
- 保护列：`rating`、`review_count`、`fav_count`、`created_by`、`created_at`；用户、收藏、评价、提醒等业务表不覆盖。
- 写入待复核队列 676：坐标 521、集期 71、OCR 名称 84；已有处理结果永不更新。
- 同步后再次 dry-run：集市变更 0、队列新增 0；幂等性通过。
- 实际 diff：`market-sync-applied/market_sync_diff_20260714_022345_022248.csv`。
- 同步前库：`market-sync-backups/zhaojishi.before_market_sync.20260714_022344_921120.db`。

## 上线冒烟结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| 公网/本机健康检查 | 通过 | HTTP 200，systemd active |
| 附近推荐 | 通过 | 王京坐标返回 3 条，均含整数米 `distance` 与派生状态 |
| 阴历月历 | 通过 | 临时李官大集 2026-07 返回 5、10、14、19、24、29；小字廿一、廿六、初一、初六、十一、十六 |
| needs_review 红线 | 通过 | 后台校验拒绝写入；运行时不参与派生 |
| 注册/密码登录 | 通过 | 随机合成账号通过生产 API，测试后删除 |
| 收藏/提醒 | 通过 | 收藏 ID 可回读；提醒返回前晚 20:00 + 当天 06:30；取消后清理 |
| 后台登录/待办/队列 | 通过 | 真实总数 676；坐标 521、名称 84、集期 71；真实 OCR 内容可处理 |
| 审核员权限 | 通过 | 审核员访问系统设置 HTTP 403 |
| open_time 编辑保存 | 通过 | 完整 days 数组保存并回读，月历由服务端换算 |
| PWA 静态资源 | 通过 | manifest JSON、10 个 icon、favicon、logo、本地图标字体均 HTTP 200 |
| 生产数据恢复 | 通过 | 写入型冒烟前备份；清理后 users=2、favorites=0、reviews=0、reminders=0、markets=3030，与测试前完全一致 |
| 后台 HTML 更新 | 通过 | Nginx 返回 `Cache-Control: no-cache, no-store, must-revalidate` |
| 真实高德地图 | 通过 | Web 端 JS API 配置已写入生产数据库且不进 Git；`/api/app-config/map` 返回 `configured=true`；线上默认/拖起两态均出现 AutoNavi 底图与版权标识，控制台记录 `[AMap] SDK ready` 且无 error |
| 手机实机定位/唤起高德 | 待人工实机 | 桌面浏览器不能证明系统定位授权与地图 App 唤起 |

写入型冒烟备份：`/www/wwwroot/zhaogedaji/data/backups/smoke_20260714_024849/zhaojishi.db`。首次脚本因合成 UID 冲突未提交任何写入，随后核验主库与备份均 `integrity_check=ok`；成功轮结束后无合成用户、集市或验证码残留。

## 回滚信息

- 代码回滚基线：`production-pre-rollout-20260714`（`3c6477d`）。
- 全量数据同步前备份：`/www/wwwroot/zhaogedaji/data/backups/deploy_20260714_022327/zhaojishi.db`。
- 当前最终代码部署前备份：`/www/wwwroot/zhaogedaji/data/backups/deploy_20260714_025904/zhaojishi.db`。
- 后台 Nginx 配置备份：`/www/server/panel/vhost/nginx/admin.xingjiawu.cn.conf.bak_20260714_030156`。
- 高德 Web 端配置写入前数据库备份：`/www/wwwroot/zhaogedaji/data/backups/amap_js_config_20260714_100038/zhaojishi.db`，备份与写入后主库均 `integrity_check=ok`。
- 回滚时先停止 `zhaojishi`，恢复目标 SQLite 备份并校验 `PRAGMA integrity_check`，再切换到对应 tag、重启服务并执行内外网健康检查；不使用 `git reset --hard` 覆盖未知服务器改动。

## 待决

详见 `output/questions.md`。高德 Web 端配置与线上地图加载已经通过；只剩真实手机上的定位授权、附近列表、详情与高德 App 唤起验收，完成前不把总目标标记为全部完成。
