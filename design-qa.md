# 找个大集 · 前端设计 QA

## 首页四态

- 设计源：Figma `3u4j48Ti8r2Er7gipvfAJT`，节点 `4:8`、`4:11`、`4:14`、`4:17`
- 视口：375 × 812
- 预览入口：`?preview=home-morning`、`home-afternoon`、`home-location-failed`、`home-empty`

| 状态 | 设计截图 | 实现截图 | 并排对比 |
| --- | --- | --- | --- |
| 上午默认 | `output/frontend-home/reference-home-morning.png` | `output/frontend-home/implementation-home-morning.jpg` | `output/frontend-home/comparison-home-morning.png` |
| 下午默认 | `output/frontend-home/reference-home-afternoon.png` | `output/frontend-home/implementation-home-afternoon.jpg` | `output/frontend-home/comparison-home-afternoon.png` |
| 定位失败 | `output/frontend-home/reference-home-location-failed.png` | `output/frontend-home/implementation-home-location-failed.jpg` | `output/frontend-home/comparison-home-location-failed.png` |
| 附近无集市 | `output/frontend-home/reference-home-empty.png` | `output/frontend-home/implementation-home-empty.jpg` | `output/frontend-home/comparison-home-empty.png` |

## Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：Material Symbols 与 Figma 草稿中的文本占位图标字形略有差异；实现统一使用正式图标库，视觉含义与对齐均成立，作为预期差异保留。

## 五项保真检查

- 字体与排版：通过。正文 Noto Sans SC、标题 Noto Serif SC；正文与次要信息字号符合 tokens，四态均无异常换行或截断。截图前确认 `document.fonts.status=loaded`。
- 间距与布局：通过。顶部状态区 24px、应用头部 92px、底部导航 72px；列表卡片 343 × 116px，首卡 Y=288；定位失败卡片 343 × 258px、Y=140；空状态卡片 343 × 300px、Y=210。四态均无横向溢出。
- 色彩与 tokens：通过。上午使用暖陶红信息横幅，下午使用夜市功能色蓝紫；今晚开市、明天开集、今日已结束分别使用独立文字标签和底色。状态不只靠色差辨认。
- 图像与图标：通过。品牌闪屏使用现有正式图标；页面操作使用 Material Symbols，无 emoji、CSS 图形或手绘 SVG 替代可见资产。
- 文案与数据：通过。下午态为“夜市 + 明日大集”，已结束集市沉底；12.4 km 保留一位小数；定位失败与无集市文案和操作均与 Figma 稿一致。

## 状态与交互检查

- 上午态：三张卡片、分类 chips、地图入口与三 Tab 正常；收藏热区 64 × 44px。
- 下午态：今晚开市优先、明天大集其次、今日已结束沉底；夜市标签使用 `#4E5FA8` 对应功能色。
- 定位失败态：重新定位、手动选地区、地区搜索与最近访问入口可操作。
- 附近无集市态：打开地图、切换地区、提交线索入口可操作；不设置距离提示明确。
- 四个预览入口均加载到对应 `data-home-state`，控制台无 error / warn。

## 启动闪屏检查

- 闪屏内容仅包含品牌图标、品牌名、短标语与加载点，不含登录、引导步骤或操作门槛。
- `SPLASH_MIN=550ms`，异常保底 900ms 开始退场，退场 250ms；游客无需操作即可直达首页。
- 四次浏览器实测闪屏 DOM 完全移除时间为 757–793ms，均不超过 1 秒；归档截图均在闪屏移除、字体加载完成后拍摄。

## Comparison history

1. 用户验收发现上午态归档图截在闪屏半透明退场阶段。修正闪屏时长与截图等待条件，重截后无品牌层叠加。
2. 首轮下午态对照发现“今晚开市”被通用 `today` 状态抢占为陶红色，且 12.4 km 被四舍五入为 12 km。调整状态优先级与距离格式化后，复核为蓝紫标签与 12.4 km。
3. 修正后重新以同一 375 × 812 视口采集四态，并分别与对应 Figma 节点并排复核；未发现新的 P0 / P1 / P2 差异。

## Focused evidence

- 重要文字、标签、按钮和卡片在完整 375px 画面中均可清晰读取，因此无需额外裁切图。
- DOM 定点测量覆盖卡片尺寸、Y 坐标、底部导航、横向溢出、状态文案、距离文案、字体加载与闪屏移除。

## 真机下拉刷新回归（2026-07-15）

- 设备：小米 M2007J1SC，Android 13，小米浏览器；使用真实系统定位与生产域名。
- 修正前下拉刷新会分页下载全部 3030 条集市并串行等待两套定位，且异常时缺少强制收尾。
- 修正后先重新定位，再重置无限滚动并只拉取第一页 20 条；附近请求最多阻塞 3 秒，指示条由 `finally` 保证收起。
- 刷新保留当前分类和“只看今天”开关；真机手势回归确认刷新会重置 offset、回到列表顶部并重新请求第一页。
- 截图：`output/production-qa/real-device-20260715/pull-refresh-fixed.png`。

## 首页与地图无限滚动回归（2026-07-15）

- 接口契约：`/api/markets/nearby` 支持 `limit=20&offset=N`，响应包含匹配总数 `total` 和 `has_more`；offset 越界返回 200 与空数组。
- 真实数据口径：以唐县中心点 `38.7483, 114.9829` 验证，100km 内共 1003 个已发布且有坐标的集市。
- 三页结果：offset 0 / 20 / 40 各返回 20 条，距离范围依次为 21–6198m、6240–9576m、9864–12877m；累计 60 条、ID 去重后仍为 60 条，距离全局单调递增。
- 首页与地图拖起态分别完成三页滚动，均加载 60 条且无重复；分类、地图“只看今天”变化都会重置 offset、清空旧数据并滚回顶部。
- “只看今天”由接口先过滤再分页，稀疏结果不会为凑满首屏连续扫描数百条非今日集市；下拉刷新后分类和开关保持不变。
- 状态组件：请求中显示两张骨架卡；断开本地服务后显示“加载失败，点击重试”，恢复服务并重试后由 20 条继续到 40 条且无重复；夜市筛选仅 8 条时显示“已看完附近 8 个集市”。
- 排序规则：首页请求固定使用 `sort=distance`；“今日有集”横幅只作筛选引导，不向第一页插入远距离集市。
- 375px 归档：`output/frontend-infinite-scroll/home-after-3-pages-375.png`；原始真机截图：`output/frontend-infinite-scroll/home-after-3-pages-device.png`。

## 定位状态统一回归（2026-07-15）

- 单一来源：`zjsLocationState = {gpsRegion, manualRegion, effectiveRegion}`；`effectiveRegion` 固定为手动地区优先、GPS 地区其次，旧 `zjsSelectedRegion` 仅迁移一次后删除。
- 冷启动拒绝：在全新本地域名、无历史状态下拒绝定位，首页进入定位失败态；首页显示“未定位”、地图显示“选择地区”、地区搜索显示“未定位”、“我的”显示“未定位 · 点击选择”，最近访问区块隐藏。
- 授权定位：用高德逆地理结构 `city=保定市, district=唐县` 回归，四处同步显示“保定市 · 唐县”，并提示“已定位到 保定市 · 唐县”；GPS 地区自动写入最近访问。
- 手动切换：从地区搜索页选择“石家庄市 · 正定县”后，四处同步且最近访问按“石家庄市 · 正定县、保定市 · 唐县”排列；首页停止 GPS 分页，改按手动地区展示。
- 手动锁定：随后 GPS 更新为“邢台市 · 信都区”，`gpsRegion` 正常更新并进入历史，但 `effectiveRegion` 与四处显示仍保持“石家庄市 · 正定县”；刷新页面后手动地区仍保留。
- 历史规则：重复地区移动到首位，始终去重，加入第 6 个地区后仍只保留最新 5 条；首页定位失败态和地区搜索页使用同一列表。
- 375px 真机归档：`output/frontend-location-state/my-region-375.png`、`output/frontend-location-state/region-search-375.png`、`output/frontend-location-state/my-location-denied-375.png`。

## final result

passed

---

## 地图页 C 两态

- 设计源：Figma `3u4j48Ti8r2Er7gipvfAJT`，节点 `12:8`、`12:11`
- 视口：375 × 812
- 预览入口：`?preview=map-default`、`?preview=map-raised`

| 状态 | 设计截图 | 实现截图 | 并排对比 |
| --- | --- | --- | --- |
| 地图默认 | `output/frontend-map/reference-map-default.png` | `output/frontend-map/implementation-map-default.png` | `output/frontend-map/comparison-map-default.png` |
| 列表拖起 | `output/frontend-map/reference-map-raised.png` | `output/frontend-map/implementation-map-raised.png` | `output/frontend-map/comparison-map-raised.png` |

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：实现使用真实高德底图，Figma 使用抽象街区占位图；真实道路、POI 标签和版权标识属于预期生产差异。
- P3：系统状态栏电量示例由 Figma 的 88% 调整为预览统一值 92%，不影响应用布局。

### 五项保真检查

- 字体与排版：通过。标题使用 Noto Serif SC，控件与卡片信息使用 Noto Sans SC；次要信息保持 13px 下限。
- 间距与布局：通过。搜索栏全局 Y=36、高 48px；默认抽屉全局 Y=500，拖起态全局 Y=170；定位按钮随抽屉同步上移。
- 色彩与 tokens：通过。大集/早市/庙会/夜市标记分别使用暖陶红、深松绿、稻谷金衍生色、夜市蓝紫；标签均带文字。
- 图像与图标：通过。底图使用高德 JS API；操作图标统一 Material Symbols，无 emoji、手绘 SVG 或 CSS 图形代替地图资产。
- 文案与数据：通过。分类 chips 两态完全同构；“只看今天”保持独立维度；650 m、2.8 km、6.3 km、9.1 km 格式正确。

### 状态与交互检查

- 默认态：附近集市标题、五分类、独立开关、卡片列表与地图标记正常。
- 拖起态：抽屉 top=146px（加 24px 状态栏后全局 Y=170），标题为“18 个附近集市”，四张卡片完整可见。
- 收藏按钮实测 44 × 44px；地图搜索、返回、定位、地区选择热区均不小于 44px。
- “只看今天”开启后由 4 张筛为 3 张；再选“夜市”后仅显示“半程夜市”，地图标记同步刷新。
- 抽屉支持点击手柄与指针拖拽，超过 40px 后在默认/拖起两个锚点间吸附。

### Comparison history

1. 首轮对照发现收藏按钮缺少 Figma 中的暖色圆形底，评分与集期挤在同一行；修正为 44px 暖色收藏按钮，评分独立右对齐。
2. raised 预览标题曾按示例卡片数显示“4 个附近集市”；修正为与设计稿一致的“18 个附近集市”，生产态仍使用真实筛选数量。
3. 修正后重新在 375 × 812 视口采集两态并排图；未发现新的 P0 / P1 / P2 差异。
4. 地图页验收后将预览示例名称由“沂州庙会”统一为设计稿使用的“沂州古城庙会”；375px 拖起态无文字或页面横向溢出。

### 生产高德地图复验（2026-07-14）

- 生产域名：`https://app.xingjiawu.cn`；Web 端 JS API 配置由生产数据库提供，源码和 QA 档案均不包含凭据。
- 默认态与列表拖起态均加载真实高德底图、POI 与 AutoNavi 版权标识；控制台记录 `[AMap] SDK ready`，两个状态均无 error。
- 运行态继续使用“沂州古城庙会”完整名称，拖起态包含 650 m 距离案例；截图归档为 `output/production-qa/map-default-live.png` 与 `map-raised-live.png`。
- 桌面线上复验不能替代手机系统定位授权和高德 App 唤起，后者仍作为唯一硬件待验项。

### final result

passed

---

## 集市搜索页

- 设计源：Figma `3u4j48Ti8r2Er7gipvfAJT`，节点 `30:5` 的地区搜索页作为已验收视觉母版；集市搜索保留既有业务语义，不把地区聚合内容照搬为搜索结果。
- 视口：375 × 812
- 预览入口：`?preview=search`、`?preview=search-results`
- 视觉参考：`output/frontend-trigger-sheets/reference-region-search.png`
- 实现截图：`output/frontend-search/implementation-search.png`、`output/frontend-search/implementation-search-results.png`
- 并排对比：`output/frontend-search/comparison-search.png`

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：视觉母版是地区搜索，集市搜索按业务增加一级分类 chips、历史搜索和结果卡片；页面层级、tokens、控件尺寸与卡片语言保持一致。

### 五项保真检查

- 字体与排版：通过。标题使用本地中文 serif 回退，正文使用设备中文 sans 回退；正文 16px，卡片次要信息 13px。
- 间距与布局：通过。复用 16px 页面边距、64px 白色标题栏、52px 搜索框、14/16/18px 圆角；全屏搜索时不显示底部导航。
- 色彩与 tokens：通过。页面使用 `#F8F5EF` 暖灰底、白色卡片、暖陶红与深松绿；夜市和状态标签继续复用 C 端同义色。
- 图像与图标：通过。搜索、返回、清空、热门项与空结果均使用已自托管 Material Symbols 子集，无 emoji 或外部字体请求。
- 文案与数据：通过。搜索范围覆盖名称、县区乡镇、分类与标签；结果卡沿用县/乡镇名称、米/公里距离、状态、集期和评价降级文案。

### 状态与交互检查

- 初始态包含分类、最近搜索、热门搜索和搜索范围说明；历史可单条删除或全部清空。
- 输入关键词实时筛选，回车或点击热门词写入历史；分类维度固定为全部 / 大集 / 庙会 / 早市 / 夜市。
- 空结果使用图标 + 文字，不只依赖颜色；结果最多首屏渲染 50 条，避免 3030 条数据同时生成 DOM。
- 搜索结果可进入详情，详情返回后恢复原结果；从首页或地图进入搜索时，返回原页面并恢复地图状态。
- 本地回归覆盖热门词、分类筛选、清空、结果进详情与详情返回；全量 pytest `57 passed`。

### Comparison history

1. 旧实现使用橙色顶栏、emoji 分类、旧卡片和常驻底部导航，与新版 UI 契约不一致。
2. 重建为地区搜索同源的全屏层级后，以 375 × 812 将设计母版与实现并排复核；修正了新增图标超出本地字体子集导致的字面量渲染。
3. 最终对照未发现新的 P0 / P1 / P2 差异，搜索结果态另行截图完成结构与数据层级检查。

### final result

passed

---

## 后台三种核心页面样板（B1 / B2 / B3）

- 设计源：Figma `8qnzoTS24jw7gfJp5QxYP1`，节点 `15:3`、`15:4`、`15:5`
- 设计画板：1440 × 1024
- 预览入口：`?preview=admin-markets`、`admin-review`、`admin-drawer`

| 样板 | 设计截图 | 实现截图 | 并排对比 |
| --- | --- | --- | --- |
| B1 集市管理表格 | `output/admin-b1/reference-markets.png` | `output/admin-b1/implementation-markets-1440.png` | `output/admin-b1/comparison-markets.png` |
| B2 评价审核队列 | `output/admin-b2/reference-review.png` | `output/admin-b2/implementation-review-1440.png` | `output/admin-b2/comparison-review.png` |
| B3 open_time v2 抽屉 | `output/admin-b3/reference-open-time-drawer.png` | `output/admin-b3/implementation-open-time-drawer-1440.png` | `output/admin-b3/comparison-open-time-drawer.png` |

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：实现使用正式品牌图标与 Material Symbols，Figma 草稿使用简化标志；导航槽位、选中态、文字层级与对比度一致。
- P3：实现为保证五种主规则可直接编辑，阴历/阳历日序展示 1–30 全量选择器；默认已选值仍严格为展开入库的 `[1,6,11,16,21,26]`。

### 契约与交互检查

- 全局框架为固定 240px 深色侧栏、浅灰工作区、72px 顶栏；侧栏待办徽标表达待处理总数，三位数不折行，超过 999 显示 `999+`。
- B1 使用固定筛选条、批量操作、表格、分页；坐标统一显示村庄级 / POI / 乡镇级 / 待复核；更新人支持 `AI 修复 → 王宁` 留痕。
- B2 左列表右详情，评价总数与可见样本分离；预览显示 12 项待审、坐标 521、集期 71；图片/视频审核、AI 初审意见、通过/驳回原因、A/R/J/K 快捷键均可操作。
- B3 为 480px 右侧抽屉；五种主规则互斥，指定日期支持多条，阴历 days 完整展开入库，ISO 周日为 7，例外优先级为 `closed > override > 主规则`。
- `specific_dates` 加载时不会残留自动创建的空白行；保存前经过 JSON Schema 2020-12 校验；派生字段不入库，`needs_review` 不参与计算。
- 登录与会话恢复均进入新版框架；角色显示为超级管理员 / 审核员，审核员不可进入 APP 设置与数据采集等系统页。
- 三个预览入口控制台无阻断错误；内联脚本全部通过 `new Function` 语法检查；全量 pytest `51 passed`。

### final result

passed

---

## C 端阶段一完成性复核

- 复核日期：2026-07-14
- 设计契约：C 端 Figma 8 分区及 04「说明与映射」页
- 视口：列表、弹层与空状态 375 × 812；详情页 375 × 1220

### 交付矩阵

| 契约页面 | 运行态与证据 | 结论 |
| --- | --- | --- |
| 首页 | 上午、下午、定位失败、附近无集市；`output/frontend-home/` | 通过 |
| 地图页 C | 默认、列表拖起；`output/frontend-map/` | 通过 |
| 详情页 | 完整、无评价；`output/frontend-detail/` | 通过 |
| 地区与触发弹层 | 地区搜索、登录、提醒、导航；`output/frontend-trigger-sheets/` | 通过 |
| 收藏与我的 | 提醒管理、每日营业降级、摊主入口、大字模式、默认地图、评价状态；`output/frontend-favorites-my/` | 通过 |
| 空状态与反馈 | 定位失败、附近无集市、无网络、三语义 Toast；`output/frontend-home/`、`output/frontend-feedback/` | 通过 |

### 契约红线回归

- 李官大集 2026 年 7 月日历继续由统一历法服务派生：5、10、14、19、24、29 日，带阴历廿一、廿六、初一、初六、十一、十六。
- `needs_review` 不参与今天有集、下一场或月历派生；阴历日序不会直接当作阳历日期渲染。
- 游客首页可直接浏览；收藏、提醒、评价与摊主认证按动作触发一步登录。
- 收藏提醒为前一天 20:00 与当天 06:30；每日营业类不创建开集提醒。
- 首页、收藏、我的保持三 Tab；地图为首页浮动入口与独立页面，不新增第四 Tab。
- 示例名称统一为“沂州古城庙会”，同一预览内数据均按基准日 2026-07-13 推算。
- 高德 Web 服务 Key 与浏览器 JS API 配置已分流；源码无 Key，未配置时地图可降级且不阻塞其他功能；当前生产 Web 端配置已启用并通过真实底图复验。

### 最终自动化

- `pytest`：48 passed。
- `app.py`、`open_time.py`、迁移脚本、坐标脚本：编译通过。
- `index.html` 4 段、`admin.html` 3 段内联脚本：语法通过。
- 已归档页面均无 375px 横向溢出；预览态无控制台 error。

### final result

passed

---

## 后台 P0、动态热门地区与最终回归

- 后台评价审核：图片与视频均在待审核卡片内可预览；拒绝必须填写原因，审核结果与操作人、时间一并落库；只有 `approved` 评价参与公开列表和评分重算。
- 提醒任务：前一天 20:00 与当天 06:30 分两个幂等任务生成；每日营业、`needs_review`、非开集日不会建单；一次性提醒在最后一个启用时段发送后结束。
- open_time v2 编辑器：每日 / 阴历 / 阳历 / 每周 / 多个指定日期五种主规则，加停市、临时加场、调整时段例外层；周日使用 ISO 7，阴历支持 exclude / include / only，后台保存只接受 version=2。
- needs_review 队列：实际数据库中 `open_time_v2` 71 条、`market_name_ocr` 84 条、`geocode` 521 条待处理；编辑为有效 v2 后对应集期复核项自动转为 resolved。
- 旅行热门地区：`/api/regions/popular` 按已发布集市聚合区县数量，仅返回语义明确的特色标签；375 × 812 地区页已实测从真实接口替换设计稿示例，接口失败时保留示例降级。
- 浏览器验收：桌面后台的提醒订阅、待发送任务、调度说明，以及集市编辑器的指定日期/例外行均可见、可操作；移动端地区页无横向溢出。
- 数据安全：高德 Web 服务 key 仍只存在被 `.gitignore` 排除的数据库中，tracked 文件检索无命中。
- 最终自动化：`45 passed`；`app.py`、`open_time.py`、坐标与迁移脚本均通过编译检查；`index.html`、`admin.html` 内联脚本通过语法检查。

### final result

passed

---

## 收藏页与我的页

- 设计源：Figma `3u4j48Ti8r2Er7gipvfAJT`，节点 `42:14`、`42:16`
- 视口：375 × 812
- 预览入口：`?preview=favorites`、`?preview=my`

| 页面 | 设计截图 | 实现截图 | 并排对比 |
| --- | --- | --- | --- |
| 收藏与提醒管理 | `output/frontend-favorites-my/reference-favorites.png` | `output/frontend-favorites-my/implementation-favorites.png` | `output/frontend-favorites-my/comparison-favorites.png` |
| 我的与偏好设置 | `output/frontend-favorites-my/reference-my.png` | `output/frontend-favorites-my/implementation-my.png` | `output/frontend-favorites-my/comparison-my.png` |

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：实现使用正式 Material Symbols 图标，摊主、定位、评价图标与 Figma 草稿中的单字占位符字形不同；槽位、色彩和含义一致，作为预期差异保留。
- P3：实现侧的底部当前态图标较草稿略小约 1–2px，不影响选中态辨识或 44px 热区。

### 五项保真检查

- 字体与排版：通过。页面标题、集市名称和分区标题使用 Noto Serif SC，正文与操作使用 Noto Sans SC；默认正文 16px，大字模式将核心正文提升至 18px，次要信息不低于 13px。
- 间距与布局：通过。收藏页头部、82px 概览、66px chips 区、三张收藏卡与 68px 底栏按 375 × 812 frame 对齐；我的页各卡片、分区间距与底栏无重叠、无横向溢出。
- 色彩与 tokens：通过。提醒已开启使用深松绿，收藏与主操作使用暖陶红，每日营业使用早市绿；开关均同时带文字，不只依赖色差。
- 图像与图标：通过。本分区没有需要生成或裁切的栅格图；功能图标统一使用 Material Symbols，无 emoji、手绘 SVG 或 CSS 图形替代可见资产。
- 文案与数据：通过。示例统一以 2026-07-13 为“今天”：7月14日庙会显示“明天开集”；650 m 使用米；每日营业卡明确“特殊天气及通知除外”且不出现提醒模块。

### 状态与交互检查

- 收藏页可按“全部 / 已开提醒 / 即将开集”筛选；概览中的“提醒管理”直接切到已开提醒。
- 实测沂州古城庙会点击“开启提醒”后显示“提醒已开启”；页面共 2 个可提醒模块，枣沟头早市的提醒模块数量为 0。
- 已开启提醒显示前晚 20:00 与当天 06:30；关闭后同步更新本地状态，并调用现有提醒接口更新长期提醒。
- 大字模式实测在关闭/开启间切换，`aria-label` 与页面字号同步；测试结束后恢复标准字号。
- 默认地图实测打开平台选择，选择百度后“我的”页更新为“百度地图 · 点击修改”且不跳转导航；随后恢复高德默认。iOS 仍由同一弹层动态追加苹果地图。
- 摊主认证仅保留入口；未登录时触发一步登录，已登录时说明流程后置。评价入口保留已发布与审核中状态。
- 收藏、开关、地图设置、摊主入口与底部导航热区均不小于 44 × 44px；两个预览入口控制台无 error / warn；全量 pytest `37 passed`。

### Comparison history

1. 首轮收藏卡的名称与集期仍按行内元素排版，文字与收藏按钮发生重叠；修正为分块信息层级，并为普通、未提醒、每日营业三种高度分别收口。
2. 首轮我的页个人统计被继承样式拆成竖列，个人卡宽度也未撑满；修正为 343px 全宽卡和横向统计行。
3. 第二轮对照补齐本分区 68px 底栏和 78 × 56px 当前态暖色底，调整摊主卡与评价卡高度后，与 Figma 的页面节奏一致。
4. 最终重新采集两个 375 × 812 实现截图并与对应 Figma frame 并排复核，未发现新的 P0 / P1 / P2 差异。

### Focused evidence

- 收藏卡的状态、名称、集期、距离、提醒开关和 650 m 案例在完整并排图中均清晰可读，无需额外裁切。
- 我的页重点复核了大字模式开关、默认地图文案、摊主入口、审核中状态与底部当前态；DOM 交互检查补充验证了动态状态。

### final result

passed

---

## 地区搜索与三个触发式弹层

- 设计源：Figma `3u4j48Ti8r2Er7gipvfAJT`，节点 `30:5`、`30:6`、`30:7`、`30:8`
- 视口：375 × 812
- 预览入口：`?preview=region-search`、`trigger-login`、`trigger-reminder`、`trigger-navigation`

| 状态 | 设计截图 | 实现截图 | 并排对比 |
| --- | --- | --- | --- |
| 地区搜索页 | `output/frontend-trigger-sheets/reference-region-search.png` | `output/frontend-trigger-sheets/implementation-region-search.png` | `output/frontend-trigger-sheets/comparison-region-search.png` |
| 触发式登录 | `output/frontend-trigger-sheets/reference-login-sheet.png` | `output/frontend-trigger-sheets/implementation-login-sheet.png` | `output/frontend-trigger-sheets/comparison-login-sheet.png` |
| 收藏后提醒 | `output/frontend-trigger-sheets/reference-reminder-sheet.png` | `output/frontend-trigger-sheets/implementation-reminder-sheet.png` | `output/frontend-trigger-sheets/comparison-reminder-sheet.png` |
| 首次导航选择 | `output/frontend-trigger-sheets/reference-navigation-sheet.png` | `output/frontend-trigger-sheets/implementation-navigation-sheet.png` | `output/frontend-trigger-sheets/comparison-navigation-sheet.png` |

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：微信按钮使用 Material Symbols 的正式消息图标，Figma 草稿使用圆形占位符；尺寸与基线一致。
- P3：地图应用图标为文字缩写容器，待接入各平台官方品牌资产时替换，不影响选项识别与交互。

### 五项保真检查

- 字体与排版：通过。页标题和弹层标题使用 Noto Serif SC，正文与控件使用 Noto Sans SC；正文 16px，说明 12–14px。
- 间距与布局：通过。地区搜索为 375 × 812 独立页；登录、提醒、导航弹层分别从 Y=346、286、224 开始，高度 466、526、588px。
- 色彩与 tokens：通过。登录/导航主操作使用深松绿，提醒确认使用暖陶红，地区热度点使用稻谷金与深松绿；状态均带文字。
- 图像与图标：通过。功能图标统一 Material Symbols；地图平台以可替换的 44px 品牌槽承载，不使用 emoji。
- 文案与数据：通过。游客可继续浏览；收藏、提醒、评价时触发一步登录；收藏后只询问一次提醒；提醒时间为前晚 20:00 与当天 06:30。

### 状态与交互检查

- 地区搜索支持城市、区县、乡镇关键词；实测输入“四川”后只保留“四川 · 彭州市”。选择后立即更新区县并重算推荐，不再打开四级联动。
- 登录弹层提供微信主入口与手机号验证码入口；新用户登录后直接回内容，不再进入多步 onboarding；待执行的收藏/提醒/评价动作可恢复。
- 每日营业类集市不会创建提醒；非每日营业类收藏后仅首次询问，两个提醒开关均可操作，至少保留一个时间。
- 导航首次弹出高德/百度/腾讯选择，高德置顶；iOS 按 user agent 动态追加苹果地图，四选项时列表可滚动。
- 实测选择百度地图后主按钮同步为“用百度地图导航”；记住偏好后后续直接打开，设置页可修改的入口在下一分区落地。
- 关闭按钮、返回、重定位、登录、提醒与导航主按钮热区均不小于 44 × 44px；四个预览入口控制台无 error。

### Comparison history

1. 首轮登录和导航上下文仍露出首页顶部搜索栏，修正预览与运行时页面层级后，弹层上下文从状态栏下方开始，与 Figma 一致。
2. 首轮登录内容整体偏上约 24px，调整微信按钮与分隔区间距；导航标题偏上约 10px，按独立弹层修正。
3. 地区热门卡片首轮名称与标签挤在同一行，修正为名称/特色标签两行，并保持集市数量右对齐。
4. 修正后重新以同一 375 × 812 视口采集四态并排图；未发现新的 P0 / P1 / P2 差异。

### final result

passed

---

## 集市详情页两态

- 设计源：Figma `3u4j48Ti8r2Er7gipvfAJT`，节点 `21:11`、`21:14`
- 画板：375 × 1220
- 预览入口：`?preview=detail-full`、`?preview=detail-empty`

| 状态 | 设计截图 | 实现截图 | 并排对比 |
| --- | --- | --- | --- |
| 完整态 | `output/frontend-detail/reference-detail-full.png` | `output/frontend-detail/implementation-detail-full.png` | `output/frontend-detail/comparison-detail-full.png` |
| 无评价态 | `output/frontend-detail/reference-detail-empty.png` | `output/frontend-detail/implementation-detail-empty.png` | `output/frontend-detail/comparison-detail-empty.png` |

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：操作区使用正式 Material Symbols，字形与 Figma 草稿的文本图标占位略有差异，含义、热区与对齐均一致。
- P3：无评价态图标使用正式 `hotel_class` 图标替代草稿中的抽象星形，占位尺寸与色彩一致。

### 五项保真检查

- 字体与排版：通过。标题使用 Noto Serif SC，正文使用 Noto Sans SC；集期规律为 14px，次要信息不低于 13px。
- 间距与布局：通过。头图 180px、核心信息卡 166px、动作栏 64px、日历 268px、地图卡 112px，与 375px Figma frame 同构。
- 色彩与 tokens：通过。明天开集使用深松绿浅底；阴历集期使用暖陶红高亮，庙会指定日期使用稻谷金衍生色；所有状态均带文字。
- 图像与图标：通过。详情头图复用已验收 Figma 示例头图的栅格资产；操作图标统一使用 Material Symbols，无 emoji、手绘 SVG 或 CSS 图形资产。
- 文案与数据：通过。李官大集统一命名；2026-07-13 不标今天有集，下一场为 7 月 14 日；庙会指定日期 7 月 14 日与 10 月 2 日自洽；示例评价均明确标注。

### 历法与运行时检查

- 新增 `GET /api/markets/:id/calendar?year=&month=`，服务端调用统一 `compute_month_calendar`，前端不自行把阴历日序当阳历日。
- 李官大集 2026 年 7 月开集日回归为 5、10、14、19、24、29 日，对应阴历廿一、廿六、初一、初六、十一、十六。
- `migration_status=needs_review` 返回 422，不生成月历；前端展示“集期待人工复核”。
- 无评价完整文案为“暂无评价，来分享你的赶集体验”；卡片短文案仍保持“暂无评价”。
- 评价区仅承诺展示审核通过内容，预览评价显式标记为示例数据。

### 状态与交互检查

- 返回、分享、收藏提醒、导航、写评价均为可操作控件；所有主要按钮热区不小于 44 × 44px。
- 完整态展示综合评分、关键词和图文/视频示例评价；无评价态展示审核说明和“写第一条评价”主按钮。
- 两个预览入口控制台均无 error；全量 pytest `37 passed`。

### Comparison history

1. 首轮实现沿用了旧详情页的信息块和评分分布，重排为核心信息、动作栏、月历、地图、评价五段结构。
2. 首轮对照发现指定日期庙会仍使用暖陶红高亮，修正为稻谷金衍生色；无评价态补齐“评价需审核”和当地特色第二槽。
3. 修正后以同一 375 × 1220 画板重采两态，并与 Figma 原节点并排复核；未发现新的 P0 / P1 / P2 差异。

### final result

passed

---

## 无网络与操作反馈

- 设计依据：已验收 Figma 空状态组件（节点 `9:87`）与 Figma 03 tokens；设计文件未提供独立“无网络”整屏 frame，因此并排图仅用于核对同一空状态组件语言，不把两种业务状态作逐字逐像素比较。
- 视口：375 × 812
- 预览入口：`?preview=no-network`
- 组件参考：`output/frontend-home/reference-home-empty.png`
- 实现截图：`output/frontend-feedback/implementation-no-network.png`
- 并排模式对比：`output/frontend-feedback/comparison-no-network-pattern.png`

### Findings

- 无遗留 P0 / P1 / P2 问题。
- P3：无网络态只保留一个主卡片，较“附近无集市”状态更聚焦连接恢复；这是业务状态差异，不是布局缺失。

### 五项保真检查

- 字体与排版：通过。标题使用 Noto Serif SC，正文与按钮使用 Noto Sans SC；正文 14px/22px，按钮 16px。
- 间距与布局：通过。复用 343px 内容宽度、20px 卡片圆角与 16px 页面边距；主按钮 50px，次按钮 46px，均满足 44px 热区。
- 色彩与 tokens：通过。主操作使用暖陶红，离线图标使用稻谷金衍生色，次操作使用深松绿；错误或离线状态始终带图标与文字。
- 图像与图标：通过。`cloud_off`、成功、错误和信息提示均使用 Material Symbols，无 CSS 图形或 emoji 替代。
- 文案与数据：通过。明确网络故障、重试动作和本机收藏的降级能力；不承诺未缓存数据可离线访问。

### 状态与交互检查

- 浏览器 `offline` 事件打开无网络状态，`online` 事件关闭并显示“网络已恢复”成功 toast。
- 实测点击“重新连接”关闭状态页，toast 文案为“连接成功，内容已刷新”，图标为 `check_circle`，类型为 `success`。
- Toast 统一为 44px 最小高度；成功、错误、信息分别带 `check_circle`、`error`、`info` 图标，避免只靠相近色系表达状态。
- “先看我的收藏”在已登录时进入收藏页，游客则触发一步登录，不新增阻塞引导。
- 预览入口控制台无 error / warn；全量 pytest `37 passed`。

### Comparison history

1. 原实现只有纯文字深色 toast，升级为图标 + 文案的三语义反馈，并保留所有旧调用兼容。
2. 新增无网络运行时状态、重试和本机收藏降级路径；以空状态组件参考图并排复核卡片宽度、圆角、标题层级、按钮高度与底部导航。

### final result

passed

---

## 后台其余页面与角色权限回归

- 设计依据：后台 Figma A 全局框架、B1 表格样板、B2 队列样板、C1 tokens 差异与 C2 映射契约。
- 桌面基准：1440 × 1024；实现截图位于 `output/admin-pages/`。
- 预览入口：`admin-todo`、`admin-users`、`admin-operations`、`admin-reminders`、`admin-settings`、`admin-collection`、`admin-logs`。

### 页面覆盖

- 待办中心：六类队列共用 `/api/admin/todo-summary`，指标卡只保留四张精简数据；完整指标仍在独立数据概览页。
- 用户与摊主：用户账号、摊主认证、后台账号管理三 Tab；超级管理员受保护，普通用户与审核员状态可辨识。
- 内容运营：公告、前台默认停用的轮播图、旅行热门地区三 Tab；热门地区读取真实聚合接口。
- 提醒推送：待发送任务、提醒订阅、生成规则三 Tab；明确前晚 20:00、当天 06:30、每日营业与 `needs_review` 跳过规则。
- APP 设置：APP 设置、短信服务、帮助三 Tab；短信不再占独立侧栏入口。
- 数据采集：爬虫中心、采集数据库、运行日志、豆包提示词四 Tab；原流程不改，只统一导航、按钮、表格并清理 emoji。
- 操作日志：展示操作人、动作、目标、详情与 IP；审核员不可进入三个系统页。

### 权限与数据检查

- 角色只保留超级管理员 / 审核员两级后台权限；审核员直接访问 APP 设置、数据采集或操作日志时回到待办中心。
- 审核员不可提升任何账号角色；摊主申请审核必须记录操作人和原因，通过后只把普通用户升级为 `seller`。
- 新增 `seller_applications` 队列表与索引；列表、通过、驳回接口均使用 JWT 后台权限与操作日志。
- 浏览器实测七个页面可打开；设置/采集各 Tab、摊主、热门地区与提醒规则可切换；新会话控制台无 error / warning。
- `admin.html` 内联脚本语法通过，`app.py` 编译通过，全量 pytest `53 passed`。

### final result

passed

---

## OCR 名称复核生产闭环

- 触发原因：坐标补救后已有 84 条 `market_name_ocr` 待复核记录，但原后台审核页只暴露坐标与集期两类数据复核，待办总数也未包含名称复核。
- 修正：待办中心新增“名称复核”一行；审核中心新增“名称复核”Tab；两处均读取 `data_review_queue.issue_type=market_name_ocr` 的真实数量与队列内容。
- 同步修正：集市线索、用户反馈、摊主申请三个审核 Tab 从示例数据切换到各自真实接口；当前 Tab 的徽标与队列总数使用接口返回值，不再把设计示例数当生产数据。
- 交互：OCR 队列明确提示“AI 仅做风险标记，需人工核对原文与证据”；通过/忽略均写 `reviewed_by` 与原因，驳回必须选择原因。
- 本地预览：`?preview=admin-review` 可见评价、线索、坐标、名称、集期、反馈、摊主七个 Tab；名称复核显示 84 项并可进入左列表右详情。
- 截图：`output/admin-pages/name-review-1280.png`（额外运行态档案；原后台 1440px 设计对照档案保持不变）。
- 自动化：全量 pytest `57 passed`，`app.py` 编译通过，浏览器运行时无脚本中断。

### final result

passed
