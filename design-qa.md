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
