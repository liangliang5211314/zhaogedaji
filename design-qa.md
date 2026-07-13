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
