# 高德 Key 分流与源码清理 QA

- 日期：2026-07-14
- 结论：通过
- 范围：C 端地图加载、后台地图配置、服务端 Web 服务调用

## 验收结果

- 高德 Web 服务 Key 仅从 `app_settings.amap_ws_key` 读取，不再存在源码回退值。
- 浏览器 JS API Key 与安全密钥使用独立配置项，不与 Web 服务 Key 混用。
- 公开配置接口只返回浏览器 JS API 所需字段，不返回 Web 服务 Key。
- 后台读取配置时只显示末六位；完整值不回传到管理页面。
- JS API 未配置或加载失败时，地图功能降级，不阻塞列表、定位和其他页面。
- 375×812 地图页拖起态无横向溢出，示例名为“沂州古城庙会”。

## 自动化验证

```text
pytest: 48 passed
app.py/open_time.py/迁移与地理编码脚本: py_compile passed
index.html: 4 段内联脚本语法通过
admin.html: 3 段内联脚本语法通过
app.py/index.html/admin.html: 无32位高德配置值残留
```

## 上线前人工门槛

- 在后台 APP 设置填写 Web端(JS API) Key 与安全密钥。
- 在高德控制台为 JS API Key 配置正式域名白名单。
- Web 服务 Key 继续单独配置，禁止复制到浏览器端配置项。
