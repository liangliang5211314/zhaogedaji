# 找个大集生产发布手册

## 发布保障

- 不允许跳过备份；备份使用 SQLite 在线备份 API，并在更新代码前校验完整性。
- 只允许 Git 快进更新；服务器存在已跟踪文件改动时停止。
- 集市同步默认只出差异报告；只有显式设置 `APPLY_MARKET_SYNC=1` 才写入。
- 集市同步不覆盖评分、点评数、收藏数、创建人和其它业务表。
- 部署后同时执行数据库完整性、本机健康检查；配置公网 URL 时再做公网检查。
- 任一步失败都保留部署前数据库、原提交号和目标提交号，不自动执行破坏性回滚。

## 生产执行模板

```bash
PROJECT=/www/wwwroot/zhaogedaji \
VENV=/www/wwwroot/zhaogedaji/38b982d1de7beb5083833ca4c8158371_venv \
DEPLOY_REMOTE=origin \
DEPLOY_BRANCH=main \
SERVICE_NAME=zhaojishi \
MARKET_SOURCE_DB=/安全临时目录/zhaojishi-market-source.db \
APPLY_MARKET_SYNC=1 \
PUBLIC_HEALTH_URL=https://正式域名/api/health \
bash deploy.sh
```

生产实际参数以 `output/questions.md` 回答为准，不把 SSH 凭据、密钥或生产数据库提交到 Git。
