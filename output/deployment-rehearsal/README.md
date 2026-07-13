# 集市数据同步本地演练

- 演练日期：2026-07-14（Asia/Shanghai）
- 来源：本地 `data/zhaojishi.db`（只读）
- 目标：来源库的临时副本，仅人为改动 1 条集市名称
- 首次预览：新增 0、更新 1
- 应用同步：写入前成功生成 SQLite 在线备份，新增 0、更新 1
- 二次预览：新增 0、更新 0，幂等性通过
- 同步后完整性：`PRAGMA integrity_check = ok`
- 用户数据逻辑哈希（users/reviews/favorites/market_reminders/app_settings）：
  - 同步前：`c68b85cf01334c7a2d6ad509956d0420c385071777f0857af21596518f5ca898`
  - 同步后：`c68b85cf01334c7a2d6ad509956d0420c385071777f0857af21596518f5ca898`

结论：同步器仅按集市 ID 更新基础资料；评分、评价数、收藏数、创建人及其它业务表未被覆盖。
