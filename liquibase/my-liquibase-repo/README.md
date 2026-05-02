# Liquibase ETL 部署

## 目录结构

```
my-liquibase-repo/
├── pom.xml
├── liquibase/
│   ├── master.xml
│   ├── liquibase.properties
│   ├── changelogs/
│   │   ├── baseline/                 # 基线版本
│   │   │   ├── v1.0.0_baseline.sql  # 当前基线
│   │   │   └── v1.1.0_baseline.sql  # 未来的新基线
│   │   ├── master/                  # 核心流程定义表
│   │   ├── mapper/                  # ETLmapper 表 ★最活跃
│   │   │   ├── 20260501_xxx.sql    # 活跃的 changeset
│   │   │   └── ARCHIVE_2025_Q4/     # 归档 (CI 跳过)
│   │   └── config/
│   └── scripts/
│       ├── pre-commit.sh              # Git hook 检查
│       ├── new_changeset.sh          # 快速生成 changeset
│       ├── uat_validate.sh           # UAT 数据校验
│       ├── check_conflicts.py        # 冲突检测
│       ├── generate_diff.sh          # UAT vs PRD diff
│       ├── archive_mapper.sh         # 归档脚本
│       └── rebaseline.sh            # 重新基线脚本
├── archived/                         # 归档目录
│   ├── backup/                       # ★ 数据库备份 (用于恢复!)
│   │   └── v1.0.0_full_backup.sql  # rebaseline 时的全量备份
│   └── changelogs/                  # 归档的 changeset
└── .github/workflows/
    ├── ci.yml
    └── deploy.yml
```

---

## 核心概念

### Liquibase 执行逻辑

```
mvn liquibase:update 执行时:

1. 读取 master.xml，找到所有 changeset
2. 检查 DATABASECHANGELOG 表 (Liquibase 自动维护)
3. 对每个 changeset:
   - ID + AUTHOR 已在表中?  → 跳过
   - 不在表中?               → 执行，写入 DATABASECHANGELOG
```

## 幂等部署策略

| 表 | 唯一键 | 幂等方式 |
|----|--------|----------|
| `aaa_pre_pro_master` | ✅ `uk_configuration_id` | `INSERT IGNORE` |
| `s2t_mapping` | ❌ **没有唯一键!** | `NOT EXISTS` |
| `setting_day` | ✅ `uk_setting_key` | `INSERT IGNORE` |

### 为什么 s2t_mapping 没有唯一键？

因为同一 `(mapname, mapversion)` 可以有多条记录（多种 ETL 类型）：

| map_name | map_version | maptype | view_name | execution_order |
|----------|-------------|---------|-----------|----------------|
| user_etl | v1 | SQL-FILE | user_view | 1 |
| user_etl | v1 | SQL-SUBQUERY | active_view | 2 |
| user_etl | v1 | SQL-DIRECT | final_view | 3 |

### s2t_mapping 必须用 NOT EXISTS

```sql
-- 正确写法
INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, ...)
SELECT NULL, 'user_etl', 'v1', 'SQL-FILE', 'user_view', ...
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping
    WHERE map_name = 'user_etl'
    AND map_version = 'v1'
    AND maptype = 'SQL-FILE'
    AND view_name = 'user_view'
);

-- 【警告】不能用 INSERT IGNORE，会导致重复数据!
```

---

## 开发和 UAT 阶段自动化

### 脚本列表

| 脚本 | 功能 |
|------|------|
| `scripts/pre-commit.sh` | Git hook，检查 changeset 格式 |
| `scripts/new_changeset.sh` | 快速生成新的 changeset 文件 |
| `scripts/uat_validate.sh` | UAT 部署后数据校验 |
| `scripts/check_conflicts.py` | 检测多人修改同一记录的冲突 |
| `scripts/generate_diff.sh` | UAT vs PRD 生成 diff changeset |

### 安装 Git Hook (开发阶段)

```bash
# 每次提交前自动检查格式
cp liquibase/scripts/pre-commit.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

### 快速创建 Changeset (开发阶段)

```bash
# 创建新的 mapper changeset
bash liquibase/scripts/new_changeset.sh mapper "新增用户过滤"

# 创建 master changeset
bash liquibase/scripts/new_changeset.sh master "修改流程参数"

# 输出:
# ✓ 已创建: changelogs/mapper/20260501_143000_map_001_new_xxx.sql
```

### UAT 部署后校验

```bash
# 设置数据库连接
export DB_HOST=uat-mysql.internal
export DB_PORT=3306
export DB_NAME=etl_db
export DB_USER=liquibase
export DB_PASSWORD=xxx

# 运行校验
bash liquibase/scripts/uat_validate.sh

# 检查内容:
#   ✓ 表是否存在
#   ✓ 数据量是否合理
#   ✓ 关键记录是否存在
#   ✓ 数据一致性
```

### 检测冲突 (多团队场景)

```bash
# 检查是否有多个 author 修改了同一记录
python liquibase/scripts/check_conflicts.py

# 发现冲突输出示例:
# 冲突: user_etl/v1/SQL-SUBQUERY/active_view
#   - 文件: changelogs/mapper/20260501_xxx.sql
#     changeset: map:001
#     author: team_a
#   - 文件: changelogs/mapper/20260502_xxx.sql
#     changeset: map:005
#     author: team_b
```

### UAT 到 PRD 的 Diff 生成

```bash
# 设置 UAT 和 PRD 数据库连接
export DB_UAT_HOST=uat-mysql.internal
export DB_PRD_HOST=prd-mysql.internal
export DB_UAT_USER=liquibase
export DB_PRD_USER=liquibase
export DB_UAT_PASSWORD=xxx
export DB_PRD_PASSWORD=xxx

# 生成 diff changeset
bash liquibase/scripts/generate_diff.sh

# 输出:
# ✓ Diff 文件已生成: changelogs/mapper/20260501_diff_to_prd.sql

# 下一步:
#   1. 检查生成的文件
#   2. 人工确认每一条变更
#   3. 添加到仓库并提交 PR
```

---

## 完整操作流程

### 1. INIT — 初始化 (只需要一次)

**场景**: 新环境 / 首次使用 Liquibase

```bash
# ============================================================
# Step 1: 导出生产库现有数据
# ============================================================
# 在生产库执行，导出现有业务数据为 INSERT 语句
mysqldump -t --no-create-info \
  --skip-lock-tables \
  -u root -p etl_db \
  aaa_pre_pro_master s2t_mapping setting_day \
  > /tmp/existing_data.sql

# ============================================================
# Step 2: 整理数据格式
# ============================================================
# 导出的数据需要整理成 Liquibase 格式:
#
# aaa_pre_pro_master, setting_day → 用 INSERT IGNORE
# s2t_mapping                    → 用 NOT EXISTS
#
# 放入 changelogs/baseline/v1.0.0_baseline.sql
# 参考文件中的示例格式

# ============================================================
# Step 3: 在 UAT 测试 INIT
# ============================================================
mvn liquibase:update -Dliquibase.contexts=uat

# 查看执行结果
mvn liquibase:history

# ============================================================
# Step 4: 验证数据正确
# ============================================================
mysql -u root -p etl_db -e "SELECT COUNT(*) FROM s2t_mapping"

# ============================================================
# Step 5: 打 tag 标记基线
# ============================================================
mvn liquibase:tag -Dliquibase.tag=v1.0.0

# ============================================================
# Step 6: 同样流程部署到 PRD
# ============================================================
mvn liquibase:update -Dliquibase.contexts=prd
mvn liquibase:tag -Dliquibase.tag=v1.0.0
```

---

### 2. UPDATE — 日常部署

**场景**: 新增 ETL mapper / 修改配置

```bash
# ============================================================
# Step 1: 创建新的 changeset
# ============================================================
# 文件: changelogs/mapper/20260502_100000_map_003_new_filter.sql

-- changeset: map:003 author:team_a context:uat,prd
-- 【重要】s2t_mapping 没有唯一键，必须用 NOT EXISTS!

INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
SELECT NULL, 'user_filter', 'v1', 'SQL-SUBQUERY', 'filtered_view', 'SELECT * FROM user_view WHERE age > 18', 1, TRUE, '过滤成年用户', 'team_a'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping
    WHERE map_name = 'user_filter'
    AND map_version = 'v1'
    AND maptype = 'SQL-SUBQUERY'
    AND view_name = 'filtered_view'
);

# ============================================================
# Step 2: UAT 测试
# ============================================================
mvn liquibase:update -Dliquibase.contexts=uat

# 验证
mvn liquibase:history

# ============================================================
# Step 3: PR Review → Merge
# ============================================================

# ============================================================
# Step 4: 部署到 PRD (CI/CD 自动)
# ============================================================
mvn liquibase:update -Dliquibase.contexts=prd
```

---

### 3. ROLLBACK — 回滚

**场景**: 部署出问题了，需要撤回

```bash
# ============================================================
# 查看历史，找到要回滚的 changeset
# ============================================================
mvn liquibase:history

# 输出示例:
# +-------+------------------+------------+--------+
# | ID    | CHANGESET        | AUTHOR     | DATE   |
# +-------+------------------+------------+--------+
# | 0001  | baseline:001     | infra      | 10:00  |
# | 0002  | baseline:002     | infra      | 10:05  |
# | 0003  | map:001          | team_a     | 10:10  |  ← A 的
# | 0004  | map:002          | team_a     | 10:15  |  ← A 的
# | 0005  | map:003          | team_b     | 10:20  |  ← B 的 (不要动!)
# +-------+------------------+------------+--------+

# ============================================================
# 方式 A: 回滚最近 N 个 changeset (推荐!)
# ============================================================
# A 要回滚自己的 2 个 changeset
mvn liquibase:rollbackCount 2 -Dliquibase.contexts=prd

# 效果: 只回滚 map:002 和 map:001
# B 的 map:003 不受影响

# ============================================================
# 方式 B: 回滚到某个 tag
# ============================================================
# 会回滚 tag 之后的所有变更 (小心! 会影响 B)
mvn liquibase:rollback -Dliquibase.rollbackTag=v1.0.0

# ============================================================
# 方式 C: 回滚到指定时间
# ============================================================
mvn liquibase:rollbackDate -Dliquibase.rollbackDate="2026-05-01 10:00:00"
```

---

### 4. RESTORE — 数据库恢复

**场景**: 数据库坏了，需要从备份恢复

```
┌─────────────────────────────────────────────────────────────────┐
│  Rebaseline 之后的数据库恢复                                        │
│                                                                 │
│  baseline v1.1.0 ────────────────────────────▶ 现在              │
│       │                                                       │
│       │  ARCHIVE 里的 changeset 不再参与!                       │
│       │  恢复只能恢复到 v1.1.0 的状态                           │
│       │                                                        │
│       ▼                                                        │
│  用 baseline + archived/backup 恢复                              │
└─────────────────────────────────────────────────────────────────┘
```

```bash
# ============================================================
# Step 1: 确认损坏程度
# ============================================================
# 是表被删了，还是整个数据库没了？

# ============================================================
# 情况 A: 用 Liquibase 备份恢复 (推荐)
# ============================================================
# 每次 rebaseline 前会保存全量备份到 archived/backup/
# 用最新的备份恢复

# 找到最新的备份
ls -la archived/backup/

# 恢复全量数据
mysql -u root -p etl_db < archived/backup/v1.1.0_full_backup_20260501.sql

# ============================================================
# 情况 B: 用 Liquibase 重建
# ============================================================
# 如果备份不可用，用 baseline 重建

# 清理数据库
mvn liquibase:drop-all -Dliquibase.contexts=prd

# 从 baseline 重建
mvn liquibase:update -Dliquibase.contexts=prd

# 打 tag
mvn liquibase:tag -Dliquibase.tag=v1.1.0

# ============================================================
# 重要提醒
# ============================================================
# - REBASELINE 之后的恢复，只能恢复到最后一次 REBASELINE 的状态
# - REBASELINE 之后的新变更会丢失!
# - 所以 REBASELINE 前要确认所有变更已稳定
```

---

### 5. REBASELINE — 重新基线化

**场景**: changeset 太多了，扫描变慢 / 需要清理历史

```bash
# ============================================================
# 什么是 REBASELINE?
# ============================================================
# 把 baseline 之后的"增量变更"合并到新的 baseline
# 旧的 changeset 归档，新的 baseline 包含所有数据
#
# 原理:
#   BEFORE: baseline (旧) + 100个changeset → 扫描101个文件
#   AFTER:  baseline (新)                 → 只扫描1个文件

# ============================================================
# Step 1: 创建归档目录
# ============================================================
mkdir -p changelogs/mapper/ARCHIVE_2025_Q2

# ============================================================
# Step 2: 导出当前数据库的全部数据
# ============================================================
mysqldump -t --no-create-info \
  --skip-lock-tables \
  -u root -p etl_db \
  aaa_pre_pro_master s2t_mapping setting_day \
  > /tmp/current_data.sql

# ============================================================
# Step 3: 创建新的 baseline 文件
# ============================================================
# 文件: changelogs/baseline/v1.1.0_baseline.sql
# 内容: 表结构 + Step 2 导出的全部数据

# ============================================================
# Step 4: 移动旧的 changeset 到 ARCHIVE
# ============================================================
mv changelogs/mapper/20260501_*.sql changelogs/mapper/ARCHIVE_2025_Q2/
mv changelogs/master/20260501_*.sql changelogs/mapper/ARCHIVE_2025_Q2/
mv changelogs/config/20260501_*.sql changelogs/mapper/ARCHIVE_2025_Q2/

# ============================================================
# Step 5: 在 UAT 测试
# ============================================================
mvn liquibase:drop-all -Dliquibase.contexts=uat   # 清理
mvn liquibase:update -Dliquibase.contexts=uat      # 从新 baseline 重建

# 验证数据完整
mvn liquibase:history

# ============================================================
# Step 6: 打新 tag
# ============================================================
mvn liquibase:tag -Dliquibase.tag=v1.1.0

# ============================================================
# Step 7: 同样流程部署到 PRD
# ============================================================
mvn liquibase:drop-all -Dliquibase.contexts=prd
mvn liquibase:update -Dliquibase.contexts=prd
mvn liquibase:tag -Dliquibase.tag=v1.1.0
```

---

## ARCHIVE 策略

### 什么时候 ARCHIVE?

| 触发条件 | 动作 |
|---------|------|
| changeset 超过 50 个 | 考虑 rebaseline |
| 季度结束 | 归档 Q1 → ARCHIVE_2025_Q1 |
| CI 构建超慢 | 立即 rebaseline |

### CI 构建跳过 ARCHIVE

```yaml
# .github/workflows/ci.yml
- name: Build (exclude ARCHIVE)
  run: |
    # 只扫描非 ARCHIVE 的文件
    find liquibase/changelogs \
      -type f -name "*.sql" \
      ! -path "*/ARCHIVE_*/*" \
      > scanned_files.txt

    mvn liquibase:validate \
      -Dliquibase.changeLogFile=scanned_files.txt
```

### 归档后文件结构

```
changelogs/
├── baseline/
│   ├── v1.0.0_baseline.sql     ← 旧基线 (保留,标记用)
│   └── v1.1.0_baseline.sql     ← 新基线 (活跃)
├── mapper/
│   ├── 20260501_xxx.sql        ← 活跃 changeset
│   └── ARCHIVE_2025_Q1/        ← Q1 归档
│       ├── 001_merged.sql       ← 合并后的 changeset
│       ├── 002_merged.sql
│       └── ...
```

---

## 完整流程图

```
┌─────────────────────────────────────────────────────────────────┐
│  INIT (一次性)                                                   │
│  1. 导出生产库数据                                               │
│  2. 创建 v1.0.0_baseline.sql                                    │
│  3. mvn liquibase:update                                        │
│  4. mvn liquibase:tag v1.0.0                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  UPDATE (日常)                                                   │
│  1. 创建新的 .sql changeset                                     │
│  2. PR Review → Merge                                           │
│  3. CI 自动部署到 UAT → PRD                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  ROLLBACK (出问题)                                               │
│  mvn liquibase:rollbackCount N    ← 推荐，只回滚自己的           │
│  mvn liquibase:rollback --tag X   ← 慎用，会回滚所有之后变更    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  RESTORE (数据库坏了)                                             │
│  用 archived/backup/ 恢复全量数据                                 │
│  或: mvn liquibase:drop-all + update (只能恢复到 baseline 状态)  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  REBASELINE (定期/超50个changeset)                              │
│  1. 导出当前全量数据到 archived/backup/                          │
│  2. 创建 v1.1.0_baseline.sql                                   │
│  3. 移动旧 changeset 到 ARCHIVE_*/                              │
│  4. UAT 测试: drop-all → update                                 │
│  5. 打 tag v1.1.0                                               │
└─────────────────────────────────────────────────────────────────┘
```
│  4. UAT 测试: drop-all → update                                 │
│  5. 打 tag v1.1.0                                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 常用命令速查

```bash
# INIT
mvn liquibase:update -Dliquibase.contexts=uat
mvn liquibase:tag -Dliquibase.tag=v1.0.0

# UPDATE
mvn liquibase:update
mvn liquibase:validate    # 验证 changeset

# ROLLBACK
mvn liquibase:history
mvn liquibase:rollbackCount 3
mvn liquibase:rollback -Dliquibase.rollbackTag=v1.0.0

# REBASELINE
mvn liquibase:drop-all
mvn liquibase:update
mvn liquibase:tag -Dliquibase.tag=v1.1.0
```

## 环境要求

- Java 11+
- Maven 3.6+
- Liquibase 4.25.1
- MySQL 8.0+
