#!/bin/bash
# ============================================================================
# Rebaseline 脚本 - 重新基线化
# ============================================================================
#
# 使用场景:
#   - changeset 太多，CI 扫描变慢
#   - 所有变更已稳定，不需要回滚
#   - 定期维护
#
# 【重要】数据库恢复说明:
#   rebaseline 会归档之前的 changeset
#   归档后，数据库恢复流程:
#   1. 用 baseline v1.X 恢复到最后一次 rebaseline 的状态
#   2. 所以每次 rebaseline 前必须备份全量数据!
#
# 使用方法:
#   bash scripts/rebaseline.sh v1.1.0
#
# ============================================================================

set -e

NEW_TAG=${1:-"v1.1.0"}
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
ARCHIVE_DIR="changelogs/mapper/ARCHIVE_rebased_${TIMESTAMP}"
BACKUP_DIR="archived/backup"

echo "============================================"
echo "  Rebaseline 脚本"
echo "  新基线版本: $NEW_TAG"
echo "  时间戳: $TIMESTAMP"
echo "============================================"

# 检查参数
if [ -z "$NEW_TAG" ]; then
    echo "错误: 请提供新版本号"
    echo "使用方法: bash scripts/rebaseline.sh v1.1.0"
    exit 1
fi

# Step 1: 确认操作
echo ""
echo "[1/7] 确认操作"
echo ""
echo "警告: 此操作将:"
echo "  1. 创建新的 baseline 文件: $NEW_TAG"
echo "  2. 归档所有当前的 changeset"
echo "  3. 备份当前全量数据 (用于数据库恢复)"
echo ""
echo "【重要】在继续之前，请确保:"
echo "  - 所有变更已通过 UAT 测试"
echo "  - 所有变更已通过 PRD 验证"
echo "  - 不需要回滚到 rebaseline 之前的版本"
echo ""
read -p "继续? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "已取消"
    exit 0
fi

# Step 2: 导出当前数据作为备份
echo ""
echo "[2/7] 导出当前数据作为备份"
echo "请在另一个终端执行以下命令，导出全量数据:"
echo ""
echo "  mysqldump -t --no-create-info \\"
echo "    --skip-lock-tables \\"
echo "    -u root -p etl_db \\"
echo "    aaa_pre_pro_master s2t_mapping setting_day \\"
echo "    > /tmp/current_data.sql"
echo ""
echo "【重要】此备份文件用于数据库恢复，请妥善保存!"
echo ""
read -p "数据已导出到 /tmp/current_data.sql? (yes/no): " exported

if [ "$exported" != "yes" ]; then
    echo "请先导出数据"
    exit 1
fi

# Step 3: 保存备份到 archived
echo ""
echo "[3/7] 保存备份"
mkdir -p "$BACKUP_DIR"
cp /tmp/current_data.sql "$BACKUP_DIR/${NEW_TAG}_full_backup_${TIMESTAMP}.sql"
echo "已保存到: $BACKUP_DIR/${NEW_TAG}_full_backup_${TIMESTAMP}.sql"

# Step 4: 创建新的 baseline 文件
echo ""
echo "[4/7] 创建新的 baseline 文件"
NEW_BASELINE="changelogs/baseline/${NEW_TAG}_baseline.sql"

cat > "$NEW_BASELINE" << 'EOF'
-- liquibase formatted sql
-- ============================================================================
-- 基线版本: NEW_TAG_PLACEHOLDER
-- ============================================================================
--
-- 由 rebaseline.sh 自动生成
-- 生成时间: TIMESTAMP_PLACEHOLDER
--
-- 【重要概念】
--
-- 1. 此文件包含当前数据库的全部业务数据
--    部署时会执行 INSERT IGNORE / NOT EXISTS，已存在的数据会被跳过
--
-- 2. 此 baseline 之后，所有之前的 changeset 已被归档
--    数据库恢复时，直接用这个 baseline 恢复即可
--
-- 3. 备份文件位于: archived/backup/
--    如需恢复到最后一次 rebaseline 之前的状态，使用备份文件
--
-- ============================================================================

-- changeset baseline:001 author:infra context:uat,prd
-- comment: 创建核心业务表
-- rollback: NOT SUPPORTED

CREATE TABLE IF NOT EXISTS aaa_pre_pro_master (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    configuration_id VARCHAR(100) NOT NULL,
    flow_name VARCHAR(200) NOT NULL,
    source_type VARCHAR(50),
    source_connection VARCHAR(500),
    target_type VARCHAR(50),
    target_connection VARCHAR(500),
    etl_mapper_version VARCHAR(100),
    schedule_cron VARCHAR(100),
    param_config TEXT,
    country_code VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_configuration_id (configuration_id),
    KEY idx_etl_mapper_version (etl_mapper_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS s2t_mapping (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    map_name VARCHAR(200) NOT NULL,
    map_version VARCHAR(50) NOT NULL,
    maptype VARCHAR(50) NOT NULL,
    view_name VARCHAR(100),
    map_detail TEXT,
    execution_order INT DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    description VARCHAR(500),
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_map_name_version (map_name, map_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS setting_day (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    setting_key VARCHAR(100) NOT NULL,
    setting_value VARCHAR(500) NOT NULL,
    setting_type VARCHAR(50) DEFAULT 'GENERAL',
    country_code VARCHAR(10),
    effective_date DATE,
    expiry_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    description VARCHAR(255),
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_setting_key (setting_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- changeset baseline:002 author:infra context:uat,prd
-- comment: 导入当前全量业务数据
-- rollback: NOT SUPPORTED
-- 【请将 /tmp/current_data.sql 的内容追加到此处】
-- 【格式: aaa_pre_pro_master 和 setting_day 用 INSERT IGNORE】
-- 【      s2t_mapping 用 NOT EXISTS】
EOF

# 替换占位符
sed -i "s/NEW_TAG_PLACEHOLDER/$NEW_TAG/g" "$NEW_BASELINE"
sed -i "s/TIMESTAMP_PLACEHOLDER/$(date '+%Y-%m-%d %H:%M:%S')/g" "$NEW_BASELINE"

# 追加导出的数据
if [ -f /tmp/current_data.sql ]; then
    echo "" >> "$NEW_BASELINE"
    echo "-- -----------------------------------------------------------" >> "$NEW_BASELINE"
    echo "-- 业务数据 (从数据库导出)" >> "$NEW_BASELINE"
    echo "-- -----------------------------------------------------------" >> "$NEW_BASELINE"
    cat /tmp/current_data.sql >> "$NEW_BASELINE"
fi

echo "已创建: $NEW_BASELINE"

# Step 5: 归档旧的 changeset
echo ""
echo "[5/7] 归档旧的 changeset"
mkdir -p "$ARCHIVE_DIR"

echo "移动以下文件到归档目录:"
for dir in changelogs/master changelogs/mapper changelogs/config; do
    if [ -d "$dir" ]; then
        files=$(find "$dir" -maxdepth 1 -type f -name "*.sql" ! -path "*/ARCHIVE_*" 2>/dev/null | wc -l)
        if [ "$files" -gt 0 ]; then
            echo "  $dir/: $files 个文件"
            find "$dir" -maxdepth 1 -type f -name "*.sql" ! -path "*/ARCHIVE_*" -exec mv -v {} "$ARCHIVE_DIR/" \;
        fi
    fi
done

# Step 6: 更新 master.xml
echo ""
echo "[6/7] 更新 master.xml"
echo "请手动检查 master.xml:"
echo "  1. 新 baseline 在最前面"
echo "  2. include 顺序正确"

# Step 7: UAT 测试
echo ""
echo "[7/7] UAT 测试"
echo ""
echo "请执行以下命令在 UAT 测试:"
echo ""
echo "  # 清理现有数据 (小心! 这会删除数据)"
echo "  mvn liquibase:drop-all -Dliquibase.contexts=uat"
echo ""
echo "  # 从新 baseline 重建"
echo "  mvn liquibase:update -Dliquibase.contexts=uat"
echo ""
echo "  # 验证数据完整"
echo "  mvn liquibase:history"
echo ""
echo "  # 打 tag"
echo "  mvn liquibase:tag -Dliquibase.tag=$NEW_TAG"
echo ""
echo "============================================"
echo "  Rebaseline 完成!"
echo "============================================"
echo ""
echo "【重要提醒】"
echo "  备份文件: $BACKUP_DIR/${NEW_TAG}_full_backup_${TIMESTAMP}.sql"
echo "  如需恢复到 rebaseline 之前的状态:"
echo "  1. 找到对应的备份文件"
echo "  2. 用 mysql 命令恢复: mysql -u root -p etl_db < backup_file.sql"
