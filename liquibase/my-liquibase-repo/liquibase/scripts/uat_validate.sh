#!/bin/bash
# ============================================================================
# UAT 部署后数据校验脚本
# ============================================================================
#
# 使用方法:
#   bash scripts/uat_validate.sh
#
# 功能:
#   1. 检查表是否存在
#   2. 检查数据量是否合理
#   3. 检查关键记录是否存在
#   4. 生成校验报告
#
# ============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-3306}
DB_NAME=${DB_NAME:-"etl_db"}
DB_USER=${DB_USER:-"liquibase"}
DB_PASS=${DB_PASS:-""}

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

echo "============================================"
echo "  UAT 数据校验"
echo "  主机: $DB_HOST:$DB_PORT/$DB_NAME"
echo "============================================"
echo ""

# 执行 SQL 并检查结果
check_sql() {
    local name="$1"
    local sql="$2"
    local expected_type="$3"  # count, exists, not_exists

    echo "----------------------------------------"
    echo "检查: $name"
    echo "SQL: $sql"
    echo "----------------------------------------"

    result=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -N -e "$sql" 2>/dev/null)

    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ SQL 执行失败${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        return 1
    fi

    case $expected_type in
        count)
            if [ "$result" -gt 0 ]; then
                echo -e "${GREEN}✓ 通过 (结果: $result)${NC}"
                PASS_COUNT=$((PASS_COUNT + 1))
            else
                echo -e "${RED}❌ 失败 (期望 > 0, 实际: $result)${NC}"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
            ;;
        exists)
            if [ -n "$result" ]; then
                echo -e "${GREEN}✓ 通过 (记录存在)${NC}"
                PASS_COUNT=$((PASS_COUNT + 1))
            else
                echo -e "${RED}❌ 失败 (记录不存在)${NC}"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
            ;;
        not_exists)
            if [ -z "$result" ]; then
                echo -e "${GREEN}✓ 通过 (记录不存在)${NC}"
                PASS_COUNT=$((PASS_COUNT + 1))
            else
                echo -e "${RED}❌ 失败 (记录不应该存在)${NC}"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
            ;;
    esac
    echo ""
}

# ============================================================================
# 1. 检查表是否存在
# ============================================================================
echo "【1. 表结构检查】"
echo ""

check_sql "aaa_pre_pro_master 表存在" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='aaa_pre_pro_master'" \
    "count"

check_sql "s2t_mapping 表存在" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='s2t_mapping'" \
    "count"

check_sql "setting_day 表存在" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='setting_day'" \
    "count"

# ============================================================================
# 2. 检查数据量
# ============================================================================
echo "【2. 数据量检查】"
echo ""

check_sql "aaa_pre_pro_master 有数据" \
    "SELECT COUNT(*) FROM aaa_pre_pro_master" \
    "count"

check_sql "s2t_mapping 有数据" \
    "SELECT COUNT(*) FROM s2t_mapping" \
    "count"

check_sql "setting_day 有数据" \
    "SELECT COUNT(*) FROM setting_day" \
    "count"

# ============================================================================
# 3. 检查关键记录
# ============================================================================
echo "【3. 关键记录检查】"
echo ""

check_sql "aaa_pre_pro_master 有活跃记录" \
    "SELECT COUNT(*) FROM aaa_pre_pro_master WHERE is_active=TRUE" \
    "count"

check_sql "s2t_mapping 有活跃记录" \
    "SELECT COUNT(*) FROM s2t_mapping WHERE is_active=TRUE" \
    "count"

check_sql "setting_day 有假期配置" \
    "SELECT COUNT(*) FROM setting_day WHERE setting_type='HOLIDAY'" \
    "count"

# ============================================================================
# 4. 数据一致性检查
# ============================================================================
echo "【4. 数据一致性检查】"
echo ""

# 检查 mapper version 是否都有对应的 s2t_mapping
check_sql "所有流程的 mapper version 都有对应记录" \
    "SELECT COUNT(DISTINCT etl_mapper_version)
     FROM aaa_pre_pro_master p
     LEFT JOIN s2t_mapping m ON CONCAT(p.map_name, p.map_version) = m.etl_mapper_version
     WHERE m.id IS NULL AND p.etl_mapper_version IS NOT NULL" \
    "count"

# 检查是否有重复的 (map_name, map_version, maptype, view_name)
check_sql "s2t_mapping 无重复记录" \
    "SELECT COUNT(*) FROM (
        SELECT map_name, map_version, maptype, view_name, COUNT(*) as cnt
        FROM s2t_mapping
        GROUP BY map_name, map_version, maptype, view_name
        HAVING cnt > 1
    ) t" \
    "count"

# ============================================================================
# 5. 检查 Liquibase 历史
# ============================================================================
echo "【5. Liquibase 变更历史】"
echo ""

check_sql "DATABASECHANGELOG 表存在" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='DATABASECHANGELOG'" \
    "count"

check_sql "有已执行的 changeset" \
    "SELECT COUNT(*) FROM DATABASECHANGELOG" \
    "count"

# ============================================================================
# 总结
# ============================================================================
echo "============================================"
echo "  校验结果"
echo -e "  ${GREEN}通过: $PASS_COUNT${NC}"
echo -e "  ${RED}失败: $FAIL_COUNT${NC}"
echo -e "  ${YELLOW}警告: $WARN_COUNT${NC}"
echo "============================================"
echo ""

if [ $FAIL_COUNT -gt 0 ]; then
    echo -e "${RED}❌ 校验失败，请检查上述问题${NC}"
    exit 1
fi

if [ $WARN_COUNT -gt 0 ]; then
    echo -e "${YELLOW}⚠ 有警告，建议检查${NC}"
fi

echo -e "${GREEN}✓ 所有校验通过!${NC}"
exit 0
