#!/bin/bash
# ============================================================================
# Rollback 管理工具 - 静态版本
# ============================================================================
#
# 使用方式:
#   bash rollback_manager.sh execute <rollback_file>
#
# 示例:
#   bash rollback_manager.sh execute changelogs/mapper/rollback/001--rollback.sql
#
# ============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ============================================================================
# 执行 Rollback
# ============================================================================

execute() {
    local rollback_file=${1:-""}

    if [ -z "$rollback_file" ]; then
        echo ""
        echo -e "${RED}错误: 请指定 rollback 文件${NC}"
        echo ""
        echo "用法: bash rollback_manager.sh execute <rollback_file>"
        echo ""
        echo "示例:"
        echo "  bash rollback_manager.sh execute changelogs/mapper/rollback/001--rollback.sql"
        echo ""
        list_available
        return 1
    fi

    if [ ! -f "$rollback_file" ]; then
        echo ""
        echo -e "${RED}错误: 文件不存在: $rollback_file${NC}"
        return 1
    fi

    echo ""
    echo "============================================"
    echo "  执行 Rollback"
    echo "  文件: $rollback_file"
    echo "============================================"
    echo ""

    echo "Rollback SQL 内容:"
    echo "----------------------------------------"
    head -20 "$rollback_file"
    echo "..."
    echo "----------------------------------------"
    echo ""

    read -p "确认执行? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "已取消"
        return
    fi

    echo ""
    echo -e "${YELLOW}正在执行 rollback...${NC}"
    echo ""

    mvn liquibase:update \
        -Dliquibase.changeLogFile="$rollback_file" \
        -Dliquibase.contexts=rollback

    echo ""
    echo -e "${GREEN}✓ Rollback 执行完成!${NC}"
    echo ""
    echo "下一步:"
    echo "  1. 验证数据是否正确"
    echo "  2. 如果 rollback 文件需要合并，复制到 master:rollback/"
    echo "  3. DATABASECHANGELOG 已记录，无需额外操作"
    echo ""
}

# ============================================================================
# 查看可用的 Rollback 文件
# ============================================================================

list_available() {
    echo ""
    echo "============================================"
    echo "  可用的 Rollback 文件"
    echo "============================================"
    echo ""

    if [ -d "changelogs/mapper/rollback" ]; then
        echo "目录: changelogs/mapper/rollback/"
        ls -la changelogs/mapper/rollback/*.sql 2>/dev/null || echo "  (空)"
    else
        echo "  (rollback 目录不存在)"
    fi

    echo ""
}

# ============================================================================
# 帮助
# ============================================================================

show_help() {
    echo ""
    echo "Rollback 管理工具"
    echo ""
    echo "用法:"
    echo "  bash rollback_manager.sh execute <rollback_file>"
    echo "      执行指定的 rollback 文件"
    echo ""
    echo "  bash rollback_manager.sh list"
    echo "      查看可用的 rollback 文件"
    echo ""
    echo "示例:"
    echo "  bash rollback_manager.sh execute changelogs/mapper/rollback/001--rollback.sql"
    echo ""
    echo "Rollback 文件来源:"
    echo "  - rollback-version branch"
    echo "  - 合并后: master:changelogs/mapper/rollback/"
    echo ""
    echo "注意:"
    echo "  - Rollback 文件不在 master.xml 里"
    echo "  - 需要时手动执行"
    echo "  - DATABASECHANGELOG 会自动记录"
    echo ""
}

# ============================================================================
# 主命令
# ============================================================================

case "${1:-}" in
    execute|run)
        shift
        execute "$1"
        ;;
    list|ls)
        list_available
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        show_help
        ;;
esac
