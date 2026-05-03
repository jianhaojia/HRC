#!/bin/bash
# ============================================================================
# Git Pre-commit Hook
# 自动检查 changeset 格式
# ============================================================================
#
# 安装方法:
#   cp scripts/pre-commit.sh .git/hooks/pre-commit
#   chmod +x .git/hooks/pre-commit
#
# 检查内容:
#   1. changeset 必须有 author
#   2. changeset 必须有 context
#   3. s2t_mapping 必须用 NOT EXISTS，不能用 INSERT IGNORE
#   4. 文件名格式: YYYYMMDD_HHMMSS_type_xxx.sql
#
# ============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ERROR_COUNT=0
WARN_COUNT=0

echo "============================================"
echo "  Changeset 格式检查"
echo "============================================"

CHANGED_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep "\.sql$")

if [ -z "$CHANGED_FILES" ]; then
    echo "没有暂存的 .sql 文件，跳过检查"
    exit 0
fi

echo ""
echo "检查以下文件:"
echo "$CHANGED_FILES"
echo ""

for file in $CHANGED_FILES; do
    echo "----------------------------------------"
    echo "检查: $file"
    echo "----------------------------------------"

    filename=$(basename "$file")
    if [[ ! $filename =~ ^[0-9]{8}_[0-9]{6}_[a-z_]+\.[0-9]+\.sql$ ]]; then
        echo -e "${RED}❌ 文件名格式错误!${NC}"
        echo "   期望格式: YYYYMMDD_HHMMSS_type_序号.sql"
        echo "   示例: 20260501_100000_map_001_new_etl.sql"
        ERROR_COUNT=$((ERROR_COUNT + 1))
    else
        echo -e "${GREEN}✓ 文件名格式正确${NC}"
    fi

    content=$(cat "$file")

    if [[ ! $content =~ --[[:space:]]*changeset[[:space:]]*: ]]; then
        echo -e "${RED}❌ 缺少 changeset 声明!${NC}"
        ERROR_COUNT=$((ERROR_COUNT + 1))
    else
        echo -e "${GREEN}✓ 有 changeset 声明${NC}"
    fi

    if [[ ! $content =~ --[[:space:]]*changeset.*author: ]]; then
        echo -e "${RED}❌ 缺少 author!${NC}"
        echo "   格式: -- changeset: xxx author:your_name"
        ERROR_COUNT=$((ERROR_COUNT + 1))
    else
        echo -e "${GREEN}✓ 有 author${NC}"
    fi

    if [[ ! $content =~ --[[:space:]]*context: ]]; then
        echo -e "${RED}❌ 缺少 context!${NC}"
        echo "   格式: -- changeset: xxx context:uat,prd"
        ERROR_COUNT=$((ERROR_COUNT + 1))
    else
        echo -e "${GREEN}✓ 有 context${NC}"
    fi

    if [[ $content =~ s2t_mapping ]]; then
        if [[ $content =~ INSERT[[:space:]]+IGNORE[[:space:]]+INTO[[:space:]]+s2t_mapping ]]; then
            echo -e "${RED}❌ s2t_mapping 不能用 INSERT IGNORE!${NC}"
            echo "   必须用 NOT EXISTS 语法"
            ERROR_COUNT=$((ERROR_COUNT + 1))
        elif [[ $content =~ INSERT[[:space:]]+INTO[[:space:]]+s2t_mapping ]] && [[ ! $content =~ WHERE[[:space:]]+NOT[[:space:]]+EXISTS ]]; then
            echo -e "${RED}❌ s2t_mapping 必须用 NOT EXISTS!${NC}"
            ERROR_COUNT=$((ERROR_COUNT + 1))
        else
            echo -e "${GREEN}✓ s2t_mapping 使用了正确的 NOT EXISTS 语法${NC}"
        fi
    fi

    echo ""
done

echo "============================================"
echo "  检查结果"
echo -e "  ${RED}错误: $ERROR_COUNT${NC}"
echo -e "  ${YELLOW}警告: $WARN_COUNT${NC}"
echo "============================================"

if [ $ERROR_COUNT -gt 0 ]; then
    echo -e "${RED}❌ 检查失败，请修复上述错误${NC}"
    exit 1
fi

echo -e "${GREEN}✓ 所有检查通过!${NC}"
exit 0
