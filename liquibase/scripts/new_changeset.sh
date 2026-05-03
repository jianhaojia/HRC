#!/bin/bash
# ============================================================================
# 生成新的 Changeset 文件
# ============================================================================
#
# 使用方法:
#   bash scripts/new_changeset.sh mapper "新增用户过滤"
#   bash scripts/new_changeset.sh master "修改流程参数"
#   bash scripts/new_changeset.sh config "更新假期配置"
#
# 生成文件:
#   changelogs/mapper/20260501_143000_map_001_new_xxx.sql
#
# ============================================================================

set -e

TYPE=${1:-"mapper"}
DESCRIPTION=${2:-"new_changeset"}
AUTHOR=${3:-$(git config user.name 2>/dev/null || echo "unknown")}
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')

case $TYPE in
    mapper)
        TYPE_PREFIX="map"
        ;;
    master)
        TYPE_PREFIX="mst"
        ;;
    config)
        TYPE_PREFIX="cfg"
        ;;
    *)
        echo "错误: 类型必须是 mapper, master, 或 config"
        exit 1
        ;;
esac

# 查找当前目录下同类型最新的序号
SEQ=001
if ls changelogs/${TYPE}/${TIMESTAMP:0:8}_*_${TYPE_PREFIX}_*.sql 1>/dev/null 2>&1; then
    LAST_SEQ=$(ls changelogs/${TYPE}/${TIMESTAMP:0:8}_*_${TYPE_PREFIX}_*.sql 2>/dev/null | \
               sed -E "s/.*${TYPE_PREFIX}_([0-9]+).*/\1/" | sort -n | tail -1)
    SEQ=$(printf "%03d" $((LAST_SEQ + 1)))
fi

FILENAME="changelogs/${TYPE}/${TIMESTAMP}_${TYPE_PREFIX}_${SEQ}_${DESCRIPTION}.sql"

# 创建文件
cat > "$FILENAME" << EOF
-- liquibase formatted sql
-- ============================================================================
-- changeset: ${TYPE_PREFIX}:${SEQ} author:${AUTHOR} context:uat,prd
-- comment: ${DESCRIPTION}
-- rollback: DELETE FROM ${TYPE} WHERE ...;
-- ============================================================================

-- 在此添加 SQL 语句

EOF

echo ""
echo -e "${GREEN}✓ 已创建: $FILENAME${NC}"
echo ""
echo "提示:"
echo "  - s2t_mapping 必须用 NOT EXISTS 语法"
echo "  - 其他表用 INSERT IGNORE"
echo ""
echo "示例 (s2t_mapping):"
echo "  INSERT INTO s2t_mapping (id, map_name, map_version, maptype, ...)"
echo "  SELECT NULL, 'xxx', 'v1', 'SQL-SUBQUERY', ..."
echo "  FROM DUAL"
echo "  WHERE NOT EXISTS ("
echo "      SELECT 1 FROM s2t_mapping WHERE map_name='xxx' AND ..."
echo "  );"
echo ""

# 打开文件 (如果系统支持)
if command -v code &> /dev/null; then
    code "$FILENAME"
elif command -v vim &> /dev/null; then
    vim "$FILENAME"
fi
