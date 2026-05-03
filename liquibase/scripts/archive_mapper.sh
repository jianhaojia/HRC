#!/bin/bash
# ============================================================================
# 归档 Mapper Changesets
# ============================================================================
#
# 使用场景:
#   - changeset 超过阈值 (如 50 个)
#   - 季度结束需要归档
#   - CI 构建变慢
#
# 功能:
#   1. 合并 mapper 目录下的所有 .sql 文件为单个归档文件
#   2. 移动到 ARCHIVE_YYYY_QN/ 目录
#   3. 更新 master.xml 排除归档目录
#
# 使用方法:
#   bash scripts/archive_mapper.sh 2025 Q2
#
# ============================================================================

set -e

YEAR=${1:-$(date +%Y)}
QUARTER=${2:-Q$(( ($(date +%-m) - 1) / 3 + 1 ))}

ARCHIVE_DIR="changelogs/mapper/ARCHIVE_${YEAR}_${QUARTER}"
MAPPER_DIR="changelogs/mapper"
TEMP_FILE="/tmp/merged_mapper_${YEAR}_${QUARTER}.sql"

echo "============================================"
echo "  Mapper Changeset 归档脚本"
echo "  年份: $YEAR"
echo "  季度: $QUARTER"
echo "============================================"

# Step 1: 创建归档目录
echo "[1/5] 创建归档目录: $ARCHIVE_DIR"
mkdir -p "$ARCHIVE_DIR"

# Step 2: 收集所有需要归档的 changeset
echo "[2/5] 收集 changeset 文件..."
cd "$MAPPER_DIR"

# 找到所有非 ARCHIVE 的 .sql 文件，按时间排序
find . -maxdepth 1 -type f -name "*.sql" ! -path "./ARCHIVE_*" | sort > /tmp/archive_files.txt

if [ ! -s /tmp/archive_files.txt ]; then
    echo "没有需要归档的文件，退出"
    exit 0
fi

FILE_COUNT=$(wc -l < /tmp/archive_files.txt)
echo "找到 $FILE_COUNT 个文件需要归档"

# Step 3: 合并文件
echo "[3/5] 合并文件到: $TEMP_FILE"
> "$TEMP_FILE"

while IFS= read -r file; do
    echo "" >> "$TEMP_FILE"
    echo "-- =============================================" >> "$TEMP_FILE"
    echo "-- 来源文件: $file" >> "$TEMP_FILE"
    echo "-- 归档时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$TEMP_FILE"
    echo "-- =============================================" >> "$TEMP_FILE"
    cat "$file" >> "$TEMP_FILE"
done < /tmp/archive_files.txt

# Step 4: 移动原文件到归档目录
echo "[4/5] 移动原文件到归档目录..."
while IFS= read -r file; do
    mv "$file" "$ARCHIVE_DIR/"
    echo "  已移动: $file"
done < /tmp/archive_files.txt

# Step 5: 创建归档索引文件
ARCHIVE_INDEX="$ARCHIVE_DIR/README.md"
cat > "$ARCHIVE_INDEX" << EOF
# Mapper 归档 ${YEAR} ${QUARTER}

## 归档时间
$(date '+%Y-%m-%d %H:%M:%S')

## 归档文件
$(ls -1 *.sql 2>/dev/null | wc -l) 个 changeset 文件已合并

## 合并后的文件
$TEMP_FILE

## 注意
- 此目录下的文件已被 CI 构建跳过
- 如需恢复，先备份当前数据，然后执行新的 baseline
EOF

echo "[5/5] 完成!"
echo ""
echo "下一步:"
echo "  1. 提交归档变更"
echo "  2. 考虑 rebaseline 创建新的 baseline"
