#!/usr/bin/env python3
# ============================================================================
# Changeset 冲突检测
# ============================================================================
#
# 使用方法:
#   python scripts/check_conflicts.py
#
# 功能:
#   检查是否有重复的 (map_name, map_version, maptype, view_name)
#   来自不同的 author
#
# ============================================================================

import os
import re
import sys
from collections import defaultdict
from pathlib import Path

def extract_changesets(filepath):
    """从文件中提取 changeset 信息"""
    changesets = []

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 提取所有 changeset 块
    pattern = r'--\s*changeset:\s*(\S+)\s+author:(\S+)(.*?)(?=--\s*changeset:|--\s*rollback:|$)'
    matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

    for match in matches:
        changeset_id = match[0]
        author = match[1]
        body = match[2]

        # 检查是否涉及 s2t_mapping
        if 's2t_mapping' in body:
            # 提取 map_name, map_version, maptype, view_name
            map_pattern = r"map_name\s*=\s*'([^']+)'.*?map_version\s*=\s*'([^']+)'.*?maptype\s*=\s*'([^']+)'.*?view_name\s*=\s*'([^']+)'"
            map_matches = re.findall(map_pattern, body, re.IGNORECASE | re.DOTALL)

            for mm in map_matches:
                changesets.append({
                    'id': changeset_id,
                    'author': author,
                    'file': str(filepath),
                    'map_name': mm[0],
                    'map_version': mm[1],
                    'maptype': mm[2],
                    'view_name': mm[3]
                })

    return changesets

def check_conflicts(changesets):
    """检查是否有冲突"""
    # 按 (map_name, map_version, maptype, view_name) 分组
    groups = defaultdict(list)

    for cs in changesets:
        key = (cs['map_name'], cs['map_version'], cs['maptype'], cs['view_name'])
        groups[key].append(cs)

    conflicts = []
    for key, items in groups.items():
        if len(items) > 1:
            # 有多个 author 修改了同一个记录
            authors = set([item['author'] for item in items])
            if len(authors) > 1:
                conflicts.append({
                    'key': key,
                    'changesets': items
                })

    return conflicts

def main():
    changelogs_dir = Path('changelogs')

    if not changelogs_dir.exists():
        print("错误: changelogs 目录不存在")
        sys.exit(1)

    all_changesets = []

    # 遍历所有 .sql 文件
    for sql_file in changelogs_dir.rglob('*.sql'):
        # 跳过 ARCHIVE 目录
        if 'ARCHIVE' in str(sql_file):
            continue

        changesets = extract_changesets(sql_file)
        all_changesets.extend(changesets)

    print(f"找到 {len(all_changesets)} 个涉及 s2t_mapping 的 changeset")

    # 检查冲突
    conflicts = check_conflicts(all_changesets)

    if conflicts:
        print(f"\n发现 {len(conflicts)} 个潜在冲突:")
        print("=" * 60)

        for conflict in conflicts:
            key = conflict['key']
            items = conflict['changesets']

            print(f"\n冲突: {'/'.join(key)}")
            for item in items:
                print(f"  - 文件: {item['file']}")
                print(f"    changeset: {item['id']}")
                print(f"    author: {item['author']}")

        print("\n" + "=" * 60)
        print("建议: 让涉及的团队协商谁的处理是正确的")
        sys.exit(1)
    else:
        print("\n✓ 没有发现冲突")
        sys.exit(0)

if __name__ == '__main__':
    main()
