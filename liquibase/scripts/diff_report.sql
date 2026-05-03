-- ============================================================================
-- Diff 报告查询
-- 用于生成变更 diff 报告
-- ============================================================================

-- changeset script:002 author:infra

-- 查询最近 N 天的变更
SELECT
    h.deployment_id,
    h.change_rn,
    h.complete_dt,
    h.liquibase_command,
    h.installed_by,
    h.execution_time,
    CASE WHEN h.undo_executed THEN 'Yes' ELSE 'No' END as rolled_back
FROM liquibase_change_history h
WHERE h.complete_dt >= DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY h.complete_dt DESC;

-- 按 author 统计变更数量
SELECT
    installed_by as author,
    COUNT(*) as total_changes,
    SUM(execution_time) as total_ms
FROM liquibase_change_history
WHERE complete_dt >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY installed_by
ORDER BY total_changes DESC;
