-- ============================================================================
-- Validate Changeset 脚本
-- 用于 CI 阶段校验 changeset 语法
-- ============================================================================

-- changeset script:003 author:infra

-- 检查是否有重复的 changeset id (在同一文件内)
-- 此查询需要在 CI 阶段通过 liquibase validate 或自定义脚本执行

-- 示例: 检查未完成的 rollback 标记
SELECT
    change_rn,
    complete_dt,
    installed_by,
    CASE
        WHEN undo_executed = FALSE AND liquibase_command = 'UPDATE'
        THEN 'WARNING: May need rollback'
        ELSE 'OK'
    END as validation_status
FROM liquibase_change_history
WHERE complete_dt >= DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY complete_dt DESC;
