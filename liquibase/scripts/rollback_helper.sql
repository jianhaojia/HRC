-- ============================================================================
-- Rollback 辅助存储过程
-- 用于向前 Rollback (基于 tag 的 forward-only 模式)
-- ============================================================================

-- changeset script:001 author:infra

DELIMITER //

-- 获取指定 tag 之后的变更记录
CREATE PROCEDURE sp_get_changes_after_tag(
    IN p_tag VARCHAR(100),
    OUT p_count INT
)
BEGIN
    SELECT COUNT(*) INTO p_count
    FROM liquibase_change_history
    WHERE installed_rank > (
        SELECT COALESCE(MAX(installed_rank), 0)
        FROM liquibase_change_history
        WHERE description = CONCAT('Tag: ', p_tag)
    )
    AND Liquibase_command = 'UPDATE';
END //

-- 生成指定 tag 的 rollback 脚本
CREATE PROCEDURE sp_generate_rollback_script(
    IN p_tag VARCHAR(100)
)
BEGIN
    SELECT
        change_rn AS changeset_id,
        complete_dt AS executed_at,
        CASE
            WHEN undo_executed = FALSE THEN 'NOT_EXECUTED'
            ELSE 'EXECUTED'
        END AS status
    FROM liquibase_change_history
    WHERE installed_rank > (
        SELECT COALESCE(MAX(installed_rank), 0)
        FROM liquibase_change_history
        WHERE description = CONCAT('Tag: ', p_tag)
    )
    ORDER BY installed_rank;
END //

-- 标记回滚点
CREATE PROCEDURE sp_mark_rollback_point(
    IN p_tag VARCHAR(100),
    IN p_deployment_id VARCHAR(100)
)
BEGIN
    INSERT INTO liquibase_change_history (
        deployment_id, change_rn, complete_dt,
        Liquibase_command, Liquibase_command_parameters,
        deployment_brief, installed_by, installed_on, installed_rank
    ) VALUES (
        p_deployment_id,
        CONCAT('sp_', p_tag, '_', DATE_FORMAT(NOW(), '%Y%m%d%H%i%s')),
        NOW(),
        'TAG',
        p_tag,
        CONCAT('Rollback point marked: ', p_tag),
        CURRENT_USER(),
        NOW(),
        (SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM liquibase_change_history)
    );
END //

DELIMITER ;

-- rollback: DROP PROCEDURE IF EXISTS sp_get_changes_after_tag; DROP PROCEDURE IF EXISTS sp_generate_rollback_script; DROP PROCEDURE IF EXISTS sp_mark_rollback_point;
