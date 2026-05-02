-- liquibase formatted sql
-- ============================================================================
-- Q1 2026 归档的 Mapper Changesets (已合并)
-- ============================================================================

-- changeset archived:2026Q1:001 author:team_a context:uat,prd
-- comment: [归档] 2026年Q1 mapper合并变更

INSERT INTO s2t_mapping (map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
VALUES
    ('log_analysis', 'v1', 'SQL-FILE', 'log_view', 'L->/data/hive/warehouse/log_db/user_actions/dt=${current_date}', 1, TRUE, '[归档]日志分析ETL', 'team_a'),
    ('metrics_calc', 'v1', 'SQL-SUBQUERY', 'metrics_view', 'M->SELECT date, SUM(revenue) as total_revenue FROM daily_metrics GROUP BY date', 1, TRUE, '[归档]指标计算', 'team_b');
