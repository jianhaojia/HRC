-- liquibase formatted sql
-- ============================================================================
-- ETLmapper 表 changeset
-- ============================================================================

-- changeset map:004 author:team_c context:uat,prd
-- comment: 新增订单ETL - 直接SQL查询
-- rollback: DELETE FROM s2t_mapping WHERE map_name='order_etl' AND map_version='v1';

INSERT INTO s2t_mapping (
    map_name,
    map_version,
    maptype,
    view_name,
    map_detail,
    execution_order,
    is_active,
    description,
    created_by
) VALUES (
    'order_etl',
    'v1',
    'SQL-DIRECT',
    'order_detail_view',
    'C->SELECT o.order_id, o.user_id, o.amount, o.create_time, u.name as user_name
       FROM order_db.order_main o
       LEFT JOIN order_db.user_info u ON o.user_id = u.user_id
       WHERE o.create_time >= ''${start_date}'' AND o.create_time < ''${end_date}''',
    1,
    TRUE,
    '订单明细关联查询',
    'team_c'
);

-- changeset map:005 author:team_b context:uat
-- comment: 废弃旧版mapper,标记为非活跃
-- rollback: UPDATE s2t_mapping SET is_active=TRUE WHERE map_name='order_report' AND map_version='deprecated_v1';

UPDATE s2t_mapping
SET is_active = FALSE,
    description = CONCAT(description, ' [已废弃于2026-05-01]')
WHERE map_name = 'order_report' AND map_version = 'deprecated_v1';
