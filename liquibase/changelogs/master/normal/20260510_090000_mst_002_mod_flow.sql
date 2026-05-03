-- liquibase formatted sql
-- ============================================================================
-- 核心流程定义表 changeset
-- ============================================================================

-- changeset mst:003 author:team_b context:uat,prd
-- comment: 新增 ETL 流程 - 订单数据同步
-- rollback: DELETE FROM aaa_pre_pro_master WHERE configuration_id='order_sync_v1';

INSERT INTO aaa_pre_pro_master (
    configuration_id,
    flow_name,
    source_type,
    source_connection,
    target_type,
    target_connection,
    etl_mapper_version,
    schedule_cron,
    param_config,
    country_code,
    is_active,
    created_by
) VALUES (
    'order_sync_v1',
    '订单数据同步',
    'MYSQL',
    'jdbc:mysql://mysql-replica:3306/order_db',
    'HIVE',
    '/data/hive/warehouse/order_db/tbl_orders',
    'order_etl_v1',
    '0 0 3 * * ?',  -- 每天凌晨3点
    '{"batchSize": 50000, "parallelism": 8}',
    'CN',
    TRUE,
    'team_b'
);

-- changeset mst:004 author:team_c context:uat
-- comment: 禁用老旧流程
-- rollback: UPDATE aaa_pre_pro_master SET is_active=TRUE WHERE configuration_id='legacy_report_v2';

UPDATE aaa_pre_pro_master
SET is_active = FALSE,
    updated_at = CURRENT_TIMESTAMP
WHERE configuration_id = 'legacy_report_v2';
