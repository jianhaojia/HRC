-- liquibase formatted sql
-- ============================================================================
-- 核心流程定义表 changeset
-- 对应表: aaa_pre_pro_master
-- 更新频率: 中
-- ============================================================================

-- changeset mst:001 author:team_a context:uat,prd
-- comment: 新增 ETL 流程 - 用户数据同步
-- rollback: DELETE FROM aaa_pre_pro_master WHERE configuration_id='user_data_sync_v1';

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
    'user_data_sync_v1',
    '用户数据同步',
    'HIVE',
    '/data/hive/warehouse/user_db/tbl_users',
    'MYSQL',
    'jdbc:mysql://mysql-primary:3306/user_db',
    'user_etl_v1',
    '0 0 2 * * ?',  -- 每天凌晨2点
    '{"batchSize": 10000, "parallelism": 4}',
    'CN',
    TRUE,
    'team_a'
);

-- changeset mst:002 author:team_a context:uat
-- comment: 修改流程参数 - 增加重试次数
-- rollback: UPDATE aaa_pre_pro_master SET param_config='{"batchSize": 10000}' WHERE configuration_id='user_data_sync_v1';

UPDATE aaa_pre_pro_master
SET param_config = '{"batchSize": 10000, "parallelism": 4, "maxRetry": 3}',
    updated_at = CURRENT_TIMESTAMP
WHERE configuration_id = 'user_data_sync_v1';
