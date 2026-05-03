-- liquibase formatted sql
-- ============================================================================
-- ETLmapper 表 changeset
-- 对应表: s2t_mapping
-- 更新频率: ★高 (最活跃)
-- ============================================================================

-- changeset map:001 author:team_a context:uat,prd
-- comment: 新增用户ETL任务 - 从Hive读取用户数据
-- rollback: DELETE FROM s2t_mapping WHERE map_name='user_etl' AND map_version='v1';

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
    'user_etl',
    'v1',
    'SQL-FILE',
    'user_view',
    'A->/data/hive/warehouse/user_db/tbl_users/dt=${current_date}',
    1,
    TRUE,
    '从Hive读取用户数据创建临时视图',
    'team_a'
);

-- changeset map:002 author:team_c context:uat,prd
-- comment: 新增SQL子查询操作 - 过滤有效用户
-- rollback: DELETE FROM s2t_mapping WHERE map_name='user_etl' AND map_version='v1' AND view_name='active_user_view';

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
    'user_etl',
    'v1',
    'SQL-SUBQUERY',
    'active_user_view',
    'B->SELECT id, name, age, status FROM user_view WHERE status = 1 AND age >= 18',
    2,
    TRUE,
    '过滤有效用户(状态正常且成年)',
    'team_c'
);

-- changeset map:003 author:team_a context:uat
-- comment: 修改mapper逻辑 - 添加地区过滤
-- rollback: UPDATE s2t_mapping SET map_detail='B->SELECT id, name, age, status FROM user_view WHERE status = 1' WHERE map_name='user_etl' AND map_version='v1';

UPDATE s2t_mapping
SET map_detail = 'B->SELECT id, name, age, status, region FROM user_view WHERE status = 1 AND age >= 18 AND region IN (''CN'', ''HK'', ''TW'')'
WHERE map_name = 'user_etl' AND map_version = 'v1' AND maptype = 'SQL-SUBQUERY';
