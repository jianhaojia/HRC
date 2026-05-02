-- liquibase formatted sql
-- ============================================================================
-- 基线版本 v1.0.0
-- ============================================================================
--
-- 【重要概念 - 先读这个!】
--
-- Liquibase 执行逻辑:
--   1. 读取这个文件，找到所有 changeset
--   2. 检查 DATABASECHANGELOG 表 (Liquibase 自己建的)
--   3. 如果 changeset 的 ID+AUTHOR 已在表中 → 跳过
--   4. 如果不在 → 执行，并写入 DATABASECHANGELOG
--
-- 【你的场景】
--   - 表已经存在，里面有数据
--   - 这些数据是"代码"，不是业务数据
--   - 需要用 Liquibase 管理这些"代码"
--
-- 【Init 流程】
--   1. 第一次 liquibase:update → 执行 baseline，数据写入表
--   2. 打 tag v1.0.0 → 标记"已知状态"
--   3. 之后每次 deploy → 只部署新的 changeset
--
-- 【幂等策略】
--   - aaa_pre_pro_master: 有唯一键 uk_configuration_id → 用 INSERT IGNORE
--   - setting_day: 有唯一键 uk_setting_key → 用 INSERT IGNORE
--   - s2t_mapping: 没有唯一键! 同 (mapname, mapversion) 可有多条记录
--     → 用 NOT EXISTS 判断
--
-- ============================================================================

-- changeset baseline:001 author:infra context:uat,prd
-- comment: 创建核心业务表
-- rollback: NOT SUPPORTED (baseline 不允许 rollback)

-- -------------------------------------------------------
-- 表1: aaa_pre_pro_master (流程定义表)
-- 有唯一键: configuration_id
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS aaa_pre_pro_master (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    configuration_id VARCHAR(100) NOT NULL COMMENT '流程唯一标识ID',
    flow_name VARCHAR(200) NOT NULL COMMENT '流程名称',
    source_type VARCHAR(50) COMMENT '数据源类型: HIVE, MYSQL, POSTGRESQL, FILE',
    source_connection VARCHAR(500) COMMENT '数据源连接信息或HDFS路径',
    target_type VARCHAR(50) COMMENT '目标类型: MYSQL, HIVE, HDFS',
    target_connection VARCHAR(500) COMMENT '目标连接信息',
    etl_mapper_version VARCHAR(100) COMMENT 'ETLmapper版本,对应s2t_mapping的concat(mapName,mapversion)',
    schedule_cron VARCHAR(100) COMMENT '调度Cron表达式',
    param_config TEXT COMMENT 'JSON格式的参配置',
    country_code VARCHAR(10) COMMENT '国家代码: CN, US, SG等,用于关联假期表',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否启用',
    created_by VARCHAR(100) COMMENT '创建人',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_configuration_id (configuration_id),
    KEY idx_etl_mapper_version (etl_mapper_version),
    KEY idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETL流程定义主表';

-- -------------------------------------------------------
-- 表2: s2t_mapping (ETLmapper表)
-- 【重要】没有唯一键!
-- 同一个 (mapname, mapversion) 可以有多条记录 (多种ETL类型)
-- maptype 类型:
--   SQL-FILE     -> A->HDFS路径
--   SQL-SUBQUERY -> B->SELECT语句
--   SQL-DIRECT   -> 直接SQL
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS s2t_mapping (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    map_name VARCHAR(200) NOT NULL COMMENT 'Mapper名称',
    map_version VARCHAR(50) NOT NULL COMMENT 'Mapper版本',
    maptype VARCHAR(50) NOT NULL COMMENT '操作类型: SQL-FILE, SQL-SUBQUERY, SQL-DIRECT',
    view_name VARCHAR(100) COMMENT '生成的临时视图名称',
    map_detail TEXT COMMENT '详细逻辑',
    execution_order INT DEFAULT 1 COMMENT '执行顺序',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否启用',
    description VARCHAR(500) COMMENT '描述说明',
    created_by VARCHAR(100) COMMENT '创建人',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_map_name_version (map_name, map_version),
    KEY idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETL数据映射表';

-- -------------------------------------------------------
-- 表3: setting_day (配置表)
-- 有唯一键: setting_key
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS setting_day (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    setting_key VARCHAR(100) NOT NULL COMMENT '配置键',
    setting_value VARCHAR(500) NOT NULL COMMENT '配置值',
    setting_type VARCHAR(50) DEFAULT 'GENERAL' COMMENT '配置类型: HOLIDAY, PARAM, RULE',
    country_code VARCHAR(10) COMMENT '国家代码',
    effective_date DATE COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否生效',
    description VARCHAR(255) COMMENT '描述',
    created_by VARCHAR(100) COMMENT '创建人',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_setting_key (setting_key),
    KEY idx_country_code (country_code),
    KEY idx_effective_date (effective_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='配置表';


-- changeset baseline:002 author:infra context:uat,prd
-- comment: 插入现有业务数据 (从生产库导出)
-- rollback: NOT SUPPORTED (baseline 不允许 rollback)

-- ============================================================================
-- aaa_pre_pro_master 现有数据
-- 【有唯一键 uk_configuration_id，用 INSERT IGNORE】
-- ============================================================================
INSERT IGNORE INTO aaa_pre_pro_master
    (id, configuration_id, flow_name, source_type, source_connection, target_type, target_connection, etl_mapper_version, schedule_cron, param_config, country_code, is_active, created_by)
VALUES
    (NULL, 'user_data_sync_v1', '用户数据同步', 'HIVE', '/data/hive/warehouse/user_db/tbl_users', 'MYSQL', 'jdbc:mysql://mysql:3306/user_db', 'user_etl_v1', '0 0 2 * * ?', '{"batchSize":10000}', 'CN', TRUE, 'infra'),
    (NULL, 'order_sync_v1', '订单数据同步', 'MYSQL', 'jdbc:mysql://mysql:3306/order_db', 'HIVE', '/data/hive/warehouse/order_db', 'order_etl_v1', '0 0 3 * * ?', '{"batchSize":50000}', 'CN', TRUE, 'infra'),
    (NULL, 'product_sync_v1', '商品数据同步', 'HIVE', '/data/hive/warehouse/product_db', 'MYSQL', 'jdbc:mysql://mysql:3306/product_db', 'product_etl_v1', '0 0 4 * * ?', '{}', 'CN', TRUE, 'infra');

-- ============================================================================
-- s2t_mapping 现有数据
-- 【没有唯一键! 用 NOT EXISTS 判断】
--
-- 重要: 同 (mapname, mapversion) 可有多条记录
--       用 (map_name, map_version, maptype, view_name, execution_order) 判断
-- ============================================================================

-- 用户 ETL - 步骤1: 读取文件
INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
SELECT NULL, 'user_etl', 'v1', 'SQL-FILE', 'user_view', 'A->/data/hive/warehouse/user_db/tbl_users/dt=${current_date}', 1, TRUE, '读取用户数据', 'infra'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping
    WHERE map_name = 'user_etl'
    AND map_version = 'v1'
    AND maptype = 'SQL-FILE'
    AND view_name = 'user_view'
    AND execution_order = 1
);

-- 用户 ETL - 步骤2: 过滤有效用户
INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
SELECT NULL, 'user_etl', 'v1', 'SQL-SUBQUERY', 'active_user_view', 'B->SELECT id, name, age FROM user_view WHERE status=1', 2, TRUE, '过滤有效用户', 'infra'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping
    WHERE map_name = 'user_etl'
    AND map_version = 'v1'
    AND maptype = 'SQL-SUBQUERY'
    AND view_name = 'active_user_view'
    AND execution_order = 2
);

-- 订单 ETL - 步骤1: 直接查询
INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
SELECT NULL, 'order_etl', 'v1', 'SQL-DIRECT', 'order_view', 'C->SELECT o.order_id, o.user_id, o.amount FROM order_db.order_main o', 1, TRUE, '读取订单数据', 'infra'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping
    WHERE map_name = 'order_etl'
    AND map_version = 'v1'
    AND maptype = 'SQL-DIRECT'
    AND view_name = 'order_view'
    AND execution_order = 1
);

-- 商品 ETL - 步骤1: 读取文件
INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, map_detail, execution_order, is_active, description, created_by)
SELECT NULL, 'product_etl', 'v1', 'SQL-FILE', 'product_view', 'P->/data/hive/warehouse/product_db/tbl_products', 1, TRUE, '读取商品数据', 'infra'
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping
    WHERE map_name = 'product_etl'
    AND map_version = 'v1'
    AND maptype = 'SQL-FILE'
    AND view_name = 'product_view'
    AND execution_order = 1
);

-- ============================================================================
-- setting_day 现有数据
-- 【有唯一键 uk_setting_key，用 INSERT IGNORE】
-- ============================================================================
INSERT IGNORE INTO setting_day
    (id, setting_key, setting_value, setting_type, country_code, effective_date, is_active, description, created_by)
VALUES
    (NULL, 'HOLIDAY_CN_2026', '2026-01-01:元旦,2026-02-10:春节,2026-04-04:清明,2026-05-01:劳动节', 'HOLIDAY', 'CN', '2026-01-01', TRUE, '2026年中国假期', 'infra'),
    (NULL, 'HOLIDAY_SG_2026', '2026-01-01:元旦,2026-08-09:国庆', 'HOLIDAY', 'SG', '2026-01-01', TRUE, '2026年新加坡假期', 'infra'),
    (NULL, 'DEFAULT_BATCH_SIZE', '10000', 'PARAM', NULL, '2026-01-01', TRUE, '默认批次大小', 'infra');


-- ============================================================================
-- 【重要】未来新增 changeset 的写法
-- ============================================================================
--
-- 对于有唯一键的表 (aaa_pre_pro_master, setting_day):
--   INSERT IGNORE INTO ...
--
-- 对于 s2t_mapping (没有唯一键):
--   必须用 NOT EXISTS 判断:
--
--   INSERT INTO s2t_mapping (id, map_name, map_version, maptype, view_name, ...)
--   SELECT NULL, '新mapper', 'v1', 'SQL-FILE', 'new_view', ...
--   FROM DUAL
--   WHERE NOT EXISTS (
--       SELECT 1 FROM s2t_mapping
--       WHERE map_name = '新mapper'
--       AND map_version = 'v1'
--       AND maptype = 'SQL-FILE'
--       AND view_name = 'new_view'
--   );
--
-- 【警告】如果忘记用 NOT EXISTS，第二次部署会插入重复数据!
-- ============================================================================
