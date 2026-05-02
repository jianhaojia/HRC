-- liquibase formatted sql
-- ============================================================================
-- 配置表 changeset
-- 对应表: setting_day
-- 更新频率: 低
-- ============================================================================

-- changeset cfg:001 author:infra context:uat,prd
-- comment: 初始化中国假期配置
-- rollback: DELETE FROM setting_day WHERE setting_key LIKE 'HOLIDAY_CN%';

INSERT INTO setting_day (
    setting_key,
    setting_value,
    setting_type,
    country_code,
    effective_date,
    is_active,
    description,
    created_by
) VALUES
    ('HOLIDAY_CN_2026', '2026-01-01:元旦,2026-02-10:春节,2026-04-04:清明,2026-05-01:劳动节,2026-06-01:端午节,2026-10-01:国庆', 'HOLIDAY', 'CN', '2026-01-01', TRUE, '2026年中国法定节假日', 'infra'),
    ('HOLIDAY_CN_WORKER', '2026-02-07:春节前调休,2026-02-08:春节前调休', 'HOLIDAY', 'CN', '2026-01-01', TRUE, '2026年调休工作日', 'infra');

-- changeset cfg:002 author:infra context:uat,prd
-- comment: 初始化新加坡假期配置
-- rollback: DELETE FROM setting_day WHERE setting_key LIKE 'HOLIDAY_SG%';

INSERT INTO setting_day (
    setting_key,
    setting_value,
    setting_type,
    country_code,
    effective_date,
    is_active,
    description,
    created_by
) VALUES
    ('HOLIDAY_SG_2026', '2026-01-01:元旦,2026-01-29:春节,2026-03-20:复活节,2026-05-01:劳动节,2026-08-09:国庆,2026-12-25:圣诞节', 'HOLIDAY', 'SG', '2026-01-01', TRUE, '2026年新加坡法定节假日', 'infra');
