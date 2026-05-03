-- liquibase formatted sql
-- ============================================================================
-- Rollback Changeset
-- ============================================================================
--
-- 用法:
--   1. 切换到 rollback-version branch
--   2. 执行: mvn liquibase:update -Dliquibase.changeLogFile=changelogs/mapper/rollback/001--rollback.sql
--   3. 合并到 master:rollback/
--
-- 注意: changeset ID 和原 changeset 不同!
--   原: map:001
--   回: map:001:rb
--
-- ============================================================================

-- changeset: map:001:rb author:team_a context:uat,prd
-- comment: 回滚 map:001 的变更
-- rollback: 重新执行原 INSERT (Liquibase 会自动记录)

-- 删除 s2t_mapping 中 map:001 插入的数据
-- WHERE 条件要和原 INSERT 完全对应

-- 用户 ETL - 步骤1
DELETE FROM s2t_mapping
WHERE map_name = 'user_etl'
  AND map_version = 'v1'
  AND maptype = 'SQL-FILE'
  AND view_name = 'user_view';

-- 用户 ETL - 步骤2
DELETE FROM s2t_mapping
WHERE map_name = 'user_etl'
  AND map_version = 'v1'
  AND maptype = 'SQL-SUBQUERY'
  AND view_name = 'active_user_view';
