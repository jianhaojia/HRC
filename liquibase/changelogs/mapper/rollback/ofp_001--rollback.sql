-- liquibase formatted sql
-- ============================================================================
-- Rollback Changeset for ofp:001
-- ============================================================================
--
-- [Usage]
--   mvn liquibase:update \
--     -DchangeLogFile=liquibase/changelogs/mapper/rollback/ofp_001--rollback.sql \
--     -Dliquibase.url=jdbc:mysql://${DB_HOST}:3306/${DB_NAME} \
--     -Dliquibase.username=liquibase \
--     -Dliquibase.password=${DB_PASSWORD}
--
-- [Trigger]
--   GitHub Actions workflow_dispatch (deploy_type = rollback)
--   Required input: rollback_file = "ofp_001--rollback.sql"
--
-- [Execution Behavior]
--   • Liquibase reads this rollback file directly
--   • Checks DATABASECHANGELOG for previously executed changesets
--   • Compares checksums to verify changeset integrity
--   • Executes only if: changeset ID not already marked as executed
--   • Idempotent: if rollback already executed → Skipped
--
-- ============================================================================

-- changeset ofp:001:rb author:infra context:uat,prd
-- comment: Rollback ofp:001 - drop test_ofp_flow table
-- rollback: Recreate table (Liquibase auto-records)

-- Delete initial test record first
DELETE FROM test_ofp_flow WHERE id = 1;

-- Then drop the table
DROP TABLE IF EXISTS test_ofp_flow;
