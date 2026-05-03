-- liquibase formatted sql
-- ============================================================================
-- Rollback Changeset for ofp:003 and ofp:004
-- ============================================================================
--
-- [Usage]
--   mvn liquibase:update \
--     -DchangeLogFile=liquibase/changelogs/mapper/rollback/ofp_003--rollback.sql \
--     -Dliquibase.url=jdbc:mysql://${DB_HOST}:3306/${DB_NAME} \
--     -Dliquibase.username=liquibase \
--     -Dliquibase.password=${DB_PASSWORD}
--
-- [Trigger]
--   GitHub Actions workflow_dispatch (deploy_type = rollback)
--   Required input: rollback_file = "ofp_003--rollback.sql"
--
-- ============================================================================

-- changeset ofp:003:rb author:infra context:uat,prd
-- comment: Rollback ofp:004 - clear flow_name
-- rollback: None needed (data change)

UPDATE test_ofp_flow
SET flow_name = NULL
WHERE flow_name = 'initial_flow';

-- changeset ofp:004:rb author:infra context:uat,prd
-- comment: Rollback ofp:003 - drop flow_name column
-- rollback: None needed (handled by ALTER)

ALTER TABLE test_ofp_flow DROP COLUMN flow_name;
