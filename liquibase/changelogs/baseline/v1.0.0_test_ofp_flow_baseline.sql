-- liquibase formatted sql
-- ============================================================================
-- Baseline Version v1.0.0 - test_ofp_flow Table
-- ============================================================================
--
-- [Liquibase Execution Logic]
--   1. Read this file, find all changesets
--   2. Check DATABASECHANGELOG table (created by Liquibase)
--   3. If changeset ID+AUTHOR already exists → Skip
--   4. If not exists → Execute and record in DATABASECHANGELOG
--
-- [INIT Scenario]
--   1. First liquibase:update → Execute baseline, create table + insert data
--   2. Tag v1.0.0 → Mark known state
--   3. Future deploys → Only deploy new changesets
--
-- [Usage]
--   mvn liquibase:update \
--     -DchangeLogFile=liquibase/changelogs/baseline/v1.0.0_test_ofp_flow_baseline.sql \
--     -Dliquibase.url=jdbc:mysql://${DB_HOST}:3306/${DB_NAME} \
--     -Dliquibase.username=liquibase \
--     -Dliquibase.password=${DB_PASSWORD}
--
--   mvn liquibase:tag -Dliquibase.tag=v1.0.0
--
-- ============================================================================

-- changeset ofp:baseline:001 author:infra context:uat,prd
-- comment: Create test_ofp_flow table
-- rollback: DROP TABLE IF EXISTS test_ofp_flow;

CREATE TABLE IF NOT EXISTS test_ofp_flow (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary key ID',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation time',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update time',
    UNIQUE KEY uk_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Test OFP Flow table';

-- changeset ofp:baseline:002 author:infra context:uat,prd
-- comment: Insert initial test data
-- rollback: NOT SUPPORTED (baseline does not allow rollback)

INSERT IGNORE INTO test_ofp_flow (id) VALUES (NULL);


-- ============================================================================
-- [Future Changeset Examples]
-- ============================================================================
--
-- To add new changesets after baseline, follow these patterns:
--
-- For UPDATE/DELETE operations on this table:
--   Use proper WHERE conditions to ensure idempotency
--
-- Example - Add a new column:
--   changeset ofp:003 author:infra context:uat,prd
--   ALTER TABLE test_ofp_flow ADD COLUMN flow_name VARCHAR(100) COMMENT 'Flow name';
--   rollback: ALTER TABLE test_ofp_flow DROP COLUMN flow_name;
--
-- Example - Insert new data:
--   changeset ofp:004 author:infra context:uat,prd
--   INSERT IGNORE INTO test_ofp_flow (id) VALUES (NULL);
--   rollback: DELETE FROM test_ofp_flow WHERE id = (SELECT MAX(id) FROM test_ofp_flow);
--
-- ============================================================================
