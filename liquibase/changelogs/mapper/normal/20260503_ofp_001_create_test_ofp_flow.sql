-- liquibase formatted sql
-- ============================================================================
-- Normal Changeset - Create test_ofp_flow Table
-- ============================================================================
--
-- [RELEASE Scenario]
--   This file is included via master.xml during normal deployment
--
-- [Changeset ID Format]
--   {team}:{number} where team = ofp (OFP team)
--   Example: ofp:001, ofp:002
--
-- [Rollback File]
--   Rollback for this changeset is in:
--   changelogs/mapper/rollback/ofp_001--rollback.sql
--
-- ============================================================================

-- changeset ofp:001 author:infra context:uat,prd
-- comment: Create test_ofp_flow table for OFP testing
-- rollback: DROP TABLE IF EXISTS test_ofp_flow;

CREATE TABLE IF NOT EXISTS test_ofp_flow (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary key ID',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation time',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update time'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Test OFP Flow table';


-- changeset ofp:002 author:infra context:uat,prd
-- comment: Insert initial test record
-- rollback: DELETE FROM test_ofp_flow WHERE id = 1;

INSERT IGNORE INTO test_ofp_flow (id) VALUES (NULL);
