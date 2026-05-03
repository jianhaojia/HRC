-- liquibase formatted sql
-- ============================================================================
-- Normal Changeset - Add flow_name column to test_ofp_flow
-- ============================================================================
--
-- [RELEASE Scenario]
--   This file is included via master.xml during normal deployment
--
-- [Changeset ID Format]
--   {team}:{number} where team = ofp (OFP team)
--   Example: ofp:003 (continuing from ofp:001, ofp:002)
--
-- ============================================================================

-- changeset ofp:003 author:infra context:uat,prd
-- comment: Add flow_name column to test_ofp_flow
-- rollback: ALTER TABLE test_ofp_flow DROP COLUMN flow_name;

ALTER TABLE test_ofp_flow
ADD COLUMN flow_name VARCHAR(200) COMMENT 'Flow name' AFTER id;


-- changeset ofp:004 author:infra context:uat,prd
-- comment: Update existing record with flow_name
-- rollback: UPDATE test_ofp_flow SET flow_name = NULL WHERE flow_name = 'initial_flow';

UPDATE test_ofp_flow
SET flow_name = 'initial_flow'
WHERE flow_name IS NULL AND id = (SELECT id FROM (SELECT id FROM test_ofp_flow LIMIT 1) AS t);
