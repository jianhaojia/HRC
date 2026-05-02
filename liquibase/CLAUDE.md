# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Liquibase-based CI/CD pipeline for MySQL database maintenance. ETL logic is stored in MySQL tables (`aaa_pre_pro_master`, `s2t_mapping`, `setting_day`).

## Architecture

- **master.xml** is the single source of truth - only included changesets are executed by Liquibase
- Rollback and archived changesets are NOT included in master.xml - they are independent
- **Four deployment scenarios**: INIT, RELEASE, ROLLBACK, REBASELINE

## Directory Structure

```
liquibase/
├── master.xml                          # Main entry point (static)
├── changelogs/
│   ├── baseline/v*.0.0_baseline.sql   # Baseline versions
│   ├── master/normal/                  # Master team changesets
│   ├── mapper/normal/                  # Mapper team changesets
│   ├── config/normal/                   # Config team changesets
│   ├── mapper/rollback/                # Rollback files (NOT in master)
│   └── mapper/archived/                # Archived (NOT in master, not deployed)
```

## Key Patterns

### s2t_mapping has no unique key - use NOT EXISTS pattern

```sql
-- CORRECT (NOT EXISTS)
INSERT INTO s2t_mapping (...)
SELECT ... FROM dual WHERE NOT EXISTS (
    SELECT 1 FROM s2t_mapping WHERE map_name = 'xxx' AND map_version = 'v1'
);

-- WRONG (INSERT IGNORE would cause duplicates)
INSERT IGNORE INTO s2t_mapping ...;
```

### Other tables use INSERT IGNORE

For `aaa_pre_pro_master` and `setting_day` which have proper unique keys.

## Deployment Commands

| Scenario | Command |
|----------|---------|
| INIT | `mvn liquibase:update -DchangeLogFile=liquibase/changelogs/baseline/v1.0.0_baseline.sql` |
| RELEASE | `mvn liquibase:update -Dliquibase.changeLogFile=liquibase/master.xml` |
| ROLLBACK | `mvn liquibase:update -DchangeLogFile=liquibase/changelogs/mapper/rollback/xxx--rollback.sql` |
| REBASELINE | `mvn liquibase:drop-all` then `mvn liquibase:update -DchangeLogFile=...v1.1.0_baseline.sql` |

## Liquibase Uniqueness

Changeset ID = `author:id` format. Rollback files use different IDs (e.g., `map:001:rb` vs `map:001`).

## Rollback Strategy

- Rollback files stay in separate branch, not merged to master
- Execute rollback via `rollback_manager.sh` script or directly with mvn command
- DATABASECHANGELOG auto-records all changes

## CI/CD Pipelines

- `.github/workflows/ci.yml` - Runs scan + deploy on push/PR
- `.github/workflows/deploy.yml` - Manual deploy for release/init/rebaseline
