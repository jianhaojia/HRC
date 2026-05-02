# Rollback Version Management

## Directory Structure

```
master branch:
├── liquibase/
│   ├── master.xml                   ← Does NOT include rollback files
│   └── changelogs/
│       └── mapper/
│           ├── normal/             ← Normal changesets
│           ├── rollback/           ← Executed/pending rollback files
│           │   ├── 001--rollback.sql
│           │   └── 002--rollback.sql
│           └── archived/           ← Historical archives
└── ...
```

## Execution Method

Rollback is executed via CI parameter specifying the filename, **not via master.xml**:

```bash
# When triggering rollback, specify the filename
ROLLBACK_FILE=001--rollback.sql

# CI executes
mvn liquibase:update \
  -Dliquibase.changeLogFile=liquibase/changelogs/mapper/rollback/$ROLLBACK_FILE
```

## GitHub Actions Trigger

| Parameter | Description | Example |
|------|------|------|
| `deploy_type` | Select rollback scenario | rollback |
| `rollback_file` | Rollback filename | 001--rollback.sql |

## Rollback Flow

```
1. Developer creates xxx--rollback.sql in rollback/ directory
2. Trigger GitHub Actions workflow_dispatch
3. Select deploy_type = "rollback"
4. Fill rollback_file = "xxx--rollback.sql"
5. CI executes the specified rollback file
6. DATABASECHANGELOG records the changeset ID
```

## Key Design

| Problem | Solution |
|------|---------|
| Multiple developers modifying master.xml causes conflicts | master.xml does not include any rollback |
| Rollback files being executed repeatedly | DATABASECHANGELOG already recorded, skip |
| CI knows which rollback to execute | Via workflow_dispatch parameter |

## Rollback File Naming

```
Original changeset:  001.sql      → changeset ID: map:001
Rollback:            001--rollback.sql → changeset ID: map:001:rb
                              ↑ Different!
```

## Rollback File Content

```sql
-- changeset: map:001:rb author:team_a context:uat,prd
-- comment: Rollback map:001

DELETE FROM s2t_mapping
WHERE map_name = 'user_etl'
  AND map_version = 'v1';
```

## Deprecated Design

~~rollback-version branch~~ - No longer needed, rollback files are managed directly on master branch
