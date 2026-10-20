# Workflow-owned SQL

The workflow engine owns seven tables: `process_definitions`, `process_instances`, `tokens`,
`process_commands`, `process_events` (+ partitions), `tasks`, and `jobs`.

`manual/workflow_schema_baseline.sql` is the consolidated schema for all seven, generated from the
live DEV schema (PostgreSQL 18.6) and validated on a throwaway PG18 database. It is **inactive**:

- it is not part of the app's applied `db/migrations` chain or `schema_migration` ledger;
- it must not be run against DEV/PROD;
- it exists so a future fresh-database replay (W-3 migration ownership) has the complete workflow
  schema in one place, including `jobs`, which historically had no creating migration.

When migration ownership moves, this becomes the workflow project's ordered migration consumed by
the one applier.
