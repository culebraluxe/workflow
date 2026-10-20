# Workflow-owned SQL

The workflow engine owns `process_instances`, `tokens`, `tasks`, and `jobs`. The live app still
applies migrations through one shared `schema_migration` ledger in Culebraluxe-web (`db-tool apply`);
this repo holds the SQL that belongs to the workflow tables.

`manual/jobs_baseline.sql` is an **inactive reference baseline**: the live `jobs` table exists on DEV
and PROD but no historical migration creates it. It is not part of any applied chain and must not be
run automatically. When migration ownership moves (W-3), this becomes a numbered migration consumed
by the one applier.
