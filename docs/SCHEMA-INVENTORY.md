# Workflow schema inventory

Read-only audit 2026-10-19 against live DEV and PROD (PostgreSQL 18.6; both identical) plus the
workflow crate's own SQL. **No SQL is applied, stored as a migration, or owned here as an active
chain.** This file records what exists so a future fresh-DB replay can be generated deliberately.

## Engine-owned tables (7)

Used directly by `workflow-core`/`workflow-neon`:

| Table | Purpose |
| --- | --- |
| `process_definitions` | versioned workflow definitions |
| `process_instances` | running workflow instances |
| `tokens` | token positions / forks |
| `process_commands` | command intents and results |
| `process_events` | partitioned event log (see partitions) |
| `tasks` | workflow tasks |
| `jobs` | timer/due jobs (historically had no creating migration) |

Partition children of `process_events`: `process_events_2026_08`, `process_events_2026_09`,
`process_events_2026_10`, `process_events_default`. They exist on DEV and PROD.

## Workflow-adjacent tables (3)

These share the workflow prefix but are written by the app/RE host, not the engine kernel:

| Table | Writer | Notes |
| --- | --- | --- |
| `workflow_command_receipt` | app `db::CommandReceiptDao` / re-host receipt | idempotency receipts for app commands |
| `workflow_task_correlation` | app `db::WorkflowOpsDao` / support | task correlation for app integration |
| `workflow_execution_trace_event` | engine trace insert + app flight recorder | shared trace log |

They are **not** part of the engine core migration baseline; ownership is a W-3 decision.

## Explicitly not workflow

- `task` — app business table (41 app references), distinct from engine `tasks`.
- `forge_engine_task_execution`, `forge_workflow_evidence` — Forge factory tables.
- `google_calendar_token_store` — app integration.

## Ledger status

No row in `schema_migration` records `jobs` or a workflow baseline. The engine core tables are
created by the app's `000_forge_base_schema.sql` and follow-on migrations; `jobs` is external. If a
fresh-DB replay is ever needed, generate the DDL from live at that time and apply it through the one
ledger/applier (W-3); do not run anything against DEV/PROD.
