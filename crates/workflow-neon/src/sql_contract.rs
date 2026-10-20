//! SQL the Neon adapter issues. Same tables, same lock order as the TS kernel.
//! Review this file when the TypeScript engine's `db.ts` changes.

pub const LOCK_INSTANCE: &str = "SELECT ... FROM process_instances WHERE id = $1::uuid FOR UPDATE";

pub const CLAIM_DUE_JOBS: &str = "\
UPDATE jobs SET status = 'locked', locked_by = $1, locked_until = ..., attempts = attempts + 1
 WHERE id IN (
   SELECT id FROM jobs
    WHERE status = 'pending' AND due_at <= now AND attempts < max_attempts
    ORDER BY due_at ASC LIMIT $n
    FOR UPDATE SKIP LOCKED
 )";

pub const ACTIVE_SUBJECT: &str = "\
SELECT ... FROM process_instances
 WHERE definition_id = $1::uuid
   AND subject_type = $2
   AND subject_id = $3
   AND status = 'active'
 LIMIT 1";

pub const INSERT_COMMAND: &str = "\
INSERT INTO process_commands (
  process_instance_id, token_id, node_id, visit_sequence, command_id,
  command_type, subject_type, subject_id, correlation_id, causation_id,
  input, outcome, message
) VALUES (...)";

/// Workflow-owned copy of the trace-event insert. The app's `db` crate no longer owns this SQL for
/// the workflow adapter; the table and its partial unique index are unchanged (one shared schema).
pub const TRACE_EVENT_INSERT_SQL: &str = "\
    insert into workflow_execution_trace_event (
        event_type, system, occurred_at, outcome, summary,
        source_system, source_event_id,
        workflow_instance_id, workflow_node_id, task_id, timer_job_id, correlation_id
     ) values (
        $1, $2, now(), 'ok', $3,
        $4, $5,
        $6, $7, $8, $9, $10
     ) on conflict (source_system, source_event_id) where source_event_id is not null do nothing";
