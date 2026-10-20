//! Moved from `neon.rs` (move only): run_exec, run_exec_n, GET_INSTANCE, LOCK_INSTANCE, GET_TOKEN, LOCK_TOKEN, GET_TASK, LOCK_TASK, GET_JOB, LOCK_JOB, one_instance, one_token, one_task, one_job, list_tokens, list_tasks, list_jobs, count_sql, s_get, s_opt, s_f64, s_opt_f64, s_i64, json_col, map_instance, map_token, map_task, map_job, map_definition.

#[allow(unused_imports)]
use super::*;

pub(super) fn run_exec<'q>(tx: &mut NeonTx<'_>, q: Query<'q, Postgres, PgArguments>) -> Result<()> {
    exec_q(tx, q).map(|_| ())
}

pub(super) fn run_exec_n<'q>(
    tx: &mut NeonTx<'_>,
    q: Query<'q, Postgres, PgArguments>,
) -> Result<u64> {
    exec_q(tx, q)
}

pub(super) const GET_INSTANCE: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, definition_id::text AS definition_id, business_key, status, outcome, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, started_by, parent_instance_id::text AS parent_instance_id, root_token_id::text AS root_token_id, subject_type, subject_id, variables::text AS variables, version FROM process_instances WHERE id = $1::uuid";
pub(super) const LOCK_INSTANCE: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, definition_id::text AS definition_id, business_key, status, outcome, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, started_by, parent_instance_id::text AS parent_instance_id, root_token_id::text AS root_token_id, subject_type, subject_id, variables::text AS variables, version FROM process_instances WHERE id = $1::uuid FOR UPDATE";
pub(super) const GET_TOKEN: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, parent_token_id::text AS parent_token_id, node_id, status, outcome, required, is_able_to_reactivate_parent, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, version FROM tokens WHERE id = $1::uuid";
pub(super) const LOCK_TOKEN: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, parent_token_id::text AS parent_token_id, node_id, status, outcome, required, is_able_to_reactivate_parent, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, version FROM tokens WHERE id = $1::uuid FOR UPDATE";
pub(super) const GET_TASK: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE id = $1::uuid";
pub(super) const LOCK_TASK: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE id = $1::uuid FOR UPDATE";
pub(super) const GET_JOB: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, type AS job_type, extract(epoch from due_at)*1000 AS due_at, status, locked_by, extract(epoch from locked_until)*1000 AS locked_until, attempts, max_attempts, payload::text AS payload, last_error, extract(epoch from created_at)*1000 AS created_at, extract(epoch from updated_at)*1000 AS updated_at, extract(epoch from completed_at)*1000 AS completed_at FROM jobs WHERE id = $1::uuid";
pub(super) const LOCK_JOB: &str = "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, type AS job_type, extract(epoch from due_at)*1000 AS due_at, status, locked_by, extract(epoch from locked_until)*1000 AS locked_until, attempts, max_attempts, payload::text AS payload, last_error, extract(epoch from created_at)*1000 AS created_at, extract(epoch from updated_at)*1000 AS updated_at, extract(epoch from completed_at)*1000 AS completed_at FROM jobs WHERE id = $1::uuid FOR UPDATE";

pub(super) fn one_instance(tx: &mut NeonTx<'_>, id: &str, lock: bool) -> Result<ProcessInstance> {
    let sql = if lock { LOCK_INSTANCE } else { GET_INSTANCE };
    let row = fetch_optional_q(tx, sqlx::query(sql).bind(id))?
        .ok_or_else(|| WorkflowError::NotFound(format!("Process not found: {id}")))?;
    map_instance(&row)
}

pub(super) fn one_token(tx: &mut NeonTx<'_>, id: &str, lock: bool) -> Result<Token> {
    let sql = if lock { LOCK_TOKEN } else { GET_TOKEN };
    let row = fetch_optional_q(tx, sqlx::query(sql).bind(id))?
        .ok_or_else(|| WorkflowError::NotFound(format!("Token not found: {id}")))?;
    map_token(&row)
}

pub(super) fn one_task(tx: &mut NeonTx<'_>, id: &str, lock: bool) -> Result<Task> {
    let sql = if lock { LOCK_TASK } else { GET_TASK };
    let row = fetch_optional_q(tx, sqlx::query(sql).bind(id))?
        .ok_or_else(|| WorkflowError::NotFound(format!("Task not found: {id}")))?;
    map_task(&row)
}

pub(super) fn one_job(tx: &mut NeonTx<'_>, id: &str, lock: bool) -> Result<Job> {
    let sql = if lock { LOCK_JOB } else { GET_JOB };
    let row = fetch_optional_q(tx, sqlx::query(sql).bind(id))?
        .ok_or_else(|| WorkflowError::NotFound(format!("Job not found: {id}")))?;
    map_job(&row)
}

pub(super) fn list_tokens(tx: &mut NeonTx<'_>, sql: &'static str, id: &str) -> Result<Vec<Token>> {
    let rows = fetch_all_q(tx, sqlx::query(sql).bind(id))?;
    rows.iter().map(map_token).collect()
}

pub(super) fn list_tasks(tx: &mut NeonTx<'_>, sql: &'static str, id: &str) -> Result<Vec<Task>> {
    let rows = fetch_all_q(tx, sqlx::query(sql).bind(id))?;
    rows.iter().map(map_task).collect()
}

pub(super) fn list_jobs(tx: &mut NeonTx<'_>, sql: &'static str, id: &str) -> Result<Vec<Job>> {
    let rows = fetch_all_q(tx, sqlx::query(sql).bind(id))?;
    rows.iter().map(map_job).collect()
}

pub(super) fn count_sql(tx: &mut NeonTx<'_>, sql: &'static str, id: &str) -> Result<i32> {
    let row = fetch_one_q(tx, sqlx::query(sql).bind(id))?;
    Ok(row.try_get("cnt").unwrap_or(0))
}

pub(super) fn s_get(row: &PgRow, col: &str) -> String {
    row.try_get::<String, _>(col).unwrap_or_default()
}
pub(super) fn s_opt(row: &PgRow, col: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(col).ok().flatten()
}
pub(super) fn s_f64(row: &PgRow, col: &str) -> i64 {
    row.try_get::<f64, _>(col)
        .ok()
        .or_else(|| row.try_get::<Option<f64>, _>(col).ok().flatten())
        .unwrap_or(0.0) as i64
}
pub(super) fn s_opt_f64(row: &PgRow, col: &str) -> Option<i64> {
    row.try_get::<Option<f64>, _>(col)
        .ok()
        .flatten()
        .map(|n| n as i64)
}
pub(super) fn s_i64(row: &PgRow, col: &str) -> i64 {
    row.try_get::<i64, _>(col).unwrap_or(0)
}
pub(super) fn json_col(row: &PgRow, col: &str) -> Value {
    row.try_get::<String, _>(col)
        .ok()
        .and_then(|s| parse_json(&s).ok())
        .unwrap_or_else(Value::object)
}

pub(super) fn map_instance(row: &PgRow) -> Result<ProcessInstance> {
    Ok(ProcessInstance {
        id: s_get(row, "id"),
        tenant_id: s_opt(row, "tenant_id"),
        definition_id: s_get(row, "definition_id"),
        business_key: s_opt(row, "business_key"),
        status: parse_process_status(&s_get(row, "status")),
        outcome: s_opt(row, "outcome")
            .as_deref()
            .and_then(parse_process_outcome),
        started_at: s_f64(row, "started_at"),
        ended_at: s_opt_f64(row, "ended_at"),
        started_by: s_opt(row, "started_by"),
        parent_instance_id: s_opt(row, "parent_instance_id"),
        root_token_id: s_opt(row, "root_token_id"),
        subject_type: s_opt(row, "subject_type"),
        subject_id: s_opt(row, "subject_id"),
        variables: json_col(row, "variables"),
        version: row.try_get("version").unwrap_or(1),
    })
}

pub(super) fn map_token(row: &PgRow) -> Result<Token> {
    Ok(Token {
        id: s_get(row, "id"),
        tenant_id: s_opt(row, "tenant_id"),
        process_instance_id: s_get(row, "process_instance_id"),
        parent_token_id: s_opt(row, "parent_token_id"),
        node_id: s_get(row, "node_id"),
        status: parse_token_status(&s_get(row, "status")),
        outcome: s_opt(row, "outcome")
            .as_deref()
            .and_then(parse_token_outcome),
        required: row.try_get("required").unwrap_or(true),
        is_able_to_reactivate_parent: row.try_get("is_able_to_reactivate_parent").unwrap_or(true),
        started_at: s_f64(row, "started_at"),
        ended_at: s_opt_f64(row, "ended_at"),
        version: row.try_get("version").unwrap_or(1),
    })
}

pub(super) fn map_task(row: &PgRow) -> Result<Task> {
    Ok(Task {
        id: s_get(row, "id"),
        tenant_id: s_opt(row, "tenant_id"),
        process_instance_id: s_get(row, "process_instance_id"),
        token_id: s_opt(row, "token_id"),
        node_id: s_opt(row, "node_id"),
        name: s_get(row, "name"),
        description: s_opt(row, "description"),
        status: parse_task_status(&s_get(row, "status")),
        assignee: s_opt(row, "assignee"),
        candidates: row
            .try_get::<Vec<String>, _>("candidates")
            .unwrap_or_default(),
        swimlane: s_opt(row, "swimlane"),
        priority: row.try_get("priority").unwrap_or(0),
        due_date: s_opt_f64(row, "due_date"),
        form_key: s_opt(row, "form_key"),
        form_data: json_col(row, "form_data"),
        created_at: s_f64(row, "created_at"),
        claimed_at: s_opt_f64(row, "claimed_at"),
        completed_at: s_opt_f64(row, "completed_at"),
        completed_by: s_opt(row, "completed_by"),
        version: row.try_get("version").unwrap_or(1),
    })
}

pub(super) fn map_job(row: &PgRow) -> Result<Job> {
    Ok(Job {
        id: s_get(row, "id"),
        tenant_id: s_opt(row, "tenant_id"),
        process_instance_id: s_opt(row, "process_instance_id"),
        token_id: s_opt(row, "token_id"),
        job_type: s_get(row, "job_type"),
        due_at: s_f64(row, "due_at"),
        status: parse_job_status(&s_get(row, "status")),
        locked_by: s_opt(row, "locked_by"),
        locked_until: s_opt_f64(row, "locked_until"),
        attempts: row.try_get("attempts").unwrap_or(0),
        max_attempts: row.try_get("max_attempts").unwrap_or(5),
        payload: json_col(row, "payload"),
        last_error: s_opt(row, "last_error"),
        created_at: s_f64(row, "created_at"),
        updated_at: s_f64(row, "updated_at"),
        completed_at: s_opt_f64(row, "completed_at"),
    })
}

pub(super) fn map_definition(row: &PgRow) -> Result<ProcessDefinition> {
    let raw = s_get(row, "definition");
    let graph = graph_from_json(&raw).map_err(WorkflowError::generic)?;
    Ok(ProcessDefinition {
        id: s_get(row, "id"),
        tenant_id: s_opt(row, "tenant_id"),
        key: s_get(row, "key"),
        version: row.try_get("version").unwrap_or(1),
        name: s_get(row, "name"),
        description: s_opt(row, "description"),
        definition: graph,
        status: parse_def_status(&s_get(row, "status")),
    })
}
