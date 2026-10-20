use crate::value::Value;

use crate::error::Result;
use crate::types::*;

/// Persistence surface the kernel talks to.
/// Memory store for tests; Postgres/Neon for the server process.
/// Same tables either way — no second schema.
pub trait Store {
    /// Memory: `pi-1`. Neon: `gen_random_uuid()`-shaped id.
    fn new_id(&mut self, prefix: &str) -> String;

    fn load_definition(
        &mut self,
        key: &str,
        version: Option<i32>,
        tenant_id: Option<&str>,
    ) -> Result<ProcessDefinition>;

    fn definition_by_id(&mut self, id: &str) -> Result<ProcessDefinition>;

    /// Register a definition by `(tenant_id, key, version)`: insert it when that row is absent, adopt the row
    /// that is already there when it is present. `process_definitions` is the authority on a definition's
    /// identity — its `id` is a uuid with a `gen_random_uuid()` default and `(tenant_id, key, version)` is
    /// unique — so the caller's `def.id` is a human key (`FORGE_SDLC-v6`), not the identity, and must never be
    /// written to the column. Called on every process start (the engine seeds the definition it is about to
    /// run), so it has to be safe to call any number of times.
    ///
    /// This replaces a plain insert that bound the caller's key as `$1::uuid`: from 2026-09-25 every Rust
    /// engine start against Neon died on `invalid input syntax for type uuid: "FORGE_SDLC-v6"` before a single
    /// instance existed, because a NULL tenant is not caught by `unique (tenant_id, key, version)`.
    fn ensure_definition(&mut self, def: ProcessDefinition) -> Result<ProcessDefinition>;

    fn insert_instance(&mut self, inst: ProcessInstance) -> Result<ProcessInstance>;
    fn set_root_token(&mut self, instance_id: &str, token_id: &str) -> Result<()>;
    fn get_instance(&mut self, id: &str) -> Result<ProcessInstance>;
    fn find_active_by_subject(
        &mut self,
        definition_id: &str,
        subject_type: &str,
        subject_id: &str,
    ) -> Result<Option<ProcessInstance>>;
    fn lock_instance(&mut self, id: &str) -> Result<ProcessInstance>;
    fn update_instance_variables(&mut self, id: &str, variables: Value) -> Result<()>;
    fn terminate_instance(
        &mut self,
        id: &str,
        status: ProcessStatus,
        outcome: ProcessOutcome,
        ended_at: i64,
    ) -> Result<()>;

    fn insert_token(&mut self, token: Token) -> Result<Token>;
    fn get_token(&mut self, id: &str) -> Result<Token>;
    fn lock_token(&mut self, id: &str) -> Result<Token>;
    fn move_token(&mut self, id: &str, expected_version: i32, to_node: &str) -> Result<bool>;
    fn complete_token(&mut self, id: &str, outcome: TokenOutcome, ended_at: i64) -> Result<()>;
    fn count_active_tokens(&mut self, instance_id: &str) -> Result<i32>;
    fn list_active_tokens(&mut self, instance_id: &str) -> Result<Vec<Token>>;
    fn count_required_active_siblings(&mut self, parent_id: &str) -> Result<i32>;
    fn list_optional_active_siblings(&mut self, parent_id: &str) -> Result<Vec<Token>>;
    fn list_children(&mut self, parent_id: &str) -> Result<Vec<Token>>;
    fn tokens_for_instance(&mut self, instance_id: &str) -> Result<Vec<Token>>;

    fn insert_task(&mut self, task: Task) -> Result<Task>;
    fn get_task(&mut self, id: &str) -> Result<Task>;
    fn lock_task(&mut self, id: &str) -> Result<Task>;
    fn cas_task(&mut self, task: &Task) -> Result<bool>;
    fn tasks_for_instance(&mut self, instance_id: &str) -> Result<Vec<Task>>;
    fn open_tasks_for_instance(&mut self, instance_id: &str) -> Result<Vec<Task>>;
    fn open_tasks_for_token(&mut self, token_id: &str) -> Result<Vec<Task>>;
    fn active_tasks_for_user(
        &mut self,
        user_id: &str,
        tenant_id: Option<&str>,
    ) -> Result<Vec<Task>>;
    fn patch_ready_task_form(&mut self, token_id: &str, form_data: Value) -> Result<()>;

    fn insert_job(&mut self, job: Job) -> Result<Job>;
    fn get_job(&mut self, id: &str) -> Result<Job>;
    fn lock_job(&mut self, id: &str) -> Result<Job>;
    fn update_job(&mut self, job: &Job) -> Result<()>;
    fn claim_job(
        &mut self,
        job_id: &str,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<Job>>;

    fn claim_due_jobs(
        &mut self,
        worker_id: &str,
        now: i64,
        lease_until: i64,
        limit: usize,
    ) -> Result<Vec<Job>>;
    /// Claim only due jobs of one executor type.
    ///
    /// Generic workers share the `jobs` table (timers, Forge roles, future async
    /// executors). A worker must never lease work owned by a different executor.
    fn claim_due_jobs_by_type(
        &mut self,
        worker_id: &str,
        job_type: &str,
        now: i64,
        lease_until: i64,
        limit: usize,
    ) -> Result<Vec<Job>>;

    fn reclaim_stale_jobs(
        &mut self,
        now: i64,
        batch: usize,
        instance_id: Option<&str>,
    ) -> Result<usize>;
    fn open_jobs_for_instance(&mut self, instance_id: &str) -> Result<Vec<Job>>;
    fn open_jobs_for_token(&mut self, token_id: &str) -> Result<Vec<Job>>;
    fn list_overdue_jobs(&mut self, now: i64, limit: usize) -> Result<Vec<Job>>;

    fn insert_event(&mut self, event: ProcessEvent) -> Result<()>;
    fn history(&mut self, instance_id: &str, limit: usize) -> Result<Vec<ProcessEvent>>;
    /// Read a stable page of events after a durable event id, in insertion order.
    fn history_after_event_id(
        &mut self,
        instance_id: &str,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<ProcessEvent>> {
        let mut events = self.history(instance_id, usize::MAX)?;
        events.retain(|event| event.id > after_event_id);
        events.sort_by_key(|event| event.id);
        events.truncate(limit);
        Ok(events)
    }

    fn command_visit_count(&mut self, instance_id: &str, node_id: &str) -> Result<i32>;
    fn insert_command(&mut self, cmd: ProcessCommand) -> Result<()>;

    fn find_instances(
        &mut self,
        tenant_id: Option<&str>,
        status: Option<&[ProcessStatus]>,
        definition_key: Option<&str>,
        business_key: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProcessInstance>>;
}

/// One engine step = one transaction (ENG-09).
///
/// `FnMut`, NOT `FnOnce` (2026-09-29): a step whose connection failed has to be repeatable, because nothing committed
/// and the alternative is losing the whole run (`NeonStore::with_tx`). A closure that moves a captured value out of
/// itself cannot be called twice, so this bound is the compiler refusing a step that could not be retried — which is
/// the right place for that refusal to happen, at the call site that built the step.
/// One engine step = one transaction. The store itself is shareable across threads:
/// each `with_tx` takes its own connection (Neon) or the memory mutex (tests).
pub trait TxStore: Send + Sync {
    fn with_tx<R, F>(&self, f: F) -> Result<R>
    where
        F: FnMut(&mut dyn Store) -> Result<R>;
}

/// Repeat a step while — and only while — its failure belonged to the connection.
///
/// THE RULE LIVES HERE, ONCE (2026-09-29). `NeonStore::with_tx` is the only production store, and its step is what dies
/// when a socket breaks mid-transaction; but the decision — repeat, or hand the failure back — belongs next to the
/// trait that defines what a step IS, and here it can be tested without a database. That matters more than the tidiness:
/// this decision is the difference between a database hiccup costing one step and costing a three-hour run, so it is
/// the part of the repair that must not be taken on faith.
///
/// Everything that is not a connection failure goes back to the caller on the first attempt, because repeating it
/// either wastes a round trip or changes the meaning of the answer: a step the database refused has been refused.
///
/// `pub` so a contract test can compose this exact rule at the `TxStore` seam
/// (`tests/tests/wf_command__003__retry_produces_same_identity.rs`). A test that copied the retry loop
/// would prove a second implementation; the seam lets the retry be this one. The function stays pure — the caller
/// supplies the step and the wait — so it is still exercised here without a database by the unit tests below.
pub fn repeat_connection_failures<R, S, W>(mut step: S, attempts: u32, mut wait: W) -> Result<R>
where
    S: FnMut() -> Result<R>,
    W: FnMut(u32),
{
    let attempts = attempts.max(1);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match step() {
            Ok(value) => return Ok(value),
            Err(error) => {
                if !error.is_connection_failure() || attempt >= attempts {
                    return Err(error);
                }
                wait(attempt);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::WorkflowError;

    /// The engine is strictly serial and the run this repairs was two hours fifty-three minutes long, so the repeat has
    /// to be visible: this asserts the step is repeated, that the wait is told which attempt failed (so backoff can
    /// grow), and that the caller gets the SECOND attempt's result rather than the first attempt's failure.
    #[test]
    fn a_connection_failure_is_repeated_and_the_success_is_returned() {
        let mut calls = 0;
        let mut waits = Vec::new();
        let result = repeat_connection_failures(
            || {
                calls += 1;
                if calls < 3 {
                    Err(WorkflowError::unavailable(
                        "error communicating with database: Broken pipe (os error 32)",
                    ))
                } else {
                    Ok("committed")
                }
            },
            3,
            |attempt| waits.push(attempt),
        );
        assert_eq!(result.unwrap(), "committed");
        assert_eq!(calls, 3, "two broken sockets were survived, not one");
        assert_eq!(waits, vec![1, 2], "the wait must know which attempt failed");
    }

    /// A refusal is an answer, not a hiccup. Repeating it wastes a round trip and hides what the database said — this
    /// is the assertion that keeps the retry from becoming a blanket "try three times".
    #[test]
    fn a_refusal_is_not_repeated() {
        let mut calls = 0;
        let result: Result<()> = repeat_connection_failures(
            || {
                calls += 1;
                Err(WorkflowError::conflict(
                    "PROCESS_NOT_ACTIVE",
                    "process is not active",
                ))
            },
            3,
            |_| panic!("a refusal must not be waited on"),
        );
        assert_eq!(result.unwrap_err().code(), "PROCESS_NOT_ACTIVE");
        assert_eq!(calls, 1, "repeating a refusal only repeats the refusal");
    }

    /// Bounded, so a database that is genuinely down fails the step instead of holding the engine forever: the caller
    /// gets the connection failure and the run records a verdict instead of hanging.
    #[test]
    fn a_connection_that_never_recovers_gives_up_after_the_policy_runs_out() {
        let mut calls = 0;
        let result: Result<()> = repeat_connection_failures(
            || {
                calls += 1;
                Err(WorkflowError::unavailable("broken pipe"))
            },
            3,
            |_| {},
        );
        assert!(result.unwrap_err().is_connection_failure());
        assert_eq!(calls, 3, "attempts come from the policy and are finite");
    }

    /// `attempts: 0` must not mean "no attempts": a misconfigured policy still has to run the step once, or the engine
    /// would silently do nothing at all.
    #[test]
    fn a_zero_attempt_policy_still_runs_the_step_once() {
        let mut calls = 0;
        let result = repeat_connection_failures(
            || {
                calls += 1;
                Ok("ran")
            },
            0,
            |_| {},
        );
        assert_eq!(result.unwrap(), "ran");
        assert_eq!(calls, 1);
    }
}
