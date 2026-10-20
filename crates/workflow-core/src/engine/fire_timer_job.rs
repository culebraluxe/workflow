//! Moved from `engine.rs` (move only): fire_timer_job.

#[allow(unused_imports)]
use super::*;

impl<S: TxStore> WorkflowEngine<S> {
    pub fn fire_timer_job(&self, params: FireTimerParams) -> Result<()> {
        self.store.with_tx(|tx| {
            let peek = tx.get_job(&params.job_id)?;
            let mut instance = match &peek.process_instance_id {
                Some(id) => Some(tx.lock_instance(id)?),
                None => None,
            };
            let job = tx.lock_job(&params.job_id)?;
            if job.status == JobStatus::Completed {
                return Err(WorkflowError::conflict(
                    "TIMER_ALREADY_FIRED",
                    format!("Timer job {} already fired", params.job_id),
                ));
            }
            if job.status != JobStatus::Locked {
                return Err(WorkflowError::conflict(
                    "TIMER_NOT_LOCKED",
                    format!(
                        "Timer job {} is not locked (status={:?})",
                        params.job_id, job.status
                    ),
                ));
            }
            if job.locked_by.as_deref() != Some(&params.worker_id) {
                return Err(WorkflowError::conflict(
                    "TIMER_LOCK_OWNER",
                    format!("Timer job {} is locked by another worker", params.job_id),
                ));
            }
            if let Some(inst) = &instance {
                if inst.status != ProcessStatus::Active {
                    let mut cancelled = job.clone();
                    cancelled.status = JobStatus::Cancelled;
                    cancelled.locked_by = None;
                    cancelled.locked_until = None;
                    tx.update_job(&cancelled)?;
                    return Ok(());
                }
            }
            let mut completed = job.clone();
            completed.status = JobStatus::Completed;
            completed.completed_at = Some(self.now());
            completed.locked_by = None;
            completed.locked_until = None;
            tx.update_job(&completed)?;

            if let Some(token_id) = &job.token_id {
                let token = tx.lock_token(token_id)?;
                if token.status == TokenStatus::Active {
                    if let Some(inst) = instance.as_mut() {
                        if inst.status == ProcessStatus::Active {
                            let definition = tx.definition_by_id(&inst.definition_id)?;
                            let graph = definition.definition;
                            let node = graph
                                .nodes
                                .get(&token.node_id)
                                .cloned()
                                .ok_or_else(|| WorkflowError::generic("timer node missing"))?;
                            let mut current = inst.variables.clone();
                            if params
                                .variables
                                .as_object()
                                .map(|o| !o.is_empty())
                                .unwrap_or(false)
                            {
                                merge_json(&mut current, &params.variables);
                                tx.update_instance_variables(&inst.id, current.clone())?;
                                inst.variables = current.clone();
                            }
                            let transition_name = node
                                .timer
                                .as_ref()
                                .and_then(|t| t.transition.clone())
                                .or_else(|| {
                                    node.transitions
                                        .as_ref()
                                        .and_then(|ts| ts.first().map(|t| t.name.clone()))
                                });
                            let transition = node
                                .transitions
                                .as_ref()
                                .and_then(|ts| {
                                    ts.iter()
                                        .find(|t| Some(&t.name) == transition_name.as_ref())
                                        .or_else(|| ts.first())
                                })
                                .cloned()
                                .ok_or_else(|| {
                                    WorkflowError::generic(format!(
                                        "Timer node {} has no resume transition",
                                        token.node_id
                                    ))
                                })?;
                            self.move_token(
                                tx,
                                &token,
                                &transition.to,
                                &transition.name,
                                &params.worker_id,
                            )?;
                            let fresh = tx.get_token(&token.id)?;
                            self.arrive_at_node(
                                tx,
                                &fresh,
                                inst,
                                &graph,
                                &params.worker_id,
                                None,
                                &current,
                            )?;
                        }
                    }
                }
            }

            if let Some(pid) = job.process_instance_id {
                self.event(
                    tx,
                    EventInput {
                        tenant_id: job.tenant_id,
                        process_instance_id: pid,
                        token_id: job.token_id,
                        job_id: Some(params.job_id.clone()),
                        event_type: "timer.fired",
                        actor: params.worker_id.clone(),
                        data: json!({"type": job.job_type}),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn run_due_jobs(&self, worker_id: &str, batch: usize) -> Result<DueJobReport> {
        let reclaimed = self
            .store
            .with_tx(|tx| tx.reclaim_stale_jobs(self.now(), batch, None))?;
        // This runner is the timer executor. The jobs table is shared with
        // Forge role work and future async executors, so it must never claim
        // another executor's rows.
        let jobs = self.claim_jobs_by_type(worker_id, "timer", batch)?;
        let mut report = DueJobReport {
            reclaimed,
            claimed: jobs.clone(),
            ..Default::default()
        };
        if jobs.is_empty() {
            return Ok(report);
        }
        // Each job is its own transaction. Fire them on a bounded thread pool so one
        // slow timer does not hold the others, and so Neon takes separate connections
        // rather than one serial session. A panic in a worker is a step failure.
        let outcomes =
            crate::concurrency::run_bounded(&jobs, crate::concurrency::job_workers(), |job| {
                if job.job_type == "timer" && job.token_id.is_some() {
                    match self.fire_timer_job(FireTimerParams {
                        job_id: job.id.clone(),
                        worker_id: worker_id.to_string(),
                        variables: json!({}),
                    }) {
                        Ok(()) => Ok(true),
                        Err(error) => {
                            self.fail_job(&job.id, worker_id, &error.to_string(), false)?;
                            Ok(false)
                        }
                    }
                } else {
                    self.fail_job(
                        &job.id,
                        worker_id,
                        &format!("no executor registered for job type '{}'", job.job_type),
                        false,
                    )?;
                    Ok(false)
                }
            });
        for outcome in outcomes {
            match outcome {
                Ok(true) => report.fired += 1,
                Ok(false) => report.failed += 1,
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    pub fn fail_job(
        &self,
        job_id: &str,
        worker_id: &str,
        error: &str,
        permanent: bool,
    ) -> Result<()> {
        self.store.with_tx(|tx| {
            let job = tx.lock_job(job_id)?;
            if job.locked_by.as_deref() != Some(worker_id) {
                if job.status.is_settled() {
                    return Ok(());
                }
                return Err(WorkflowError::generic("Job is not locked by this worker"));
            }
            let should_retry = !permanent && job.attempts < job.max_attempts;
            let mut next = job.clone();
            next.status = if should_retry {
                JobStatus::Pending
            } else {
                JobStatus::Failed
            };
            next.last_error = Some(error.to_string());
            next.locked_by = None;
            next.locked_until = None;
            if should_retry {
                let exp = job.attempts.max(0).min(10) as u32;
                next.due_at = self.now() + JOB_BACKOFF_BASE_MS.saturating_mul(2i64.pow(exp));
            }
            tx.update_job(&next)?;
            if let Some(pid) = job.process_instance_id {
                self.event(
                    tx,
                    EventInput {
                        tenant_id: job.tenant_id,
                        process_instance_id: pid,
                        token_id: job.token_id,
                        job_id: Some(job_id.to_string()),
                        event_type: if should_retry {
                            "job.retry_scheduled"
                        } else {
                            "job.failed"
                        },
                        actor: worker_id.to_string(),
                        data: json!({"error": error, "attempts": job.attempts, "permanent": permanent}),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn cancel_job(&self, job_id: &str, actor: &str) -> Result<()> {
        self.store.with_tx(|tx| {
            let peek = tx.get_job(job_id)?;
            if let Some(pid) = &peek.process_instance_id {
                let _locked = tx.lock_instance(pid)?;
            }
            let mut job = tx.lock_job(job_id)?;
            if job.status.is_settled() {
                return Ok(());
            }
            job.status = JobStatus::Cancelled;
            job.locked_by = None;
            job.locked_until = None;
            job.completed_at = Some(self.now());
            tx.update_job(&job)?;
            if let Some(pid) = job.process_instance_id {
                self.event(
                    tx,
                    EventInput {
                        tenant_id: job.tenant_id,
                        process_instance_id: pid,
                        token_id: job.token_id,
                        job_id: Some(job_id.to_string()),
                        event_type: "job.cancelled",
                        actor: actor.to_string(),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn cancel_timer(&self, job_id: &str, actor: &str) -> Result<()> {
        self.cancel_job(job_id, actor)
    }

    pub fn reschedule_timer(&self, job_id: &str, due_at: i64, actor: &str) -> Result<()> {
        self.store.with_tx(|tx| {
            let peek = tx.get_job(job_id)?;
            if let Some(pid) = &peek.process_instance_id {
                let _locked = tx.lock_instance(pid)?;
            }
            let mut job = tx.lock_job(job_id)?;
            if job.status.is_settled() {
                return Err(WorkflowError::conflict(
                    "JOB_SETTLED",
                    format!(
                        "Job {job_id} cannot be rescheduled in status {:?}",
                        job.status
                    ),
                ));
            }
            job.status = JobStatus::Pending;
            job.due_at = due_at;
            job.locked_by = None;
            job.locked_until = None;
            tx.update_job(&job)?;
            if let Some(pid) = job.process_instance_id {
                self.event(
                    tx,
                    EventInput {
                        tenant_id: job.tenant_id,
                        process_instance_id: pid,
                        token_id: job.token_id,
                        job_id: Some(job_id.to_string()),
                        event_type: "job.rescheduled",
                        actor: actor.to_string(),
                        data: json!({"dueAt": due_at}),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn get_process_instance(&self, id: &str) -> Result<ProcessInstance> {
        self.store.with_tx(|tx| tx.get_instance(id))
    }

    pub fn get_task(&self, id: &str) -> Result<Task> {
        self.store.with_tx(|tx| tx.get_task(id))
    }

    pub fn tasks_for_instance(&self, id: &str) -> Result<Vec<Task>> {
        self.store.with_tx(|tx| tx.tasks_for_instance(id))
    }

    pub fn tokens_for_instance(&self, id: &str) -> Result<Vec<Token>> {
        self.store.with_tx(|tx| tx.tokens_for_instance(id))
    }

    pub fn history(&self, id: &str, limit: usize) -> Result<Vec<ProcessEvent>> {
        self.store.with_tx(|tx| tx.history(id, limit))
    }

    pub fn history_after_event_id(
        &self,
        id: &str,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<ProcessEvent>> {
        self.store
            .with_tx(|tx| tx.history_after_event_id(id, after_event_id, limit))
    }

    pub fn jobs_for_instance(&self, id: &str) -> Result<Vec<Job>> {
        self.store.with_tx(|tx| tx.open_jobs_for_instance(id))
    }

    pub fn list_instances(
        &self,
        tenant_id: Option<&str>,
        status: Option<&[ProcessStatus]>,
        definition_key: Option<&str>,
        business_key: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProcessInstance>> {
        self.store.with_tx(|tx| {
            tx.find_instances(
                tenant_id,
                status,
                definition_key,
                business_key,
                limit,
                offset,
            )
        })
    }

    pub fn tasks_for_user(&self, user_id: &str, tenant_id: Option<&str>) -> Result<Vec<Task>> {
        self.store
            .with_tx(|tx| tx.active_tasks_for_user(user_id, tenant_id))
    }

    /// Renew the lease for a running generic job.
    ///
    /// Heartbeats are ownership checks, not advisory timestamps: only the worker
    /// holding the current lock may extend it, and settled jobs cannot be revived.
    pub fn heartbeat_job(&self, job_id: &str, worker_id: &str) -> Result<i64> {
        self.store.with_tx(|tx| {
            let mut job = tx.lock_job(job_id)?;
            if job.status != JobStatus::Locked {
                return Err(WorkflowError::conflict(
                    "JOB_NOT_LOCKED",
                    format!("Job {job_id} is not locked (status={:?})", job.status),
                ));
            }
            if job.locked_by.as_deref() != Some(worker_id) {
                return Err(WorkflowError::conflict(
                    "JOB_LOCK_OWNER",
                    format!("Job {job_id} is locked by another worker"),
                ));
            }

            let lease_until = self.now() + JOB_LEASE_MS;
            job.locked_until = Some(lease_until);
            job.updated_at = self.now();
            tx.update_job(&job)?;
            Ok(lease_until)
        })
    }

    pub fn complete_job(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.store.with_tx(|tx| {
            let job = tx.lock_job(job_id)?;
            if job.locked_by.as_deref() != Some(worker_id) {
                if job.status.is_settled() {
                    return Ok(());
                }
                return Err(WorkflowError::generic("Job is not locked by this worker"));
            }
            let mut next = job.clone();
            next.status = JobStatus::Completed;
            next.completed_at = Some(self.now());
            next.locked_by = None;
            next.locked_until = None;
            tx.update_job(&next)?;
            if let Some(pid) = job.process_instance_id {
                self.event(
                    tx,
                    EventInput {
                        tenant_id: job.tenant_id,
                        process_instance_id: pid,
                        token_id: job.token_id,
                        job_id: Some(job_id.to_string()),
                        event_type: "job.completed",
                        actor: worker_id.to_string(),
                        data: json!({"type": job.job_type}),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn create_job(
        &self,
        process_instance_id: Option<&str>,
        token_id: Option<&str>,
        tenant_id: Option<&str>,
        job_type: &str,
        due_at: i64,
        payload: Value,
        max_attempts: Option<i32>,
    ) -> Result<String> {
        self.store.with_tx(|tx| {
            if let Some(pid) = process_instance_id {
                let inst = tx.lock_instance(pid)?;
                if inst.status != ProcessStatus::Active {
                    return Err(WorkflowError::conflict(
                        "PROCESS_NOT_ACTIVE",
                        format!(
                            "Process {pid} is not active (status={:?}); cannot create job",
                            inst.status
                        ),
                    ));
                }
            }
            let now = self.now();
            let job = Job {
                id: tx.new_id("job"),
                tenant_id: tenant_id.map(str::to_string),
                process_instance_id: process_instance_id.map(str::to_string),
                token_id: token_id.map(str::to_string),
                job_type: job_type.to_string(),
                due_at,
                status: JobStatus::Pending,
                locked_by: None,
                locked_until: None,
                attempts: 0,
                max_attempts: max_attempts.unwrap_or(5),
                // CLONED, NOT MOVED (2026-09-29). A step whose connection failed is repeated, so this closure runs
                // more than once and cannot consume the payload it captures: `payload` would work for the first
                // attempt and silently insert an empty job on the second. The `FnMut` bound on `with_tx` is what
                // forced this to be stated instead of discovered in production.
                payload: payload.clone(),
                last_error: None,
                created_at: now,
                updated_at: now,
                completed_at: None,
            };
            Ok(tx.insert_job(job)?.id)
        })
    }

    /// Create a durable job with a caller-owned stable identity.
    ///
    /// Repeating the same request returns the existing job instead of inserting
    /// a duplicate. When a process instance is supplied, its row lock serializes
    /// competing creates for that process, which makes this suitable for
    /// idempotent bridges such as Workflow task -> async job.
    pub fn create_job_with_id(
        &self,
        job_id: &str,
        process_instance_id: Option<&str>,
        token_id: Option<&str>,
        tenant_id: Option<&str>,
        job_type: &str,
        due_at: i64,
        payload: Value,
        max_attempts: Option<i32>,
    ) -> Result<String> {
        self.store.with_tx(|tx| {
            let process_active = if let Some(pid) = process_instance_id {
                let inst = tx.lock_instance(pid)?;
                inst.status == ProcessStatus::Active
            } else {
                true
            };

            match tx.get_job(job_id) {
                Ok(existing) => {
                    let same_request = existing.process_instance_id.as_deref()
                        == process_instance_id
                        && existing.token_id.as_deref() == token_id
                        && existing.tenant_id.as_deref() == tenant_id
                        && existing.job_type == job_type
                        && existing.payload == payload;
                    if same_request {
                        return Ok(existing.id);
                    }
                    return Err(WorkflowError::conflict(
                        "JOB_ID_COLLISION",
                        format!("Job {job_id} already exists for a different request"),
                    ));
                }
                Err(WorkflowError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }

            if !process_active {
                return Err(WorkflowError::conflict(
                    "PROCESS_NOT_ACTIVE",
                    format!(
                        "Process {} is not active; cannot create job",
                        process_instance_id.unwrap_or_default()
                    ),
                ));
            }

            let now = self.now();
            let job = Job {
                id: job_id.to_string(),
                tenant_id: tenant_id.map(str::to_string),
                process_instance_id: process_instance_id.map(str::to_string),
                token_id: token_id.map(str::to_string),
                job_type: job_type.to_string(),
                due_at,
                status: JobStatus::Pending,
                locked_by: None,
                locked_until: None,
                attempts: 0,
                max_attempts: max_attempts.unwrap_or(5),
                payload: payload.clone(),
                last_error: None,
                created_at: now,
                updated_at: now,
                completed_at: None,
            };
            Ok(tx.insert_job(job)?.id)
        })
    }

    pub fn reclaim_stale_jobs(&self, batch: usize) -> Result<usize> {
        self.store
            .with_tx(|tx| tx.reclaim_stale_jobs(self.now(), batch, None))
    }

    pub fn reclaim_stale_jobs_for_instance(&self, process_instance_id: &str) -> Result<usize> {
        self.store
            .with_tx(|tx| tx.reclaim_stale_jobs(self.now(), 10_000, Some(process_instance_id)))
    }

    pub fn requeue_job(&self, job_id: &str, actor: &str) -> Result<()> {
        self.store.with_tx(|tx| {
            let peek = tx.get_job(job_id)?;
            if let Some(pid) = &peek.process_instance_id {
                let inst = tx.lock_instance(pid)?;
                if inst.status != ProcessStatus::Active {
                    return Err(WorkflowError::conflict(
                        "JOB_NOT_REQUEUEABLE",
                        format!(
                            "Process {pid} is not active (status={:?}); job {job_id} cannot be requeued",
                            inst.status
                        ),
                    ));
                }
            }
            let mut job = tx.lock_job(job_id)?;
            if job.status != JobStatus::Failed {
                return Err(WorkflowError::conflict(
                    "JOB_NOT_REQUEUEABLE",
                    format!("Job {job_id} cannot be requeued (status={:?})", job.status),
                ));
            }
            job.status = JobStatus::Pending;
            job.attempts = 0;
            job.last_error = None;
            job.locked_by = None;
            job.locked_until = None;
            job.due_at = self.now();
            tx.update_job(&job)?;
            if let Some(pid) = job.process_instance_id {
                self.event(
                    tx,
                    EventInput {
                        tenant_id: job.tenant_id,
                        process_instance_id: pid,
                        token_id: job.token_id,
                        job_id: Some(job_id.to_string()),
                        event_type: "job.requeued",
                        actor: actor.to_string(),
                        ..Default::default()
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn get_job(&self, id: &str) -> Result<Job> {
        self.store.with_tx(|tx| tx.get_job(id))
    }

    pub fn get_token(&self, id: &str) -> Result<Token> {
        self.store.with_tx(|tx| tx.get_token(id))
    }

    pub fn list_overdue_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        self.store
            .with_tx(|tx| tx.list_overdue_jobs(self.now(), limit))
    }

    pub fn get_process_instance_with_details(
        &self,
        id: &str,
    ) -> Result<(ProcessInstance, Vec<Token>, Vec<Task>, Vec<Job>)> {
        self.store.with_tx(|tx| {
            Ok((
                tx.get_instance(id)?,
                tx.tokens_for_instance(id)?,
                tx.tasks_for_instance(id)?,
                tx.open_jobs_for_instance(id)?,
            ))
        })
    }

    // ------------------------------------------------------------------
    // internals
    // ------------------------------------------------------------------

    pub(super) fn arrive_at_node(
        &self,
        tx: &mut dyn Store,
        token: &Token,
        instance: &ProcessInstance,
        graph: &ProcessGraph,
        actor: &str,
        preferred: Option<&str>,
        variables: &Value,
    ) -> Result<()> {
        let node = graph
            .nodes
            .get(&token.node_id)
            .ok_or_else(|| WorkflowError::generic(format!("Node {} not found", token.node_id)))?;
        if node.node_type == "task" {
            self.create_human_task(tx, instance, &token.id, node, actor)?;
            return Ok(());
        }
        self.execute_node_leave(tx, token, instance, graph, actor, preferred, variables)
    }
}
