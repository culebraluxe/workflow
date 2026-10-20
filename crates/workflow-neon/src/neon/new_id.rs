//! Moved from `neon.rs` (move only): new_id.

#[allow(unused_imports)]
use super::*;

impl Store for NeonTx<'_> {
    fn new_id(&mut self, _prefix: &str) -> String {
        uuid_v4()
    }

    fn load_definition(
        &mut self,
        key: &str,
        version: Option<i32>,
        tenant_id: Option<&str>,
    ) -> Result<ProcessDefinition> {
        // Store trait is &self; NeonTx methods that query need &mut for connection.
        // Reborrow via pointer — connection() needs &mut DbTransaction.
        let this = self;
        let row = match (version, tenant_id) {
            (Some(v), Some(tid)) => fetch_optional_q(
                this,
                sqlx::query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, key, version, name, description, definition::text AS definition, status FROM process_definitions WHERE key = $1 AND version = $2 AND status = 'active' AND tenant_id = $3::uuid",
                )
                .bind(key)
                .bind(v)
                .bind(tid),
            )?,
            (Some(v), None) => fetch_optional_q(
                this,
                sqlx::query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, key, version, name, description, definition::text AS definition, status FROM process_definitions WHERE key = $1 AND version = $2 AND status = 'active' AND tenant_id IS NULL",
                )
                .bind(key)
                .bind(v),
            )?,
            (None, Some(tid)) => fetch_optional_q(
                this,
                sqlx::query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, key, version, name, description, definition::text AS definition, status FROM process_definitions WHERE key = $1 AND status = 'active' AND tenant_id = $2::uuid ORDER BY version DESC LIMIT 1",
                )
                .bind(key)
                .bind(tid),
            )?,
            (None, None) => fetch_optional_q(
                this,
                sqlx::query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, key, version, name, description, definition::text AS definition, status FROM process_definitions WHERE key = $1 AND status = 'active' AND tenant_id IS NULL ORDER BY version DESC LIMIT 1",
                )
                .bind(key),
            )?,
        };
        row.ok_or_else(|| WorkflowError::generic(format!("Process definition not found: {key}")))
            .and_then(|r| map_definition(&r))
    }

    fn definition_by_id(&mut self, id: &str) -> Result<ProcessDefinition> {
        let this = self;
        let row = fetch_one_q(
            this,
            sqlx::query(
                "SELECT id::text AS id, tenant_id::text AS tenant_id, key, version, name, description, definition::text AS definition, status FROM process_definitions WHERE id = $1::uuid",
            )
            .bind(id),
        )?;
        map_definition(&row)
    }

    /// Register the definition unless `(tenant_id, key, version)` is already registered, in which case the
    /// registered row is returned untouched.
    ///
    /// The id is the database's, never the caller's: `process_definitions.id` is a uuid with a
    /// `gen_random_uuid()` default, while the engine's definition carries a human id (`FORGE_SDLC-v6`). The
    /// guard cannot be `on conflict (tenant_id, key, version)` either — a NULL tenant makes the row distinct
    /// from every other NULL, so the unique index does not catch a second insert of a tenant-less definition,
    /// and the `where not exists` below is what makes this path idempotent.
    fn ensure_definition(&mut self, def: ProcessDefinition) -> Result<ProcessDefinition> {
        let json = graph_to_json(&def.definition);
        let inserted = fetch_optional_q(
            &mut *self,
            sqlx::query(
                "INSERT INTO process_definitions (tenant_id, key, version, name, description, definition, status)
                 SELECT $1::uuid, $2, $3, $4, $5, $6::jsonb, $7
                  WHERE NOT EXISTS (
                        SELECT 1 FROM process_definitions
                         WHERE key = $2 AND version = $3
                           AND tenant_id IS NOT DISTINCT FROM $1::uuid)
                 RETURNING id::text AS id, tenant_id::text AS tenant_id, key, version, name, description,
                           definition::text AS definition, status",
            )
            .bind(&def.tenant_id)
            .bind(&def.key)
            .bind(def.version)
            .bind(&def.name)
            .bind(&def.description)
            .bind(&json)
            .bind(def_status(def.status)),
        )?;
        if let Some(row) = inserted {
            return map_definition(&row);
        }
        let row = fetch_one_q(
            &mut *self,
            sqlx::query(
                "SELECT id::text AS id, tenant_id::text AS tenant_id, key, version, name, description, definition::text AS definition, status FROM process_definitions WHERE key = $1 AND version = $2 AND tenant_id IS NOT DISTINCT FROM $3::uuid LIMIT 1",
            )
            .bind(&def.key)
            .bind(def.version)
            .bind(&def.tenant_id),
        )?;
        map_definition(&row)
    }

    fn insert_instance(&mut self, inst: ProcessInstance) -> Result<ProcessInstance> {
        let vars = stringify(&inst.variables);
        exec_q(
            self,
            sqlx::query(
                "INSERT INTO process_instances (
                    id, tenant_id, definition_id, business_key, status, started_by,
                    variables, subject_type, subject_id, started_at
                 ) VALUES (
                    $1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7::jsonb, $8, $9,
                    to_timestamp($10::double precision / 1000.0)
                 )",
            )
            .bind(&inst.id)
            .bind(&inst.tenant_id)
            .bind(&inst.definition_id)
            .bind(&inst.business_key)
            .bind(process_status(inst.status))
            .bind(&inst.started_by)
            .bind(&vars)
            .bind(&inst.subject_type)
            .bind(&inst.subject_id)
            .bind(inst.started_at),
        )?;
        Ok(inst)
    }

    fn set_root_token(&mut self, instance_id: &str, token_id: &str) -> Result<()> {
        run_exec(
            self,
            sqlx::query(
                "UPDATE process_instances SET root_token_id = $2::uuid WHERE id = $1::uuid",
            )
            .bind(instance_id)
            .bind(token_id),
        )
    }

    fn find_active_by_subject(
        &mut self,
        definition_id: &str,
        subject_type: &str,
        subject_id: &str,
    ) -> Result<Option<ProcessInstance>> {
        let this = self;
        let row = fetch_optional_q(
            this,
            sqlx::query(
                "SELECT id::text AS id, tenant_id::text AS tenant_id, definition_id::text AS definition_id, business_key, status, outcome, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, started_by, parent_instance_id::text AS parent_instance_id, root_token_id::text AS root_token_id, subject_type, subject_id, variables::text AS variables, version FROM process_instances WHERE definition_id = $1::uuid AND subject_type = $2 AND subject_id = $3 AND status = 'active' LIMIT 1",
            )
            .bind(definition_id)
            .bind(subject_type)
            .bind(subject_id),
        )?;
        match row {
            Some(row) => map_instance(&row).map(Some),
            None => Ok(None),
        }
    }

    fn get_instance(&mut self, id: &str) -> Result<ProcessInstance> {
        let this = self;
        one_instance(this, id, false)
    }

    fn lock_instance(&mut self, id: &str) -> Result<ProcessInstance> {
        one_instance(self, id, true)
    }

    fn update_instance_variables(&mut self, id: &str, variables: Value) -> Result<()> {
        let vars = stringify(&variables);
        run_exec(
            self,
            sqlx::query(
                "UPDATE process_instances SET variables = $2::jsonb, version = version + 1
                 WHERE id = $1::uuid",
            )
            .bind(id)
            .bind(vars),
        )
    }

    fn terminate_instance(
        &mut self,
        id: &str,
        status: ProcessStatus,
        outcome: ProcessOutcome,
        ended_at: i64,
    ) -> Result<()> {
        run_exec(
            self,
            sqlx::query(
                "UPDATE process_instances
                 SET status = $2, outcome = $3,
                     ended_at = to_timestamp($4::double precision / 1000.0),
                     version = version + 1
                 WHERE id = $1::uuid",
            )
            .bind(id)
            .bind(process_status(status))
            .bind(process_outcome(outcome))
            .bind(ended_at),
        )
    }

    fn insert_token(&mut self, token: Token) -> Result<Token> {
        run_exec(
            self,
            sqlx::query(
                "INSERT INTO tokens (
                    id, tenant_id, process_instance_id, parent_token_id, node_id,
                    status, required, started_at
                 ) VALUES (
                    $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6, $7,
                    to_timestamp($8::double precision / 1000.0)
                 )",
            )
            .bind(&token.id)
            .bind(&token.tenant_id)
            .bind(&token.process_instance_id)
            .bind(&token.parent_token_id)
            .bind(&token.node_id)
            .bind(token_status(token.status))
            .bind(token.required)
            .bind(token.started_at),
        )?;
        Ok(token)
    }

    fn get_token(&mut self, id: &str) -> Result<Token> {
        let this = self;
        one_token(this, id, false)
    }

    fn lock_token(&mut self, id: &str) -> Result<Token> {
        one_token(self, id, true)
    }

    fn move_token(&mut self, id: &str, expected_version: i32, to_node: &str) -> Result<bool> {
        let n = run_exec_n(
            self,
            sqlx::query(
                "UPDATE tokens SET node_id = $3, version = version + 1
                 WHERE id = $1::uuid AND version = $2",
            )
            .bind(id)
            .bind(expected_version)
            .bind(to_node),
        )?;
        Ok(n == 1)
    }

    fn complete_token(&mut self, id: &str, outcome: TokenOutcome, ended_at: i64) -> Result<()> {
        run_exec(
            self,
            sqlx::query(
                "UPDATE tokens
                 SET status = 'completed', outcome = $2,
                     ended_at = to_timestamp($3::double precision / 1000.0),
                     version = version + 1
                 WHERE id = $1::uuid",
            )
            .bind(id)
            .bind(token_outcome(outcome))
            .bind(ended_at),
        )
    }

    fn count_active_tokens(&mut self, instance_id: &str) -> Result<i32> {
        let this = self;
        count_sql(
            this,
            "SELECT count(*)::int AS cnt FROM tokens WHERE process_instance_id = $1::uuid AND status = 'active'",
            instance_id,
        )
    }

    fn list_active_tokens(&mut self, instance_id: &str) -> Result<Vec<Token>> {
        let this = self;
        list_tokens(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, parent_token_id::text AS parent_token_id, node_id, status, outcome, required, is_able_to_reactivate_parent, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, version FROM tokens WHERE process_instance_id = $1::uuid AND status = 'active'",
            instance_id,
        )
    }

    fn count_required_active_siblings(&mut self, parent_id: &str) -> Result<i32> {
        let this = self;
        count_sql(
            this,
            "SELECT count(*)::int AS cnt FROM tokens
             WHERE parent_token_id = $1::uuid AND status = 'active' AND required = true",
            parent_id,
        )
    }

    fn list_optional_active_siblings(&mut self, parent_id: &str) -> Result<Vec<Token>> {
        let this = self;
        list_tokens(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, parent_token_id::text AS parent_token_id, node_id, status, outcome, required, is_able_to_reactivate_parent, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, version FROM tokens WHERE parent_token_id = $1::uuid AND status = 'active' AND required = false",
            parent_id,
        )
    }

    fn list_children(&mut self, parent_id: &str) -> Result<Vec<Token>> {
        let this = self;
        list_tokens(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, parent_token_id::text AS parent_token_id, node_id, status, outcome, required, is_able_to_reactivate_parent, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, version FROM tokens WHERE parent_token_id = $1::uuid",
            parent_id,
        )
    }

    fn tokens_for_instance(&mut self, instance_id: &str) -> Result<Vec<Token>> {
        let this = self;
        list_tokens(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, parent_token_id::text AS parent_token_id, node_id, status, outcome, required, is_able_to_reactivate_parent, extract(epoch from started_at)*1000 AS started_at, extract(epoch from ended_at)*1000 AS ended_at, version FROM tokens WHERE process_instance_id = $1::uuid ORDER BY started_at",
            instance_id,
        )
    }

    fn insert_task(&mut self, task: Task) -> Result<Task> {
        let form = stringify(&task.form_data);
        run_exec(
            self,
            sqlx::query(
                "INSERT INTO tasks (
                    id, tenant_id, process_instance_id, token_id, name, description,
                    status, candidates, form_key, priority, node_id, form_data, created_at
                 ) VALUES (
                    $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10, $11, $12::jsonb,
                    to_timestamp($13::double precision / 1000.0)
                 )",
            )
            .bind(&task.id)
            .bind(&task.tenant_id)
            .bind(&task.process_instance_id)
            .bind(&task.token_id)
            .bind(&task.name)
            .bind(&task.description)
            .bind(task_status(task.status))
            .bind(&task.candidates)
            .bind(&task.form_key)
            .bind(task.priority)
            .bind(&task.node_id)
            .bind(&form)
            .bind(task.created_at),
        )?;
        Ok(task)
    }

    fn get_task(&mut self, id: &str) -> Result<Task> {
        let this = self;
        one_task(this, id, false)
    }

    fn lock_task(&mut self, id: &str) -> Result<Task> {
        one_task(self, id, true)
    }

    fn cas_task(&mut self, task: &Task) -> Result<bool> {
        let form = stringify(&task.form_data);
        let n = run_exec_n(
            self,
            sqlx::query(
                "UPDATE tasks SET
                    status = $3, assignee = $4,
                    claimed_at = CASE WHEN $5::bigint IS NULL THEN NULL
                        ELSE to_timestamp($5::double precision / 1000.0) END,
                    completed_at = CASE WHEN $6::bigint IS NULL THEN NULL
                        ELSE to_timestamp($6::double precision / 1000.0) END,
                    completed_by = $7, form_data = $8::jsonb, version = $9
                 WHERE id = $1::uuid AND version = $2",
            )
            .bind(&task.id)
            .bind(task.version - 1)
            .bind(task_status(task.status))
            .bind(&task.assignee)
            .bind(task.claimed_at)
            .bind(task.completed_at)
            .bind(&task.completed_by)
            .bind(form)
            .bind(task.version),
        )?;
        Ok(n == 1)
    }

    fn tasks_for_instance(&mut self, instance_id: &str) -> Result<Vec<Task>> {
        let this = self;
        list_tasks(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE process_instance_id = $1::uuid ORDER BY created_at",
            instance_id,
        )
    }

    fn open_tasks_for_instance(&mut self, instance_id: &str) -> Result<Vec<Task>> {
        let this = self;
        list_tasks(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE process_instance_id = $1::uuid AND status IN ('ready','reserved','in_progress')",
            instance_id,
        )
    }

    fn open_tasks_for_token(&mut self, token_id: &str) -> Result<Vec<Task>> {
        let this = self;
        list_tasks(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE token_id = $1::uuid AND status IN ('ready','reserved','in_progress')",
            token_id,
        )
    }

    fn active_tasks_for_user(
        &mut self,
        user_id: &str,
        tenant_id: Option<&str>,
    ) -> Result<Vec<Task>> {
        let this = self;
        let rows = if let Some(tid) = tenant_id {
            fetch_all_q(
                this,
                sqlx::query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE status IN ('ready','reserved','in_progress') AND (assignee = $1 OR $1 = ANY(candidates)) AND tenant_id = $2::uuid ORDER BY priority DESC, created_at ASC",
                )
                .bind(user_id)
                .bind(tid),
            )?
        } else {
            fetch_all_q(
                this,
                sqlx::query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, node_id, name, description, status, assignee, candidates, swimlane, priority, extract(epoch from due_date)*1000 AS due_date, form_key, form_data::text AS form_data, extract(epoch from created_at)*1000 AS created_at, extract(epoch from claimed_at)*1000 AS claimed_at, extract(epoch from completed_at)*1000 AS completed_at, completed_by, version FROM tasks WHERE status IN ('ready','reserved','in_progress') AND (assignee = $1 OR $1 = ANY(candidates)) ORDER BY priority DESC, created_at ASC",
                )
                .bind(user_id),
            )?
        };
        rows.iter().map(map_task).collect()
    }

    fn patch_ready_task_form(&mut self, token_id: &str, form_data: Value) -> Result<()> {
        let form = stringify(&form_data);
        run_exec(
            self,
            sqlx::query(
                "UPDATE tasks SET form_data = $2::jsonb, version = version + 1
                 WHERE token_id = $1::uuid AND status = 'ready'",
            )
            .bind(token_id)
            .bind(form),
        )
    }

    fn insert_job(&mut self, job: Job) -> Result<Job> {
        let payload = stringify(&job.payload);
        run_exec(
            self,
            sqlx::query(
                "INSERT INTO jobs (
                    id, tenant_id, process_instance_id, token_id, type, due_at,
                    payload, max_attempts, status
                 ) VALUES (
                    $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5,
                    to_timestamp($6::double precision / 1000.0),
                    $7::jsonb, $8, $9
                 )",
            )
            .bind(&job.id)
            .bind(&job.tenant_id)
            .bind(&job.process_instance_id)
            .bind(&job.token_id)
            .bind(&job.job_type)
            .bind(job.due_at)
            .bind(&payload)
            .bind(job.max_attempts)
            .bind(job_status(job.status)),
        )?;
        Ok(job)
    }

    fn get_job(&mut self, id: &str) -> Result<Job> {
        let this = self;
        one_job(this, id, false)
    }

    fn lock_job(&mut self, id: &str) -> Result<Job> {
        one_job(self, id, true)
    }

    fn update_job(&mut self, job: &Job) -> Result<()> {
        let payload = stringify(&job.payload);
        run_exec(
            self,
            sqlx::query(
                "UPDATE jobs SET
                    status = $2, locked_by = $3,
                    locked_until = CASE WHEN $4::bigint IS NULL THEN NULL
                        ELSE to_timestamp($4::double precision / 1000.0) END,
                    attempts = $5, last_error = $6, payload = $7::jsonb,
                    due_at = to_timestamp($8::double precision / 1000.0),
                    completed_at = CASE WHEN $9::bigint IS NULL THEN NULL
                        ELSE to_timestamp($9::double precision / 1000.0) END
                 WHERE id = $1::uuid",
            )
            .bind(&job.id)
            .bind(job_status(job.status))
            .bind(&job.locked_by)
            .bind(job.locked_until)
            .bind(job.attempts)
            .bind(&job.last_error)
            .bind(payload)
            .bind(job.due_at)
            .bind(job.completed_at),
        )
    }

    fn claim_job(
        &mut self,
        job_id: &str,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<Job>> {
        let row = fetch_optional_q(
            self,
            sqlx::query(
                "UPDATE jobs
                    SET status = 'locked',
                        locked_by = $2,
                        locked_until = to_timestamp($4::double precision / 1000.0),
                        attempts = attempts + 1
                  WHERE id = $1::uuid
                    AND status = 'pending'
                    AND due_at <= to_timestamp($3::double precision / 1000.0)
                    AND attempts < max_attempts
              RETURNING id::text AS id, tenant_id::text AS tenant_id,
                        process_instance_id::text AS process_instance_id,
                        token_id::text AS token_id, type AS job_type,
                        extract(epoch from due_at)*1000 AS due_at, status, locked_by,
                        extract(epoch from locked_until)*1000 AS locked_until,
                        attempts, max_attempts, payload::text AS payload, last_error,
                        extract(epoch from created_at)*1000 AS created_at,
                        extract(epoch from updated_at)*1000 AS updated_at,
                        extract(epoch from completed_at)*1000 AS completed_at",
            )
            .bind(job_id)
            .bind(worker_id)
            .bind(now)
            .bind(lease_until),
        )?;
        row.as_ref().map(map_job).transpose()
    }

    fn claim_due_jobs(
        &mut self,
        worker_id: &str,
        now: i64,
        lease_until: i64,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let rows = fetch_all_q(
            self,
            sqlx::query(
                "UPDATE jobs SET status = 'locked', locked_by = $1, locked_until = to_timestamp($3::double precision / 1000.0), attempts = attempts + 1 WHERE id IN (SELECT id FROM jobs WHERE status = 'pending' AND due_at <= to_timestamp($2::double precision / 1000.0) AND attempts < max_attempts ORDER BY due_at ASC LIMIT $4 FOR UPDATE SKIP LOCKED) RETURNING id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, type AS job_type, extract(epoch from due_at)*1000 AS due_at, status, locked_by, extract(epoch from locked_until)*1000 AS locked_until, attempts, max_attempts, payload::text AS payload, last_error, extract(epoch from created_at)*1000 AS created_at, extract(epoch from updated_at)*1000 AS updated_at, extract(epoch from completed_at)*1000 AS completed_at",
            )
            .bind(worker_id)
            .bind(now)
            .bind(lease_until)
            .bind(limit as i64),
        )?;
        rows.iter().map(map_job).collect()
    }

    fn claim_due_jobs_by_type(
        &mut self,
        worker_id: &str,
        job_type: &str,
        now: i64,
        lease_until: i64,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let rows = fetch_all_q(
            self,
            sqlx::query(
                "UPDATE jobs SET status = 'locked', locked_by = $1,
                    locked_until = to_timestamp($4::double precision / 1000.0),
                    attempts = attempts + 1
                 WHERE id IN (
                    SELECT id FROM jobs
                    WHERE status = 'pending'
                      AND type = $2
                      AND due_at <= to_timestamp($3::double precision / 1000.0)
                      AND attempts < max_attempts
                    ORDER BY due_at ASC
                    LIMIT $5
                    FOR UPDATE SKIP LOCKED
                 )
                 RETURNING id::text AS id, tenant_id::text AS tenant_id,
                    process_instance_id::text AS process_instance_id,
                    token_id::text AS token_id, type AS job_type,
                    extract(epoch from due_at)*1000 AS due_at, status, locked_by,
                    extract(epoch from locked_until)*1000 AS locked_until,
                    attempts, max_attempts, payload::text AS payload, last_error,
                    extract(epoch from created_at)*1000 AS created_at,
                    extract(epoch from updated_at)*1000 AS updated_at,
                    extract(epoch from completed_at)*1000 AS completed_at",
            )
            .bind(worker_id)
            .bind(job_type)
            .bind(now)
            .bind(lease_until)
            .bind(limit as i64),
        )?;
        rows.iter().map(map_job).collect()
    }

    fn reclaim_stale_jobs(
        &mut self,
        now: i64,
        batch: usize,
        instance_id: Option<&str>,
    ) -> Result<usize> {
        let n = if let Some(pid) = instance_id {
            run_exec_n(
                self,
                sqlx::query(
                    "UPDATE jobs
                     SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
                         last_error = CASE
                           WHEN attempts >= max_attempts THEN 'job lease expired after max attempts'
                           ELSE last_error
                         END,
                         locked_by = NULL,
                         locked_until = NULL
                     WHERE process_instance_id = $1::uuid AND status = 'locked'
                       AND locked_until < to_timestamp($2::double precision / 1000.0)",
                )
                .bind(pid)
                .bind(now),
            )?
        } else {
            run_exec_n(
                self,
                sqlx::query(
                    "UPDATE jobs
                     SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
                         last_error = CASE
                           WHEN attempts >= max_attempts THEN 'job lease expired after max attempts'
                           ELSE last_error
                         END,
                         locked_by = NULL,
                         locked_until = NULL
                     WHERE id IN (
                        SELECT id FROM jobs
                        WHERE status = 'locked'
                          AND locked_until < to_timestamp($1::double precision / 1000.0)
                        ORDER BY locked_until ASC
                        LIMIT $2
                        FOR UPDATE SKIP LOCKED
                     )",
                )
                .bind(now)
                .bind(batch as i64),
            )?
        };
        Ok(n as usize)
    }

    fn open_jobs_for_instance(&mut self, instance_id: &str) -> Result<Vec<Job>> {
        let this = self;
        list_jobs(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, type AS job_type, extract(epoch from due_at)*1000 AS due_at, status, locked_by, extract(epoch from locked_until)*1000 AS locked_until, attempts, max_attempts, payload::text AS payload, last_error, extract(epoch from created_at)*1000 AS created_at, extract(epoch from updated_at)*1000 AS updated_at, extract(epoch from completed_at)*1000 AS completed_at FROM jobs WHERE process_instance_id = $1::uuid AND status IN ('pending','locked')",
            instance_id,
        )
    }

    fn open_jobs_for_token(&mut self, token_id: &str) -> Result<Vec<Job>> {
        let this = self;
        list_jobs(
            this,
            "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, type AS job_type, extract(epoch from due_at)*1000 AS due_at, status, locked_by, extract(epoch from locked_until)*1000 AS locked_until, attempts, max_attempts, payload::text AS payload, last_error, extract(epoch from created_at)*1000 AS created_at, extract(epoch from updated_at)*1000 AS updated_at, extract(epoch from completed_at)*1000 AS completed_at FROM jobs WHERE token_id = $1::uuid AND status IN ('pending','locked')",
            token_id,
        )
    }

    fn list_overdue_jobs(&mut self, now: i64, limit: usize) -> Result<Vec<Job>> {
        let this = self;
        let rows = fetch_all_q(
            this,
            sqlx::query(
                "SELECT id::text AS id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id, token_id::text AS token_id, type AS job_type, extract(epoch from due_at)*1000 AS due_at, status, locked_by, extract(epoch from locked_until)*1000 AS locked_until, attempts, max_attempts, payload::text AS payload, last_error, extract(epoch from created_at)*1000 AS created_at, extract(epoch from updated_at)*1000 AS updated_at, extract(epoch from completed_at)*1000 AS completed_at FROM jobs WHERE status = 'pending' AND due_at < to_timestamp($1::double precision / 1000.0) ORDER BY due_at ASC LIMIT $2",
            )
            .bind(now)
            .bind(limit as i64),
        )?;
        rows.iter().map(map_job).collect()
    }

    fn insert_event(&mut self, event: ProcessEvent) -> Result<()> {
        let data = stringify(&event.data);
        run_exec(
            self,
            sqlx::query(
                "INSERT INTO process_events (
                    tenant_id, process_instance_id, token_id, task_id, job_id,
                    event_type, node_id, actor, data
                 ) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6, $7, $8, $9::jsonb)",
            )
            .bind(&event.tenant_id)
            .bind(&event.process_instance_id)
            .bind(&event.token_id)
            .bind(&event.task_id)
            .bind(&event.job_id)
            .bind(&event.event_type)
            .bind(&event.node_id)
            .bind(&event.actor)
            .bind(data),
        )?;
        // Observer-only. Never fail the engine step. The statement is the flight recorder's, shared with Forge's
        // observer, so the replay backstop (`on conflict (source_system, source_event_id)` against migration 090's
        // partial unique index) has one spelling rather than three. This writer is the `workflow` system in
        // `workflow_engine`, records the event type as the row's summary, names its timer job and correlates by
        // nothing — Forge's observer does the reverse two. The parameter order is
        // `crate::sql_contract::TRACE_EVENT_INSERT_SQL`'s contract.
        let source_event_id = format!(
            "engine:{}:{}:{}:{}",
            event.process_instance_id,
            event.event_type,
            event.node_id.as_deref().unwrap_or("-"),
            event.created_at
        );
        let _ = run_exec(
            self,
            sqlx::query(crate::sql_contract::TRACE_EVENT_INSERT_SQL)
                .bind(&event.event_type)
                .bind("workflow")
                .bind(&event.event_type)
                .bind("workflow_engine")
                .bind(&source_event_id)
                .bind(&event.process_instance_id)
                .bind(event.node_id.as_deref())
                .bind(event.task_id.as_deref())
                .bind(event.job_id.as_deref())
                .bind(None::<&str>),
        );
        Ok(())
    }

    fn history(&mut self, instance_id: &str, limit: usize) -> Result<Vec<ProcessEvent>> {
        let this = self;
        let sql = "SELECT id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id,
                    token_id::text AS token_id, task_id::text AS task_id, job_id::text AS job_id,
                    event_type, node_id, actor, data::text AS data,
                    extract(epoch from created_at)*1000 AS created_at
                 FROM process_events
                 WHERE process_instance_id = $1::uuid
                 ORDER BY created_at DESC, id DESC
                 LIMIT $2";
        let rows = fetch_all_q(this, sqlx::query(sql).bind(instance_id).bind(limit as i64))?;
        Ok(rows
            .iter()
            .map(|row| ProcessEvent {
                id: s_i64(row, "id"),
                tenant_id: s_opt(row, "tenant_id"),
                process_instance_id: s_get(row, "process_instance_id"),
                token_id: s_opt(row, "token_id"),
                task_id: s_opt(row, "task_id"),
                job_id: s_opt(row, "job_id"),
                event_type: s_get(row, "event_type"),
                node_id: s_opt(row, "node_id"),
                actor: s_get(row, "actor"),
                data: json_col(row, "data"),
                created_at: s_f64(row, "created_at"),
            })
            .collect())
    }

    fn history_after_event_id(
        &mut self,
        instance_id: &str,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<ProcessEvent>> {
        let this = self;
        let sql = "SELECT id, tenant_id::text AS tenant_id, process_instance_id::text AS process_instance_id,
                    token_id::text AS token_id, task_id::text AS task_id, job_id::text AS job_id,
                    event_type, node_id, actor, data::text AS data,
                    extract(epoch from created_at)*1000 AS created_at
                 FROM process_events
                 WHERE process_instance_id = $1::uuid AND id > $2
                 ORDER BY id ASC
                 LIMIT $3";
        let rows = fetch_all_q(
            this,
            sqlx::query(sql)
                .bind(instance_id)
                .bind(after_event_id)
                .bind(limit as i64),
        )?;
        Ok(rows
            .iter()
            .map(|row| ProcessEvent {
                id: s_i64(row, "id"),
                tenant_id: s_opt(row, "tenant_id"),
                process_instance_id: s_get(row, "process_instance_id"),
                token_id: s_opt(row, "token_id"),
                task_id: s_opt(row, "task_id"),
                job_id: s_opt(row, "job_id"),
                event_type: s_get(row, "event_type"),
                node_id: s_opt(row, "node_id"),
                actor: s_get(row, "actor"),
                data: json_col(row, "data"),
                created_at: s_f64(row, "created_at"),
            })
            .collect())
    }

    fn command_visit_count(&mut self, instance_id: &str, node_id: &str) -> Result<i32> {
        let this = self;
        let row = fetch_one_q(
            this,
            sqlx::query(
                "SELECT count(*)::int AS visit_count FROM process_commands
                 WHERE process_instance_id = $1::uuid AND node_id = $2",
            )
            .bind(instance_id)
            .bind(node_id),
        )?;
        Ok(row.try_get("visit_count").unwrap_or(0))
    }

    fn insert_command(&mut self, cmd: ProcessCommand) -> Result<()> {
        let input = stringify(&cmd.input);
        run_exec(
            self,
            sqlx::query(
                "INSERT INTO process_commands (
                    process_instance_id, token_id, node_id, visit_sequence, command_id,
                    command_type, subject_type, subject_id, correlation_id, causation_id,
                    input, outcome, message
                 ) VALUES (
                    $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13
                 )",
            )
            .bind(&cmd.process_instance_id)
            .bind(&cmd.token_id)
            .bind(&cmd.node_id)
            .bind(cmd.visit_sequence)
            .bind(&cmd.command_id)
            .bind(&cmd.command_type)
            .bind(&cmd.subject_type)
            .bind(&cmd.subject_id)
            .bind(&cmd.correlation_id)
            .bind(&cmd.causation_id)
            .bind(input)
            .bind(&cmd.outcome)
            .bind(&cmd.message),
        )
    }

    fn find_instances(
        &mut self,
        tenant_id: Option<&str>,
        status: Option<&[ProcessStatus]>,
        definition_key: Option<&str>,
        business_key: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProcessInstance>> {
        let this = self;
        let mut qb = QueryBuilder::<Postgres>::new(
            "SELECT pi.id::text AS id, pi.tenant_id::text AS tenant_id, \
             pi.definition_id::text AS definition_id, pi.business_key, \
             pi.status, pi.outcome, \
             extract(epoch from pi.started_at)*1000 AS started_at, \
             extract(epoch from pi.ended_at)*1000 AS ended_at, \
             pi.started_by, pi.parent_instance_id::text AS parent_instance_id, \
             pi.root_token_id::text AS root_token_id, \
             pi.subject_type, pi.subject_id, pi.variables::text AS variables, pi.version \
             FROM process_instances pi \
             JOIN process_definitions pd ON pd.id = pi.definition_id \
             WHERE 1=1",
        );
        if let Some(tid) = tenant_id {
            qb.push(" AND pi.tenant_id = ");
            qb.push_bind(tid);
            qb.push("::uuid");
        }
        if let Some(key) = definition_key {
            qb.push(" AND pd.key = ");
            qb.push_bind(key);
        }
        if let Some(bk) = business_key {
            qb.push(" AND pi.business_key = ");
            qb.push_bind(bk);
        }
        if let Some(ss) = status {
            let labels: Vec<String> = ss
                .iter()
                .copied()
                .map(|s| process_status(s).to_string())
                .collect();
            qb.push(" AND pi.status = ANY(");
            qb.push_bind(labels);
            qb.push(")");
        }
        qb.push(" ORDER BY pi.started_at DESC LIMIT ");
        qb.push_bind(limit as i64);
        qb.push(" OFFSET ");
        qb.push_bind(offset as i64);
        let query = qb.build();
        let handle = this.handle.clone();
        let conn = this.conn();
        let rows = handle
            .block_on(query.fetch_all(conn))
            .map_err(|e| WorkflowError::generic(e.to_string()))?;
        rows.iter().map(map_instance).collect()
    }
}
