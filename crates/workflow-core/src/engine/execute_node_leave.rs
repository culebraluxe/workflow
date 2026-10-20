//! Moved from `engine.rs` (move only): execute_node_leave.

#[allow(unused_imports)]
use super::*;

impl<S: TxStore> WorkflowEngine<S> {
    pub(super) fn execute_node_leave(
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

        let no_transitions = node
            .transitions
            .as_ref()
            .map(|t| t.is_empty())
            .unwrap_or(true);
        if node.node_type == "end"
            || (no_transitions
                && node.node_type != "timer"
                && node.node_type != "command"
                && node.node_type != "dynamic-fork")
        {
            let end_outcome = if node.node_type == "end" {
                node.outcome.unwrap_or(ProcessOutcome::Completed)
            } else {
                ProcessOutcome::Completed
            };
            self.complete_token(tx, token, actor, token_outcome_for_end(end_outcome))?;
            return self.resolve_process_after_token(tx, &instance.id, actor, token, end_outcome);
        }

        match node.node_type.as_str() {
            "timer" => self.handle_timer(tx, token, node, instance, actor, variables),
            "command" => self.handle_command(tx, token, node, instance, graph, actor, variables),
            "decision" => {
                let vars = if node.refresh_facts != Some(false) && self.app.is_some() {
                    self.refresh_facts(tx, instance, variables)?
                } else {
                    variables.clone()
                };
                let chosen = self
                    .evaluate_decision(node, &vars, preferred)
                    .ok_or_else(|| {
                        WorkflowError::generic(format!(
                            "No valid transition from decision node {}",
                            node.id
                        ))
                    })?;
                self.move_token(tx, token, &chosen.to, &chosen.name, actor)?;
                let fresh = tx.get_token(&token.id)?;
                self.arrive_at_node(tx, &fresh, instance, graph, actor, None, &vars)
            }
            "fork" => self.handle_fork(tx, token, node, instance, graph, actor, variables),
            "dynamic-fork" => {
                self.handle_dynamic_fork(tx, token, node, instance, graph, actor, variables)
            }
            "join" => self.handle_join(tx, token, node, instance, graph, actor, variables),
            _ => {
                let transition = if let Some(name) = preferred {
                    node.transitions
                        .as_ref()
                        .and_then(|ts| ts.iter().find(|t| t.name == name))
                        .cloned()
                } else {
                    node.transitions.as_ref().and_then(|ts| ts.first().cloned())
                }
                .ok_or_else(|| {
                    WorkflowError::generic(format!("No transition found from node {}", node.id))
                })?;
                self.move_token(tx, token, &transition.to, &transition.name, actor)?;
                let fresh = tx.get_token(&token.id)?;
                self.arrive_at_node(tx, &fresh, instance, graph, actor, None, variables)
            }
        }
    }

    pub(super) fn move_token(
        &self,
        tx: &mut dyn Store,
        token: &Token,
        to_node: &str,
        transition_name: &str,
        actor: &str,
    ) -> Result<()> {
        if !tx.move_token(&token.id, token.version, to_node)? {
            return Err(WorkflowError::stale_token(format!(
                "Token {} moved concurrently (expected version {})",
                token.id, token.version
            )));
        }
        self.event(
            tx,
            EventInput {
                tenant_id: token.tenant_id.clone(),
                process_instance_id: token.process_instance_id.clone(),
                token_id: Some(token.id.clone()),
                event_type: "token.moved",
                node_id: Some(to_node.to_string()),
                actor: actor.to_string(),
                data: json!({"from": token.node_id, "transition": transition_name}),
                ..Default::default()
            },
        )
    }

    pub(super) fn complete_token(
        &self,
        tx: &mut dyn Store,
        token: &Token,
        actor: &str,
        outcome: TokenOutcome,
    ) -> Result<()> {
        tx.complete_token(&token.id, outcome, self.now())?;
        self.event(
            tx,
            EventInput {
                tenant_id: token.tenant_id.clone(),
                process_instance_id: token.process_instance_id.clone(),
                token_id: Some(token.id.clone()),
                event_type: "token.completed",
                node_id: Some(token.node_id.clone()),
                actor: actor.to_string(),
                data: json!({"outcome": format!("{:?}", outcome).to_lowercase()}),
                ..Default::default()
            },
        )
    }

    /// Mark a task obsolete. A failed CAS is a hard error: the event log must not claim
    /// `task.obsoleted` when the row is still Ready/Claimed.
    pub(super) fn obsolete_task(
        &self,
        tx: &mut dyn Store,
        task: &Task,
        actor: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        let mut next = task.clone();
        next.status = TaskStatus::Obsolete;
        next.version += 1;
        if !tx.cas_task(&next)? {
            return Err(WorkflowError::conflict(
                "STALE_TASK",
                format!(
                    "Task {} could not be obsoleted (expected version {})",
                    task.id, task.version
                ),
            ));
        }
        self.event(
            tx,
            EventInput {
                process_instance_id: task.process_instance_id.clone(),
                task_id: Some(task.id.clone()),
                event_type: "task.obsoleted",
                actor: actor.to_string(),
                data: json!({"reason": reason}),
                ..Default::default()
            },
        )
    }

    pub(super) fn check_process_completion(
        &self,
        tx: &mut dyn Store,
        process_instance_id: &str,
        actor: &str,
    ) -> Result<()> {
        let guard = tx.lock_instance(process_instance_id)?;
        if guard.status != ProcessStatus::Active {
            return Ok(());
        }
        if tx.count_active_tokens(process_instance_id)? == 0 {
            tx.terminate_instance(
                process_instance_id,
                ProcessStatus::Completed,
                ProcessOutcome::Completed,
                self.now(),
            )?;
            self.event(
                tx,
                EventInput {
                    process_instance_id: process_instance_id.to_string(),
                    event_type: "process.completed",
                    actor: actor.to_string(),
                    data: json!({"outcome": "completed"}),
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }

    pub(super) fn terminate_process(
        &self,
        tx: &mut dyn Store,
        process_instance_id: &str,
        actor: &str,
        outcome: ProcessOutcome,
        reason: Option<&str>,
    ) -> Result<()> {
        let guard = tx.lock_instance(process_instance_id)?;
        if guard.status != ProcessStatus::Active {
            return Ok(());
        }
        let status = match outcome {
            ProcessOutcome::Cancelled => ProcessStatus::Aborted,
            ProcessOutcome::Completed => ProcessStatus::Completed,
            _ => ProcessStatus::Error,
        };
        tx.terminate_instance(process_instance_id, status, outcome, self.now())?;

        for token in tx.list_active_tokens(process_instance_id)? {
            tx.complete_token(&token.id, TokenOutcome::Cancelled, self.now())?;
            self.event(
                tx,
                EventInput {
                    tenant_id: token.tenant_id,
                    process_instance_id: process_instance_id.to_string(),
                    token_id: Some(token.id),
                    event_type: "token.cancelled",
                    node_id: Some(token.node_id),
                    actor: actor.to_string(),
                    data: json!({"reason": reason}),
                    ..Default::default()
                },
            )?;
        }

        for task in tx.open_tasks_for_instance(process_instance_id)? {
            self.obsolete_task(tx, &task, actor, reason)?;
        }

        for mut job in tx.open_jobs_for_instance(process_instance_id)? {
            job.status = JobStatus::Cancelled;
            job.locked_by = None;
            job.locked_until = None;
            tx.update_job(&job)?;
            self.event(
                tx,
                EventInput {
                    process_instance_id: process_instance_id.to_string(),
                    token_id: job.token_id,
                    job_id: Some(job.id),
                    event_type: "job.cancelled",
                    actor: actor.to_string(),
                    data: json!({"type": job.job_type, "reason": reason}),
                    ..Default::default()
                },
            )?;
        }

        let event_type = match outcome {
            ProcessOutcome::Cancelled => "process.cancelled",
            ProcessOutcome::Failed => "process.failed",
            _ => "process.conflict",
        };
        self.event(
            tx,
            EventInput {
                process_instance_id: process_instance_id.to_string(),
                event_type,
                actor: actor.to_string(),
                data: json!({"outcome": format!("{:?}", outcome).to_lowercase(), "reason": reason}),
                ..Default::default()
            },
        )
    }

    pub(super) fn resolve_process_after_token(
        &self,
        tx: &mut dyn Store,
        process_instance_id: &str,
        actor: &str,
        token: &Token,
        end_outcome: ProcessOutcome,
    ) -> Result<()> {
        if end_outcome == ProcessOutcome::Completed || !token.required {
            return self.check_process_completion(tx, process_instance_id, actor);
        }
        self.terminate_process(
            tx,
            process_instance_id,
            actor,
            end_outcome,
            Some(&format!("end node outcome: {:?}", end_outcome)),
        )
    }

    pub(super) fn create_human_task(
        &self,
        tx: &mut dyn Store,
        instance: &ProcessInstance,
        token_id: &str,
        node: &NodeDefinition,
        actor: &str,
    ) -> Result<()> {
        let candidates = node.candidate_groups.clone().unwrap_or_default();
        let task = Task {
            id: tx.new_id("task"),
            tenant_id: instance.tenant_id.clone(),
            process_instance_id: instance.id.clone(),
            token_id: Some(token_id.to_string()),
            node_id: Some(node.id.clone()),
            name: node.name.clone().unwrap_or_else(|| node.id.clone()),
            description: node.description.clone(),
            status: TaskStatus::Ready,
            assignee: None,
            candidates: candidates.clone(),
            swimlane: None,
            priority: node.priority.unwrap_or(0),
            due_date: None,
            form_key: node.form_key.clone(),
            form_data: json!({}),
            created_at: self.now(),
            claimed_at: None,
            completed_at: None,
            completed_by: None,
            version: 1,
        };
        let inserted = tx.insert_task(task)?;
        self.event(
            tx,
            EventInput {
                process_instance_id: instance.id.clone(),
                token_id: Some(token_id.to_string()),
                task_id: Some(inserted.id),
                event_type: "task.created",
                node_id: Some(node.id.clone()),
                actor: actor.to_string(),
                data: json!({"name": node.name, "candidates": candidates}),
                ..Default::default()
            },
        )
    }

    pub(super) fn evaluate_decision(
        &self,
        node: &NodeDefinition,
        variables: &Value,
        preferred: Option<&str>,
    ) -> Option<TransitionDefinition> {
        if let Some(name) = preferred {
            return node
                .transitions
                .as_ref()
                .and_then(|ts| ts.iter().find(|t| t.name == name).cloned());
        }
        if let Some(decisions) = &node.decisions {
            for d in decisions {
                if evaluate_condition(&d.condition, variables).unwrap_or(false) {
                    return node
                        .transitions
                        .as_ref()
                        .and_then(|ts| ts.iter().find(|t| t.name == d.transition).cloned());
                }
            }
            if !decisions.is_empty() {
                return otherwise_transition(node, decisions);
            }
        }
        node.transitions.as_ref().and_then(|ts| ts.first().cloned())
    }

    pub(super) fn handle_fork(
        &self,
        tx: &mut dyn Store,
        parent: &Token,
        node: &NodeDefinition,
        instance: &ProcessInstance,
        graph: &ProcessGraph,
        actor: &str,
        variables: &Value,
    ) -> Result<()> {
        self.complete_token(tx, parent, actor, TokenOutcome::Completed)?;
        for transition in node.transitions.clone().unwrap_or_default() {
            let live = tx.lock_instance(&parent.process_instance_id)?;
            if live.status != ProcessStatus::Active {
                break;
            }
            let required = transition.required.unwrap_or(true);
            let child = Token {
                id: tx.new_id("tok"),
                tenant_id: parent.tenant_id.clone(),
                process_instance_id: parent.process_instance_id.clone(),
                parent_token_id: Some(parent.id.clone()),
                node_id: transition.to.clone(),
                status: TokenStatus::Active,
                outcome: None,
                required,
                is_able_to_reactivate_parent: true,
                started_at: self.now(),
                ended_at: None,
                version: 1,
            };
            let child = tx.insert_token(child)?;
            self.event(
                tx,
                EventInput {
                    process_instance_id: instance.id.clone(),
                    token_id: Some(child.id.clone()),
                    event_type: "token.forked",
                    node_id: Some(transition.to.clone()),
                    actor: actor.to_string(),
                    data: json!({"parentTokenId": parent.id, "transition": transition.name, "required": required}),
                    ..Default::default()
                },
            )?;
            self.arrive_at_node(tx, &child, instance, graph, actor, None, variables)?;
        }
        Ok(())
    }

    pub(super) fn handle_dynamic_fork(
        &self,
        tx: &mut dyn Store,
        parent: &Token,
        node: &NodeDefinition,
        instance: &ProcessInstance,
        graph: &ProcessGraph,
        actor: &str,
        variables: &Value,
    ) -> Result<()> {
        self.complete_token(tx, parent, actor, TokenOutcome::Completed)?;
        let minimum = node.minimum.unwrap_or(2);
        let maximum = node.maximum.unwrap_or(8);
        let raw = node
            .count_variable
            .as_ref()
            .and_then(|k| variables.get(k))
            .and_then(|v| v.as_i64())
            .unwrap_or(minimum as i64) as i32;
        let count = raw.clamp(minimum, maximum);
        let branch_target = node
            .branch_node
            .clone()
            .or_else(|| node.join.clone())
            .ok_or_else(|| {
                WorkflowError::generic(format!(
                    "dynamic-fork node {} has no valid branch or join target",
                    node.id
                ))
            })?;
        if !graph.nodes.contains_key(&branch_target) {
            return Err(WorkflowError::generic(format!(
                "dynamic-fork node {} has no valid branch or join target",
                node.id
            )));
        }

        let mut children = Vec::new();
        for i in 0..count {
            let live = tx.lock_instance(&parent.process_instance_id)?;
            if live.status != ProcessStatus::Active {
                break;
            }
            let child = Token {
                id: tx.new_id("tok"),
                tenant_id: parent.tenant_id.clone(),
                process_instance_id: parent.process_instance_id.clone(),
                parent_token_id: Some(parent.id.clone()),
                node_id: branch_target.clone(),
                status: TokenStatus::Active,
                outcome: None,
                required: true,
                is_able_to_reactivate_parent: true,
                started_at: self.now(),
                ended_at: None,
                version: 1,
            };
            let child = tx.insert_token(child)?;
            self.event(
                tx,
                EventInput {
                    process_instance_id: instance.id.clone(),
                    token_id: Some(child.id.clone()),
                    event_type: "token.forked",
                    node_id: Some(branch_target.clone()),
                    actor: actor.to_string(),
                    data: json!({"parentTokenId": parent.id, "branchIndex": i, "dynamicCount": count}),
                    ..Default::default()
                },
            )?;
            children.push(child);
        }

        for (branch_index, child) in children.iter().enumerate() {
            let live = tx.lock_instance(&parent.process_instance_id)?;
            if live.status != ProcessStatus::Active {
                break;
            }
            self.arrive_at_node(tx, child, instance, graph, actor, None, variables)?;
            if graph
                .nodes
                .get(&branch_target)
                .map(|n| n.node_type.as_str())
                == Some("task")
            {
                let plan = node
                    .plan_variable
                    .as_ref()
                    .and_then(|k| variables.get(k))
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.get(branch_index))
                    .cloned()
                    .unwrap_or(Value::Null);
                tx.patch_ready_task_form(
                    &child.id,
                    json!({
                        "splitBranchIndex": branch_index,
                        "splitBranchCount": count,
                        "splitBranch": plan,
                    }),
                )?;
            }
        }
        Ok(())
    }
}

/// The transition a decision takes when none of its conditions matched: its OTHERWISE, which is the transition no
/// condition names. `None` when every transition is guarded — the decision has no answer, and the caller refuses
/// ("No valid transition from decision node …") instead of inventing one.
///
/// It used to be the FIRST transition, whatever it was. A definition that names every branch then answered an
/// unknown fact with its first branch: Forge's `execution_shape` sent every story with no Lead decision to SOLO,
/// and `qa_failure_route` — whose only unguarded transition is `hold`, by design, so an exhausted repair budget
/// stops — sent every QA failure back to Smith with no bound. A definition whose otherwise IS its first transition
/// (every decision in the RE supermodel) behaves exactly as before.
fn otherwise_transition(
    node: &NodeDefinition,
    decisions: &[DecisionArm],
) -> Option<TransitionDefinition> {
    node.transitions.as_ref().and_then(|transitions| {
        transitions
            .iter()
            .find(|t| !decisions.iter().any(|d| d.transition == t.name))
            .cloned()
    })
}

#[cfg(test)]
mod otherwise_tests {
    use super::*;

    fn decision(arms: &[(&str, &str)], transitions: &[&str]) -> NodeDefinition {
        NodeDefinition {
            id: "d".into(),
            node_type: "decision".into(),
            decisions: Some(
                arms.iter()
                    .map(|(condition, transition)| DecisionArm {
                        condition: (*condition).into(),
                        transition: (*transition).into(),
                    })
                    .collect(),
            ),
            transitions: Some(
                transitions
                    .iter()
                    .map(|name| TransitionDefinition {
                        name: (*name).into(),
                        to: format!("to_{name}"),
                        condition: None,
                        required: None,
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// Forge's `qa_failure_route`: both eligibilities false matches nothing, and the designed answer is its one
    /// unguarded transition, `hold` — not its first, `smith`, which made the repair budget unable to stop anything.
    #[test]
    fn no_match_takes_the_unguarded_otherwise_not_the_first_transition() {
        let node = decision(
            &[
                ("qaRepairEligible == true", "smith"),
                ("qaReplanEligible == true", "architect"),
            ],
            &["smith", "architect", "hold"],
        );
        let chosen = otherwise_transition(&node, node.decisions.as_deref().unwrap());
        assert_eq!(chosen.map(|t| t.name).as_deref(), Some("hold"));
    }

    /// Forge's `execution_shape`: every branch guarded, so an absent decision has no answer and the caller refuses.
    #[test]
    fn a_fully_guarded_decision_with_no_match_has_no_answer() {
        let node = decision(
            &[
                ("leadDecision == 'SOLO'", "solo"),
                ("leadDecision == 'SMITH'", "smith"),
            ],
            &["solo", "smith"],
        );
        assert!(otherwise_transition(&node, node.decisions.as_deref().unwrap()).is_none());
    }

    /// The RE supermodel's shape — the otherwise IS the first transition — routes exactly as before.
    #[test]
    fn an_otherwise_that_is_also_first_is_unchanged() {
        let node = decision(&[("inspectionRequired == true", "run")], &["skip", "run"]);
        let chosen = otherwise_transition(&node, node.decisions.as_deref().unwrap());
        assert_eq!(chosen.map(|t| t.name).as_deref(), Some("skip"));
    }
}
