//! In-process concurrency for the workflow engine.
//!
//! What this module guarantees:
//! - **Multithreaded:** `run_bounded` runs independent steps on a thread pool.
//!   `WorkflowEngine` and `TxStore` are `Send + Sync`, so one engine is shared.
//! - **Thread-safe:** no engine field is mutated without a lock or a fresh
//!   transaction. Neon takes one pool connection per `with_tx`. Memory takes
//!   the store mutex for the duration of the step.
//! - **ACID per step:** one `with_tx` = one BEGIN/COMMIT (or ROLLBACK). Two
//!   threads never share a transaction. Isolation on Neon is Postgres
//!   READ COMMITTED plus `FOR UPDATE` and task CAS. A whole Forge *run*
//!   (claim → role → receipt) is still several steps, not one transaction.
//!
//! What this module does not claim: an async `Store` trait, SERIALIZABLE
//! isolation through the Neon pooler, or one ACID unit for an entire story.

use crate::error::{Result, WorkflowError};

/// How many due jobs one `run_due_jobs` pass may fire at once.
/// Default 4; `FORGE_JOB_WORKERS` overrides; clamp 1..=16 so a mistype cannot
/// open a thread per row against a 5-connection DEV pool.
pub fn job_workers() -> usize {
    job_workers_from(std::env::var("FORGE_JOB_WORKERS").ok().as_deref())
}

pub fn job_workers_from(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
        .clamp(1, 16)
}

/// Run `f` over `items` with at most `workers` threads at a time.
/// Order of results matches `items`. A panicked worker becomes `Generic`.
pub fn run_bounded<T, R, F>(items: &[T], workers: usize, f: F) -> Vec<Result<R>>
where
    T: Sync,
    R: Send,
    F: Fn(&T) -> Result<R> + Sync,
{
    if items.is_empty() {
        return Vec::new();
    }
    let workers = workers.max(1).min(items.len());
    let mut out = Vec::with_capacity(items.len());
    let mut index = 0;
    while index < items.len() {
        let end = (index + workers).min(items.len());
        let slice = &items[index..end];
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(slice.len());
            for item in slice {
                handles.push(scope.spawn(|| f(item)));
            }
            for handle in handles {
                out.push(
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(WorkflowError::generic("engine worker panicked"))),
                );
            }
        });
        index = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryStore;
    use crate::types::*;
    use crate::value::Value;
    use crate::{EngineOptions, WorkflowEngine};

    fn ready_task(id: &str, version: i32) -> Task {
        Task {
            id: id.into(),
            tenant_id: None,
            process_instance_id: "pi-1".into(),
            token_id: Some("tok-1".into()),
            node_id: Some("smith".into()),
            name: "smith".into(),
            description: None,
            status: TaskStatus::Ready,
            assignee: None,
            candidates: vec![],
            swimlane: None,
            priority: 0,
            due_date: None,
            form_key: None,
            form_data: Value::Null,
            created_at: 0,
            claimed_at: None,
            completed_at: None,
            completed_by: None,
            version,
        }
    }

    #[test]
    fn job_worker_clamp() {
        assert_eq!(job_workers_from(None), 4);
        assert_eq!(job_workers_from(Some("")), 4);
        assert_eq!(job_workers_from(Some("0")), 4);
        assert_eq!(job_workers_from(Some("2")), 2);
        assert_eq!(job_workers_from(Some("99")), 16);
    }

    #[test]
    fn engine_and_stores_are_shareable_across_threads() {
        fn assert_shareable<T: Send + Sync>() {}
        assert_shareable::<MemoryStore>();
        assert_shareable::<WorkflowEngine<MemoryStore>>();
    }

    #[test]
    fn two_threads_cas_the_same_row_and_exactly_one_wins() {
        let store = MemoryStore::new();
        store
            .with_tx(|tx| {
                tx.insert_task(ready_task("t-race", 1))?;
                Ok(())
            })
            .unwrap();

        let results = run_bounded(&[1_i32, 2], 2, |_| {
            store.with_tx(|tx| {
                let mut next = ready_task("t-race", 2);
                next.status = TaskStatus::Obsolete;
                Ok(tx.cas_task(&next)?)
            })
        });
        let wins = results
            .iter()
            .map(|row| row.as_ref().expect("step"))
            .filter(|won| **won)
            .count();
        assert_eq!(wins, 1, "CAS must admit one writer: {results:?}");
    }

    #[test]
    fn two_threads_can_write_distinct_rows_without_losing_either() {
        let store = MemoryStore::new();
        let results = run_bounded(&["a", "b"], 2, |id| {
            store.with_tx(|tx| {
                tx.insert_task(ready_task(&format!("t-{id}"), 1))?;
                Ok(())
            })
        });
        assert!(results.iter().all(|row| row.is_ok()), "{results:?}");
        store
            .with_tx(|tx| {
                assert_eq!(tx.get_task("t-a")?.version, 1);
                assert_eq!(tx.get_task("t-b")?.version, 1);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn a_panicked_worker_is_a_step_failure_not_a_process_abort() {
        let results = run_bounded(&[1, 2], 2, |n| {
            if *n == 1 {
                panic!("boom");
            }
            Ok(*n)
        });
        assert!(results[0].is_err(), "{results:?}");
        assert_eq!(results[1].as_ref().unwrap(), &2);
    }

    #[test]
    fn a_default_engine_is_constructible_on_the_shared_clock() {
        let _engine = WorkflowEngine::new(MemoryStore::new(), EngineOptions::default());
    }
}
