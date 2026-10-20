//! NeonStore and the transaction/query helpers, over the workflow project's own `neon_db` layer.
//! Callers pass their existing `PgPool` via `NeonStore::from_pool`; standalone tools may use
//! `NeonStore::connect_from_env`.

#[allow(unused_imports)]
use super::*;
use crate::neon_db::{
    policy, sleep_before_retry, FailureKind, NeonDb, NeonFailure, NeonTransaction,
};
use sqlx::PgPool;

pub struct NeonStore {
    pub(super) db: NeonDb,
    pub(super) rt: &'static tokio::runtime::Runtime,
}

/// The operation name every statement of an engine step is reported under. It is the name an operator sees in
/// `app_error` and in the flight recorder, so the transaction, the body's statements and the rollback all carry the
/// same one: "workflow.step" is the thing that failed, not whichever query happened to send first after the socket died.
const WORKFLOW_STEP: &str = "workflow.step";

/// The runtime every store shares.
///
/// This used to be built PER STORE with `worker_threads(1)`, which is why the engine could not overlap database work:
/// every call in the process funnelled through a single worker thread. A shared multi-threaded runtime is the smallest
/// change that removes that funnel without touching the synchronous `Store` trait - the trait still blocks its caller,
/// but the database work itself now spreads across workers instead of queueing behind one.
///
/// The larger fix is making `Store`/`TxStore` async (45 methods) so nothing blocks at all; the engine being a
/// synchronous process per call is the second half of the same problem, and the shape for that is a long-running
/// engine service rather than a spawned binary.
pub(super) fn shared_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("workflow runtime")
    })
}

impl NeonStore {
    /// Build a store over the caller's pool. Hosts pass `db.pool().clone()` so one process keeps
    /// one pool; the store never opens a second one.
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            db: NeonDb::from_pool(pool),
            rt: shared_runtime(),
        }
    }

    pub fn connect_from_env() -> Result<Self> {
        let rt = shared_runtime();
        // Standalone tools (the `workflow` binary) may have no composition root; one pool is opened
        // for this process just as the app's `db::shared::get_or_connect` used to do.
        let db = rt
            .block_on(NeonDb::connect_from_env())
            .map_err(|e| WorkflowError::generic(e.to_string()))?;
        Ok(Self { db, rt })
    }

    pub fn ping(&self) -> Result<()> {
        self.rt
            .block_on(self.db.ping())
            .map_err(|e| WorkflowError::generic(e.to_string()))
    }

    pub fn pool(&self) -> &PgPool {
        self.db.pool()
    }
}

impl TxStore for NeonStore {
    /// One step = one transaction, and a broken CONNECTION is repeated instead of ending the run.
    ///
    /// WHY THIS EXISTS (2026-09-29, 19:25Z). A run that had been going for two hours and fifty-three minutes died
    /// with `error communicating with database: Broken pipe (os error 32); and rolling this step's transaction back
    /// failed too: DatabaseUnavailable during workflow.step`. The step body failed on a socket that was already gone,
    /// and a failed body was the end of the run: the flight recorder row carries no verdict at all (`result_status`,
    /// `failure_code`, `tests_*` and even `cost_usd` are NULL), the work item went back to `Ready`, and the queue
    /// stopped advancing until a human restarted it. A database hiccup cost three hours of work and the progress
    /// report that would have explained it.
    ///
    /// ONLY THE BODY of a step is repeated, and that is safe for a reason rather than by hope: `step_once` reaches the
    /// repeat path only when no `COMMIT` was sent, so the server holds nothing of the attempt and the second attempt
    /// cannot double-apply the step. Two neighbours are deliberately excluded:
    ///   * a failed `BEGIN` is already retried inside `Database::begin`, against the same classification;
    ///   * a failed `COMMIT` is NOT repeated, because the work may have been applied and only the acknowledgement
    ///     lost, and repeating it could apply the step twice. That ambiguity is the caller's to resolve, so the commit
    ///     failure stays `Generic` and stops here.
    ///
    /// The attempts and the wait come from the db crate's own policy (`FORGE_DB_RETRY_ATTEMPTS`, default 3, with
    /// backoff), so there is one retry policy in the workspace rather than two. The DECISION to repeat is
    /// `store::repeat_connection_failures`, which is unit-tested without a database — this function supplies the step
    /// and the real wait, and nothing else. Each failed attempt has already been announced as an `app_error` by the
    /// `DbFailure` that classified it, so a database that is genuinely down is visible rather than silently retried.
    fn with_tx<R, F>(&self, f: F) -> Result<R>
    where
        F: FnMut(&mut dyn Store) -> Result<R>,
    {
        let policy = policy();
        let mut f = f;
        crate::store::repeat_connection_failures(
            || self.step_once(&mut f),
            policy.attempts,
            |attempt| {
                self.rt.block_on(sleep_before_retry(policy, attempt));
            },
        )
    }
}

impl NeonStore {
    /// One attempt at a step: begin, run the body, commit — or roll back and say so.
    ///
    /// CONCURRENCY: `Database::begin` checks out a pool connection. Two threads calling
    /// `with_tx` at once take two connections and two transactions. They never share a
    /// session. Isolation is Postgres READ COMMITTED plus the row locks / CAS the body takes.
    fn step_once<R, F>(&self, f: &mut F) -> Result<R>
    where
        F: FnMut(&mut dyn Store) -> Result<R>,
    {
        let mut tx = self
            .rt
            .block_on(self.db.begin(WORKFLOW_STEP))
            .map_err(|e| WorkflowError::generic(e.to_string()))?;
        let result = {
            let mut store = NeonTx {
                tx: &mut tx,
                handle: self.rt.handle().clone(),
            };
            f(&mut store)
        };
        match result {
            Ok(v) => {
                // A FAILED COMMIT IS NOT CLASSIFIED AS TRANSIENT, so `with_tx` does not repeat it.
                //
                // The commit may have been applied by the server with only the acknowledgement lost, and repeating the
                // step in that case would apply it twice. `driver_failure` below therefore classifies the body's
                // failures (which provably never committed) and this one stays `Generic` — the difference between "this
                // step never ran" and "this step may have run" is carried in the error type rather than guessed at.
                self.rt
                    .block_on(tx.commit())
                    .map_err(|e| WorkflowError::generic(e.to_string()))?;
                Ok(v)
            }
            Err(e) => {
                // A FAILED ROLLBACK IS NOT SILENT (2026-09-29).
                //
                // This used to be `let _ = tx.rollback();`, and the connection goes back to the pool either way. A
                // connection returned INSIDE a transaction is the one the server later terminates with
                // `idle_in_transaction_session_timeout`, and the client that receives the 25P03 is whoever sends the
                // next statement on it — which for the engine is the `BEGIN` of the following step. Swallowing this
                // threw away the only evidence that the session which just failed is now poison, while the step's own
                // error (a validation refusal, "no work", a conflict) looked like the whole story.
                //
                // The rollback failure has already announced itself as an `app_error` row — every `DbFailure`
                // announces from its constructor — so it is not announced again here; it is carried, so that a
                // reader of the step failure is told both things at once.
                if let Err(failure) = self.rt.block_on(tx.rollback()) {
                    return Err(WorkflowError::generic(format!(
                        "{e}; and rolling this step's transaction back failed too: {failure}"
                    )));
                }
                Err(e)
            }
        }
    }
}

pub(super) struct NeonTx<'a> {
    pub(super) tx: &'a mut NeonTransaction,
    pub(super) handle: tokio::runtime::Handle,
}

impl NeonTx<'_> {
    pub(super) fn conn(&mut self) -> &mut PgConnection {
        self.tx.connection()
    }
}

/// Map a driver failure the way the db crate classifies it, so a broken CONNECTION is distinguishable from a refusal
/// by the database.
///
/// EVERY DRIVER ERROR USED TO BECOME `WorkflowError::generic` (2026-09-29). That threw away the difference between
/// "the socket died and this step never ran" and "the database said no", which is exactly the difference `with_tx`
/// needs in order to know whether repeating the step is safe. The classification comes from `DbFailure`, where the
/// sqlstate taxonomy already lives (`08*`, `25P03`, `53300`), instead of being re-derived from message text here.
///
/// `Timeout` is deliberately NOT transient. A statement cancelled by our own 30s ceiling (`57014`) has already run, and
/// repeating the step around it would repeat the work it was cancelled for.
fn driver_failure(e: sqlx::Error) -> WorkflowError {
    let failure = NeonFailure::from_sqlx(WORKFLOW_STEP, &e);
    if failure.kind == FailureKind::DatabaseUnavailable {
        WorkflowError::unavailable(failure.to_string())
    } else {
        WorkflowError::generic(failure.to_string())
    }
}

pub(super) fn exec_q<'q>(tx: &mut NeonTx<'_>, q: Query<'q, Postgres, PgArguments>) -> Result<u64> {
    let handle = tx.handle.clone();
    let conn = tx.conn();
    handle
        .block_on(q.execute(conn))
        .map(|r| r.rows_affected())
        .map_err(driver_failure)
}

pub(super) fn fetch_all_q<'q>(
    tx: &mut NeonTx<'_>,
    q: Query<'q, Postgres, PgArguments>,
) -> Result<Vec<PgRow>> {
    let handle = tx.handle.clone();
    let conn = tx.conn();
    handle.block_on(q.fetch_all(conn)).map_err(driver_failure)
}

pub(super) fn fetch_one_q<'q>(
    tx: &mut NeonTx<'_>,
    q: Query<'q, Postgres, PgArguments>,
) -> Result<PgRow> {
    let handle = tx.handle.clone();
    let conn = tx.conn();
    handle.block_on(q.fetch_one(conn)).map_err(driver_failure)
}

pub(super) fn fetch_optional_q<'q>(
    tx: &mut NeonTx<'_>,
    q: Query<'q, Postgres, PgArguments>,
) -> Result<Option<PgRow>> {
    let handle = tx.handle.clone();
    let conn = tx.conn();
    handle
        .block_on(q.fetch_optional(conn))
        .map_err(driver_failure)
}
