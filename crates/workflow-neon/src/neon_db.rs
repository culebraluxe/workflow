//! The workflow project's own PostgreSQL layer: pool, transaction, retry and failure taxonomy.
//!
//! This is the only database surface the adapter needs. It deliberately does not import the app
//! `db` crate (which owns app tables, error capture and the shared pool) or `model`; hosts that
//! already have a pool pass it in via [`NeonDb::from_pool`] / `NeonStore::from_pool`, while
//! standalone tools (the `workflow` binary) can use [`NeonDb::connect_from_env`].

use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

/// The operation name every statement of an engine step is reported under.
pub(crate) const WORKFLOW_STEP: &str = "workflow.step";

/// Default statement ceiling; 0 disables it. Mirrors the app's ceiling so a workflow step cannot
/// hold a connection forever.
const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    DatabaseUnavailable,
    SchemaMismatch,
    Constraint,
    Timeout,
    Unknown,
}

impl FailureKind {
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::DatabaseUnavailable | Self::Timeout)
    }
}

#[derive(Debug, Clone)]
pub struct NeonFailure {
    pub kind: FailureKind,
    pub operation: &'static str,
    pub code: Option<String>,
    pub detail: Option<String>,
    pub retryable: bool,
}

impl std::fmt::Display for NeonFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?} during {}", self.kind, self.operation)?;
        if let Some(code) = &self.code {
            write!(formatter, " (sqlstate {code})")?;
        }
        if let Some(detail) = &self.detail {
            write!(formatter, ": {detail}")?;
        }
        Ok(())
    }
}

impl std::error::Error for NeonFailure {}

pub type NeonResult<T> = Result<T, NeonFailure>;

impl NeonFailure {
    pub fn configuration(operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::DatabaseUnavailable,
            operation,
            code: None,
            detail: Some(detail.into()),
            retryable: false,
        }
    }

    pub fn from_sqlx(operation: &'static str, error: &sqlx::Error) -> Self {
        if let Some(database_error) = error.as_database_error() {
            let code = database_error.code().map(|value| value.to_string());
            let kind = code
                .as_deref()
                .map(classify_sqlstate)
                .unwrap_or(FailureKind::Unknown);
            let mut detail = database_error.message().to_string();
            if let Some(constraint) = database_error.constraint() {
                detail = format!("{detail} (constraint {constraint})");
            }
            return Self {
                retryable: kind.is_retryable(),
                detail: Some(detail),
                kind,
                operation,
                code,
            };
        }

        let message = error.to_string().to_lowercase();
        let kind = if message.contains("timed out") || message.contains("timeout") {
            FailureKind::Timeout
        } else if message.contains("connection")
            || message.contains("pool closed")
            || message.contains("broken pipe")
            || message.contains("dns")
            || message.contains("tls")
            || message.contains("socket")
        {
            FailureKind::DatabaseUnavailable
        } else {
            FailureKind::Unknown
        };
        Self {
            retryable: kind.is_retryable(),
            detail: Some(error.to_string()),
            kind,
            operation,
            code: None,
        }
    }
}

fn classify_sqlstate(code: &str) -> FailureKind {
    if code.starts_with("08") || matches!(code, "25P03" | "53300") {
        FailureKind::DatabaseUnavailable
    } else if code == "57014" {
        FailureKind::Timeout
    } else if code.starts_with("23") {
        FailureKind::Constraint
    } else if code.starts_with("42") {
        FailureKind::SchemaMismatch
    } else {
        FailureKind::Unknown
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            base_delay: Duration::from_millis(150),
            max_delay: Duration::from_millis(2_000),
        }
    }
}

impl RetryPolicy {
    /// Keep the operator-facing env names the app used, so a deployment does not need a second
    /// tuning surface when the engine moves.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            attempts: positive_u32("FORGE_DB_RETRY_ATTEMPTS", default.attempts),
            base_delay: Duration::from_millis(positive_u64(
                "FORGE_DB_RETRY_BASE_MS",
                default.base_delay.as_millis() as u64,
            )),
            max_delay: default.max_delay,
        }
    }
}

pub fn policy() -> RetryPolicy {
    static POLICY: std::sync::OnceLock<RetryPolicy> = std::sync::OnceLock::new();
    *POLICY.get_or_init(RetryPolicy::from_env)
}

pub async fn sleep_before_retry(policy: RetryPolicy, attempt: u32) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64)
        .unwrap_or(0);
    let exponent = attempt.saturating_sub(1).min(16);
    let scaled = policy
        .base_delay
        .saturating_mul(1u32 << exponent)
        .min(policy.max_delay);
    let jitter = nanos % (scaled.as_millis().max(1) as u64);
    let delay = scaled.saturating_add(Duration::from_millis(jitter / 2));
    tokio::time::sleep(delay).await;
}

/// One workflow database: the caller's pool (app host) or one this process opens (standalone).
#[derive(Debug, Clone)]
pub struct NeonDb {
    pool: PgPool,
}

impl NeonDb {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Open a pool from the environment for standalone tools. Hosts should pass their existing
    /// pool via [`NeonDb::from_pool`] so the process keeps one pool.
    pub async fn connect_from_env() -> NeonResult<Self> {
        let url = database_url_from_env()?;
        let options = PgConnectOptions::from_str(&normalize_ssl_mode(&url))
            .map_err(|_| {
                NeonFailure::configuration("workflow.neon.connect", "invalid database URL")
            })?
            .application_name("culebraluxe-workflow");
        let pool = PgPoolOptions::new()
            .max_connections(positive_u32("FORGE_DB_POOL_MAX", 10))
            .min_connections(positive_u32("FORGE_DB_POOL_MIN", 0))
            .acquire_timeout(Duration::from_millis(positive_u64(
                "FORGE_DB_CONNECT_TIMEOUT_MS",
                10_000,
            )))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    let millis = statement_timeout_ms();
                    sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                        .bind(format!("{millis}ms"))
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .connect_with(options)
            .await
            .map_err(|error| NeonFailure::from_sqlx("workflow.neon.connect", &error))?;
        Ok(Self { pool })
    }

    pub async fn ping(&self) -> NeonResult<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|error| NeonFailure::from_sqlx("workflow.neon.ping", &error))
    }

    /// Begin one transaction, retrying a retryable session failure. BEGIN carries no work, so
    /// repeating it cannot double-apply anything; the step body is retried by `NeonStore`.
    pub async fn begin(&self, operation: &'static str) -> NeonResult<NeonTransaction> {
        let policy = policy();
        let attempts = policy.attempts.max(1);
        let mut attempt = 1;
        loop {
            match self.pool.begin().await {
                Ok(inner) => return Ok(NeonTransaction::new(inner, operation)),
                Err(error) => {
                    let failure = NeonFailure::from_sqlx(operation, &error);
                    if !failure.retryable || attempt >= attempts {
                        return Err(failure);
                    }
                    attempt += 1;
                    tokio::time::sleep(policy.base_delay).await;
                }
            }
        }
    }
}

pub struct NeonTransaction {
    inner: Option<Transaction<'static, Postgres>>,
    operation: &'static str,
}

impl NeonTransaction {
    fn new(inner: Transaction<'static, Postgres>, operation: &'static str) -> Self {
        Self {
            inner: Some(inner),
            operation,
        }
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        self.inner
            .as_mut()
            .expect("workflow transaction already finalized")
            .as_mut()
    }

    pub async fn commit(mut self) -> NeonResult<()> {
        let transaction = self
            .inner
            .take()
            .expect("workflow transaction already finalized");
        transaction
            .commit()
            .await
            .map_err(|error| NeonFailure::from_sqlx(self.operation, &error))
    }

    pub async fn rollback(mut self) -> NeonResult<()> {
        let transaction = self
            .inner
            .take()
            .expect("workflow transaction already finalized");
        transaction
            .rollback()
            .await
            .map_err(|error| NeonFailure::from_sqlx(self.operation, &error))
    }
}

fn database_url_from_env() -> NeonResult<String> {
    if let Some(url) = std::env::var("DATABASE_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Ok(url);
    }
    let declared = std::env::var("APP_ENV")
        .or_else(|_| std::env::var("VERCEL_ENV"))
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let key = match declared.as_str() {
        "production" | "prod" => "DATABASE_URL_PROD",
        "development" | "dev" | "preview" | "test" | "testing" => "DATABASE_URL_DEV",
        "" => {
            return Err(NeonFailure::configuration(
                "workflow.neon.connect",
                "DATABASE_URL is not set (or set APP_ENV/VERCEL_ENV plus DATABASE_URL_DEV/PROD)",
            ));
        }
        other => {
            return Err(NeonFailure::configuration(
                "workflow.neon.connect",
                format!("APP_ENV/VERCEL_ENV {other:?} is not a database environment"),
            ));
        }
    };
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            NeonFailure::configuration("workflow.neon.connect", format!("{key} is not set"))
        })
}

fn statement_timeout_ms() -> u64 {
    static TIMEOUT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TIMEOUT.get_or_init(|| match std::env::var("FORGE_DB_STATEMENT_TIMEOUT_MS") {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .unwrap_or(DEFAULT_STATEMENT_TIMEOUT_MS),
        Err(_) => DEFAULT_STATEMENT_TIMEOUT_MS,
    })
}

fn normalize_ssl_mode(url: &str) -> String {
    // sqlx 0.9's rustls feature rejects `sslmode=require` in some URL spellings; the app normalizes
    // it the same way. Keep this local to the adapter.
    if url.contains("sslmode=require") && !url.contains("sslmode=verify") {
        url.replace("sslmode=require", "sslmode=verify-full")
    } else {
        url.to_string()
    }
}

fn positive_u32(name: &str, fallback: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn positive_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_classification_is_retryability_aware() {
        assert_eq!(classify_sqlstate("08006"), FailureKind::DatabaseUnavailable);
        assert_eq!(classify_sqlstate("25P03"), FailureKind::DatabaseUnavailable);
        assert_eq!(classify_sqlstate("57014"), FailureKind::Timeout);
        assert_eq!(classify_sqlstate("23505"), FailureKind::Constraint);
        assert_eq!(classify_sqlstate("42P01"), FailureKind::SchemaMismatch);
        assert!(FailureKind::DatabaseUnavailable.is_retryable());
        assert!(FailureKind::Timeout.is_retryable());
        assert!(!FailureKind::Constraint.is_retryable());
    }

    #[test]
    fn retry_policy_default_is_bounded() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.attempts, 3);
        assert!(policy.base_delay > Duration::ZERO);
        assert!(policy.max_delay >= policy.base_delay);
    }

    #[tokio::test]
    async fn from_pool_keeps_the_callers_pool() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://workflow:workflow@127.0.0.1:1/workflow")
            .expect("lazy pool");
        let db = NeonDb::from_pool(pool.clone());
        assert_eq!(db.pool().size(), pool.size());
        assert!(!db.pool().is_closed());
    }
}
