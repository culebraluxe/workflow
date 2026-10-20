//! Neon/PostgreSQL adapter for `workflow-core`: the pool, retry/timeout policy, SQL and store.
//!
//! This crate is the workflow project's database boundary: its own `neon_db` layer (pool, transaction,
//! retry/failure taxonomy) means it does not depend on the app `db` or `model` crates. The `crate::`
//! names the moved Neon modules use are re-exported from `workflow-core` here so the adapter keeps
//! its engine types without the core depending on a database.

pub use workflow_core::*;

pub mod neon;
pub mod neon_db;
pub mod sql_contract;

pub use neon::NeonStore;
pub use neon_db::{FailureKind, NeonDb, NeonFailure, NeonResult, NeonTransaction, RetryPolicy};
