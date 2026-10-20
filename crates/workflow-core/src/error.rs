use std::fmt;

#[derive(Debug)]
pub enum WorkflowError {
    Generic {
        message: String,
    },
    Conflict {
        code: &'static str,
        message: String,
    },
    StaleToken {
        message: String,
    },
    MissingApplicationPort {
        message: String,
    },
    /// The CONNECTION failed; the work did not.
    ///
    /// `Generic` says "this step refused" and must not be repeated. This one says "this step never got to run", and
    /// that difference is the whole reason the variant exists: `TxStore::with_tx` repeats a step that failed this way
    /// — nothing committed, so repeating it cannot double-apply anything — and repeats nothing else (2026-09-29).
    ///
    /// It is produced by mapping the db crate's own classification (`DbFailureKind::DatabaseUnavailable`) rather than
    /// by matching on message text, so the two crates cannot disagree about what a broken socket is.
    Unavailable {
        message: String,
    },
    Expression(String),
    NotFound(String),
}

impl WorkflowError {
    pub fn generic(msg: impl Into<String>) -> Self {
        Self::Generic {
            message: msg.into(),
        }
    }
    pub fn conflict(code: &'static str, msg: impl Into<String>) -> Self {
        Self::Conflict {
            code,
            message: msg.into(),
        }
    }
    pub fn stale_token(msg: impl Into<String>) -> Self {
        Self::StaleToken {
            message: msg.into(),
        }
    }
    pub fn missing_port(msg: impl Into<String>) -> Self {
        Self::MissingApplicationPort {
            message: msg.into(),
        }
    }
    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self::Unavailable {
            message: msg.into(),
        }
    }

    /// Whether the failure belonged to the connection rather than to the work, and so is worth repeating.
    pub fn is_connection_failure(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Generic { .. } => "ERROR",
            Self::Conflict { code, .. } => code,
            Self::StaleToken { .. } => "STALE_TOKEN",
            Self::MissingApplicationPort { .. } => "MISSING_APPLICATION_PORT",
            Self::Unavailable { .. } => "DB_UNAVAILABLE",
            Self::Expression(_) => "EXPRESSION",
            Self::NotFound(_) => "NOT_FOUND",
        }
    }
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Generic { message }
            | Self::Conflict { message, .. }
            | Self::StaleToken { message }
            | Self::MissingApplicationPort { message }
            | Self::Unavailable { message } => write!(f, "{message}"),
            Self::Expression(m) | Self::NotFound(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for WorkflowError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The retry in `TxStore::with_tx` keys off this predicate, and it must be narrow: everything that is not a
    /// connection failure is a decision by the database or by the engine, and repeating it either wastes a round trip
    /// or changes the meaning of the answer.
    #[test]
    fn only_a_connection_failure_is_worth_repeating() {
        assert!(WorkflowError::unavailable("broken pipe").is_connection_failure());
        assert!(!WorkflowError::generic("no work").is_connection_failure());
        assert!(!WorkflowError::conflict("PROCESS_NOT_ACTIVE", "x").is_connection_failure());
        assert!(!WorkflowError::stale_token("x").is_connection_failure());
        assert!(!WorkflowError::NotFound("x".into()).is_connection_failure());
    }

    /// An operator reading a failure needs to know which side failed, and the code is what the engine logs.
    #[test]
    fn a_connection_failure_says_so_in_its_code_and_text() {
        let error = WorkflowError::unavailable(
            "error communicating with database: Broken pipe (os error 32)",
        );
        assert_eq!(error.code(), "DB_UNAVAILABLE");
        assert!(
            error.to_string().contains("Broken pipe"),
            "the driver's own words must survive: {error}"
        );
    }
}

pub type Result<T> = std::result::Result<T, WorkflowError>;
