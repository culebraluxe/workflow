use crate::types::*;

pub fn process_status(s: ProcessStatus) -> &'static str {
    match s {
        ProcessStatus::Active => "active",
        ProcessStatus::Completed => "completed",
        ProcessStatus::Suspended => "suspended",
        ProcessStatus::Aborted => "aborted",
        ProcessStatus::Error => "error",
    }
}
pub fn parse_process_status(s: &str) -> ProcessStatus {
    match s {
        "completed" => ProcessStatus::Completed,
        "suspended" => ProcessStatus::Suspended,
        "aborted" => ProcessStatus::Aborted,
        "error" => ProcessStatus::Error,
        _ => ProcessStatus::Active,
    }
}
pub fn process_outcome(o: ProcessOutcome) -> &'static str {
    match o {
        ProcessOutcome::Completed => "completed",
        ProcessOutcome::Cancelled => "cancelled",
        ProcessOutcome::Failed => "failed",
        ProcessOutcome::Conflict => "conflict",
    }
}
pub fn parse_process_outcome(s: &str) -> Option<ProcessOutcome> {
    match s {
        "completed" => Some(ProcessOutcome::Completed),
        "cancelled" => Some(ProcessOutcome::Cancelled),
        "failed" => Some(ProcessOutcome::Failed),
        "conflict" => Some(ProcessOutcome::Conflict),
        _ => None,
    }
}
pub fn token_status(s: TokenStatus) -> &'static str {
    match s {
        TokenStatus::Active => "active",
        TokenStatus::Completed => "completed",
        TokenStatus::Suspended => "suspended",
    }
}
pub fn parse_token_status(s: &str) -> TokenStatus {
    match s {
        "completed" => TokenStatus::Completed,
        "suspended" => TokenStatus::Suspended,
        _ => TokenStatus::Active,
    }
}
pub fn token_outcome(o: TokenOutcome) -> &'static str {
    match o {
        TokenOutcome::Completed => "completed",
        TokenOutcome::Cancelled => "cancelled",
        TokenOutcome::Failed => "failed",
        TokenOutcome::Skipped => "skipped",
    }
}
pub fn parse_token_outcome(s: &str) -> Option<TokenOutcome> {
    match s {
        "completed" => Some(TokenOutcome::Completed),
        "cancelled" => Some(TokenOutcome::Cancelled),
        "failed" => Some(TokenOutcome::Failed),
        "skipped" => Some(TokenOutcome::Skipped),
        _ => None,
    }
}
pub fn task_status(s: TaskStatus) -> &'static str {
    match s {
        TaskStatus::Created => "created",
        TaskStatus::Ready => "ready",
        TaskStatus::Reserved => "reserved",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Exited => "exited",
        TaskStatus::Obsolete => "obsolete",
    }
}
pub fn parse_task_status(s: &str) -> TaskStatus {
    match s {
        "created" => TaskStatus::Created,
        "reserved" => TaskStatus::Reserved,
        "in_progress" => TaskStatus::InProgress,
        "completed" => TaskStatus::Completed,
        "failed" => TaskStatus::Failed,
        "exited" => TaskStatus::Exited,
        "obsolete" => TaskStatus::Obsolete,
        _ => TaskStatus::Ready,
    }
}
pub fn job_status(s: JobStatus) -> &'static str {
    match s {
        JobStatus::Pending => "pending",
        JobStatus::Locked => "locked",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
}
pub fn parse_job_status(s: &str) -> JobStatus {
    match s {
        "locked" => JobStatus::Locked,
        "completed" => JobStatus::Completed,
        "failed" => JobStatus::Failed,
        "cancelled" => JobStatus::Cancelled,
        _ => JobStatus::Pending,
    }
}
pub fn def_status(s: DefinitionStatus) -> &'static str {
    match s {
        DefinitionStatus::Draft => "draft",
        DefinitionStatus::Active => "active",
        DefinitionStatus::Deprecated => "deprecated",
    }
}
pub fn parse_def_status(s: &str) -> DefinitionStatus {
    match s {
        "draft" => DefinitionStatus::Draft,
        "deprecated" => DefinitionStatus::Deprecated,
        _ => DefinitionStatus::Active,
    }
}
