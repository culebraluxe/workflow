use crate::value::Value;

pub const JOB_BACKOFF_BASE_MS: i64 = 60_000;
pub const JOB_LEASE_MS: i64 = 5 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    Active,
    Completed,
    Suspended,
    Aborted,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    Completed,
    Cancelled,
    Failed,
    Conflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStatus {
    Active,
    Completed,
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenOutcome {
    Completed,
    Cancelled,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Created,
    Ready,
    Reserved,
    InProgress,
    Completed,
    Failed,
    Exited,
    Obsolete,
}

impl TaskStatus {
    pub fn is_actionable(self) -> bool {
        matches!(self, Self::Ready | Self::Reserved | Self::InProgress)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Locked,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn is_settled(self) -> bool {
        matches!(self, Self::Cancelled | Self::Completed | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobType {
    Timer,
    Async,
    Message,
    Signal,
    Other(String),
}

impl JobType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Timer => "timer",
            Self::Async => "async",
            Self::Message => "message",
            Self::Signal => "signal",
            Self::Other(s) => s,
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "timer" => Self::Timer,
            "async" => Self::Async,
            "message" => Self::Message,
            "signal" => Self::Signal,
            other => Self::Other(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefinitionStatus {
    Draft,
    Active,
    Deprecated,
}

#[derive(Debug, Clone)]
pub struct ProcessDefinition {
    pub id: String,
    pub tenant_id: Option<String>,
    pub key: String,
    pub version: i32,
    pub name: String,
    pub description: Option<String>,
    pub definition: ProcessGraph,
    pub status: DefinitionStatus,
}

#[derive(Debug, Clone, Default)]
pub struct ProcessGraph {
    pub nodes: std::collections::BTreeMap<String, NodeDefinition>,
    pub start_node_id: String,
    pub display_order: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct TimerSpec {
    pub due_at: Option<String>,
    pub due_at_variable: Option<String>,
    pub transition: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct NodeDefinition {
    pub id: String,
    pub node_type: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub responsibility: Option<String>,
    pub transitions: Option<Vec<TransitionDefinition>>,
    pub form_key: Option<String>,
    pub candidate_groups: Option<Vec<String>>,
    pub priority: Option<i32>,
    pub decisions: Option<Vec<DecisionArm>>,
    pub subprocess_key: Option<String>,
    pub input_mappings: Option<Value>,
    pub outcome: Option<ProcessOutcome>,
    pub timer: Option<TimerSpec>,
    pub command_type: Option<String>,
    pub transition: Option<String>,
    pub refresh_facts: Option<bool>,
    pub count_variable: Option<String>,
    pub plan_variable: Option<String>,
    pub branch_command_type: Option<String>,
    pub branch_node: Option<String>,
    pub join: Option<String>,
    pub minimum: Option<i32>,
    pub maximum: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct DecisionArm {
    pub condition: String,
    pub transition: String,
}

#[derive(Debug, Clone)]
pub struct TransitionDefinition {
    pub name: String,
    pub to: String,
    pub condition: Option<String>,
    pub required: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ProcessInstance {
    pub id: String,
    pub tenant_id: Option<String>,
    pub definition_id: String,
    pub business_key: Option<String>,
    pub status: ProcessStatus,
    pub outcome: Option<ProcessOutcome>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub started_by: Option<String>,
    pub parent_instance_id: Option<String>,
    pub root_token_id: Option<String>,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
    pub variables: Value,
    pub version: i32,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub id: String,
    pub tenant_id: Option<String>,
    pub process_instance_id: String,
    pub parent_token_id: Option<String>,
    pub node_id: String,
    pub status: TokenStatus,
    pub outcome: Option<TokenOutcome>,
    pub required: bool,
    pub is_able_to_reactivate_parent: bool,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub version: i32,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub tenant_id: Option<String>,
    pub process_instance_id: String,
    pub token_id: Option<String>,
    pub node_id: Option<String>,
    pub name: String,
    pub description: Option<String>,
    pub status: TaskStatus,
    pub assignee: Option<String>,
    pub candidates: Vec<String>,
    pub swimlane: Option<String>,
    pub priority: i32,
    pub due_date: Option<i64>,
    pub form_key: Option<String>,
    pub form_data: Value,
    pub created_at: i64,
    pub claimed_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub completed_by: Option<String>,
    pub version: i32,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub tenant_id: Option<String>,
    pub process_instance_id: Option<String>,
    pub token_id: Option<String>,
    pub job_type: String,
    pub due_at: i64,
    pub status: JobStatus,
    pub locked_by: Option<String>,
    pub locked_until: Option<i64>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub payload: Value,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ProcessEvent {
    pub id: i64,
    pub tenant_id: Option<String>,
    pub process_instance_id: String,
    pub token_id: Option<String>,
    pub task_id: Option<String>,
    pub job_id: Option<String>,
    pub event_type: String,
    pub node_id: Option<String>,
    pub actor: String,
    pub data: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct ProcessCommand {
    pub process_instance_id: String,
    pub token_id: String,
    pub node_id: String,
    pub visit_sequence: i32,
    pub command_id: String,
    pub command_type: String,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub input: Value,
    pub outcome: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkflowSubject {
    pub subject_type: String,
    pub subject_id: String,
}

#[derive(Debug, Clone)]
pub struct StartProcessParams {
    pub definition_key: String,
    pub version: Option<i32>,
    pub business_key: Option<String>,
    pub variables: Value,
    pub started_by: String,
    pub tenant_id: Option<String>,
    pub subject: Option<WorkflowSubject>,
}

#[derive(Debug, Clone)]
pub struct SignalTokenParams {
    pub token_id: String,
    pub transition_name: Option<String>,
    pub variables: Value,
    pub actor: String,
}

#[derive(Debug, Clone)]
pub struct CompleteTaskParams {
    pub task_id: String,
    pub user_id: String,
    pub form_data: Value,
    pub transition_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CancelProcessParams {
    pub process_instance_id: String,
    pub actor: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FireTimerParams {
    pub job_id: String,
    pub worker_id: String,
    pub variables: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationCommandOutcome {
    Success,
    ValidationFailure,
    NotFound,
    Conflict,
    Unauthorized,
    PreconditionFailure,
    /// An external command may have taken effect, but its durable settlement could not be completed.
    /// The workflow must park on an explicit hold route instead of treating this as success or failure.
    SettlementRequired,
}

impl ApplicationCommandOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ValidationFailure => "validation_failure",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unauthorized => "unauthorized",
            Self::PreconditionFailure => "precondition_failure",
            Self::SettlementRequired => "settlement_required",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApplicationCommandRequest {
    pub command_id: String,
    pub command_type: String,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub input: Value,
}

#[derive(Debug, Clone)]
pub struct ApplicationCommandResult {
    pub command_id: String,
    pub outcome: ApplicationCommandOutcome,
    pub message: Option<String>,
}

pub trait ApplicationPort: Send + Sync {
    fn execute_command(&self, request: &ApplicationCommandRequest) -> ApplicationCommandResult;
    fn read_facts(&self, subject: &WorkflowSubject) -> Value;
}

#[derive(Debug, Clone)]
pub struct WorkflowTraceRecord {
    pub event_type: String,
    pub system: String,
    pub occurred_at: String,
    pub outcome: Option<String>,
    pub workflow_instance_id: Option<String>,
    pub workflow_node_id: Option<String>,
    pub workflow_transition_id: Option<String>,
    pub summary: Option<String>,
    pub deal_id: Option<String>,
    pub property_id: Option<String>,
    pub person_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StartProcessResult {
    pub process_instance_id: String,
    pub root_token_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct DueJobReport {
    pub reclaimed: usize,
    pub claimed: Vec<Job>,
    pub fired: usize,
    pub completed: usize,
    pub failed: usize,
}
