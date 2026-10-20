//! Host-neutral workflow kernel: types, values, store traits, expressions, JSON codec, memory store
//! and (step 2) the engine. No database, no app/Forge host types.

pub mod concurrency;
pub mod engine;
pub mod error;
pub mod expr;
pub mod ids;
pub mod json_codec;
pub use json_codec as json;
pub mod memory;
pub mod sha256;
pub mod status;
pub mod store;
pub mod types;
pub mod value;
pub mod xml;

pub use engine::{command_id, wall_clock_ms, EngineOptions, WorkflowEngine};
pub use error::{Result, WorkflowError};
pub use expr::{evaluate_condition, is_supported_expression};
pub use memory::MemoryStore;
pub use store::{Store, TxStore};
pub use types::*;
pub use value::Value;
pub use xml::{definition_from_xml, parse_process_definition_xml, ParsedDefinition, XmlError};
