//! durable-execution: workflows written as ordinary code that survive process
//! crashes by replaying an append-only history, so a completed step never runs
//! twice - plus the hashed timing wheel these engines use for their many timers.
//!
//! This is a faithful Rust port of the Python `resumerun` reference
//! implementation and behaves identically to the C# and Java ports.
//!
//! Because Rust has no exceptions, the reference's `_Replay` control-flow
//! exception is modelled as [`WorkflowError::Replay`] and propagated with the
//! `?` operator: a workflow body calls `ctx.step(...)?`, and the engine's run
//! loop treats a `Replay` as "a new step was recorded, re-execute from the top"
//! while letting every other error propagate out unchanged.

pub mod engine;
pub mod timing_wheel;

pub use engine::{
    new_shared_store, Engine, HistoryEvent, HistoryStore, SharedStore, Value, WorkflowContext,
    WorkflowError,
};
pub use timing_wheel::{HierarchicalTimingWheel, Timer, TimerRef};

/// The library version, kept in step with the Python package.
pub const VERSION: &str = "0.1.0";
