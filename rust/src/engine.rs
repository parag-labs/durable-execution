//! Durable execution: workflows that survive a crash by replaying their history.
//!
//! The idea, borrowed from Temporal and AWS Step Functions: a workflow is
//! ordinary code, but every side-effecting *step* it takes is first recorded in
//! an append-only history log. If the process dies and restarts, the engine
//! re-runs the workflow function from the top - and every step that's already in
//! the history returns its recorded result *without executing again*.
//!
//! The two things that make this honest:
//! - The workflow body must be **deterministic** so replay reaches the same step
//!   in the same order.
//! - Every step is **idempotent on replay** because a completed step is served
//!   from history, so re-execution after a crash never double-charges the card.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

/// A recorded step result. Results are heterogeneous in the reference (int, str,
/// dict, ...); this enum captures the shapes the workflows use while staying
/// `Clone` so history can be served on replay and `PartialEq` for assertions.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The `None`/`null` result.
    Null,
    /// A boolean result.
    Bool(bool),
    /// An integer result.
    Int(i64),
    /// A string result.
    Str(String),
    /// An ordered list result (a tuple in the reference).
    List(Vec<Value>),
    /// A string-keyed map result (a dict in the reference), ordered for
    /// deterministic comparison.
    Map(BTreeMap<String, Value>),
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Str(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Str(v)
    }
}

/// One recorded step result in the append-only history.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryEvent {
    /// The position of this event in the history.
    pub seq: usize,
    /// The id passed to [`WorkflowContext::step`] when the result was recorded.
    pub step_id: String,
    /// The value the step returned the first time it ran.
    pub result: Value,
}

/// The ways a workflow execution can end other than returning a value.
///
/// [`WorkflowError::Replay`] is the internal control-flow signal that a new step
/// was recorded and the workflow must re-execute from the top; the engine
/// handles it and never surfaces it to callers. The other variants propagate out
/// of [`Engine::run`].
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowError {
    /// Internal: a new step was recorded and persisted, so re-run from the top.
    Replay,
    /// Replay reached a step id that does not match what history recorded here.
    NonDeterministic(String),
    /// A user-raised failure (e.g. a simulated process crash) that the engine
    /// deliberately does not catch.
    Crash(String),
}

/// An in-memory append-only store. A real one would be a database table or a
/// log; the interface is deliberately tiny so it's swappable.
#[derive(Default)]
pub struct HistoryStore {
    data: HashMap<String, Vec<HistoryEvent>>,
}

impl HistoryStore {
    /// Builds an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a copy of the durable history for `wid` (empty if there is none).
    pub fn load(&self, wid: &str) -> Vec<HistoryEvent> {
        self.data.get(wid).cloned().unwrap_or_default()
    }

    /// Durably records a copy of `history` under `wid`.
    pub fn save(&mut self, wid: &str, history: &[HistoryEvent]) {
        self.data.insert(wid.to_string(), history.to_vec());
    }
}

/// A store shared between engine instances, mirroring how the reference passes a
/// single `HistoryStore` object to several engines. Cloning the handle (not the
/// data) lets a "fresh" engine resume against the same durable history.
pub type SharedStore = Rc<RefCell<HistoryStore>>;

/// Convenience constructor for a fresh [`SharedStore`].
pub fn new_shared_store() -> SharedStore {
    Rc::new(RefCell::new(HistoryStore::new()))
}

/// Handed to the workflow body. [`step`](WorkflowContext::step) is the only way
/// to do something with a side effect; its result is memoized in the history.
pub struct WorkflowContext {
    store: SharedStore,
    wid: String,
    history: Vec<HistoryEvent>,
    cursor: usize,
}

impl WorkflowContext {
    fn new(store: SharedStore, wid: String, history: Vec<HistoryEvent>) -> Self {
        Self {
            store,
            wid,
            history,
            cursor: 0,
        }
    }

    /// Runs `f` the first time this position is reached, records its result in
    /// the durable history and returns [`WorkflowError::Replay`] so the engine
    /// re-executes from the top. On every later execution (including after a
    /// crash) it returns the recorded result without running `f` again.
    ///
    /// Call it with the `?` operator: `let x = ctx.step("id", || ...)?;`.
    pub fn step<F>(&mut self, step_id: &str, f: F) -> Result<Value, WorkflowError>
    where
        F: FnOnce() -> Value,
    {
        // Already recorded? Serve from history, do NOT run the side effect again.
        if self.cursor < self.history.len() {
            let ev = &self.history[self.cursor];
            if ev.step_id != step_id {
                return Err(WorkflowError::NonDeterministic(format!(
                    "non-deterministic workflow: expected step '{}' at position {}, got '{}'",
                    ev.step_id, self.cursor, step_id
                )));
            }
            let result = ev.result.clone();
            self.cursor += 1;
            return Ok(result);
        }
        // First time: run it, append to history, and persist before unwinding so
        // the history is durable before the next step is attempted.
        let result = f();
        self.history.push(HistoryEvent {
            seq: self.history.len(),
            step_id: step_id.to_string(),
            result: result.clone(),
        });
        self.store.borrow_mut().save(&self.wid, &self.history);
        Err(WorkflowError::Replay)
    }
}

/// Runs a workflow to completion, replaying from history each time a new step is
/// recorded.
pub struct Engine {
    store: SharedStore,
    replays: usize,
}

impl Engine {
    /// Builds an engine backed by the shared `store`.
    pub fn new(store: SharedStore) -> Self {
        Self { store, replays: 0 }
    }

    /// How many times the workflow was re-executed from the top, which equals
    /// the number of steps that had to be recorded.
    pub fn replays(&self) -> usize {
        self.replays
    }

    /// Executes `workflow` to completion for workflow id `wid`, replaying
    /// completed steps from durable history and continuing live from the first
    /// unfinished one. A [`WorkflowError::Replay`] returned by a step re-runs the
    /// body; any other error propagates to the caller.
    pub fn run<F>(&mut self, wid: &str, mut workflow: F) -> Result<Value, WorkflowError>
    where
        F: FnMut(&mut WorkflowContext) -> Result<Value, WorkflowError>,
    {
        let mut history = self.store.borrow().load(wid);
        loop {
            let mut ctx = WorkflowContext::new(Rc::clone(&self.store), wid.to_string(), history);
            match workflow(&mut ctx) {
                Ok(value) => return Ok(value),
                Err(WorkflowError::Replay) => {
                    self.replays += 1;
                    history = self.store.borrow().load(wid); // reload what is durable
                }
                Err(other) => return Err(other),
            }
        }
    }

    /// Simulates a crash-and-restart: the same call as [`run`](Engine::run), but
    /// history is whatever was already persisted, so completed steps will not
    /// re-execute.
    pub fn resume<F>(&mut self, wid: &str, workflow: F) -> Result<Value, WorkflowError>
    where
        F: FnMut(&mut WorkflowContext) -> Result<Value, WorkflowError>,
    {
        self.run(wid, workflow)
    }
}
