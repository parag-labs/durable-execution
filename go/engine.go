package durableexecution

import "fmt"

// replaySignal is panicked to unwind the workflow when it reaches a step that
// has not run yet in a fresh (non-replay) execution. The engine runs the step,
// appends it to history, persists, and re-executes the workflow from the top.
type replaySignal struct{}

// HistoryEvent is one recorded step result in the append-only history.
type HistoryEvent struct {
	// Seq is the position of this event in the history.
	Seq int
	// StepID is the id passed to Step when the result was recorded.
	StepID string
	// Result is the value the step returned the first time it ran.
	Result any
}

// NonDeterministicWorkflowError is raised (via panic) when replay reaches a step
// whose id does not match what history recorded at that position. It signals a
// changed workflow shape rather than letting replay silently corrupt state.
type NonDeterministicWorkflowError struct {
	Message string
}

// Error implements the error interface.
func (e *NonDeterministicWorkflowError) Error() string { return e.Message }

// WorkflowContext is handed to the workflow body. Step is the only way to do
// something with a side effect; its result is memoized in the history.
type WorkflowContext struct {
	engine  *Engine
	history []HistoryEvent
	cursor  int
}

// Step runs fn the first time this position is reached, records its result in
// the durable history and unwinds so the engine can persist before continuing.
// On every later execution (including after a crash) it returns the recorded
// result without running fn again, which is what makes replay idempotent.
func (c *WorkflowContext) Step(stepID string, fn func() any) any {
	// Already recorded? Serve from history, do NOT run the side effect again.
	if c.cursor < len(c.history) {
		ev := c.history[c.cursor]
		if ev.StepID != stepID {
			panic(&NonDeterministicWorkflowError{Message: fmt.Sprintf(
				"non-deterministic workflow: expected step %q at position %d, got %q",
				ev.StepID, c.cursor, stepID)})
		}
		c.cursor++
		return ev.Result
	}
	// First time: run it, append to history, and unwind so the engine persists
	// before continuing. The history is durable before the next step is tried.
	result := fn()
	c.history = append(c.history, HistoryEvent{Seq: len(c.history), StepID: stepID, Result: result})
	c.engine.persist(c.history)
	panic(replaySignal{})
}

// Engine runs a workflow to completion, replaying from history each time a new
// step is recorded.
type Engine struct {
	store *HistoryStore
	wid   string
	// Replays counts how many times the workflow was re-executed from the top,
	// which equals the number of steps that had to be recorded.
	Replays int
}

// NewEngine builds an engine backed by store.
func NewEngine(store *HistoryStore) *Engine {
	return &Engine{store: store}
}

func (e *Engine) persist(history []HistoryEvent) {
	e.store.Save(e.wid, history)
}

// Run executes workflow to completion for workflow id wid, replaying completed
// steps from durable history and continuing live from the first unfinished one.
func (e *Engine) Run(wid string, workflow func(*WorkflowContext) any) any {
	e.wid = wid
	history := e.store.Load(wid)
	for {
		ctx := &WorkflowContext{engine: e, history: history}
		result, replayed := e.attempt(ctx, workflow)
		if replayed {
			e.Replays++
			history = e.store.Load(wid) // reload exactly what is durable
			continue
		}
		return result
	}
}

// attempt runs the workflow once, translating a replaySignal panic into a
// replayed=true return while letting every other panic propagate unchanged.
func (e *Engine) attempt(ctx *WorkflowContext, workflow func(*WorkflowContext) any) (result any, replayed bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(replaySignal); ok {
				replayed = true
				return
			}
			panic(r)
		}
	}()
	result = workflow(ctx)
	return result, false
}

// Resume simulates a crash-and-restart: the same call as Run, but history is
// whatever was already persisted, so completed steps will not re-execute.
func (e *Engine) Resume(wid string, workflow func(*WorkflowContext) any) any {
	return e.Run(wid, workflow)
}

// HistoryStore is an in-memory append-only store. A real one would be a database
// table or a log; the interface is deliberately tiny so it is swappable.
type HistoryStore struct {
	data map[string][]HistoryEvent
}

// NewHistoryStore builds an empty in-memory store.
func NewHistoryStore() *HistoryStore {
	return &HistoryStore{data: make(map[string][]HistoryEvent)}
}

// Load returns a copy of the durable history for wid (empty if there is none).
func (s *HistoryStore) Load(wid string) []HistoryEvent {
	h := s.data[wid]
	out := make([]HistoryEvent, len(h))
	copy(out, h)
	return out
}

// Save durably records a copy of history under wid.
func (s *HistoryStore) Save(wid string, history []HistoryEvent) {
	cp := make([]HistoryEvent, len(history))
	copy(cp, history)
	s.data[wid] = cp
}
