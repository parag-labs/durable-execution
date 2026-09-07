// Durable execution: workflows that survive a crash by replaying their history.
//
// The idea, borrowed from Temporal and AWS Step Functions: a workflow is ordinary
// code, but every side-effecting *step* it takes is first recorded in an
// append-only history log. If the process dies and restarts, the engine re-runs the
// workflow function from the top - and every step that's already in the history
// returns its recorded result *without executing again*. Deterministic replay walks
// the function back to exactly where it left off, then continues live.
//
// The two things that make this honest:
// - The workflow body must be deterministic (no wall-clock, no unrecorded I/O), so
//   replay reaches the same step in the same order.
// - Every step is idempotent on replay because a completed step is served from
//   history, so re-execution after a crash never double-charges the card.

package com.durableexec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class Engine {

    /** Raised to unwind the workflow when it needs a step that hasn't run yet in a
     * fresh (non-replay) execution - the engine runs the step, appends it, and
     * re-executes from the top. */
    static final class ReplayException extends RuntimeException {
        ReplayException() { super(null, null, false, false); }
    }

    /** One recorded step result in the append-only history. */
    public record HistoryEvent(int seq, String stepId, Object result) {}

    /** Thrown when replay observes a different step order than history records. */
    public static final class NonDeterministicWorkflowException extends RuntimeException {
        public NonDeterministicWorkflowException(String message) { super(message); }
    }

    /** Handed to the workflow body. {@code step} is the only way to do something
     * with a side effect; its result is memoized in the history. */
    public static final class WorkflowContext {
        private final Engine engine;
        private final List<HistoryEvent> history;
        private int cursor = 0;

        WorkflowContext(Engine engine, List<HistoryEvent> history) {
            this.engine = engine;
            this.history = history;
        }

        public Object step(String stepId, Supplier<Object> fn) {
            // Already recorded? Serve from history, do NOT run the side effect again.
            if (cursor < history.size()) {
                HistoryEvent ev = history.get(cursor);
                if (!ev.stepId().equals(stepId)) {
                    throw new NonDeterministicWorkflowException(
                        "non-deterministic workflow: expected step '" + ev.stepId()
                        + "' at position " + cursor + ", got '" + stepId + "'");
                }
                cursor++;
                return ev.result();
            }
            // First time: run it, append to history, and unwind so the engine
            // persists before continuing. This is what makes a crash safe - the
            // history is durable before the next step is attempted.
            Object result = fn.get();
            history.add(new HistoryEvent(history.size(), stepId, result));
            engine.persist(history);
            throw new ReplayException();
        }
    }

    private final HistoryStore store;
    private String wid = "";
    private int replays = 0;

    public Engine(HistoryStore store) {
        this.store = store;
    }

    public int replays() { return replays; }

    void persist(List<HistoryEvent> history) {
        store.save(wid, history);
    }

    public Object run(String wid, Function<WorkflowContext, Object> workflow) {
        this.wid = wid;
        List<HistoryEvent> history = store.load(wid);
        while (true) {
            WorkflowContext ctx = new WorkflowContext(this, history);
            try {
                return workflow.apply(ctx);
            } catch (ReplayException e) {
                replays++;
                history = store.load(wid); // reload exactly what's durable
            }
        }
    }

    /** Simulate a crash-and-restart: same call, but history is whatever was already
     * persisted. Completed steps will not re-execute. */
    public Object resume(String wid, Function<WorkflowContext, Object> workflow) {
        return run(wid, workflow);
    }

    /** In-memory append-only store. A real one would be a DB table or a log; the
     * interface is deliberately tiny so it's swappable. */
    public static final class HistoryStore {
        private final Map<String, List<HistoryEvent>> data = new HashMap<>();

        public List<HistoryEvent> load(String wid) {
            return new ArrayList<>(data.getOrDefault(wid, new ArrayList<>()));
        }

        public void save(String wid, List<HistoryEvent> history) {
            data.put(wid, new ArrayList<>(history));
        }
    }
}
