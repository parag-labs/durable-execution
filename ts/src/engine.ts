/**
 * Durable execution: workflows that survive a crash by replaying their history.
 *
 * A workflow is ordinary code, but every side-effecting *step* it takes is first
 * recorded in an append-only history log. If the process dies and restarts, the
 * engine re-runs the workflow function from the top - and every step already in
 * the history returns its recorded result *without executing again*. Replay walks
 * the function back to exactly where it left off, then continues live.
 */

/**
 * Thrown internally to unwind the workflow when it reaches a step that has not
 * run yet in a fresh (non-replay) execution. The engine runs the step, appends
 * it to history, persists, and re-executes the workflow from the top.
 */
class ReplayError extends Error {
  constructor() {
    super("replay");
    this.name = "ReplayError";
  }
}

/**
 * Thrown when replay reaches a step whose id does not match what history
 * recorded at that position. It signals a changed workflow shape rather than
 * letting replay silently corrupt state.
 */
export class NonDeterministicWorkflowError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NonDeterministicWorkflowError";
  }
}

/** One recorded step result in the append-only history. */
export interface HistoryEvent {
  /** Position of this event in the history. */
  seq: number;
  /** The id passed to step when the result was recorded. */
  stepId: string;
  /** The value the step returned the first time it ran. */
  result: unknown;
}

/** A workflow body: deterministic code that drives side effects through step. */
export type Workflow = (ctx: WorkflowContext) => unknown;

/**
 * Handed to the workflow body. step is the only way to do something with a side
 * effect; its result is memoized in the history.
 */
export class WorkflowContext {
  private cursor = 0;

  constructor(
    private readonly engine: Engine,
    private readonly history: HistoryEvent[],
  ) {}

  /**
   * Run fn the first time this position is reached, record its result in the
   * durable history and unwind so the engine can persist before continuing. On
   * every later execution (including after a crash) return the recorded result
   * without running fn again, which is what makes replay idempotent.
   */
  step(stepId: string, fn: () => unknown): unknown {
    // Already recorded? Serve from history, do NOT run the side effect again.
    if (this.cursor < this.history.length) {
      const event = this.history[this.cursor];
      if (event.stepId !== stepId) {
        throw new NonDeterministicWorkflowError(
          `non-deterministic workflow: expected step '${event.stepId}' ` +
            `at position ${this.cursor}, got '${stepId}'`,
        );
      }
      this.cursor += 1;
      return event.result;
    }
    // First time: run it, append to history, and unwind so the engine persists
    // before continuing. The history is durable before the next step is tried.
    const result = fn();
    this.history.push({ seq: this.history.length, stepId, result });
    this.engine.persist(this.history);
    throw new ReplayError();
  }
}

/**
 * Runs a workflow to completion, replaying from history each time a new step is
 * recorded. store is any object with load(wid) and save(wid, history).
 */
export class Engine {
  private wid = "";
  /**
   * How many times the workflow was re-executed from the top, which equals the
   * number of steps that had to be recorded.
   */
  replays = 0;

  constructor(private readonly store: HistoryStore) {}

  /** Internal: persist the current history under the running workflow id. */
  persist(history: HistoryEvent[]): void {
    this.store.save(this.wid, history);
  }

  /**
   * Execute workflow to completion for workflow id wid, replaying completed
   * steps from durable history and continuing live from the first unfinished
   * one.
   */
  run(wid: string, workflow: Workflow): unknown {
    this.wid = wid;
    let history = this.store.load(wid);
    for (;;) {
      const ctx = new WorkflowContext(this, history);
      try {
        return workflow(ctx);
      } catch (err) {
        if (err instanceof ReplayError) {
          this.replays += 1;
          history = this.store.load(wid); // reload exactly what is durable
          continue;
        }
        throw err;
      }
    }
  }

  /**
   * Simulate a crash-and-restart: the same call as run, but history is whatever
   * was already persisted, so completed steps will not re-execute.
   */
  resume(wid: string, workflow: Workflow): unknown {
    return this.run(wid, workflow);
  }
}

/**
 * In-memory append-only store. A real one would be a database table or a log;
 * the interface is deliberately tiny so it is swappable.
 */
export class HistoryStore {
  private readonly data = new Map<string, HistoryEvent[]>();

  /** Return a copy of the durable history for wid (empty if there is none). */
  load(wid: string): HistoryEvent[] {
    const h = this.data.get(wid);
    return h ? h.slice() : [];
  }

  /** Durably record a copy of history under wid. */
  save(wid: string, history: HistoryEvent[]): void {
    this.data.set(wid, history.slice());
  }
}
