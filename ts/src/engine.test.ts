import { describe, expect, it } from "vitest";

import {
  Engine,
  HistoryStore,
  NonDeterministicWorkflowError,
  type Workflow,
  type WorkflowContext,
} from "./engine.js";

describe("Engine", () => {
  it("runs a workflow to completion and returns each step result", () => {
    const store = new HistoryStore();
    const engine = new Engine(store);
    const calls: string[] = [];

    const wf: Workflow = (ctx) => {
      const a = ctx.step("charge", () => {
        calls.push("charge");
        return 100;
      });
      const b = ctx.step("ship", () => {
        calls.push("ship");
        return "tracking-1";
      });
      return [a, b];
    };

    const result = engine.run("order-1", wf);
    expect(result).toEqual([100, "tracking-1"]);
    expect(calls).toEqual(["charge", "ship"]);
  });

  it("replays exactly once per recorded step", () => {
    const store = new HistoryStore();
    const engine = new Engine(store);
    const wf: Workflow = (ctx) => {
      ctx.step("a", () => 1);
      ctx.step("b", () => 2);
      ctx.step("c", () => 3);
      return "done";
    };
    engine.run("wf", wf);
    expect(engine.replays).toBe(3);
  });

  it("records nothing for a stepless workflow", () => {
    const store = new HistoryStore();
    const engine = new Engine(store);
    const result = engine.run("wf", (_ctx) => "immediate");
    expect(result).toBe("immediate");
    expect(engine.replays).toBe(0);
    expect(store.load("wf")).toHaveLength(0);
  });

  it("does not re-execute completed steps when a fresh engine resumes", () => {
    const store = new HistoryStore();
    let calls: string[] = [];
    const wf: Workflow = (ctx) => {
      const a = ctx.step("charge", () => {
        calls.push("charge");
        return 100;
      });
      const b = ctx.step("ship", () => {
        calls.push("ship");
        return "t-1";
      });
      return [a, b];
    };

    new Engine(store).run("order-1", wf);
    expect(calls).toEqual(["charge", "ship"]);

    // A brand-new engine resumes against the same durable history: the side
    // effects must NOT fire again - the no-double-charge guarantee.
    calls = [];
    const result = new Engine(store).resume("order-1", wf);
    expect(result).toEqual([100, "t-1"]);
    expect(calls).toEqual([]);
  });

  it("resumes after a crash without repeating a completed step", () => {
    const store = new HistoryStore();
    let chargeCount = 0;

    const makeWf = (stepsBeforeCrash: number): Workflow => {
      return (ctx) => {
        ctx.step("charge", () => {
          chargeCount += 1;
          return 100;
        });
        if (stepsBeforeCrash === 1) {
          throw new Error("simulated process crash after charge persisted");
        }
        ctx.step("ship", () => "t-1");
        return "done";
      };
    };

    // Run until it "crashes" right after charge is durably recorded.
    expect(() => new Engine(store).run("order-1", makeWf(1))).toThrow(
      "simulated process crash",
    );
    expect(chargeCount).toBe(1);

    // Restart with the full workflow: charge is served from history, not re-run.
    const result = new Engine(store).resume("order-1", makeWf(99));
    expect(result).toBe("done");
    expect(chargeCount).toBe(1);
  });

  it("refuses to hide a non-deterministic step order on replay", () => {
    const store = new HistoryStore();

    const wfV1: Workflow = (ctx) => {
      ctx.step("a", () => 1);
      ctx.step("b", () => 2);
      return "ok";
    };
    new Engine(store).run("wf", wfV1);

    const wfV2: Workflow = (ctx) => {
      ctx.step("a", () => 1);
      ctx.step("DIFFERENT", () => 2);
      return "ok";
    };

    expect(() => new Engine(store).resume("wf", wfV2)).toThrow(
      NonDeterministicWorkflowError,
    );
  });

  it("names both the recorded and the observed step in the mismatch error", () => {
    const store = new HistoryStore();
    new Engine(store).run("wf", (ctx) => {
      ctx.step("expected", () => 1);
      return "ok";
    });

    let message = "";
    try {
      new Engine(store).resume("wf", (ctx) => {
        ctx.step("actual", () => 1);
        return "ok";
      });
    } catch (err) {
      expect(err).toBeInstanceOf(NonDeterministicWorkflowError);
      message = (err as Error).message;
    }
    expect(message).toContain("expected");
    expect(message).toContain("actual");
  });

  it("serves recorded results from history on replay", () => {
    const store = new HistoryStore();
    let runs = 0;
    new Engine(store).run("wf", (ctx) => {
      runs += 1;
      const v = ctx.step("only", () => 7);
      expect(v).toBe(7);
      return v;
    });
    // The body runs once to record the step and once more to complete.
    expect(runs).toBe(2);
  });

  it("keeps separate histories for separate workflow ids on one engine", () => {
    const store = new HistoryStore();
    const engine = new Engine(store);
    const build =
      (tag: string): Workflow =>
      (ctx) =>
        ctx.step("only", () => tag);

    expect(engine.run("left", build("L"))).toBe("L");
    expect(engine.run("right", build("R"))).toBe("R");
    expect(store.load("left")).toHaveLength(1);
    expect(store.load("right")).toHaveLength(1);
  });

  it("returns copies from the store so callers cannot mutate durable history", () => {
    const store = new HistoryStore();
    new Engine(store).run("wf", (ctx: WorkflowContext) => {
      ctx.step("only", () => 1);
      return "ok";
    });
    const loaded = store.load("wf");
    expect(loaded).toHaveLength(1);
    loaded.push({ seq: 99, stepId: "injected", result: null });
    expect(store.load("wf")).toHaveLength(1);
  });
});
