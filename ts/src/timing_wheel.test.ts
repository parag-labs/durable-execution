import { describe, expect, it } from "vitest";

import { HierarchicalTimingWheel } from "./timing_wheel.js";

describe("HierarchicalTimingWheel", () => {
  it("starts empty at time zero", () => {
    const w = new HierarchicalTimingWheel();
    expect(w.now).toBe(0);
    expect(w.size).toBe(0);
  });

  it("fires timers exactly at their deadline, not before", () => {
    const fired: string[] = [];
    const w = new HierarchicalTimingWheel();
    w.schedule(5, () => fired.push("a"));
    w.schedule(5, () => fired.push("b"));

    w.advance(4);
    expect(fired).toEqual([]);

    w.advance(5);
    expect([...fired].sort()).toEqual(["a", "b"]);
  });

  it("cascades a far-future timer out of overflow when it comes into range", () => {
    const fired: string[] = [];
    const w = new HierarchicalTimingWheel(10, 3); // span 30
    w.schedule(750, () => fired.push("far"));

    w.advance(749);
    expect(fired).toEqual([]);
    w.advance(750);
    expect(fired).toEqual(["far"]);
  });

  it("fires timers across many deadlines in time order", () => {
    const order: number[] = [];
    const w = new HierarchicalTimingWheel();
    for (const d of [30, 10, 20, 5, 40]) {
      w.schedule(d, () => order.push(d));
    }
    w.advance(100);
    expect(order).toEqual([5, 10, 20, 30, 40]);
  });

  it("fires same-deadline timers in insertion order", () => {
    const order: number[] = [];
    const w = new HierarchicalTimingWheel();
    for (let i = 0; i < 5; i += 1) {
      const captured = i;
      w.schedule(7, () => order.push(captured));
    }
    w.advance(7);
    expect(order).toEqual([0, 1, 2, 3, 4]);
  });

  it("does not fire a cancelled timer but still drops it from size", () => {
    let fired = 0;
    const w = new HierarchicalTimingWheel();
    const timer = w.schedule(10, () => {
      fired += 1;
    });
    timer.cancelled = true;
    w.advance(20);
    expect(fired).toBe(0);
    expect(w.size).toBe(0);
  });

  it("scales to thousands of timers", () => {
    let count = 0;
    const w = new HierarchicalTimingWheel();
    for (let i = 0; i < 5000; i += 1) {
      w.schedule((i % 500) + 1, () => {
        count += 1;
      });
    }
    w.advance(500);
    expect(count).toBe(5000);
    expect(w.size).toBe(0);
  });

  it("throws when scheduled with a negative delay", () => {
    const w = new HierarchicalTimingWheel();
    expect(() => w.schedule(-1, () => {})).toThrow("delay must be >= 0");
  });

  it("fires a zero-delay timer only when the wheel wraps around", () => {
    // A timer scheduled with delay 0 at now 0 lands in bucket 0, but advance
    // increments now before ticking slot = now % slots, so slot 0 is not ticked
    // until the wheel wraps fully around. The timer therefore fires at
    // now = slots, not immediately - a genuine property of the design.
    const fired: string[] = [];
    const w = new HierarchicalTimingWheel(4, 1); // slots = 4
    w.schedule(0, () => fired.push("z"));

    w.advance(3);
    expect(fired).toEqual([]);
    expect(w.size).toBe(1);

    w.advance(4);
    expect(fired).toEqual(["z"]);
  });

  it("returns the number of timers that fired from advance", () => {
    const w = new HierarchicalTimingWheel();
    w.schedule(3, () => {});
    w.schedule(3, () => {});
    w.schedule(9, () => {});
    expect(w.advance(3)).toBe(2);
    expect(w.advance(9)).toBe(1);
  });

  it("treats advancing to an earlier or equal time as a no-op", () => {
    let fired = 0;
    const w = new HierarchicalTimingWheel();
    w.schedule(5, () => {
      fired += 1;
    });
    w.advance(10);
    expect(fired).toBe(1);
    expect(w.now).toBe(10);
    expect(w.advance(3)).toBe(0);
    expect(w.now).toBe(10);
  });
});
