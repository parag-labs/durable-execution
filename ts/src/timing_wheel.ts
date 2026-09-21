/**
 * A hashed timing wheel with overflow: schedule many timers cheaply.
 *
 * A timing wheel buckets timers by fire time so insertion and per-tick work are
 * O(1) for the common case (a timer due within one revolution of the wheel).
 * Timers further out than the wheel spans go into an overflow list that is
 * re-homed when they come into range - the standard "wheel + overflow" design
 * used by Kafka and Netty for their timers.
 */

/** A scheduled callback with its absolute fire time. */
export class Timer {
  /** Set to true to prevent the callback from firing when the timer is due. */
  cancelled = false;

  constructor(
    /** Absolute virtual time at which the callback becomes due. */
    readonly deadline: number,
    /** The callback to run when the timer fires. */
    readonly callback: () => void,
  ) {}
}

/**
 * `slots` buckets, each covering one tick of virtual time. Timers due within
 * `slots` ticks land directly in a bucket (O(1)); anything further out waits in
 * an overflow list and is re-homed as time advances.
 */
export class HierarchicalTimingWheel {
  private readonly slots: number;
  private buckets: Timer[][];
  private overflow: Timer[] = [];
  private _now = 0;
  private _size = 0;

  /**
   * Build a wheel. `levels` simply widens the span (span = slotsPerWheel *
   * levels); a single wheel with overflow is correct and O(1) near-term.
   */
  constructor(slotsPerWheel = 256, levels = 1) {
    this.slots = slotsPerWheel * Math.max(1, levels);
    this.buckets = Array.from({ length: this.slots }, () => []);
  }

  /** Current virtual time. */
  get now(): number {
    return this._now;
  }

  /** Number of live (scheduled, not yet fired) timers. */
  get size(): number {
    return this._size;
  }

  /** Schedule callback to fire delay ticks from now. Throws if delay < 0. */
  schedule(delay: number, callback: () => void): Timer {
    if (delay < 0) {
      throw new Error("delay must be >= 0");
    }
    const t = new Timer(this._now + delay, callback);
    this.place(t);
    this._size += 1;
    return t;
  }

  private place(t: Timer): void {
    if (t.deadline - this._now < this.slots) {
      this.buckets[t.deadline % this.slots].push(t);
    } else {
      this.overflow.push(t);
    }
  }

  /**
   * Advance virtual time to `to`, firing every due timer in order. Returns the
   * number that fired. Advancing to a time at or before now does nothing.
   */
  advance(to: number): number {
    let fired = 0;
    while (this._now < to) {
      this._now += 1;
      fired += this.tick();
    }
    return fired;
  }

  private tick(): number {
    let fired = 0;
    const slot = this._now % this.slots;
    const bucket = this.buckets[slot];
    // A bucket can hold timers for deadline == now and (after a full wrap)
    // deadlines a multiple of `slots` away; only fire the ones actually due.
    const due: Timer[] = [];
    const keep: Timer[] = [];
    for (const t of bucket) {
      if (t.deadline <= this._now) {
        due.push(t);
      } else {
        keep.push(t);
      }
    }
    this.buckets[slot] = keep;
    for (const t of due) {
      if (!t.cancelled) {
        t.callback();
        fired += 1;
      }
      this._size -= 1;
    }
    // Each time the wheel completes a revolution, pull any overflow timers that
    // have come into range back onto the wheel.
    if (slot === this.slots - 1 && this.overflow.length > 0) {
      const still: Timer[] = [];
      for (const t of this.overflow) {
        if (t.deadline - this._now < this.slots) {
          this.buckets[t.deadline % this.slots].push(t);
        } else {
          still.push(t);
        }
      }
      this.overflow = still;
    }
    return fired;
  }
}
