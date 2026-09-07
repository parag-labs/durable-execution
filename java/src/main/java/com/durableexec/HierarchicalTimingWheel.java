// A hashed timing wheel with overflow: schedule many timers cheaply.
//
// A durable workflow engine is mostly timers - "retry in 30s", "time this step out
// in 5m", "sleep until tomorrow". A min-heap of N timers costs O(log N) per op and
// gets slow when N is large and churny. A timing wheel buckets timers by fire time
// so insertion and per-tick work are O(1) for the common case (a timer due within
// one revolution of the wheel). Timers further out than the wheel spans go into an
// overflow list that is re-homed when they come into range - the standard "wheel +
// overflow" design used by Kafka and Netty for their timers.

package com.durableexec;

import java.util.ArrayList;
import java.util.List;

public final class HierarchicalTimingWheel {

    /** One scheduled timer. Cancel by flipping {@code cancelled}. */
    public static final class Timer {
        final long deadline;
        final Runnable callback;
        public boolean cancelled = false;

        Timer(long deadline, Runnable callback) {
            this.deadline = deadline;
            this.callback = callback;
        }

        public void setCancelled(boolean c) { this.cancelled = c; }
    }

    private final int slots;
    private final List<List<Timer>> buckets;
    private List<Timer> overflow = new ArrayList<>();
    private long now = 0;
    private int size = 0;

    public HierarchicalTimingWheel() {
        this(256, 1);
    }

    /** {@code slots} buckets, each covering one tick of virtual time. Timers due
     * within {@code slots} ticks land directly in a bucket (O(1)); anything further
     * out waits in an overflow list and is re-homed as time advances. */
    public HierarchicalTimingWheel(int slotsPerWheel, int levels) {
        // `levels` simply widens the span (span = slotsPerWheel * levels). A single
        // wheel with overflow is correct and O(1) near-term, which is the property
        // that matters; a wider span just delays overflow re-homing.
        this.slots = slotsPerWheel * Math.max(1, levels);
        this.buckets = new ArrayList<>(slots);
        for (int i = 0; i < slots; i++) buckets.add(new ArrayList<>());
    }

    public long now() { return now; }
    public int size() { return size; }

    public Timer schedule(long delay, Runnable callback) {
        if (delay < 0) throw new IllegalArgumentException("delay must be >= 0");
        Timer t = new Timer(now + delay, callback);
        place(t);
        size++;
        return t;
    }

    private void place(Timer t) {
        if (t.deadline - now < slots) {
            buckets.get((int) (t.deadline % slots)).add(t);
        } else {
            overflow.add(t);
        }
    }

    /** Advance virtual time to {@code to}, firing every due timer in order. Returns
     * the number that fired. */
    public int advance(long to) {
        int fired = 0;
        while (now < to) {
            now++;
            fired += tick();
        }
        return fired;
    }

    private int tick() {
        int fired = 0;
        int slot = (int) (now % slots);
        List<Timer> bucket = buckets.get(slot);
        // A bucket can hold timers for deadline == now and (after a full wrap)
        // deadlines a multiple of `slots` away; only fire the ones actually due.
        List<Timer> due = new ArrayList<>();
        List<Timer> keep = new ArrayList<>();
        for (Timer t : bucket) {
            if (t.deadline <= now) due.add(t);
            else keep.add(t);
        }
        buckets.set(slot, keep);
        for (Timer t : due) {
            if (!t.cancelled) {
                t.callback.run();
                fired++;
            }
            size--;
        }
        // Each time the wheel completes a revolution, pull any overflow timers that
        // have come into range back onto the wheel.
        if (slot == slots - 1 && !overflow.isEmpty()) {
            List<Timer> still = new ArrayList<>();
            for (Timer t : overflow) {
                if (t.deadline - now < slots) {
                    buckets.get((int) (t.deadline % slots)).add(t);
                } else {
                    still.add(t);
                }
            }
            overflow = still;
        }
        return fired;
    }
}
