// A hashed timing wheel with overflow: schedule many timers cheaply.
//
// A durable workflow engine is mostly timers - "retry in 30s", "time this step out
// in 5m", "sleep until tomorrow". A min-heap of N timers costs O(log N) per op and
// gets slow when N is large and churny. A timing wheel buckets timers by fire time
// so insertion and per-tick work are O(1) for the common case (a timer due within
// one revolution of the wheel). Timers further out than the wheel spans go into an
// overflow list that is re-homed when they come into range - the standard "wheel +
// overflow" design used by Kafka and Netty for their timers.

using System;
using System.Collections.Generic;

namespace DurableExecution;

public sealed class Timer
{
    public long Deadline { get; }
    public Action Callback { get; }
    public bool Cancelled { get; set; }

    public Timer(long deadline, Action callback)
    {
        Deadline = deadline;
        Callback = callback;
    }
}

/// <summary>
/// <c>slots</c> buckets, each covering one tick of virtual time. Timers due within
/// <c>slots</c> ticks land directly in a bucket (O(1)); anything further out waits
/// in an overflow list and is re-homed as time advances.
/// </summary>
public sealed class HierarchicalTimingWheel
{
    private readonly int _slots;
    private readonly List<Timer>[] _buckets;
    private List<Timer> _overflow = new();

    public long Now { get; private set; }
    public int Size { get; private set; }

    public HierarchicalTimingWheel(int slotsPerWheel = 256, int levels = 1)
    {
        // `levels` simply widens the span (span = slotsPerWheel * levels). A single
        // wheel with overflow is correct and O(1) near-term, which is the property
        // that matters; a wider span just delays overflow re-homing.
        _slots = slotsPerWheel * Math.Max(1, levels);
        _buckets = new List<Timer>[_slots];
        for (var i = 0; i < _slots; i++) _buckets[i] = new List<Timer>();
    }

    public Timer Schedule(long delay, Action callback)
    {
        if (delay < 0) throw new ArgumentException("delay must be >= 0");
        var t = new Timer(Now + delay, callback);
        Place(t);
        Size++;
        return t;
    }

    private void Place(Timer t)
    {
        if (t.Deadline - Now < _slots)
            _buckets[(int)(t.Deadline % _slots)].Add(t);
        else
            _overflow.Add(t);
    }

    /// <summary>
    /// Advance virtual time to <paramref name="to"/>, firing every due timer in
    /// order. Returns the number that fired.
    /// </summary>
    public int Advance(long to)
    {
        var fired = 0;
        while (Now < to)
        {
            Now++;
            fired += Tick();
        }
        return fired;
    }

    private int Tick()
    {
        var fired = 0;
        var slot = (int)(Now % _slots);
        var bucket = _buckets[slot];
        // A bucket can hold timers for deadline == now and (after a full wrap)
        // deadlines a multiple of `slots` away; only fire the ones actually due.
        var due = new List<Timer>();
        var keep = new List<Timer>();
        foreach (var t in bucket)
        {
            if (t.Deadline <= Now) due.Add(t);
            else keep.Add(t);
        }
        _buckets[slot] = keep;
        foreach (var t in due)
        {
            if (!t.Cancelled)
            {
                t.Callback();
                fired++;
            }
            Size--;
        }
        // Each time the wheel completes a revolution, pull any overflow timers that
        // have come into range back onto the wheel.
        if (slot == _slots - 1 && _overflow.Count > 0)
        {
            var still = new List<Timer>();
            foreach (var t in _overflow)
            {
                if (t.Deadline - Now < _slots)
                    _buckets[(int)(t.Deadline % _slots)].Add(t);
                else
                    still.Add(t);
            }
            _overflow = still;
        }
        return fired;
    }
}
