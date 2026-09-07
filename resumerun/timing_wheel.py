"""A hashed timing wheel with overflow: schedule many timers cheaply.

A durable workflow engine is mostly timers - "retry in 30s", "time this step out
in 5m", "sleep until tomorrow". A min-heap of N timers costs O(log N) per op and
gets slow when N is large and churny. A timing wheel buckets timers by fire time
so insertion and per-tick work are O(1) for the common case (a timer due within
one revolution of the wheel). Timers further out than the wheel spans go into an
overflow list that is re-homed when they come into range - the standard "wheel +
overflow" design used when you want O(1) near-term behaviour without unbounded
wheels. This is the structure Kafka and Netty use for their timers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass
class Timer:
    deadline: int
    callback: Callable[[], None]
    cancelled: bool = False


class HierarchicalTimingWheel:
    """`slots` buckets, each covering one tick of virtual time. Timers due within
    `slots` ticks land directly in a bucket (O(1)); anything further out waits in
    an overflow list and is re-homed as time advances."""

    def __init__(self, slots_per_wheel: int = 256, levels: int = 1) -> None:
        # `levels` simply widens the span (span = slots_per_wheel * levels). A
        # single wheel with overflow is correct and O(1) near-term, which is the
        # property that matters; a wider span just delays overflow re-homing.
        self.slots = slots_per_wheel * max(1, levels)
        self.buckets: list[list[Timer]] = [[] for _ in range(self.slots)]
        self.overflow: list[Timer] = []
        self.now = 0
        self.size = 0

    def schedule(self, delay: int, callback: Callable[[], None]) -> Timer:
        if delay < 0:
            raise ValueError("delay must be >= 0")
        t = Timer(self.now + delay, callback)
        self._place(t)
        self.size += 1
        return t

    def _place(self, t: Timer) -> None:
        if t.deadline - self.now < self.slots:
            self.buckets[t.deadline % self.slots].append(t)
        else:
            self.overflow.append(t)

    def advance(self, to: int) -> int:
        """Advance virtual time to `to`, firing every due timer in order. Returns
        the number that fired."""
        fired = 0
        while self.now < to:
            self.now += 1
            fired += self._tick()
        return fired

    def _tick(self) -> int:
        fired = 0
        slot = self.now % self.slots
        bucket = self.buckets[slot]
        # A bucket can hold timers for deadline == now and (after a full wrap)
        # deadlines a multiple of `slots` away; only fire the ones actually due.
        due = [t for t in bucket if t.deadline <= self.now]
        self.buckets[slot] = [t for t in bucket if t.deadline > self.now]
        for t in due:
            if not t.cancelled:
                t.callback()
                fired += 1
            self.size -= 1
        # Each time the wheel completes a revolution, pull any overflow timers
        # that have come into range back onto the wheel.
        if slot == self.slots - 1 and self.overflow:
            still: list[Timer] = []
            for t in self.overflow:
                if t.deadline - self.now < self.slots:
                    self.buckets[t.deadline % self.slots].append(t)
                else:
                    still.append(t)
            self.overflow = still
        return fired
