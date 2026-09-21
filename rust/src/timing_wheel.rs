//! A hashed timing wheel with overflow: schedule many timers cheaply.
//!
//! A durable workflow engine is mostly timers - "retry in 30s", "time this step
//! out in 5m", "sleep until tomorrow". A min-heap of N timers costs O(log N) per
//! op and gets slow when N is large and churny. A timing wheel buckets timers by
//! fire time so insertion and per-tick work are O(1) for the common case (a
//! timer due within one revolution of the wheel). Timers further out than the
//! wheel spans go into an overflow list that is re-homed when they come into
//! range - the standard "wheel + overflow" design used by Kafka and Netty.

use std::cell::RefCell;
use std::rc::Rc;

/// One scheduled timer. Cancel it by setting `cancelled` to `true` before it is
/// due; a cancelled timer is dropped (and counted out of the size) without
/// running its callback.
pub struct Timer {
    /// The virtual time at which the timer is due.
    pub deadline: i64,
    /// The callback that runs when the timer fires, unless it was cancelled.
    pub callback: Box<dyn FnMut()>,
    /// When `true`, suppresses the callback.
    pub cancelled: bool,
}

/// A shared handle to a scheduled [`Timer`]. [`HierarchicalTimingWheel::schedule`]
/// returns one so the caller can cancel the timer it still owns on the wheel,
/// mirroring the reference's reference-semantics timer object.
pub type TimerRef = Rc<RefCell<Timer>>;

/// `slots` buckets, each covering one tick of virtual time. Timers due within
/// `slots` ticks land directly in a bucket (O(1)); anything further out waits in
/// an overflow list and is re-homed as time advances.
pub struct HierarchicalTimingWheel {
    slots: usize,
    buckets: Vec<Vec<TimerRef>>,
    overflow: Vec<TimerRef>,
    now: i64,
    size: usize,
}

impl Default for HierarchicalTimingWheel {
    fn default() -> Self {
        Self::new()
    }
}

impl HierarchicalTimingWheel {
    /// Builds a wheel with the default 256 slots.
    pub fn new() -> Self {
        Self::with_config(256, 1)
    }

    /// Builds a wheel spanning `slots_per_wheel * levels` ticks. `levels` simply
    /// widens the span; a single wheel with overflow is correct and O(1)
    /// near-term, and a wider span just delays overflow re-homing. `levels` below
    /// 1 is treated as 1.
    pub fn with_config(slots_per_wheel: usize, levels: usize) -> Self {
        let levels = levels.max(1);
        let slots = slots_per_wheel * levels;
        HierarchicalTimingWheel {
            slots,
            buckets: (0..slots).map(|_| Vec::new()).collect(),
            overflow: Vec::new(),
            now: 0,
            size: 0,
        }
    }

    /// The current virtual time.
    pub fn now(&self) -> i64 {
        self.now
    }

    /// How many timers are still live (scheduled and not yet fired or
    /// cancelled-out).
    pub fn size(&self) -> usize {
        self.size
    }

    /// Registers `callback` to fire `delay` ticks from now and returns the timer
    /// handle so it can be cancelled.
    ///
    /// # Panics
    ///
    /// Panics if `delay` is negative.
    pub fn schedule<F>(&mut self, delay: i64, callback: F) -> TimerRef
    where
        F: FnMut() + 'static,
    {
        if delay < 0 {
            panic!("delay must be >= 0");
        }
        let timer = Rc::new(RefCell::new(Timer {
            deadline: self.now + delay,
            callback: Box::new(callback),
            cancelled: false,
        }));
        self.place(Rc::clone(&timer));
        self.size += 1;
        timer
    }

    fn place(&mut self, timer: TimerRef) {
        let deadline = timer.borrow().deadline;
        if deadline - self.now < self.slots as i64 {
            let idx = (deadline % self.slots as i64) as usize;
            self.buckets[idx].push(timer);
        } else {
            self.overflow.push(timer);
        }
    }

    /// Advances virtual time to `to`, firing every due timer in time order, and
    /// returns the number that fired. Advancing to a time at or before the
    /// current one does nothing.
    pub fn advance(&mut self, to: i64) -> usize {
        let mut fired = 0;
        while self.now < to {
            self.now += 1;
            fired += self.tick();
        }
        fired
    }

    fn tick(&mut self) -> usize {
        let mut fired = 0;
        let slot = (self.now % self.slots as i64) as usize;
        // A bucket can hold timers for deadline == now and (after a full wrap)
        // deadlines a multiple of `slots` away; only fire the ones actually due.
        let bucket = std::mem::take(&mut self.buckets[slot]);
        let mut keep = Vec::with_capacity(bucket.len());
        let mut due = Vec::new();
        for timer in bucket {
            if timer.borrow().deadline <= self.now {
                due.push(timer);
            } else {
                keep.push(timer);
            }
        }
        self.buckets[slot] = keep;
        for timer in due {
            {
                let mut t = timer.borrow_mut();
                if !t.cancelled {
                    (t.callback)();
                    fired += 1;
                }
            }
            self.size -= 1;
        }
        // Each time the wheel completes a revolution, pull any overflow timers
        // that have come into range back onto the wheel.
        if slot == self.slots - 1 && !self.overflow.is_empty() {
            let overflow = std::mem::take(&mut self.overflow);
            let mut still = Vec::new();
            for timer in overflow {
                let deadline = timer.borrow().deadline;
                if deadline - self.now < self.slots as i64 {
                    let idx = (deadline % self.slots as i64) as usize;
                    self.buckets[idx].push(timer);
                } else {
                    still.push(timer);
                }
            }
            self.overflow = still;
        }
        fired
    }
}
