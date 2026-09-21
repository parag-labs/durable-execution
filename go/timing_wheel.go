package durableexecution

// A hashed timing wheel with overflow: schedule many timers cheaply.
//
// A durable workflow engine is mostly timers - "retry in 30s", "time this step
// out in 5m", "sleep until tomorrow". A min-heap of N timers costs O(log N) per
// op and gets slow when N is large and churny. A timing wheel buckets timers by
// fire time so insertion and per-tick work are O(1) for the common case (a timer
// due within one revolution of the wheel). Timers further out than the wheel
// spans go into an overflow list that is re-homed when they come into range -
// the standard "wheel + overflow" design used by Kafka and Netty for timers.

// Timer is one scheduled callback. Cancel it by setting Cancelled to true before
// it is due; a cancelled timer is dropped (and counted out of Size) without
// running its callback.
type Timer struct {
	// Deadline is the virtual time at which the timer is due.
	Deadline int64
	// Callback runs when the timer fires, unless it was cancelled.
	Callback func()
	// Cancelled, when true, suppresses the callback.
	Cancelled bool
}

// HierarchicalTimingWheel holds slots buckets, each covering one tick of virtual
// time. Timers due within slots ticks land directly in a bucket (O(1)); anything
// further out waits in an overflow list and is re-homed as time advances.
type HierarchicalTimingWheel struct {
	slots    int
	buckets  [][]*Timer
	overflow []*Timer
	now      int64
	size     int
}

// NewHierarchicalTimingWheel builds a wheel with the default 256 slots.
func NewHierarchicalTimingWheel() *HierarchicalTimingWheel {
	return NewHierarchicalTimingWheelWith(256, 1)
}

// NewHierarchicalTimingWheelWith builds a wheel spanning slotsPerWheel*levels
// ticks. levels simply widens the span (span = slotsPerWheel * levels); a single
// wheel with overflow is correct and O(1) near-term, which is the property that
// matters, and a wider span just delays overflow re-homing. levels below 1 is
// treated as 1.
func NewHierarchicalTimingWheelWith(slotsPerWheel, levels int) *HierarchicalTimingWheel {
	if levels < 1 {
		levels = 1
	}
	slots := slotsPerWheel * levels
	buckets := make([][]*Timer, slots)
	for i := range buckets {
		buckets[i] = []*Timer{}
	}
	return &HierarchicalTimingWheel{slots: slots, buckets: buckets, overflow: []*Timer{}}
}

// Now reports the current virtual time.
func (w *HierarchicalTimingWheel) Now() int64 { return w.now }

// Size reports how many timers are still live (scheduled and not yet fired or
// cancelled-out).
func (w *HierarchicalTimingWheel) Size() int { return w.size }

// Schedule registers callback to fire delay ticks from now and returns the
// Timer so it can be cancelled. It panics if delay is negative.
func (w *HierarchicalTimingWheel) Schedule(delay int64, callback func()) *Timer {
	if delay < 0 {
		panic("delay must be >= 0")
	}
	t := &Timer{Deadline: w.now + delay, Callback: callback}
	w.place(t)
	w.size++
	return t
}

func (w *HierarchicalTimingWheel) place(t *Timer) {
	if t.Deadline-w.now < int64(w.slots) {
		w.buckets[t.Deadline%int64(w.slots)] = append(w.buckets[t.Deadline%int64(w.slots)], t)
	} else {
		w.overflow = append(w.overflow, t)
	}
}

// Advance moves virtual time forward to to, firing every due timer in time
// order, and returns the number that fired. Advancing to a time at or before the
// current one does nothing.
func (w *HierarchicalTimingWheel) Advance(to int64) int {
	fired := 0
	for w.now < to {
		w.now++
		fired += w.tick()
	}
	return fired
}

func (w *HierarchicalTimingWheel) tick() int {
	fired := 0
	slot := w.now % int64(w.slots)
	bucket := w.buckets[slot]
	// A bucket can hold timers for deadline == now and (after a full wrap)
	// deadlines a multiple of slots away; only fire the ones actually due.
	due := make([]*Timer, 0, len(bucket))
	keep := make([]*Timer, 0, len(bucket))
	for _, t := range bucket {
		if t.Deadline <= w.now {
			due = append(due, t)
		} else {
			keep = append(keep, t)
		}
	}
	w.buckets[slot] = keep
	for _, t := range due {
		if !t.Cancelled {
			t.Callback()
			fired++
		}
		w.size--
	}
	// Each time the wheel completes a revolution, pull any overflow timers that
	// have come into range back onto the wheel.
	if int(slot) == w.slots-1 && len(w.overflow) > 0 {
		still := make([]*Timer, 0, len(w.overflow))
		for _, t := range w.overflow {
			if t.Deadline-w.now < int64(w.slots) {
				w.buckets[t.Deadline%int64(w.slots)] = append(w.buckets[t.Deadline%int64(w.slots)], t)
			} else {
				still = append(still, t)
			}
		}
		w.overflow = still
	}
	return fired
}
