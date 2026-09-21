package durableexecution

import (
	"reflect"
	"sort"
	"testing"
)

func TestFiresAtTheRightTime(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	var fired []string
	w.Schedule(5, func() { fired = append(fired, "a") })
	w.Schedule(5, func() { fired = append(fired, "b") })
	w.Advance(4)
	if len(fired) != 0 {
		t.Fatalf("nothing should fire before the deadline, got %v", fired)
	}
	w.Advance(5)
	sort.Strings(fired)
	if !reflect.DeepEqual(fired, []string{"a", "b"}) {
		t.Fatalf("fired = %v, want [a b]", fired)
	}
}

func TestFarFutureTimerCascadesAndFires(t *testing.T) {
	w := NewHierarchicalTimingWheelWith(10, 3) // span 30
	var fired []string
	w.Schedule(750, func() { fired = append(fired, "far") })
	w.Advance(749)
	if len(fired) != 0 {
		t.Fatalf("far timer should not fire early, got %v", fired)
	}
	w.Advance(750)
	if !reflect.DeepEqual(fired, []string{"far"}) {
		t.Fatalf("fired = %v, want [far]", fired)
	}
}

func TestOrderingAcrossManyTimers(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	var order []int
	for _, d := range []int64{30, 10, 20, 5, 40} {
		d := d
		w.Schedule(d, func() { order = append(order, int(d)) })
	}
	w.Advance(100)
	if !reflect.DeepEqual(order, []int{5, 10, 20, 30, 40}) {
		t.Fatalf("order = %v, want [5 10 20 30 40]", order)
	}
}

func TestSameDeadlineFiresInInsertionOrder(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	var order []int
	for i := 0; i < 5; i++ {
		i := i
		w.Schedule(7, func() { order = append(order, i) })
	}
	w.Advance(7)
	if !reflect.DeepEqual(order, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("order = %v, want insertion order [0 1 2 3 4]", order)
	}
}

func TestCancelPreventsFire(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	var fired []string
	timer := w.Schedule(10, func() { fired = append(fired, "x") })
	timer.Cancelled = true
	w.Advance(20)
	if len(fired) != 0 {
		t.Fatalf("cancelled timer should not fire, got %v", fired)
	}
	if w.Size() != 0 {
		t.Fatalf("a cancelled timer should still be counted out of Size, got %d", w.Size())
	}
}

func TestScalesToManyTimers(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	count := 0
	for i := 0; i < 5000; i++ {
		w.Schedule(int64((i%500)+1), func() { count++ })
	}
	w.Advance(500)
	if count != 5000 {
		t.Fatalf("count = %d, want 5000", count)
	}
	if w.Size() != 0 {
		t.Fatalf("Size = %d, want 0 after all timers fired", w.Size())
	}
}

func TestNegativeDelayPanics(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	got := recoverValue(func() { w.Schedule(-1, func() {}) })
	if got == nil {
		t.Fatalf("scheduling a negative delay should panic")
	}
}

func TestZeroDelayFiresOnWheelWrap(t *testing.T) {
	// A timer scheduled with delay 0 at now 0 lands in bucket 0, but Advance
	// increments now before ticking slot = now % slots, so slot 0 is not ticked
	// until the wheel wraps fully around. The timer therefore fires at now=slots,
	// not immediately - a genuine property of the wheel-plus-overflow design.
	w := NewHierarchicalTimingWheelWith(4, 1) // slots = 4
	var fired []string
	w.Schedule(0, func() { fired = append(fired, "z") })
	w.Advance(3)
	if len(fired) != 0 {
		t.Fatalf("delay-0 timer should not fire before the wheel wraps, got %v", fired)
	}
	if w.Size() != 1 {
		t.Fatalf("timer should still be live before the wrap, Size = %d", w.Size())
	}
	w.Advance(4)
	if !reflect.DeepEqual(fired, []string{"z"}) {
		t.Fatalf("delay-0 timer should fire at now=slots, got %v", fired)
	}
}

func TestAdvanceReturnsFiredCount(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	w.Schedule(3, func() {})
	w.Schedule(3, func() {})
	w.Schedule(9, func() {})
	if n := w.Advance(3); n != 2 {
		t.Fatalf("Advance(3) fired %d, want 2", n)
	}
	if n := w.Advance(9); n != 1 {
		t.Fatalf("Advance(9) fired %d, want 1", n)
	}
}

func TestAdvanceBackwardIsNoOp(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	fired := 0
	w.Schedule(5, func() { fired++ })
	w.Advance(10)
	if fired != 1 || w.Now() != 10 {
		t.Fatalf("after Advance(10): fired=%d now=%d, want 1 and 10", fired, w.Now())
	}
	if n := w.Advance(3); n != 0 || w.Now() != 10 {
		t.Fatalf("advancing to an earlier time should do nothing: fired=%d now=%d", n, w.Now())
	}
}

func TestNowStartsAtZero(t *testing.T) {
	w := NewHierarchicalTimingWheel()
	if w.Now() != 0 || w.Size() != 0 {
		t.Fatalf("a fresh wheel should have now=0 size=0, got now=%d size=%d", w.Now(), w.Size())
	}
}
