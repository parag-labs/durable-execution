package durableexecution

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// recoverValue runs fn and returns whatever it panicked with (nil if it did
// not panic), so tests can assert on the propagated exception-like value.
func recoverValue(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

func TestWorkflowRunsToCompletion(t *testing.T) {
	store := NewHistoryStore()
	eng := NewEngine(store)
	var calls []string

	wf := func(ctx *WorkflowContext) any {
		a := ctx.Step("charge", func() any { calls = append(calls, "charge"); return 100 })
		b := ctx.Step("ship", func() any { calls = append(calls, "ship"); return "tracking-1" })
		return []any{a, b}
	}

	result := eng.Run("order-1", wf)
	if !reflect.DeepEqual(result, []any{100, "tracking-1"}) {
		t.Fatalf("result = %v, want [100 tracking-1]", result)
	}
	if !reflect.DeepEqual(calls, []string{"charge", "ship"}) {
		t.Fatalf("calls = %v, want [charge ship]", calls)
	}
}

func TestReplaysEqualStepCount(t *testing.T) {
	store := NewHistoryStore()
	eng := NewEngine(store)
	wf := func(ctx *WorkflowContext) any {
		ctx.Step("a", func() any { return 1 })
		ctx.Step("b", func() any { return 2 })
		ctx.Step("c", func() any { return 3 })
		return "done"
	}
	eng.Run("wf", wf)
	if eng.Replays != 3 {
		t.Fatalf("Replays = %d, want 3 (one per recorded step)", eng.Replays)
	}
}

func TestEmptyWorkflowRecordsNothing(t *testing.T) {
	store := NewHistoryStore()
	eng := NewEngine(store)
	result := eng.Run("wf", func(ctx *WorkflowContext) any { return "immediate" })
	if result != "immediate" {
		t.Fatalf("result = %v, want immediate", result)
	}
	if eng.Replays != 0 {
		t.Fatalf("Replays = %d, want 0 for a stepless workflow", eng.Replays)
	}
	if len(store.Load("wf")) != 0 {
		t.Fatalf("history should be empty for a stepless workflow")
	}
}

func TestCompletedStepsDoNotReExecuteOnResume(t *testing.T) {
	store := NewHistoryStore()
	var calls []string
	wf := func(ctx *WorkflowContext) any {
		a := ctx.Step("charge", func() any { calls = append(calls, "charge"); return 100 })
		b := ctx.Step("ship", func() any { calls = append(calls, "ship"); return "t-1" })
		return []any{a, b}
	}

	NewEngine(store).Run("order-1", wf)
	if !reflect.DeepEqual(calls, []string{"charge", "ship"}) {
		t.Fatalf("calls = %v, want [charge ship]", calls)
	}

	// A brand-new engine resumes against the same durable history: the side
	// effects must NOT fire again - that is the no-double-charge guarantee.
	calls = nil
	result := NewEngine(store).Resume("order-1", wf)
	if !reflect.DeepEqual(result, []any{100, "t-1"}) {
		t.Fatalf("result = %v, want [100 t-1]", result)
	}
	if len(calls) != 0 {
		t.Fatalf("calls = %v, want none on resume", calls)
	}
}

func TestCrashBetweenStepsResumesWithoutRepeating(t *testing.T) {
	store := NewHistoryStore()
	chargeCount := 0
	crash := errors.New("simulated process crash after charge persisted")

	makeWf := func(stepsBeforeCrash int) func(*WorkflowContext) any {
		return func(ctx *WorkflowContext) any {
			ctx.Step("charge", func() any { chargeCount++; return 100 })
			if stepsBeforeCrash == 1 {
				panic(crash)
			}
			ctx.Step("ship", func() any { return "t-1" })
			return "done"
		}
	}

	// Run until it "crashes" right after charge is durably recorded.
	got := recoverValue(func() { NewEngine(store).Run("order-1", makeWf(1)) })
	if got != crash {
		t.Fatalf("expected the crash to propagate, got %v", got)
	}
	if chargeCount != 1 {
		t.Fatalf("chargeCount = %d, want 1 after crash", chargeCount)
	}

	// Restart with the full workflow: charge is served from history, not re-run.
	result := NewEngine(store).Resume("order-1", makeWf(99))
	if result != "done" {
		t.Fatalf("result = %v, want done", result)
	}
	if chargeCount != 1 {
		t.Fatalf("chargeCount = %d, want 1: charge must not execute twice across a crash", chargeCount)
	}
}

func TestNonDeterministicWorkflowIsCaught(t *testing.T) {
	store := NewHistoryStore()

	wfV1 := func(ctx *WorkflowContext) any {
		ctx.Step("a", func() any { return 1 })
		ctx.Step("b", func() any { return 2 })
		return "ok"
	}
	NewEngine(store).Run("wf", wfV1)

	// A different step order on replay is a bug the engine must refuse to hide.
	wfV2 := func(ctx *WorkflowContext) any {
		ctx.Step("a", func() any { return 1 })
		ctx.Step("DIFFERENT", func() any { return 2 })
		return "ok"
	}

	got := recoverValue(func() { NewEngine(store).Resume("wf", wfV2) })
	nd, ok := got.(*NonDeterministicWorkflowError)
	if !ok {
		t.Fatalf("expected *NonDeterministicWorkflowError, got %T (%v)", got, got)
	}
	if nd.Error() == "" {
		t.Fatalf("error message should not be empty")
	}
}

func TestNonDeterministicErrorMentionsBothSteps(t *testing.T) {
	store := NewHistoryStore()
	NewEngine(store).Run("wf", func(ctx *WorkflowContext) any {
		ctx.Step("expected", func() any { return 1 })
		return "ok"
	})
	got := recoverValue(func() {
		NewEngine(store).Resume("wf", func(ctx *WorkflowContext) any {
			ctx.Step("actual", func() any { return 1 })
			return "ok"
		})
	})
	nd, ok := got.(*NonDeterministicWorkflowError)
	if !ok {
		t.Fatalf("expected *NonDeterministicWorkflowError, got %T", got)
	}
	msg := nd.Error()
	if !strings.Contains(msg, "expected") || !strings.Contains(msg, "actual") {
		t.Fatalf("message %q should mention both the recorded and the observed step id", msg)
	}
}

func TestResultsAreServedFromHistoryOnReplay(t *testing.T) {
	store := NewHistoryStore()
	runs := 0
	NewEngine(store).Run("wf", func(ctx *WorkflowContext) any {
		runs++
		v := ctx.Step("only", func() any { return 7 })
		if v != 7 {
			t.Fatalf("step value = %v, want 7", v)
		}
		return v
	})
	// The body runs once to record the step and once more to complete.
	if runs != 2 {
		t.Fatalf("workflow body ran %d times, want 2", runs)
	}
}
