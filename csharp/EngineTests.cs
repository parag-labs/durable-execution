using System;
using System.Collections.Generic;
using Xunit;

namespace DurableExecution.Tests;

public class EngineTests
{
    [Fact]
    public void WorkflowRunsToCompletion()
    {
        var store = new HistoryStore();
        var eng = new Engine(store);
        var calls = new List<string>();

        object? Wf(WorkflowContext ctx)
        {
            var a = ctx.Step("charge", () => { calls.Add("charge"); return 100; });
            var b = ctx.Step("ship", () => { calls.Add("ship"); return "tracking-1"; });
            return (a, b);
        }

        var result = eng.Run("order-1", Wf);
        var (r0, r1) = ((object?, object?))result!;
        Assert.Equal(100, r0);
        Assert.Equal("tracking-1", r1);
        Assert.Equal(new[] { "charge", "ship" }, calls);
    }

    [Fact]
    public void CompletedStepsDoNotReExecuteOnResume()
    {
        var store = new HistoryStore();
        var calls = new List<string>();

        object? Wf(WorkflowContext ctx)
        {
            var a = ctx.Step("charge", () => { calls.Add("charge"); return 100; });
            var b = ctx.Step("ship", () => { calls.Add("ship"); return "t-1"; });
            return (a, b);
        }

        new Engine(store).Run("order-1", Wf);
        Assert.Equal(new[] { "charge", "ship" }, calls);

        // A brand-new engine "resumes" against the same durable history: the side
        // effects must NOT fire again - that's the no-double-charge guarantee.
        calls.Clear();
        var result = new Engine(store).Resume("order-1", Wf);
        var (r0, r1) = ((object?, object?))result!;
        Assert.Equal(100, r0);
        Assert.Equal("t-1", r1);
        Assert.Empty(calls);
    }

    [Fact]
    public void CrashBetweenStepsResumesWithoutRepeating()
    {
        var store = new HistoryStore();
        var chargeCount = 0;

        Func<WorkflowContext, object?> MakeWf(int stepsBeforeCrash) => ctx =>
        {
            ctx.Step("charge", () => { chargeCount++; return 100; });
            if (stepsBeforeCrash == 1)
                throw new InvalidOperationException("simulated process crash after charge persisted");
            ctx.Step("ship", () => "t-1");
            return "done";
        };

        // Run until it "crashes" right after charge is durably recorded.
        try { new Engine(store).Run("order-1", MakeWf(1)); }
        catch (InvalidOperationException) { }
        Assert.Equal(1, chargeCount);

        // Restart with the full workflow: charge is served from history, not re-run.
        var result = new Engine(store).Resume("order-1", MakeWf(99));
        Assert.Equal("done", result);
        Assert.Equal(1, chargeCount); // charge must not execute twice across a crash
    }

    [Fact]
    public void NonDeterministicWorkflowIsCaught()
    {
        var store = new HistoryStore();

        object? WfV1(WorkflowContext ctx)
        {
            ctx.Step("a", () => 1);
            ctx.Step("b", () => 2);
            return "ok";
        }

        new Engine(store).Run("wf", WfV1);

        // A different step order on replay is a bug the engine must refuse to hide.
        object? WfV2(WorkflowContext ctx)
        {
            ctx.Step("a", () => 1);
            ctx.Step("DIFFERENT", () => 2);
            return "ok";
        }

        Assert.Throws<NonDeterministicWorkflowException>(
            () => new Engine(store).Resume("wf", WfV2));
    }
}
