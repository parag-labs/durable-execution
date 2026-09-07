// Durable execution: workflows that survive a crash by replaying their history.
//
// The idea, borrowed from Temporal and AWS Step Functions: a workflow is ordinary
// code, but every side-effecting *step* it takes is first recorded in an
// append-only history log. If the process dies and restarts, the engine re-runs the
// workflow function from the top - and every step that's already in the history
// returns its recorded result *without executing again*. Deterministic replay walks
// the function back to exactly where it left off, then continues live.
//
// The two things that make this honest:
// - The workflow body must be deterministic (no wall-clock, no unrecorded I/O), so
//   replay reaches the same step in the same order.
// - Every step is idempotent on replay because a completed step is served from
//   history, so re-execution after a crash never double-charges the card.

using System;
using System.Collections.Generic;

namespace DurableExecution;

/// <summary>
/// Raised to unwind the workflow when it needs a step that hasn't run yet in a
/// fresh (non-replay) execution - the engine runs the step, appends it, and
/// re-executes from the top.
/// </summary>
internal sealed class ReplayException : Exception { }

public sealed record HistoryEvent(int Seq, string StepId, object? Result);

/// <summary>
/// Handed to the workflow body. <c>Step</c> is the only way to do something with a
/// side effect; its result is memoized in the history.
/// </summary>
public sealed class WorkflowContext
{
    private readonly Engine _engine;
    private readonly List<HistoryEvent> _history;
    private int _cursor;

    internal WorkflowContext(Engine engine, List<HistoryEvent> history)
    {
        _engine = engine;
        _history = history;
    }

    public object? Step(string stepId, Func<object?> fn)
    {
        // Already recorded? Serve from history, do NOT run the side effect again.
        if (_cursor < _history.Count)
        {
            var ev = _history[_cursor];
            if (ev.StepId != stepId)
            {
                throw new NonDeterministicWorkflowException(
                    $"non-deterministic workflow: expected step '{ev.StepId}' " +
                    $"at position {_cursor}, got '{stepId}'");
            }
            _cursor++;
            return ev.Result;
        }
        // First time: run it, append to history, and unwind so the engine persists
        // before continuing. This is what makes a crash safe - the history is
        // durable before the next step is attempted.
        var result = fn();
        _history.Add(new HistoryEvent(_history.Count, stepId, result));
        _engine.Persist(_history);
        throw new ReplayException();
    }
}

/// <summary>Thrown when replay observes a different step order than history records.</summary>
public sealed class NonDeterministicWorkflowException : Exception
{
    public NonDeterministicWorkflowException(string message) : base(message) { }
}

/// <summary>
/// Runs a workflow to completion, replaying from history each time a new step is
/// recorded.
/// </summary>
public sealed class Engine
{
    private readonly HistoryStore _store;
    private string _wid = "";

    public int Replays { get; private set; }

    public Engine(HistoryStore store) => _store = store;

    internal void Persist(List<HistoryEvent> history) => _store.Save(_wid, history);

    public object? Run(string wid, Func<WorkflowContext, object?> workflow)
    {
        _wid = wid;
        var history = _store.Load(wid);
        while (true)
        {
            var ctx = new WorkflowContext(this, history);
            try
            {
                return workflow(ctx);
            }
            catch (ReplayException)
            {
                Replays++;
                history = _store.Load(wid); // reload exactly what's durable
            }
        }
    }

    /// <summary>
    /// Simulate a crash-and-restart: same call, but history is whatever was already
    /// persisted. Completed steps will not re-execute.
    /// </summary>
    public object? Resume(string wid, Func<WorkflowContext, object?> workflow)
        => Run(wid, workflow);
}

/// <summary>
/// In-memory append-only store. A real one would be a DB table or a log; the
/// interface is deliberately tiny so it's swappable.
/// </summary>
public sealed class HistoryStore
{
    private readonly Dictionary<string, List<HistoryEvent>> _data = new();

    public List<HistoryEvent> Load(string wid)
        => _data.TryGetValue(wid, out var h) ? new List<HistoryEvent>(h) : new List<HistoryEvent>();

    public void Save(string wid, List<HistoryEvent> history)
        => _data[wid] = new List<HistoryEvent>(history);
}
