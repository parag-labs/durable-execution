from resumerun import Engine, HistoryStore


def test_workflow_runs_to_completion():
    store = HistoryStore()
    eng = Engine(store)
    calls = []

    def wf(ctx):
        a = ctx.step("charge", lambda: (calls.append("charge"), 100)[1])
        b = ctx.step("ship", lambda: (calls.append("ship"), "tracking-1")[1])
        return {"charged": a, "shipment": b}

    result = eng.run("order-1", wf)
    assert result == {"charged": 100, "shipment": "tracking-1"}
    assert calls == ["charge", "ship"]


def test_completed_steps_do_not_re_execute_on_resume():
    store = HistoryStore()
    calls = []

    def wf(ctx):
        a = ctx.step("charge", lambda: (calls.append("charge"), 100)[1])
        b = ctx.step("ship", lambda: (calls.append("ship"), "t-1")[1])
        return (a, b)

    # First engine runs it fully.
    Engine(store).run("order-1", wf)
    assert calls == ["charge", "ship"]

    # A brand-new engine "resumes" against the same durable history: the side
    # effects must NOT fire again - that's the no-double-charge guarantee.
    calls.clear()
    result = Engine(store).resume("order-1", wf)
    assert result == (100, "t-1")
    assert calls == []


def test_crash_between_steps_resumes_without_repeating():
    store = HistoryStore()
    charge_count = {"n": 0}

    def make_wf(steps_before_crash):
        def wf(ctx):
            ctx.step("charge", lambda: charge_count.__setitem__("n", charge_count["n"] + 1) or 100)
            if steps_before_crash == 1:
                raise RuntimeError("simulated process crash after charge persisted")
            ctx.step("ship", lambda: "t-1")
            return "done"

        return wf

    # Run until it "crashes" right after charge is durably recorded.
    try:
        Engine(store).run("order-1", make_wf(steps_before_crash=1))
    except RuntimeError:
        pass
    assert charge_count["n"] == 1

    # Restart with the full workflow: charge is served from history, not re-run.
    result = Engine(store).resume("order-1", make_wf(steps_before_crash=99))
    assert result == "done"
    assert charge_count["n"] == 1, "charge must not execute twice across a crash"


def test_nondeterministic_workflow_is_caught():
    import pytest

    store = HistoryStore()

    def wf_v1(ctx):
        ctx.step("a", lambda: 1)
        ctx.step("b", lambda: 2)
        return "ok"

    Engine(store).run("wf", wf_v1)

    # A different step order on replay is a bug the engine must refuse to hide.
    def wf_v2(ctx):
        ctx.step("a", lambda: 1)
        ctx.step("DIFFERENT", lambda: 2)
        return "ok"

    with pytest.raises(AssertionError):
        Engine(store).resume("wf", wf_v2)
