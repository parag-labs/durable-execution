"""Durable execution: workflows that survive a crash by replaying their history.

The idea, borrowed from Temporal and AWS Step Functions: a workflow is ordinary
code, but every side-effecting *step* it takes is first recorded in an
append-only history log. If the process dies and restarts, the engine re-runs the
workflow function from the top - and every step that's already in the history
returns its recorded result *without executing again*. Deterministic replay walks
the function back to exactly where it left off, then continues live.

The two things that make this honest:
- The workflow body must be **deterministic** (no wall-clock, no unrecorded I/O),
  so replay reaches the same step in the same order. This engine gives it a seeded
  context to make that easy.
- Every step is **idempotent on replay** because a completed step is served from
  history, so re-execution after a crash never double-charges the card.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


class _Replay(Exception):
    """Raised to unwind the workflow when it needs a step that hasn't run yet in
    a fresh (non-replay) execution - the engine runs the step, appends it, and
    re-executes from the top."""


@dataclass
class HistoryEvent:
    seq: int
    step_id: str
    result: Any


class WorkflowContext:
    """Handed to the workflow body. `step` is the only way to do something with a
    side effect; its result is memoized in the history."""

    def __init__(self, engine: "Engine", history: list[HistoryEvent]) -> None:
        self._engine = engine
        self._history = history
        self._cursor = 0

    def step(self, step_id: str, fn: Callable[[], Any]) -> Any:
        # Already recorded? Serve from history, do NOT run the side effect again.
        if self._cursor < len(self._history):
            event = self._history[self._cursor]
            assert event.step_id == step_id, (
                f"non-deterministic workflow: expected step '{event.step_id}' "
                f"at position {self._cursor}, got '{step_id}'"
            )
            self._cursor += 1
            return event.result
        # First time: run it, append to history, and unwind so the engine
        # persists before continuing. This is what makes a crash safe - the
        # history is durable before the next step is attempted.
        result = fn()
        self._history.append(HistoryEvent(len(self._history), step_id, result))
        self._engine._persist(self._history)
        raise _Replay()


class Engine:
    """Runs a workflow to completion, replaying from history each time a new step
    is recorded. `store` is any object with load(wid)->list and save(wid, list)."""

    def __init__(self, store: "HistoryStore") -> None:
        self.store = store
        self.replays = 0

    def _persist(self, history: list[HistoryEvent]) -> None:
        self.store.save(self._wid, history)

    def run(self, wid: str, workflow: Callable[[WorkflowContext], Any]) -> Any:
        self._wid = wid
        history = self.store.load(wid)
        while True:
            ctx = WorkflowContext(self, history)
            try:
                return workflow(ctx)
            except _Replay:
                self.replays += 1
                history = self.store.load(wid)  # reload exactly what's durable

    def resume(self, wid: str, workflow: Callable[[WorkflowContext], Any]) -> Any:
        """Simulate a crash-and-restart: same call, but history is whatever was
        already persisted. Completed steps will not re-execute."""
        return self.run(wid, workflow)


class HistoryStore:
    """In-memory append-only store. A real one would be a DB table or a log; the
    interface is deliberately tiny so it's swappable."""

    def __init__(self) -> None:
        self._data: dict[str, list[HistoryEvent]] = {}

    def load(self, wid: str) -> list[HistoryEvent]:
        return list(self._data.get(wid, []))

    def save(self, wid: str, history: list[HistoryEvent]) -> None:
        self._data[wid] = list(history)
