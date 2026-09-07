package com.durableexec;

import static org.junit.jupiter.api.Assertions.*;

import com.durableexec.Engine.HistoryStore;
import com.durableexec.Engine.NonDeterministicWorkflowException;
import com.durableexec.Engine.WorkflowContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class EngineTest {

    @Test
    void workflowRunsToCompletion() {
        HistoryStore store = new HistoryStore();
        Engine eng = new Engine(store);
        List<String> calls = new ArrayList<>();

        Function<WorkflowContext, Object> wf = ctx -> {
            Object a = ctx.step("charge", () -> { calls.add("charge"); return 100; });
            Object b = ctx.step("ship", () -> { calls.add("ship"); return "tracking-1"; });
            return List.of(a, b);
        };

        Object result = eng.run("order-1", wf);
        assertEquals(List.of(100, "tracking-1"), result);
        assertEquals(List.of("charge", "ship"), calls);
    }

    @Test
    void completedStepsDoNotReExecuteOnResume() {
        HistoryStore store = new HistoryStore();
        List<String> calls = new ArrayList<>();

        Function<WorkflowContext, Object> wf = ctx -> {
            Object a = ctx.step("charge", () -> { calls.add("charge"); return 100; });
            Object b = ctx.step("ship", () -> { calls.add("ship"); return "t-1"; });
            return List.of(a, b);
        };

        new Engine(store).run("order-1", wf);
        assertEquals(List.of("charge", "ship"), calls);

        // A brand-new engine "resumes" against the same durable history: the side
        // effects must NOT fire again - that's the no-double-charge guarantee.
        calls.clear();
        Object result = new Engine(store).resume("order-1", wf);
        assertEquals(List.of(100, "t-1"), result);
        assertTrue(calls.isEmpty());
    }

    @Test
    void crashBetweenStepsResumesWithoutRepeating() {
        HistoryStore store = new HistoryStore();
        int[] chargeCount = {0};

        Function<Integer, Function<WorkflowContext, Object>> makeWf = stepsBeforeCrash -> ctx -> {
            ctx.step("charge", () -> { chargeCount[0]++; return 100; });
            if (stepsBeforeCrash == 1) {
                throw new RuntimeException("simulated process crash after charge persisted");
            }
            ctx.step("ship", () -> "t-1");
            return "done";
        };

        // Run until it "crashes" right after charge is durably recorded.
        try {
            new Engine(store).run("order-1", makeWf.apply(1));
        } catch (RuntimeException e) {
            // expected simulated crash
        }
        assertEquals(1, chargeCount[0]);

        // Restart with the full workflow: charge is served from history, not re-run.
        Object result = new Engine(store).resume("order-1", makeWf.apply(99));
        assertEquals("done", result);
        assertEquals(1, chargeCount[0], "charge must not execute twice across a crash");
    }

    @Test
    void nonDeterministicWorkflowIsCaught() {
        HistoryStore store = new HistoryStore();

        Function<WorkflowContext, Object> wfV1 = ctx -> {
            ctx.step("a", () -> 1);
            ctx.step("b", () -> 2);
            return "ok";
        };
        new Engine(store).run("wf", wfV1);

        // A different step order on replay is a bug the engine must refuse to hide.
        Function<WorkflowContext, Object> wfV2 = ctx -> {
            ctx.step("a", () -> 1);
            ctx.step("DIFFERENT", () -> 2);
            return "ok";
        };

        assertThrows(NonDeterministicWorkflowException.class,
            () -> new Engine(store).resume("wf", wfV2));
    }
}
