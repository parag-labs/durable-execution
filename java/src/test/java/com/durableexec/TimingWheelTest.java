package com.durableexec;

import static org.junit.jupiter.api.Assertions.*;

import com.durableexec.HierarchicalTimingWheel.Timer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TimingWheelTest {

    @Test
    void firesAtTheRightTime() {
        HierarchicalTimingWheel w = new HierarchicalTimingWheel();
        List<String> fired = new ArrayList<>();
        w.schedule(5, () -> fired.add("a"));
        w.schedule(5, () -> fired.add("b"));
        w.advance(4);
        assertTrue(fired.isEmpty());
        w.advance(5);
        assertEquals(Set.of("a", "b"), new HashSet<>(fired));
    }

    @Test
    void farFutureTimerCascadesAndFires() {
        HierarchicalTimingWheel w = new HierarchicalTimingWheel(10, 3); // span 1000
        List<String> fired = new ArrayList<>();
        w.schedule(750, () -> fired.add("far"));
        w.advance(749);
        assertTrue(fired.isEmpty());
        w.advance(750);
        assertEquals(List.of("far"), fired);
    }

    @Test
    void orderingAcrossManyTimers() {
        HierarchicalTimingWheel w = new HierarchicalTimingWheel();
        List<Integer> order = new ArrayList<>();
        for (int d : new int[] {30, 10, 20, 5, 40}) {
            final int dd = d;
            w.schedule(dd, () -> order.add(dd));
        }
        w.advance(100);
        assertEquals(List.of(5, 10, 20, 30, 40), order);
    }

    @Test
    void cancelPreventsFire() {
        HierarchicalTimingWheel w = new HierarchicalTimingWheel();
        List<String> fired = new ArrayList<>();
        Timer t = w.schedule(10, () -> fired.add("x"));
        t.setCancelled(true);
        w.advance(20);
        assertTrue(fired.isEmpty());
    }

    @Test
    void scalesToManyTimers() {
        HierarchicalTimingWheel w = new HierarchicalTimingWheel();
        int[] count = {0};
        for (int i = 0; i < 5000; i++) {
            w.schedule((i % 500) + 1, () -> count[0]++);
        }
        w.advance(500);
        assertEquals(5000, count[0]);
    }
}
