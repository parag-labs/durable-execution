from resumerun import HierarchicalTimingWheel


def test_fires_at_the_right_time():
    w = HierarchicalTimingWheel()
    fired = []
    w.schedule(5, lambda: fired.append("a"))
    w.schedule(5, lambda: fired.append("b"))
    w.advance(4)
    assert fired == []
    w.advance(5)
    assert set(fired) == {"a", "b"}


def test_far_future_timer_cascades_and_fires():
    w = HierarchicalTimingWheel(slots_per_wheel=10, levels=3)  # span 1000
    fired = []
    w.schedule(750, lambda: fired.append("far"))
    w.advance(749)
    assert fired == []
    w.advance(750)
    assert fired == ["far"]


def test_ordering_across_many_timers():
    w = HierarchicalTimingWheel()
    order = []
    for d in [30, 10, 20, 5, 40]:
        w.schedule(d, lambda d=d: order.append(d))
    w.advance(100)
    assert order == [5, 10, 20, 30, 40]


def test_cancel_prevents_fire():
    w = HierarchicalTimingWheel()
    fired = []
    t = w.schedule(10, lambda: fired.append("x"))
    t.cancelled = True
    w.advance(20)
    assert fired == []


def test_scales_to_many_timers():
    w = HierarchicalTimingWheel()
    count = {"n": 0}
    for i in range(5000):
        w.schedule((i % 500) + 1, lambda: count.__setitem__("n", count["n"] + 1))
    w.advance(500)
    assert count["n"] == 5000
