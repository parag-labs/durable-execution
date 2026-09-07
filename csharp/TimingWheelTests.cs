using System.Collections.Generic;
using Xunit;

namespace DurableExecution.Tests;

public class TimingWheelTests
{
    [Fact]
    public void FiresAtTheRightTime()
    {
        var w = new HierarchicalTimingWheel();
        var fired = new List<string>();
        w.Schedule(5, () => fired.Add("a"));
        w.Schedule(5, () => fired.Add("b"));
        w.Advance(4);
        Assert.Empty(fired);
        w.Advance(5);
        Assert.Equal(new HashSet<string> { "a", "b" }, new HashSet<string>(fired));
    }

    [Fact]
    public void FarFutureTimerCascadesAndFires()
    {
        var w = new HierarchicalTimingWheel(slotsPerWheel: 10, levels: 3); // span 1000
        var fired = new List<string>();
        w.Schedule(750, () => fired.Add("far"));
        w.Advance(749);
        Assert.Empty(fired);
        w.Advance(750);
        Assert.Equal(new[] { "far" }, fired);
    }

    [Fact]
    public void OrderingAcrossManyTimers()
    {
        var w = new HierarchicalTimingWheel();
        var order = new List<int>();
        foreach (var d in new[] { 30, 10, 20, 5, 40 })
        {
            var dd = d;
            w.Schedule(dd, () => order.Add(dd));
        }
        w.Advance(100);
        Assert.Equal(new[] { 5, 10, 20, 30, 40 }, order);
    }

    [Fact]
    public void CancelPreventsFire()
    {
        var w = new HierarchicalTimingWheel();
        var fired = new List<string>();
        var t = w.Schedule(10, () => fired.Add("x"));
        t.Cancelled = true;
        w.Advance(20);
        Assert.Empty(fired);
    }

    [Fact]
    public void ScalesToManyTimers()
    {
        var w = new HierarchicalTimingWheel();
        var count = 0;
        for (var i = 0; i < 5000; i++)
            w.Schedule((i % 500) + 1, () => count++);
        w.Advance(500);
        Assert.Equal(5000, count);
    }
}
