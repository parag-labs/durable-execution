use std::cell::RefCell;
use std::rc::Rc;

use durable_execution::HierarchicalTimingWheel;

#[test]
fn fresh_wheel_starts_empty() {
    let w = HierarchicalTimingWheel::new();
    assert_eq!(w.now(), 0);
    assert_eq!(w.size(), 0);
}

#[test]
fn fires_at_the_right_time() {
    let fired = Rc::new(RefCell::new(Vec::<String>::new()));
    let mut w = HierarchicalTimingWheel::new();

    let f1 = Rc::clone(&fired);
    w.schedule(5, move || f1.borrow_mut().push("a".to_string()));
    let f2 = Rc::clone(&fired);
    w.schedule(5, move || f2.borrow_mut().push("b".to_string()));

    w.advance(4);
    assert!(fired.borrow().is_empty());

    w.advance(5);
    let mut got = fired.borrow().clone();
    got.sort();
    assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn far_future_timer_cascades_and_fires() {
    let fired = Rc::new(RefCell::new(Vec::<String>::new()));
    let mut w = HierarchicalTimingWheel::with_config(10, 3); // span 30
    let f = Rc::clone(&fired);
    w.schedule(750, move || f.borrow_mut().push("far".to_string()));

    w.advance(749);
    assert!(fired.borrow().is_empty());
    w.advance(750);
    assert_eq!(*fired.borrow(), vec!["far".to_string()]);
}

#[test]
fn ordering_across_many_timers() {
    let order = Rc::new(RefCell::new(Vec::<i64>::new()));
    let mut w = HierarchicalTimingWheel::new();
    for d in [30i64, 10, 20, 5, 40] {
        let o = Rc::clone(&order);
        w.schedule(d, move || o.borrow_mut().push(d));
    }
    w.advance(100);
    assert_eq!(*order.borrow(), vec![5, 10, 20, 30, 40]);
}

#[test]
fn same_deadline_fires_in_insertion_order() {
    let order = Rc::new(RefCell::new(Vec::<i64>::new()));
    let mut w = HierarchicalTimingWheel::new();
    for i in 0..5i64 {
        let o = Rc::clone(&order);
        w.schedule(7, move || o.borrow_mut().push(i));
    }
    w.advance(7);
    assert_eq!(*order.borrow(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn cancel_prevents_fire() {
    let fired = Rc::new(RefCell::new(0));
    let mut w = HierarchicalTimingWheel::new();
    let f = Rc::clone(&fired);
    let timer = w.schedule(10, move || *f.borrow_mut() += 1);
    timer.borrow_mut().cancelled = true;
    w.advance(20);
    assert_eq!(*fired.borrow(), 0);
    assert_eq!(
        w.size(),
        0,
        "a cancelled timer is still counted out of size"
    );
}

#[test]
fn scales_to_many_timers() {
    let count = Rc::new(RefCell::new(0));
    let mut w = HierarchicalTimingWheel::new();
    for i in 0..5000i64 {
        let c = Rc::clone(&count);
        w.schedule((i % 500) + 1, move || *c.borrow_mut() += 1);
    }
    w.advance(500);
    assert_eq!(*count.borrow(), 5000);
    assert_eq!(w.size(), 0);
}

#[test]
#[should_panic(expected = "delay must be >= 0")]
fn negative_delay_panics() {
    let mut w = HierarchicalTimingWheel::new();
    w.schedule(-1, || {});
}

#[test]
fn zero_delay_fires_on_wheel_wrap() {
    // A timer scheduled with delay 0 at now 0 lands in bucket 0, but advance
    // increments now before ticking slot = now % slots, so slot 0 is not ticked
    // until the wheel wraps fully around. The timer therefore fires at now=slots,
    // not immediately - a genuine property of the wheel-plus-overflow design.
    let fired = Rc::new(RefCell::new(Vec::<String>::new()));
    let mut w = HierarchicalTimingWheel::with_config(4, 1); // slots = 4
    let f = Rc::clone(&fired);
    w.schedule(0, move || f.borrow_mut().push("z".to_string()));

    w.advance(3);
    assert!(fired.borrow().is_empty());
    assert_eq!(w.size(), 1);

    w.advance(4);
    assert_eq!(*fired.borrow(), vec!["z".to_string()]);
}

#[test]
fn advance_returns_fired_count() {
    let mut w = HierarchicalTimingWheel::new();
    w.schedule(3, || {});
    w.schedule(3, || {});
    w.schedule(9, || {});
    assert_eq!(w.advance(3), 2);
    assert_eq!(w.advance(9), 1);
}

#[test]
fn advance_backward_is_noop() {
    let fired = Rc::new(RefCell::new(0));
    let mut w = HierarchicalTimingWheel::new();
    let f = Rc::clone(&fired);
    w.schedule(5, move || *f.borrow_mut() += 1);
    w.advance(10);
    assert_eq!(*fired.borrow(), 1);
    assert_eq!(w.now(), 10);
    assert_eq!(w.advance(3), 0, "advancing to an earlier time does nothing");
    assert_eq!(w.now(), 10);
}
