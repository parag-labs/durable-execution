use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use durable_execution::{new_shared_store, Engine, Value, WorkflowContext, WorkflowError};

#[test]
fn value_from_conversions() {
    assert_eq!(Value::from(5i64), Value::Int(5));
    assert_eq!(Value::from("x"), Value::Str("x".to_string()));
    assert_eq!(Value::from("x".to_string()), Value::Str("x".to_string()));
    assert_eq!(Value::from(true), Value::Bool(true));
}

#[test]
fn workflow_runs_to_completion() {
    let store = new_shared_store();
    let calls = Rc::new(RefCell::new(Vec::<String>::new()));
    let mut eng = Engine::new(Rc::clone(&store));

    let calls_c = Rc::clone(&calls);
    let result = eng
        .run("order-1", move |ctx| {
            let a = ctx.step("charge", || {
                calls_c.borrow_mut().push("charge".to_string());
                Value::Int(100)
            })?;
            let b = ctx.step("ship", || {
                calls_c.borrow_mut().push("ship".to_string());
                Value::from("tracking-1")
            })?;
            let mut m = BTreeMap::new();
            m.insert("charged".to_string(), a);
            m.insert("shipment".to_string(), b);
            Ok(Value::Map(m))
        })
        .unwrap();

    let mut want = BTreeMap::new();
    want.insert("charged".to_string(), Value::Int(100));
    want.insert("shipment".to_string(), Value::from("tracking-1"));
    assert_eq!(result, Value::Map(want));
    assert_eq!(
        *calls.borrow(),
        vec!["charge".to_string(), "ship".to_string()]
    );
}

#[test]
fn replays_equal_step_count() {
    let store = new_shared_store();
    let mut eng = Engine::new(Rc::clone(&store));
    eng.run("wf", |ctx| {
        ctx.step("a", || Value::Int(1))?;
        ctx.step("b", || Value::Int(2))?;
        ctx.step("c", || Value::Int(3))?;
        Ok(Value::from("done"))
    })
    .unwrap();
    assert_eq!(eng.replays(), 3, "one replay per recorded step");
}

#[test]
fn empty_workflow_records_nothing() {
    let store = new_shared_store();
    let mut eng = Engine::new(Rc::clone(&store));
    let result = eng.run("wf", |_ctx| Ok(Value::from("immediate"))).unwrap();
    assert_eq!(result, Value::from("immediate"));
    assert_eq!(eng.replays(), 0);
    assert!(store.borrow().load("wf").is_empty());
}

#[test]
fn completed_steps_do_not_re_execute_on_resume() {
    let store = new_shared_store();
    let calls = Rc::new(RefCell::new(Vec::<String>::new()));

    let make_wf = || {
        let calls_c = Rc::clone(&calls);
        move |ctx: &mut WorkflowContext| -> Result<Value, WorkflowError> {
            let a = ctx.step("charge", || {
                calls_c.borrow_mut().push("charge".to_string());
                Value::Int(100)
            })?;
            let b = ctx.step("ship", || {
                calls_c.borrow_mut().push("ship".to_string());
                Value::from("t-1")
            })?;
            Ok(Value::List(vec![a, b]))
        }
    };

    Engine::new(Rc::clone(&store))
        .run("order-1", make_wf())
        .unwrap();
    assert_eq!(
        *calls.borrow(),
        vec!["charge".to_string(), "ship".to_string()]
    );

    // A brand-new engine resumes against the same durable history: the side
    // effects must NOT fire again - that is the no-double-charge guarantee.
    calls.borrow_mut().clear();
    let result = Engine::new(Rc::clone(&store))
        .resume("order-1", make_wf())
        .unwrap();
    assert_eq!(
        result,
        Value::List(vec![Value::Int(100), Value::from("t-1")])
    );
    assert!(calls.borrow().is_empty());
}

#[test]
fn crash_between_steps_resumes_without_repeating() {
    let store = new_shared_store();
    let charge_count = Rc::new(RefCell::new(0i32));

    let make_wf = |steps_before_crash: i32| {
        let cc = Rc::clone(&charge_count);
        move |ctx: &mut WorkflowContext| -> Result<Value, WorkflowError> {
            ctx.step("charge", || {
                *cc.borrow_mut() += 1;
                Value::Int(100)
            })?;
            if steps_before_crash == 1 {
                return Err(WorkflowError::Crash(
                    "simulated process crash after charge persisted".to_string(),
                ));
            }
            ctx.step("ship", || Value::from("t-1"))?;
            Ok(Value::from("done"))
        }
    };

    // Run until it "crashes" right after charge is durably recorded.
    let crashed = Engine::new(Rc::clone(&store)).run("order-1", make_wf(1));
    assert!(matches!(crashed, Err(WorkflowError::Crash(_))));
    assert_eq!(*charge_count.borrow(), 1);

    // Restart with the full workflow: charge is served from history, not re-run.
    let result = Engine::new(Rc::clone(&store)).resume("order-1", make_wf(99));
    assert_eq!(result, Ok(Value::from("done")));
    assert_eq!(
        *charge_count.borrow(),
        1,
        "charge must not execute twice across a crash"
    );
}

#[test]
fn nondeterministic_workflow_is_caught() {
    let store = new_shared_store();
    Engine::new(Rc::clone(&store))
        .run("wf", |ctx| {
            ctx.step("a", || Value::Int(1))?;
            ctx.step("b", || Value::Int(2))?;
            Ok(Value::from("ok"))
        })
        .unwrap();

    // A different step order on replay is a bug the engine must refuse to hide.
    let result = Engine::new(Rc::clone(&store)).resume("wf", |ctx| {
        ctx.step("a", || Value::Int(1))?;
        ctx.step("DIFFERENT", || Value::Int(2))?;
        Ok(Value::from("ok"))
    });
    assert!(matches!(result, Err(WorkflowError::NonDeterministic(_))));
}

#[test]
fn nondeterministic_error_mentions_both_steps() {
    let store = new_shared_store();
    Engine::new(Rc::clone(&store))
        .run("wf", |ctx| {
            ctx.step("expected", || Value::Int(1))?;
            Ok(Value::from("ok"))
        })
        .unwrap();
    let result = Engine::new(Rc::clone(&store)).resume("wf", |ctx| {
        ctx.step("actual", || Value::Int(1))?;
        Ok(Value::from("ok"))
    });
    match result {
        Err(WorkflowError::NonDeterministic(msg)) => {
            assert!(
                msg.contains("expected") && msg.contains("actual"),
                "message was {msg:?}"
            );
        }
        other => panic!("expected a NonDeterministic error, got {other:?}"),
    }
}

#[test]
fn results_are_served_from_history_on_replay() {
    let store = new_shared_store();
    let runs = Rc::new(RefCell::new(0));
    let runs_c = Rc::clone(&runs);
    let mut eng = Engine::new(Rc::clone(&store));
    let result = eng
        .run("wf", move |ctx| {
            *runs_c.borrow_mut() += 1;
            let v = ctx.step("only", || Value::Int(7))?;
            assert_eq!(v, Value::Int(7));
            Ok(v)
        })
        .unwrap();
    assert_eq!(result, Value::Int(7));
    // The body runs once to record the step and once more to complete.
    assert_eq!(*runs.borrow(), 2);
    assert_eq!(eng.replays(), 1);
}
