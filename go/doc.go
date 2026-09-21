// Package durableexecution is a small, readable durable-execution engine:
// workflows written as ordinary code that survive process crashes by replaying
// an append-only history, so a completed step never runs twice. It also provides
// the hashed timing wheel these engines use to schedule their many timers.
//
// It is a faithful port of the Python resumerun reference implementation and
// behaves identically to the C# and Java ports: same replay semantics, same
// non-determinism detection, and the same timing-wheel firing order.
package durableexecution
