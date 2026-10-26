// The executor (docs/planning.md, Execution): a physical plan compiled into
// operators that pull rows in batches of no more than they need, subqueries
// compiled once and started over for each outer row, and INSERT, UPDATE and
// DELETE through the table layer, each reading what it changes before
// changing any of it.
//
// A statement is atomic through the engine, not here: it runs inside a
// savepoint of its write transaction, and a statement that fails part-way
// is rolled back to it, its earlier writes with it.

export { runQuery, runWrite, type WriteStatement } from "./execute.js";
export type { WriteOutcome } from "./writes/outcome.js";
