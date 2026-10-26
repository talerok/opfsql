// The executor (docs/planning.md, Execution): a physical plan compiled into
// operators that pull rows in batches, subqueries compiled once and started
// over for each outer row, and INSERT, UPDATE and DELETE through the table
// layer, each reading what it changes before changing any of it.

export { runQuery, runWrite, type WriteStatement } from "./execute.js";
export type { WriteOutcome } from "./writes/outcome.js";
