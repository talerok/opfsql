import type { Tuple } from "../expressions/index.js";
import { Result } from "./api/index.js";

// Results as callers get them. Their values are the caller's own already,
// because every source makes them so (docs/layers.md, Values in and out of
// the engine): a record decodes afresh, a cached plan's constant and a
// column default are copied each time they are taken, and parameters were
// copied at the call. So rows pass as they are.

/** The rows of a query, under its column names. */
export function rowsResult(
  columns: readonly string[],
  rows: readonly Tuple[],
): Result {
  return new Result({ columns, rows, changes: 0, lastInsertRowid: null });
}

/** What an INSERT, UPDATE or DELETE did. */
export function changesResult(
  changes: number,
  lastInsertRowid: number | null,
): Result {
  return new Result({ columns: [], rows: [], changes, lastInsertRowid });
}
