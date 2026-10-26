import type { Tuple } from "../expressions/index.js";
import type { Value } from "../values/index.js";
import { Result, type ResultRow } from "./api/index.js";

// Results as callers get them. A BLOB a query returns may be the bytes of a
// constant in a cached plan, or a view of a page: callers get a copy, so
// what they do with it can change neither (docs/semantics.md, Values).

/** The rows of a query, under its column names. */
export function rowsResult(
  columns: readonly string[],
  rows: readonly Tuple[],
): Result {
  return new Result({
    columns,
    rows: rows.map((row) => ownRow(row)),
    changes: 0,
    lastInsertRowid: null,
  });
}

/** What an INSERT, UPDATE or DELETE did. */
export function changesResult(
  changes: number,
  lastInsertRowid: number | null,
): Result {
  return new Result({ columns: [], rows: [], changes, lastInsertRowid });
}

/** `row`, with copies of its BLOBs; the row itself when it has none. */
function ownRow(row: Tuple): ResultRow {
  const hasBlob = row.some((value) => isBlob(value));
  if (!hasBlob) return row;
  return row.map((value) => copyBlob(value));
}

function copyBlob(value: Value): Value {
  return isBlob(value) ? value.slice() : value;
}

function isBlob(value: Value): value is Uint8Array {
  return value instanceof Uint8Array;
}
