import {
  compileExpression,
  type Scope,
  type Tuple,
} from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import type { KeyBound, KeyRange, KeySeek } from "../../table/index.js";
import type { Value } from "../../values/index.js";

// Seek values (docs/planning.md, Physical plans): a constant, a parameter, a
// column of an enclosing query, or on a lookup join's right side a column of
// the left row; evaluated each time the scan starts and matched as it is, as
// `=` matches it. They are compiled in the scope of the row the scan starts
// for — the left side's layout for a lookup join's right scan, no columns
// for a scan of its own — and evaluated on that row.

/** The row a scan of its own starts for: none, so its seek values read no column. */
export const NO_ROW: Tuple = [];

/** Something evaluated as a scan starts, on the row it starts for. */
export type AtStart<T> = (row: Tuple) => T;

/** A key range compiled: what to seek each time the scan starts. */
export function compileKeySeek(
  keys: physical.KeyRange,
  scope: Scope,
): AtStart<KeySeek> {
  const equal = compileEqual(keys.equal, scope);
  if (keys.range === null) return (row) => ({ equal: equal(row), range: null });
  const range = compileRange(keys.range, scope);
  return (row) => ({ equal: equal(row), range: range(row) });
}

/** Equal values compiled: their values each time the scan starts. */
export function compileEqual(
  values: readonly physical.Expression[],
  scope: Scope,
): AtStart<Value[]> {
  const compiled = values.map((value) => compileExpression(value, scope));
  return (row) => compiled.map((value) => value(row));
}

/** A range compiled: its ends each time the scan starts. */
export function compileRange(
  range: physical.Range,
  scope: Scope,
): AtStart<KeyRange> {
  const lower = compileBound(range.lower, scope);
  const upper = compileBound(range.upper, scope);
  return (row) => ({ lower: lower(row), upper: upper(row) });
}

function compileBound(
  bound: physical.Bound | null,
  scope: Scope,
): AtStart<KeyBound | null> {
  if (bound === null) return () => null;
  const value = compileExpression(bound.value, scope);
  const { isInclusive } = bound;
  return (row) => ({ value: value(row), isInclusive });
}
