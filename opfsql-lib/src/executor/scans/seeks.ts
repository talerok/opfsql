import type { physical } from "../../sql/plan/index.js";
import type { KeyBound, KeyRange, KeySeek } from "../../table/index.js";
import type { Value } from "../../values/index.js";
import { compileFixed, type Context } from "../context.js";

// Seek values (docs/planning.md, Physical plans): a constant, a parameter or
// a column of an enclosing query, evaluated each time the scan starts and
// matched as it is, as `=` matches it.

/** A key range compiled: what to seek each time the scan starts. */
export function compileKeySeek(
  keys: physical.KeyRange,
  context: Context,
): () => KeySeek {
  const equal = compileEqual(keys.equal, context);
  if (keys.range === null) return () => ({ equal: equal(), range: null });
  const range = compileRange(keys.range, context);
  return () => ({ equal: equal(), range: range() });
}

/** Equal values compiled: their values each time the scan starts. */
export function compileEqual(
  values: readonly physical.Expression[],
  context: Context,
): () => Value[] {
  const compiled = values.map((value) => compileFixed(value, context));
  return () => compiled.map((value) => value());
}

/** A range compiled: its ends each time the scan starts. */
export function compileRange(
  range: physical.Range,
  context: Context,
): () => KeyRange {
  const lower = compileBound(range.lower, context);
  const upper = compileBound(range.upper, context);
  return () => ({ lower: lower(), upper: upper() });
}

function compileBound(
  bound: physical.Bound | null,
  context: Context,
): () => KeyBound | null {
  if (bound === null) return () => null;
  const value = compileFixed(bound.value, context);
  const { isInclusive } = bound;
  return () => ({ value: value(), isInclusive });
}
