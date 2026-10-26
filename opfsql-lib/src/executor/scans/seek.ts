import type { physical } from "../../sql/plan/index.js";
import type { Value } from "../../values/index.js";
import { compileFixed, type Context } from "../context.js";

/** One end of a range, evaluated for a run: its value and whether it is included. */
export interface SeekBound {
  readonly value: Value;
  readonly isInclusive: boolean;
}

/**
 * A value an access path seeks — a constant, a parameter, or a column of an
 * enclosing query — evaluated each time its scan starts, and matched as it
 * is, as `=` matches it (docs/planning.md, Physical plans).
 */
export function compileSeek(
  expression: physical.Expression,
  context: Context,
): () => Value {
  return compileFixed(expression, context);
}

/** A range end as compileSeek evaluates it; null for an open end. */
export function compileBound(
  bound: physical.Bound | null,
  context: Context,
): () => SeekBound | null {
  if (bound === null) return () => null;
  const value = compileSeek(bound.value, context);
  const { isInclusive } = bound;
  return () => ({ value: value(), isInclusive });
}
