import type { StaticType } from "../../functions/index.js";
import { comparisonConversion } from "../../expressions/index.js";
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
 * enclosing query — evaluated each time its scan starts and converted as
 * `=` with a key part of `keyType` would convert it (docs/planning.md,
 * Physical plans), so the path finds exactly the rows a comparison finds.
 */
export function compileSeek(
  expression: physical.Expression,
  keyType: StaticType,
  context: Context,
): () => Value {
  const value = compileFixed(expression, context);
  const convert = comparisonConversion(expression.type, keyType);
  return () => convert(value());
}

/** A range end as compileSeek converts it; null for an open end. */
export function compileBound(
  bound: physical.Bound | null,
  keyType: StaticType,
  context: Context,
): () => SeekBound | null {
  if (bound === null) return () => null;
  const value = compileSeek(bound.value, keyType, context);
  const { isInclusive } = bound;
  return () => ({ value: value(), isInclusive });
}
