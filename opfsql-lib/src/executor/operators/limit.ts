import type { Tuple } from "../../expressions/index.js";
import { DatatypeMismatchError } from "../../errors.js";
import type { physical } from "../../sql/plan/index.js";
import { assignValue, describeValue, type Value } from "../../values/index.js";
import { compileFixed, type Context } from "../context.js";
import { type Operator, rowsOf, WalkOperator } from "../operator.js";

// ---------------------------------------------------------------------------
// LIMIT and OFFSET (docs/semantics.md, LIMIT and OFFSET): integers, as
// SQLite takes them — a negative LIMIT is no limit and a negative OFFSET
// none; anything that is not an integer is a DATATYPE_MISMATCH.
// ---------------------------------------------------------------------------

/** Rows to skip, then rows to return: Infinity for no limit. */
export interface Window {
  readonly offset: number;
  readonly limit: number;
}

/** At most `limit` rows after skipping `offset`; none pulled past them. */
export function limit(
  plan: physical.Limit,
  child: Operator,
  context: Context,
): Operator {
  const window = compileWindow(plan.limit, plan.offset, context);
  return new WalkOperator(() => windowRows(child, window()), [child]);
}

/** The window of a LIMIT and OFFSET, evaluated each time the node starts. */
export function compileWindow(
  limitValue: physical.Expression | null,
  offsetValue: physical.Expression | null,
  context: Context,
): () => Window {
  const limitOf = compileCount(limitValue, "LIMIT", context);
  const offsetOf = compileCount(offsetValue, "OFFSET", context);
  return () => {
    const count = limitOf() ?? -1;
    const offset = offsetOf() ?? 0;
    return {
      offset: Math.max(offset, 0),
      limit: count < 0 ? Infinity : count,
    };
  };
}

function* windowRows(
  child: Operator,
  { offset, limit: count }: Window,
): Generator<Tuple> {
  if (count === 0) return;
  let position = 0;
  const end = offset + count;
  for (const row of rowsOf(child)) {
    position++;
    if (position > offset) yield row;
    if (position >= end) return;
  }
}

/** A LIMIT or OFFSET value as an integer; null when the clause is absent. */
function compileCount(
  expression: physical.Expression | null,
  clause: "LIMIT" | "OFFSET",
  context: Context,
): () => number | null {
  if (expression === null) return () => null;
  const value = compileFixed(expression, context);
  return () => integerOf(value(), clause);
}

function integerOf(value: Value, clause: "LIMIT" | "OFFSET"): number {
  const integer = assignedInteger(value);
  if (typeof integer === "number") return integer;
  throw new DatatypeMismatchError(
    `${clause} must be an integer, not ${describeValue(value)}`,
  );
}

/** The integer `value` stores as, as a column would take it; null if none. */
function assignedInteger(value: Value): Value {
  try {
    return assignValue(value, "INTEGER");
  } catch (error) {
    if (error instanceof DatatypeMismatchError) return null;
    throw error;
  }
}
