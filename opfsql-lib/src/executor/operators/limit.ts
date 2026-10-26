import { DatatypeMismatchError } from "../../errors.js";
import type { Tuple } from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import { assignValue, describeValue, type Value } from "../../values/index.js";
import { compileFixed, type Context } from "../context.js";
import {
  BATCH_SIZE,
  type Demand,
  type Operator,
  pull,
  WalkOperator,
} from "../operator.js";

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

type Clause = "LIMIT" | "OFFSET";

/** At most `limit` rows after skipping `offset`, none evaluated past them. */
export function limit(
  plan: physical.Limit,
  child: Operator,
  context: Context,
): Operator {
  const window = compileWindow(plan.limit, plan.offset, context);
  return new WalkOperator(
    (demand) => windowRows(child, window(), demand),
    [child],
  );
}

/** The window of a LIMIT and OFFSET, evaluated each time the node starts. */
export function compileWindow(
  limitValue: physical.Expression | null,
  offsetValue: physical.Expression | null,
  context: Context,
): () => Window {
  const limitOf = compileCount(limitValue, "LIMIT", context);
  const offsetOf = compileCount(offsetValue, "OFFSET", context);
  return () => ({
    offset: Math.max(offsetOf() ?? 0, 0),
    limit: rowLimit(limitOf()),
  });
}

/** No LIMIT, or a negative one, is no limit at all. */
function rowLimit(count: number | null): number {
  const isUnlimited = count === null || count < 0;
  return isUnlimited ? Infinity : count;
}

/** The child's rows after `offset`, at most `limit`, pulled by what is left of both. */
function* windowRows(
  child: Operator,
  window: Window,
  demand: Demand,
): Generator<Tuple> {
  skip(child, window.offset);
  if (window.limit === 0) return;
  let left = window.limit;
  const wanted = {
    get wanted() {
      return Math.min(demand.wanted, left);
    },
  };
  for (const row of pull(child, wanted)) {
    yield row;
    left--;
    if (left === 0) return;
  }
}

/** Pulls `count` rows of the child, and drops them. */
function skip(child: Operator, count: number): void {
  let skipped = 0;
  while (skipped < count) {
    const batch = child.next(Math.min(count - skipped, BATCH_SIZE));
    if (batch === null) return;
    skipped += batch.length;
  }
}

/** A LIMIT or OFFSET value as an integer; null when the clause is absent. */
function compileCount(
  expression: physical.Expression | null,
  clause: Clause,
  context: Context,
): () => number | null {
  if (expression === null) return () => null;
  const value = compileFixed(expression, context);
  return () => integerOf(value(), clause);
}

/** `value` as an INTEGER column would store it, which must be an integer. */
function integerOf(value: Value, clause: Clause): number {
  try {
    const integer = assignValue(value, "INTEGER");
    if (typeof integer === "number") return integer;
  } catch (error) {
    if (!(error instanceof DatatypeMismatchError)) throw error;
  }
  throw new DatatypeMismatchError(
    `${clause} must be an integer, not ${describeValue(value)}`,
  );
}
