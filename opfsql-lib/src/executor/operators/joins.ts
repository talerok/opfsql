import { InternalError } from "../../errors.js";
import {
  compileExpression,
  type Scope,
  type Tuple,
} from "../../expressions/index.js";
import { physical } from "../../sql/plan/index.js";
import { rowKey } from "../../values/index.js";
import { compileConditions, type Context, scopeOf } from "../context.js";
import {
  type Demand,
  demandOf,
  drain,
  type Operator,
  pull,
  WalkOperator,
} from "../operator.js";

// ---------------------------------------------------------------------------
// Joins (docs/planning.md, Rules 2): a pair is a match only when every
// condition is TRUE for it, and a LEFT join emits each left row that has no
// match once, with NULLs for the right side. A hash or nested-loop join
// reads its right side whole; a lookup join seeks it for each left row. The
// left side is read in full batches when the caller takes every row, and
// otherwise one row at a time, since each may give any number of pairs.
// ---------------------------------------------------------------------------

/** How a join decides and pads its pairs. */
interface Pairing {
  readonly isMatch: (pair: Tuple) => boolean;
  readonly isLeftJoin: boolean;
  /** The right side's columns for a left row without a match. */
  readonly nulls: Tuple;
}

/** A row's hash key: its key values as they are; null if one is NULL. */
type HashKey = (row: Tuple) => string | null;

const NO_ROWS: readonly Tuple[] = [];

/**
 * The right side hashed by its keys, each left row joined with the right
 * rows whose keys equal its own.
 */
export function hashJoin(
  plan: physical.HashJoin,
  left: Operator,
  right: Operator,
  context: Context,
): Operator {
  if (plan.leftKeys.length !== plan.rightKeys.length) {
    throw new InternalError("a hash join's two sides have different keys");
  }
  const leftKey = compileHashKey(
    plan.leftKeys,
    scopeOf(context, physical.layoutOf(plan.left)),
  );
  const rightKey = compileHashKey(
    plan.rightKeys,
    scopeOf(context, physical.layoutOf(plan.right)),
  );
  const pairing = compilePairing(plan, plan.residual, context);
  function* joined(demand: Demand): Generator<Tuple> {
    const table = hashTableOf(right, rightKey);
    const leftRows = pull(left, leftDemand(demand));
    for (const row of leftRows) {
      const key = leftKey(row);
      const candidates = key === null ? undefined : table.get(key);
      yield* pairsOf(row, candidates ?? NO_ROWS, pairing);
    }
  }
  return new WalkOperator(joined, [left, right]);
}

/**
 * Each left row with every right row, the right side read once per start:
 * it cannot change from one left row to the next, as nothing in it reads
 * the left side.
 */
export function nestedLoopJoin(
  plan: physical.NestedLoopJoin,
  left: Operator,
  right: Operator,
  context: Context,
): Operator {
  const pairing = compilePairing(plan, plan.conditions, context);
  function* joined(demand: Demand): Generator<Tuple> {
    const rightRows = drain(right);
    const leftRows = pull(left, leftDemand(demand));
    for (const row of leftRows) {
      yield* pairsOf(row, rightRows, pairing);
    }
  }
  return new WalkOperator(joined, [left, right]);
}

/**
 * Each left row with the rows its right scan finds for it: the scan starts
 * again for each left row, its seek values read from that row, and its rows
 * are read through the table layer as the join walks (docs/planning.md,
 * Physical plans). Rows come in the left side's order.
 */
export function lookupJoin(
  plan: physical.LookupJoin,
  left: Operator,
  lookup: (row: Tuple) => Iterable<Tuple>,
  context: Context,
): Operator {
  const pairing = compilePairing(plan, plan.residual, context);
  function* joined(demand: Demand): Generator<Tuple> {
    const leftRows = pull(left, leftDemand(demand));
    for (const row of leftRows) {
      yield* pairsOf(row, lookup(row), pairing);
    }
  }
  // The right side is read here, not by a leaf: each batch is a checkpoint.
  const { run } = context.query;
  return new WalkOperator(joined, [left], () => {
    run.checkpoint();
  });
}

/**
 * What a join pulls its left side by: every row when its caller takes every
 * row; otherwise one at a time, as any one may give all the pairs wanted.
 */
function leftDemand(demand: Demand): Demand {
  return demandOf(() => (demand.wanted === Infinity ? Infinity : 1));
}

/** The matching pairs of `row` with `candidates`; for a LEFT join, the row padded if none. */
function* pairsOf(
  row: Tuple,
  candidates: Iterable<Tuple>,
  { isMatch, isLeftJoin, nulls }: Pairing,
): Generator<Tuple> {
  let isMatched = false;
  for (const candidate of candidates) {
    const pair = [...row, ...candidate];
    if (!isMatch(pair)) continue;
    isMatched = true;
    yield pair;
  }
  if (isLeftJoin && !isMatched) yield [...row, ...nulls];
}

function compilePairing(
  plan: physical.HashJoin | physical.NestedLoopJoin | physical.LookupJoin,
  conditions: readonly physical.Expression[],
  context: Context,
): Pairing {
  const scope = scopeOf(context, physical.layoutOf(plan));
  const rightWidth = physical.layoutOf(plan.right).length;
  return {
    isMatch: compileConditions(conditions, scope),
    isLeftJoin: plan.joinType === "left",
    nulls: Array.from({ length: rightWidth }, () => null),
  };
}

/** The hash key of `keys`: their values as they are, as `=` compares them. */
function compileHashKey(
  keys: readonly physical.Expression[],
  scope: Scope,
): HashKey {
  const parts = keys.map((key) => compileExpression(key, scope));
  return (row) => {
    const values = parts.map((part) => part(row));
    return values.includes(null) ? null : rowKey(values);
  };
}

/** The right side's rows by hash key; a row with a NULL key can match none. */
function hashTableOf(
  right: Operator,
  key: HashKey,
): ReadonlyMap<string, readonly Tuple[]> {
  const table = new Map<string, Tuple[]>();
  for (const row of drain(right)) {
    const hash = key(row);
    if (hash === null) continue;
    const bucket = table.get(hash);
    if (bucket === undefined) table.set(hash, [row]);
    else bucket.push(row);
  }
  return table;
}
