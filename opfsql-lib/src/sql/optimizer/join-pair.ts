import { physical } from "../plan/index.js";
import {
  type Planned,
  hashJoinEstimate,
  keyMatches,
  lookupJoinEstimate,
  nestedLoopEstimate,
} from "./cost.js";
import {
  isPinned,
  readsAnyColumn,
  readsOnly,
  tablesOf,
} from "./expression-tree.js";
import { type Lookup, chooseLookup } from "./lookup.js";
import type { TableRead } from "./plan-tree.js";
import type { Planner } from "./planner.js";
import { lowerExpressions } from "./lower-expression.js";

// ---------------------------------------------------------------------------
// One join of two sides, the cheapest way (cost.ts): the left side planned
// already, the right side as written. Three ways to join them:
//
//   lookup-join       the right side a table read that a seek can find by
//                     values of the left row (lookup.ts): for each left row,
//                     a rowid lookup or an index scan;
//   hash-join         equality keys — `l = r` with l reading only the left
//                     side's columns and r only the right's, each at least
//                     one — the build side hashed: the right side of a LEFT
//                     join, the side estimated smaller of an inner one;
//   nested-loop-join  every pair, when there is no key.
//
// Nothing is dropped (docs/planning.md, rule 2): a condition is a key, a
// test the seek enforces exactly, or in the residual, which for a LEFT join
// decides whether a pair matches, not whether the left row appears. A key
// matches as `=` does: values as they are, a NULL key never. A condition
// that calls a volatile function or holds a correlated subquery is never a
// key: a key is evaluated once per row of its side, not once per pair.
//
// Only a lookup join keeps the left side's order (rule 3): when the left
// side was asked to lead in an order, a lookup join is taken whenever there
// is one, so that the order lasts; whether it pays is weighed where the
// order is wanted (order.ts).
// ---------------------------------------------------------------------------

type Expression = physical.Expression;

/** A join's right side as written: a table read a seek can find rows of, if it is one. */
export interface Side {
  readonly read: TableRead | null;
  /** The side read alone, the way it would be without the left row. */
  readonly alone: () => Planned;
}

interface EqualityKey {
  readonly left: Expression;
  readonly right: Expression;
}

/** A join to plan: its type, its sides, and the conditions it checks. */
interface Pair {
  readonly joinType: "inner" | "left";
  readonly left: Planned;
  readonly right: Side;
  readonly conditions: readonly Expression[];
}

/** The left side joined to the right, the cheapest way. */
export function joinPair(pair: Pair, planner: Planner): Planned {
  const lookup = lookupOf(pair, planner);
  const lookupJoined = lookup === null ? null : lookupJoin(pair, lookup);
  // Only a lookup join keeps the order the left side leads in.
  if (lookupJoined !== null && pair.left.isOrdered) return lookupJoined;
  const bothRead = hashOrNestedLoopJoin(pair, pair.right.alone(), lookup);
  const isLookupCheaper =
    lookupJoined !== null &&
    lookupJoined.estimate.cost <= bothRead.estimate.cost;
  return isLookupCheaper ? lookupJoined : bothRead;
}

function lookupOf(pair: Pair, planner: Planner): Lookup | null {
  const { read } = pair.right;
  if (read === null) return null;
  return chooseLookup(
    {
      get: read.get,
      conditions: lowerExpressions(read.conditions, planner),
      joinConditions: pair.conditions,
      leftTables: tablesOf(physical.layoutOf(pair.left.plan)),
    },
    planner,
  );
}

function lookupJoin(pair: Pair, lookup: Lookup): Planned {
  const { joinType, left } = pair;
  return {
    plan: {
      kind: "lookup-join",
      joinType,
      left: left.plan,
      right: lookup.scan,
      residual: lookup.residual,
    },
    estimate: lookupJoinEstimate(left.estimate, lookup.estimate),
    isOrdered: left.isOrdered,
  };
}

/**
 * Both sides read once: a hash join on the equality keys, or a nested loop
 * without any. Each left row meets as many right rows as the lookup would
 * have found, or as the keys leave of the right side.
 */
function hashOrNestedLoopJoin(
  pair: Pair,
  right: Planned,
  lookup: Lookup | null,
): Planned {
  const { joinType, left, conditions } = pair;
  const { keys, residual } = splitKeys(left.plan, right.plan, conditions);
  if (keys.length === 0) {
    return {
      plan: {
        kind: "nested-loop-join",
        joinType,
        left: left.plan,
        right: right.plan,
        conditions,
      },
      estimate: nestedLoopEstimate(left.estimate, right.estimate),
      isOrdered: false,
    };
  }
  const matches =
    lookup?.estimate.rows ?? keyMatches(right.estimate, keys.length);
  const estimate = hashJoinEstimate(left.estimate, right.estimate, matches);
  // An inner join hashes the side estimated smaller: it builds on the right.
  const isSwapped =
    joinType === "inner" && left.estimate.rows < right.estimate.rows;
  const [probe, build] = isSwapped ? [right, left] : [left, right];
  return {
    plan: {
      kind: "hash-join",
      joinType,
      left: probe.plan,
      right: build.plan,
      leftKeys: keys.map((key) => (isSwapped ? key.right : key.left)),
      rightKeys: keys.map((key) => (isSwapped ? key.left : key.right)),
      residual,
    },
    estimate,
    isOrdered: false,
  };
}

/** The equality keys among `conditions`, and the rest. */
function splitKeys(
  left: physical.Plan,
  right: physical.Plan,
  conditions: readonly Expression[],
): { readonly keys: EqualityKey[]; readonly residual: Expression[] } {
  const leftTables = tablesOf(physical.layoutOf(left));
  const rightTables = tablesOf(physical.layoutOf(right));
  const keys: EqualityKey[] = [];
  const residual: Expression[] = [];
  for (const condition of conditions) {
    const key = equalityKey(condition, leftTables, rightTables);
    if (key === null) residual.push(condition);
    else keys.push(key);
  }
  return { keys, residual };
}

/** `condition` as an equality key, either way round, or null. */
function equalityKey(
  condition: Expression,
  leftTables: ReadonlySet<number>,
  rightTables: ReadonlySet<number>,
): EqualityKey | null {
  const isEquality =
    condition.kind === "comparison" && condition.operator === "EQUAL";
  if (!isEquality || isPinned(condition)) return null;
  const { left, right } = condition;
  if (isOnSide(left, leftTables) && isOnSide(right, rightTables)) {
    return { left, right };
  }
  if (isOnSide(right, leftTables) && isOnSide(left, rightTables)) {
    return { left: right, right: left };
  }
  return null;
}

/** True if `expression` reads columns of the side of `tables`, and no others. */
function isOnSide(
  expression: Expression,
  tables: ReadonlySet<number>,
): boolean {
  return readsAnyColumn(expression) && readsOnly(expression, tables);
}
