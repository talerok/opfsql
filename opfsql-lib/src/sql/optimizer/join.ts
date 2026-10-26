import { physical } from "../plan/index.js";
import { columnsOf, isPinned, readsOnly, tablesOf } from "./expression-tree.js";

// ---------------------------------------------------------------------------
// Pass 3, join planning. The FROM order stays as written (left-deep). A
// join's conditions split into equality keys — `l = r` with l reading only
// the left side's columns and r only the right's, each at least one — and
// everything else, the residual. With keys it is a hash join, the right
// side hashed; without, a nested loop. Nothing is dropped (docs/planning.md,
// rule 2): a condition is a key or it is in the residual, which for a LEFT
// join decides whether a pair matches, not whether the left row appears.
//
// A key compares as `=` does: the executor converts both sides as `=`
// would before hashing (docs/planning.md, Physical plans). A condition
// that calls a volatile function or holds a correlated subquery is never a
// key: a key is evaluated once per row of its side, not once per pair.
// ---------------------------------------------------------------------------

type Expression = physical.Expression;

interface EqualityKey {
  readonly left: Expression;
  readonly right: Expression;
}

export function planJoin(
  joinType: "inner" | "left",
  left: physical.Plan,
  right: physical.Plan,
  conditions: readonly Expression[],
): physical.Plan {
  const leftTables = tablesOf(physical.layoutOf(left));
  const rightTables = tablesOf(physical.layoutOf(right));
  const keys: EqualityKey[] = [];
  const residual: Expression[] = [];
  for (const condition of conditions) {
    const key = equalityKey(condition, leftTables, rightTables);
    if (key === null) residual.push(condition);
    else keys.push(key);
  }
  if (keys.length === 0) {
    return { kind: "nested-loop-join", joinType, left, right, conditions };
  }
  return {
    kind: "hash-join",
    joinType,
    left,
    right,
    leftKeys: keys.map((key) => key.left),
    rightKeys: keys.map((key) => key.right),
    residual,
  };
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
  const hasColumns = columnsOf(expression).next().done !== true;
  return hasColumns && readsOnly(expression, tables);
}
