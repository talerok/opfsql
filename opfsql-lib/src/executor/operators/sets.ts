import type { Tuple } from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import { rowKey } from "../../values/index.js";
import { drain, type Operator, rowsOf, WalkOperator } from "../operator.js";

// UNION, UNION ALL, INTERSECT and EXCEPT, by hashing rows with rowKey: two
// rows are one when ORDER BY would call every pair of their values equal,
// NULLs included. All but UNION ALL return each row once.

export function setOperation(
  plan: physical.SetOperation,
  left: Operator,
  right: Operator,
): Operator {
  const { operator } = plan;
  return new WalkOperator(() => setRows(operator, left, right), [left, right]);
}

function setRows(
  operator: physical.SetOperation["operator"],
  left: Operator,
  right: Operator,
): Iterator<Tuple> {
  switch (operator) {
    case "UNION ALL": {
      return bothSides(left, right);
    }
    case "UNION": {
      return keptOnce(bothSides(left, right), () => true);
    }
    case "INTERSECT": {
      return rowsOfLeft(left, right, true);
    }
    case "EXCEPT": {
      return rowsOfLeft(left, right, false);
    }
  }
}

function* bothSides(left: Operator, right: Operator): Generator<Tuple> {
  yield* rowsOf(left);
  yield* rowsOf(right);
}

/** The left side's rows that are (INTERSECT) or are not (EXCEPT) on the right. */
function rowsOfLeft(
  left: Operator,
  right: Operator,
  isOnRight: boolean,
): Iterator<Tuple> {
  let rightKeys: ReadonlySet<string> | null = null;
  return keptOnce(rowsOf(left), (key) => {
    rightKeys ??= new Set(drain(right).map((row) => rowKey(row)));
    return rightKeys.has(key) === isOnRight;
  });
}

/** The first of each set of equal rows that `isKept` keeps, decided by key. */
function* keptOnce(
  rows: Iterable<Tuple>,
  isKept: (key: string) => boolean,
): Generator<Tuple> {
  const seen = new Set<string>();
  for (const row of rows) {
    const key = rowKey(row);
    if (seen.has(key) || !isKept(key)) continue;
    seen.add(key);
    yield row;
  }
}
