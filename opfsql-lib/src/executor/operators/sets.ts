import type { Tuple } from "../../expressions/index.js";
import type { physical } from "../../sql/plan/index.js";
import { rowKey } from "../../values/index.js";
import {
  type Demand,
  drain,
  type Operator,
  pull,
  WalkOperator,
} from "../operator.js";

// UNION, UNION ALL, INTERSECT and EXCEPT, by hashing rows with rowKey: two
// rows are one when ORDER BY would call every pair of their values equal,
// NULLs included. All but UNION ALL return each row once. INTERSECT and
// EXCEPT read their right side whole; the rest pull by what they are asked.

export function setOperation(
  plan: physical.SetOperation,
  left: Operator,
  right: Operator,
): Operator {
  const { operator } = plan;
  return new WalkOperator(
    (demand) => setRows(operator, [left, right], demand),
    [left, right],
  );
}

function setRows(
  operator: physical.SetOperation["operator"],
  [left, right]: readonly [Operator, Operator],
  demand: Demand,
): Iterator<Tuple> {
  switch (operator) {
    case "UNION ALL": {
      return bothSides(left, right, demand);
    }
    case "UNION": {
      return keptOnce(bothSides(left, right, demand), () => true);
    }
    case "INTERSECT": {
      return intersection(left, right, demand);
    }
    case "EXCEPT": {
      return difference(left, right, demand);
    }
  }
}

function* bothSides(
  left: Operator,
  right: Operator,
  demand: Demand,
): Generator<Tuple> {
  yield* pull(left, demand);
  yield* pull(right, demand);
}

/** The left side's rows that are also on the right. */
function* intersection(
  left: Operator,
  right: Operator,
  demand: Demand,
): Generator<Tuple> {
  const onRight = keysOf(right);
  yield* keptOnce(pull(left, demand), (key) => onRight.has(key));
}

/** The left side's rows that are not on the right. */
function* difference(
  left: Operator,
  right: Operator,
  demand: Demand,
): Generator<Tuple> {
  const onRight = keysOf(right);
  yield* keptOnce(pull(left, demand), (key) => !onRight.has(key));
}

function keysOf(operator: Operator): ReadonlySet<string> {
  return new Set(drain(operator).map((row) => rowKey(row)));
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
