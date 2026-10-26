import { compileExpression, type Tuple } from "../../expressions/index.js";
import { physical } from "../../sql/plan/index.js";
import { firstSighting } from "../../values/index.js";
import { compileConditions, type Context, scopeOf } from "../context.js";
import {
  nextNonEmpty,
  type Operator,
  rowsOf,
  WalkOperator,
} from "../operator.js";

// Operators that decide or compute row by row: filter, project and
// hash-distinct.

/** The child's rows for which every condition is TRUE. */
export function filter(
  plan: physical.Filter,
  child: Operator,
  context: Context,
): Operator {
  const scope = scopeOf(context, physical.layoutOf(plan.child));
  const passes = compileConditions(plan.conditions, scope);
  return {
    next: () =>
      nextNonEmpty(child, (batch) => batch.filter((row) => passes(row))),
    rescan: () => {
      child.rescan();
    },
  };
}

/** One row of the expressions' values per row of the child. */
export function project(
  plan: physical.Project,
  child: Operator,
  context: Context,
): Operator {
  const scope = scopeOf(context, physical.layoutOf(plan.child));
  const expressions = plan.expressions.map((expression) =>
    compileExpression(expression, scope),
  );
  const projectRow = (row: Tuple): Tuple =>
    expressions.map((expression) => expression(row));
  return {
    next: () => child.next()?.map((row) => projectRow(row)) ?? null,
    rescan: () => {
      child.rescan();
    },
  };
}

/** The child's rows, each the first of the rows equal to it (values' rowKey). */
export function hashDistinct(child: Operator): Operator {
  return new WalkOperator(() => distinctRows(child), [child]);
}

function* distinctRows(child: Operator): Generator<Tuple> {
  const isFirst = firstSighting();
  for (const row of rowsOf(child)) {
    if (isFirst(row)) yield row;
  }
}
