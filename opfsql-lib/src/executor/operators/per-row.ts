import {
  type CompiledExpression,
  compileExpression,
  type Tuple,
} from "../../expressions/index.js";
import { isSameColumn, physical } from "../../sql/plan/index.js";
import { firstSighting } from "../../values/index.js";
import { compileConditions, type Context, scopeOf } from "../context.js";
import { type Operator, pull, WalkOperator } from "../operator.js";

// Operators that decide or compute row by row: filter, project and
// hash-distinct. Each gives at most one row per row of its child, so it
// pulls its child by what it is asked for, and evaluates no row more.

/** The child's rows for which every condition is TRUE. */
export function filter(
  plan: physical.Filter,
  child: Operator,
  context: Context,
): Operator {
  const scope = scopeOf(context, physical.layoutOf(plan.child));
  const passes = compileConditions(plan.conditions, scope);
  return new WalkOperator(
    (demand) => passing(pull(child, demand), passes),
    [child],
  );
}

/**
 * One row of the expressions' values per row of the child; the child itself
 * when they are its columns as they are, as for SELECT *.
 */
export function project(
  plan: physical.Project,
  child: Operator,
  context: Context,
): Operator {
  if (keepsChildRows(plan)) return child;
  const scope = scopeOf(context, physical.layoutOf(plan.child));
  const expressions = plan.expressions.map((expression) =>
    compileExpression(expression, scope),
  );
  return new WalkOperator(
    (demand) => projected(pull(child, demand), expressions),
    [child],
  );
}

/** The child's rows, each the first of the rows equal to it (values' rowKey). */
export function hashDistinct(child: Operator): Operator {
  return new WalkOperator(
    (demand) => distinctRows(pull(child, demand)),
    [child],
  );
}

function* passing(
  rows: Iterable<Tuple>,
  isPassing: (row: Tuple) => boolean,
): Generator<Tuple> {
  for (const row of rows) {
    if (isPassing(row)) yield row;
  }
}

function* projected(
  rows: Iterable<Tuple>,
  expressions: readonly CompiledExpression[],
): Generator<Tuple> {
  for (const row of rows)
    yield expressions.map((expression) => expression(row));
}

function* distinctRows(rows: Iterable<Tuple>): Generator<Tuple> {
  const isFirst = firstSighting();
  for (const row of rows) {
    if (isFirst(row)) yield row;
  }
}

/** True if the projection's expressions are the child's columns, in its order. */
function keepsChildRows(plan: physical.Project): boolean {
  const layout = physical.layoutOf(plan.child);
  if (plan.expressions.length !== layout.length) return false;
  return plan.expressions.every((expression, position) => {
    const binding = layout[position];
    const isOwnColumn = expression.kind === "column" && expression.depth === 0;
    return (
      isOwnColumn &&
      binding !== undefined &&
      isSameColumn(expression.binding, binding)
    );
  });
}
