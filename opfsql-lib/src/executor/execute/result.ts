import {
  BoundExpressionClass,
  LogicalOperatorType,
  type BoundExpression,
  type LogicalOperator,
  type LogicalProjection,
} from "../../sql/binder/types.js";

// Results are rows of values in column order (the executor's tuples, as
// they are); this names their columns. Names may repeat — `SELECT t.id, u.id`
// has two columns called id — so callers keep positions, never key by name.

/** The result column names of a bound plan, in order. */
export function outputColumnNames(plan: LogicalOperator): string[] {
  const projection = findProjection(plan);
  if (projection === null) {
    return plan.types.map((_, index) => `column${String(index)}`);
  }
  return projection.expressions.map(
    (expression, index) =>
      projection.aliases[index] ?? expressionName(expression, index),
  );
}

function expressionName(expression: BoundExpression, index: number): string {
  if (expression.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
    return expression.columnName;
  }
  if (expression.expressionClass === BoundExpressionClass.BOUND_AGGREGATE) {
    const name = expression.functionName.toLowerCase();
    return expression.isStar ? `${name}_star` : `${name}_${String(index)}`;
  }
  return `column${String(index)}`;
}

/** The first projection depth-first; of a WITH, the one of its main query. */
function findProjection(plan: LogicalOperator): LogicalProjection | null {
  const pending: LogicalOperator[] = [plan];
  for (let node = pending.pop(); node !== undefined; node = pending.pop()) {
    if (node.type === LogicalOperatorType.LOGICAL_PROJECTION) return node;
    if (node.type === LogicalOperatorType.LOGICAL_MATERIALIZED_CTE) {
      const [, query] = node.children;
      pending.push(query);
    } else {
      pending.push(...node.children.toReversed());
    }
  }
  return null;
}
