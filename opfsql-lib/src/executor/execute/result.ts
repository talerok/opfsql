import type {
  BoundAggregateExpression,
  BoundColumnRefExpression,
  LogicalOperator,
  LogicalProjection,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import type { Row } from "../../store/types.js";
import type { Tuple } from "../types.js";

export function extractColumnNames(plan: LogicalOperator): string[] {
  const proj = findProjection(plan);
  if (!proj) return plan.types.map((_, i) => `column${i}`);

  return proj.expressions.map((expr, i) => {
    if (proj.aliases[i]) return proj.aliases[i]!;

    if (expr.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF)
      return (expr as BoundColumnRefExpression).columnName;

    if (expr.expressionClass === BoundExpressionClass.BOUND_AGGREGATE) {
      const agg = expr as BoundAggregateExpression;
      return agg.isStar
        ? `${agg.functionName.toLowerCase()}_star`
        : `${agg.functionName.toLowerCase()}_${i}`;
    }

    return `column${i}`;
  });
}

function findProjection(plan: LogicalOperator): LogicalProjection | null {
  if (plan.type === LogicalOperatorType.LOGICAL_PROJECTION) {
    return plan as LogicalProjection;
  }

  if (plan.type === LogicalOperatorType.LOGICAL_MATERIALIZED_CTE) {
    return findProjection(plan.children[1]);
  }

  for (const child of plan.children) {
    const found = findProjection(child);
    if (found) return found;
  }
  return null;
}

export function tuplesToRows(tuples: Tuple[], columnNames: string[]): Row[] {
  return tuples.map((tuple) => {
    const row: Row = {};
    for (let i = 0; i < columnNames.length; i++) {
      row[columnNames[i]] = tuple[i] ?? null;
    }
    return row;
  });
}
