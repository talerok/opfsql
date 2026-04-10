import type {
  BoundExpression,
  BoundColumnRefExpression,
  BoundAggregateExpression,
  BoundSubqueryExpression,
  LogicalOperator,
  ColumnBinding,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import { mapExpression } from './map_expression.js';

export function collectColumnRefs(expr: BoundExpression): ColumnBinding[] {
  const refs: ColumnBinding[] = [];
  mapExpression(expr, (e) => {
    if (e.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
      refs.push((e as BoundColumnRefExpression).binding);
    } else if (e.expressionClass === BoundExpressionClass.BOUND_AGGREGATE) {
      const agg = e as BoundAggregateExpression;
      if (agg.binding) refs.push(agg.binding);
    } else if (e.expressionClass === BoundExpressionClass.BOUND_SUBQUERY) {
      // Correlated subqueries reference outer columns inside the subplan.
      // Collect all column refs from the subplan so the optimizer keeps
      // outer columns that the subquery needs.
      collectRefsFromPlan((e as BoundSubqueryExpression).subplan, refs);
    }
    return e;
  });
  return refs;
}

function collectRefsFromPlan(op: LogicalOperator, out: ColumnBinding[]): void {
  for (const expr of op.expressions) {
    out.push(...collectColumnRefs(expr));
  }
  if ('conditions' in op && Array.isArray(op.conditions)) {
    for (const c of op.conditions as { left: BoundExpression; right: BoundExpression }[]) {
      out.push(...collectColumnRefs(c.left));
      out.push(...collectColumnRefs(c.right));
    }
  }
  if ('orders' in op && Array.isArray(op.orders)) {
    for (const o of op.orders as { expression: BoundExpression }[]) {
      out.push(...collectColumnRefs(o.expression));
    }
  }
  if ('groups' in op && Array.isArray(op.groups)) {
    for (const g of op.groups as BoundExpression[]) {
      out.push(...collectColumnRefs(g));
    }
  }
  if ('havingExpression' in op && op.havingExpression) {
    out.push(...collectColumnRefs(op.havingExpression as BoundExpression));
  }
  for (const child of op.children) {
    collectRefsFromPlan(child, out);
  }
}
