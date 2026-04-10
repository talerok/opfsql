import type { LogicalType, ColumnDef } from '../../store/types.js';
import type { LogicalOperator, LogicalProjection, BoundColumnRefExpression, BoundAggregateExpression } from '../types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../types.js';

export function findProjection(plan: LogicalOperator): LogicalProjection | null {
  if (plan.type === LogicalOperatorType.LOGICAL_PROJECTION) {
    return plan as LogicalProjection;
  }
  if (plan.children.length > 0) {
    return findProjection(plan.children[0]);
  }
  return null;
}

export function extractColumnsFromPlan(
  plan: LogicalOperator,
  types: LogicalType[],
): ColumnDef[] {
  const proj = findProjection(plan);
  if (proj) {
    return proj.expressions.map((expr, i) => {
      let colName = `column${i}`;
      if (proj.aliases[i]) {
        colName = proj.aliases[i];
      } else if (expr.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
        colName = (expr as BoundColumnRefExpression).columnName;
      } else if (expr.expressionClass === BoundExpressionClass.BOUND_AGGREGATE) {
        const agg = expr as BoundAggregateExpression;
        colName = agg.isStar
          ? `${agg.functionName.toLowerCase()}_star`
          : `${agg.functionName.toLowerCase()}_${i}`;
      }
      return {
        name: colName,
        type: types[i] ?? 'ANY',
        nullable: true,
        primaryKey: false,
        unique: false,
        defaultValue: null,
      };
    });
  }

  return types.map((t, i) => ({
    name: `column${i}`,
    type: t,
    nullable: true,
    primaryKey: false,
    unique: false,
    defaultValue: null,
  }));
}
