import type {
  BoundExpression,
  BoundColumnRefExpression,
  BoundAggregateExpression,
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
    }
    return e;
  });
  return refs;
}
