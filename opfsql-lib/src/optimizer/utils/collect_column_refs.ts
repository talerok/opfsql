import type {
  BoundExpression,
  BoundColumnRefExpression,
  ColumnBinding,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import { mapExpression } from './map_expression.js';

export function collectColumnRefs(expr: BoundExpression): ColumnBinding[] {
  const refs: ColumnBinding[] = [];
  mapExpression(expr, (e) => {
    if (e.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
      refs.push((e as BoundColumnRefExpression).binding);
    }
    return e;
  });
  return refs;
}
