import type {
  BoundExpression,
  LogicalOperator,
} from '../../binder/types.js';
import { mapExpression } from './map_expression.js';

export function mapOperatorExpressions(
  op: LogicalOperator,
  fn: (e: BoundExpression) => BoundExpression,
): void {
  for (let i = 0; i < op.expressions.length; i++) {
    op.expressions[i] = mapExpression(op.expressions[i], fn);
  }

  if ('conditions' in op && Array.isArray(op.conditions)) {
    for (const cond of op.conditions) {
      cond.left = mapExpression(cond.left, fn);
      cond.right = mapExpression(cond.right, fn);
    }
  }

  if ('orders' in op && Array.isArray(op.orders)) {
    for (const order of op.orders) {
      order.expression = mapExpression(order.expression, fn);
    }
  }

  if ('groups' in op && Array.isArray(op.groups)) {
    for (let i = 0; i < op.groups.length; i++) {
      op.groups[i] = mapExpression(op.groups[i], fn);
    }
  }

  if ('havingExpression' in op && op.havingExpression) {
    op.havingExpression = mapExpression(op.havingExpression, fn);
  }

  for (const child of op.children) {
    mapOperatorExpressions(child, fn);
  }
}
