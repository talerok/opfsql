import type {
  BoundExpression,
  BoundColumnRefExpression,
  BoundConstantExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundOperatorExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function expressionsEqual(
  a: BoundExpression,
  b: BoundExpression,
): boolean {
  if (a.expressionClass !== b.expressionClass) return false;
  switch (a.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const ar = a as BoundColumnRefExpression;
      const br = b as BoundColumnRefExpression;
      return (
        ar.binding.tableIndex === br.binding.tableIndex &&
        ar.binding.columnIndex === br.binding.columnIndex
      );
    }
    case BoundExpressionClass.BOUND_CONSTANT: {
      const ac = a as BoundConstantExpression;
      const bc = b as BoundConstantExpression;
      return ac.value === bc.value && ac.returnType === bc.returnType;
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      const ac = a as BoundComparisonExpression;
      const bc = b as BoundComparisonExpression;
      return (
        ac.comparisonType === bc.comparisonType &&
        expressionsEqual(ac.left, bc.left) &&
        expressionsEqual(ac.right, bc.right)
      );
    }
    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const ac = a as BoundConjunctionExpression;
      const bc = b as BoundConjunctionExpression;
      if (ac.conjunctionType !== bc.conjunctionType) return false;
      if (ac.children.length !== bc.children.length) return false;
      return ac.children.every((c, i) => expressionsEqual(c, bc.children[i]));
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      const ao = a as BoundOperatorExpression;
      const bo = b as BoundOperatorExpression;
      if (ao.operatorType !== bo.operatorType) return false;
      if (ao.children.length !== bo.children.length) return false;
      return ao.children.every((c, i) => expressionsEqual(c, bo.children[i]));
    }
    default:
      return false;
  }
}
