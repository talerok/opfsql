import type * as BT from '../types.js';
import { BoundExpressionClass } from '../types.js';

export function sameExpression(a: BT.BoundExpression, b: BT.BoundExpression): boolean {
  if (a.expressionClass !== b.expressionClass) return false;
  switch (a.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const ar = a as BT.BoundColumnRefExpression;
      const br = b as BT.BoundColumnRefExpression;
      return ar.binding.tableIndex === br.binding.tableIndex && ar.binding.columnIndex === br.binding.columnIndex;
    }
    case BoundExpressionClass.BOUND_CONSTANT: {
      const ac = a as BT.BoundConstantExpression;
      const bc = b as BT.BoundConstantExpression;
      return ac.value === bc.value && ac.returnType === bc.returnType;
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      const ac = a as BT.BoundComparisonExpression;
      const bc = b as BT.BoundComparisonExpression;
      return ac.comparisonType === bc.comparisonType && sameExpression(ac.left, bc.left) && sameExpression(ac.right, bc.right);
    }
    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const ac = a as BT.BoundConjunctionExpression;
      const bc = b as BT.BoundConjunctionExpression;
      if (ac.conjunctionType !== bc.conjunctionType) return false;
      if (ac.children.length !== bc.children.length) return false;
      return ac.children.every((c, i) => sameExpression(c, bc.children[i]));
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      const ao = a as BT.BoundOperatorExpression;
      const bo = b as BT.BoundOperatorExpression;
      if (ao.operatorType !== bo.operatorType) return false;
      if (ao.children.length !== bo.children.length) return false;
      return ao.children.every((c, i) => sameExpression(c, bo.children[i]));
    }
    case BoundExpressionClass.BOUND_FUNCTION: {
      const af = a as BT.BoundFunctionExpression;
      const bf = b as BT.BoundFunctionExpression;
      if (af.functionName !== bf.functionName) return false;
      if (af.children.length !== bf.children.length) return false;
      return af.children.every((c, i) => sameExpression(c, bf.children[i]));
    }
    case BoundExpressionClass.BOUND_AGGREGATE: {
      const aa = a as BT.BoundAggregateExpression;
      const ba = b as BT.BoundAggregateExpression;
      return sameAggregate(aa, ba);
    }
    case BoundExpressionClass.BOUND_BETWEEN: {
      const ab = a as BT.BoundBetweenExpression;
      const bb = b as BT.BoundBetweenExpression;
      return sameExpression(ab.input, bb.input) && sameExpression(ab.lower, bb.lower) && sameExpression(ab.upper, bb.upper);
    }
    case BoundExpressionClass.BOUND_CAST: {
      const ac = a as BT.BoundCastExpression;
      const bc = b as BT.BoundCastExpression;
      return ac.castType === bc.castType && sameExpression(ac.child, bc.child);
    }
    default:
      return false;
  }
}

export function sameAggregate(
  a: BT.BoundAggregateExpression,
  b: BT.BoundAggregateExpression,
): boolean {
  if (a.functionName !== b.functionName) return false;
  if (a.isStar !== b.isStar) return false;
  if (a.distinct !== b.distinct) return false;
  if (a.children.length !== b.children.length) return false;
  for (let i = 0; i < a.children.length; i++) {
    if (!sameExpression(a.children[i], b.children[i])) return false;
  }
  return true;
}
