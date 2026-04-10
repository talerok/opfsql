import type {
  BoundExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundOperatorExpression,
  BoundBetweenExpression,
  BoundFunctionExpression,
  BoundCaseExpression,
  BoundCastExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function isFoldable(expr: BoundExpression): boolean {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_CONSTANT:
      return true;
    case BoundExpressionClass.BOUND_COLUMN_REF:
    case BoundExpressionClass.BOUND_AGGREGATE:
    case BoundExpressionClass.BOUND_SUBQUERY:
      return false;
    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BoundComparisonExpression;
      return isFoldable(cmp.left) && isFoldable(cmp.right);
    }
    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const conj = expr as BoundConjunctionExpression;
      return conj.children.every(isFoldable);
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      return op.children.every(isFoldable);
    }
    case BoundExpressionClass.BOUND_BETWEEN: {
      const bt = expr as BoundBetweenExpression;
      return isFoldable(bt.input) && isFoldable(bt.lower) && isFoldable(bt.upper);
    }
    case BoundExpressionClass.BOUND_FUNCTION: {
      const func = expr as BoundFunctionExpression;
      return func.children.every(isFoldable);
    }
    case BoundExpressionClass.BOUND_CASE: {
      const cs = expr as BoundCaseExpression;
      return (
        cs.caseChecks.every((cc) => isFoldable(cc.when) && isFoldable(cc.then)) &&
        (cs.elseExpr === null || isFoldable(cs.elseExpr))
      );
    }
    case BoundExpressionClass.BOUND_CAST:
      return isFoldable((expr as BoundCastExpression).child);
    default:
      return false;
  }
}
