import type {
  BoundExpression,
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundOperatorExpression,
  BoundBetweenExpression,
  BoundFunctionExpression,
  BoundAggregateExpression,
  BoundSubqueryExpression,
  BoundCaseExpression,
  BoundCastExpression,
  BoundJsonAccessExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

export function mapExpression(
  expr: BoundExpression,
  fn: (e: BoundExpression) => BoundExpression,
): BoundExpression {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF:
    case BoundExpressionClass.BOUND_CONSTANT:
      return fn(expr);

    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BoundComparisonExpression;
      const left = mapExpression(cmp.left, fn);
      const right = mapExpression(cmp.right, fn);
      const next: BoundComparisonExpression =
        left === cmp.left && right === cmp.right
          ? cmp
          : { ...cmp, left, right };
      return fn(next);
    }

    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const conj = expr as BoundConjunctionExpression;
      const children = conj.children.map((c) => mapExpression(c, fn));
      const changed = children.some((c, i) => c !== conj.children[i]);
      return fn(changed ? { ...conj, children } : conj);
    }

    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      const children = op.children.map((c) => mapExpression(c, fn));
      const changed = children.some((c, i) => c !== op.children[i]);
      return fn(changed ? { ...op, children } : op);
    }

    case BoundExpressionClass.BOUND_BETWEEN: {
      const bt = expr as BoundBetweenExpression;
      const input = mapExpression(bt.input, fn);
      const lower = mapExpression(bt.lower, fn);
      const upper = mapExpression(bt.upper, fn);
      const next: BoundBetweenExpression =
        input === bt.input && lower === bt.lower && upper === bt.upper
          ? bt
          : { ...bt, input, lower, upper };
      return fn(next);
    }

    case BoundExpressionClass.BOUND_FUNCTION: {
      const func = expr as BoundFunctionExpression;
      const children = func.children.map((c) => mapExpression(c, fn));
      const changed = children.some((c, i) => c !== func.children[i]);
      return fn(changed ? { ...func, children } : func);
    }

    case BoundExpressionClass.BOUND_AGGREGATE: {
      const agg = expr as BoundAggregateExpression;
      const children = agg.children.map((c) => mapExpression(c, fn));
      const changed = children.some((c, i) => c !== agg.children[i]);
      return fn(changed ? { ...agg, children } : agg);
    }

    case BoundExpressionClass.BOUND_SUBQUERY: {
      const sub = expr as BoundSubqueryExpression;
      if (sub.child) {
        const child = mapExpression(sub.child, fn);
        return fn(child === sub.child ? sub : { ...sub, child });
      }
      return fn(sub);
    }

    case BoundExpressionClass.BOUND_CASE: {
      const cs = expr as BoundCaseExpression;
      const caseChecks = cs.caseChecks.map((cc) => ({
        when: mapExpression(cc.when, fn),
        then: mapExpression(cc.then, fn),
      }));
      const elseExpr = cs.elseExpr ? mapExpression(cs.elseExpr, fn) : null;
      return fn({ ...cs, caseChecks, elseExpr });
    }

    case BoundExpressionClass.BOUND_CAST: {
      const cast = expr as BoundCastExpression;
      const child = mapExpression(cast.child, fn);
      return fn(child === cast.child ? cast : { ...cast, child });
    }

    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const ja = expr as BoundJsonAccessExpression;
      const child = mapExpression(ja.child, fn);
      return fn(child === ja.child ? ja : { ...ja, child: child as BoundColumnRefExpression });
    }

    default:
      return fn(expr);
  }
}