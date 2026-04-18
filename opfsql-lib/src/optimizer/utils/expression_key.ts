import type {
  BoundBetweenExpression,
  BoundCaseExpression,
  BoundCastExpression,
  BoundConjunctionExpression,
  BoundColumnRefExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundFunctionExpression,
  BoundJsonAccessExpression,
  BoundOperatorExpression,
  BoundParameterExpression,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';

/**
 * Structural serialization of a BoundExpression for use as a Map key.
 * Injective by construction — each expression class has a unique prefix,
 * and structural content is fully encoded.
 */
export function expressionKey(expr: BoundExpression): string {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const ref = expr as BoundColumnRefExpression;
      return `C:${ref.binding.tableIndex}:${ref.binding.columnIndex}`;
    }
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const ja = expr as BoundJsonAccessExpression;
      return `J:${expressionKey(ja.child)}:${JSON.stringify(ja.path)}`;
    }
    case BoundExpressionClass.BOUND_FUNCTION: {
      const fn = expr as BoundFunctionExpression;
      return `F:${fn.functionName}(${fn.children.map(expressionKey).join(',')})`;
    }
    case BoundExpressionClass.BOUND_CAST: {
      const cast = expr as BoundCastExpression;
      return `T:${cast.castType}(${expressionKey(cast.child)})`;
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      return `O:${op.operatorType}(${op.children.map(expressionKey).join(',')})`;
    }
    case BoundExpressionClass.BOUND_BETWEEN: {
      const b = expr as BoundBetweenExpression;
      return `B:${expressionKey(b.input)},${expressionKey(b.lower)},${expressionKey(b.upper)}`;
    }
    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const c = expr as BoundConjunctionExpression;
      return `A:${c.conjunctionType}(${c.children.map(expressionKey).join(',')})`;
    }
    case BoundExpressionClass.BOUND_CASE: {
      const cs = expr as BoundCaseExpression;
      const checks = cs.caseChecks.map(
        (ch) => `${expressionKey(ch.when)}:${expressionKey(ch.then)}`,
      ).join(',');
      const el = cs.elseExpr ? expressionKey(cs.elseExpr) : '_';
      return `K:(${checks}|${el})`;
    }
    case BoundExpressionClass.BOUND_CONSTANT: {
      const ct = expr as BoundConstantExpression;
      return `V:${ct.returnType}:${String(ct.value)}`;
    }
    case BoundExpressionClass.BOUND_PARAMETER: {
      const p = expr as BoundParameterExpression;
      return `P:${p.index}`;
    }
    default:
      return `R:${JSON.stringify(expr)}`;
  }
}
