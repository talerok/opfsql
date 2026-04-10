import type {
  LogicalOperator,
  BoundExpression,
  BoundOperatorExpression,
  BoundConstantExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
} from '../binder/types.js';
import { BoundExpressionClass } from '../binder/types.js';
import { mapExpression, mapOperatorExpressions, isConstant } from './utils/index.js';

// ============================================================================
// IN Clause Rewriter
//
// Based on DuckDB's in_clause_rewriter.cpp:
// Rewrites x IN (v1, v2, ..., vN) operators:
//   - 1 value:       x IN (5)       → x = 5
//   - ≤10 values:    x IN (1,2,3)   → x = 1 OR x = 2 OR x = 3
//   - >10 values:    left as IN — executor uses Set for O(1) lookup
// Also handles NOT IN the same way with NOT_EQUAL / AND.
// ============================================================================

const IN_EXPANSION_THRESHOLD = 10;

export function rewriteInClauses(plan: LogicalOperator): LogicalOperator {
  mapOperatorExpressions(plan, rewriteInExpression);
  return plan;
}

function rewriteInExpression(expr: BoundExpression): BoundExpression {
  if (expr.expressionClass !== BoundExpressionClass.BOUND_OPERATOR) return expr;
  const op = expr as BoundOperatorExpression;

  if (op.operatorType !== 'IN' && op.operatorType !== 'NOT_IN') return expr;
  if (op.children.length < 2) return expr;

  const input = op.children[0];
  const values = op.children.slice(1);

  // Check all values are constants
  if (!values.every(isConstant)) return expr;

  const isNot = op.operatorType === 'NOT_IN';
  const compType = isNot ? 'NOT_EQUAL' : 'EQUAL';

  // Single value: x IN (5) → x = 5, x NOT IN (5) → x <> 5
  if (values.length === 1) {
    return {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: compType,
      left: input,
      right: values[0],
      returnType: 'BOOLEAN',
    } as BoundComparisonExpression;
  }

  // Large lists: leave as IN — executor uses Set for O(1) lookup
  if (values.length > IN_EXPANSION_THRESHOLD) return expr;

  // Small lists: expand to OR (for IN) or AND (for NOT IN)
  const comparisons: BoundExpression[] = values.map((val) => ({
    expressionClass: BoundExpressionClass.BOUND_COMPARISON,
    comparisonType: compType,
    left: input,
    right: val,
    returnType: 'BOOLEAN',
  } as BoundComparisonExpression));

  const conjunctionType = isNot ? 'AND' : 'OR';

  return {
    expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
    conjunctionType,
    children: comparisons,
    returnType: 'BOOLEAN',
  } as BoundConjunctionExpression;
}
