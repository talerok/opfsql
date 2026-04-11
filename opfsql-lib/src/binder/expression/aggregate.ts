import type { LogicalType } from '../../store/types.js';
import type {
  ParsedExpression,
  FunctionExpression,
  ComparisonExpression,
  ConjunctionExpression,
  OperatorExpression,
  BetweenExpression,
  CaseExpression,
  CastExpression,
} from '../../parser/types.js';
import { ExpressionClass } from '../../parser/types.js';
import type { BoundAggregateExpression, AggregateFunctionName } from '../types.js';
import { BoundExpressionClass } from '../types.js';
import type { BindContext } from '../core/context.js';
import type { BindScope } from '../core/scope.js';
import { BindError } from '../core/errors.js';
import { bindExpression } from './index.js';
import { sameAggregate } from './same-expression.js';

export const AGGREGATE_FUNCTIONS = new Set(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']);

// ---------------------------------------------------------------------------
// Child expression traversal (single source of truth for expression structure)
// ---------------------------------------------------------------------------

/** Return all immediate child expressions of a parsed expression. */
function getChildren(expr: ParsedExpression): ParsedExpression[] {
  switch (expr.expression_class) {
    case ExpressionClass.FUNCTION:
      return (expr as FunctionExpression).children;
    case ExpressionClass.COMPARISON: {
      const cmp = expr as ComparisonExpression;
      return [cmp.left, cmp.right];
    }
    case ExpressionClass.CONJUNCTION:
      return (expr as ConjunctionExpression).children;
    case ExpressionClass.OPERATOR:
      return (expr as OperatorExpression).children;
    case ExpressionClass.CASE: {
      const cs = expr as CaseExpression;
      const children: ParsedExpression[] = [];
      for (const check of cs.case_checks) {
        children.push(check.when_expr, check.then_expr);
      }
      if (cs.else_expr) children.push(cs.else_expr);
      return children;
    }
    case ExpressionClass.BETWEEN: {
      const bt = expr as BetweenExpression;
      return [bt.input, bt.lower, bt.upper];
    }
    case ExpressionClass.CAST:
      return [(expr as CastExpression).child];
    default:
      return [];
  }
}

// ---------------------------------------------------------------------------
// Detection & validation
// ---------------------------------------------------------------------------

export function detectAggregates(exprs: ParsedExpression[]): boolean {
  return exprs.some(exprContainsAggregate);
}

export function exprContainsAggregate(expr: ParsedExpression): boolean {
  if (expr.expression_class === ExpressionClass.FUNCTION) {
    if (AGGREGATE_FUNCTIONS.has((expr as FunctionExpression).function_name.toUpperCase())) {
      return true;
    }
  }
  return getChildren(expr).some(exprContainsAggregate);
}

export function checkNoAggregates(expr: ParsedExpression, context = 'WHERE clause'): void {
  if (exprContainsAggregate(expr)) {
    throw new BindError(`Aggregate function not allowed in ${context}`);
  }
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

export function extractAggregates(
  ctx: BindContext,
  exprs: ParsedExpression[],
  scope: BindScope,
): BoundAggregateExpression[] {
  const result: BoundAggregateExpression[] = [];
  for (const expr of exprs) collectAggregates(ctx, expr, scope, result);
  return result;
}

export function extractAggregatesFromExpr(
  ctx: BindContext,
  expr: ParsedExpression,
  scope: BindScope,
): BoundAggregateExpression[] {
  const result: BoundAggregateExpression[] = [];
  collectAggregates(ctx, expr, scope, result);
  return result;
}

function collectAggregates(
  ctx: BindContext,
  expr: ParsedExpression,
  scope: BindScope,
  result: BoundAggregateExpression[],
): void {
  if (expr.expression_class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    if (AGGREGATE_FUNCTIONS.has(fn.function_name.toUpperCase())) {
      const bound = bindAggregate(ctx, fn, scope);
      if (!result.some((a) => sameAggregate(a, bound))) {
        bound.aggregateIndex = result.length;
        result.push(bound);
      }
      return;
    }
  }
  for (const child of getChildren(expr)) {
    collectAggregates(ctx, child, scope, result);
  }
}

// ---------------------------------------------------------------------------
// Binding
// ---------------------------------------------------------------------------

export function bindAggregate(
  ctx: BindContext,
  expr: FunctionExpression,
  scope: BindScope,
): BoundAggregateExpression {
  const name = expr.function_name.toUpperCase() as AggregateFunctionName;

  if (!expr.is_star) {
    for (const child of expr.children) {
      if (exprContainsAggregate(child)) {
        throw new BindError('Nested aggregate functions are not allowed');
      }
    }
  }

  const children = expr.is_star
    ? []
    : expr.children.map((c) => bindExpression(ctx, c, scope));

  let returnType: LogicalType;
  switch (name) {
    case 'COUNT': returnType = 'INTEGER'; break;
    case 'SUM':
    case 'AVG': returnType = 'REAL'; break;
    case 'MIN':
    case 'MAX': returnType = children.length > 0 ? children[0].returnType : 'ANY'; break;
    default: returnType = 'ANY';
  }

  return {
    expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
    functionName: name,
    children,
    distinct: expr.distinct,
    isStar: expr.is_star,
    aggregateIndex: 0,
    returnType,
  };
}
