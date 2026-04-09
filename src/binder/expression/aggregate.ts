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

export function detectAggregates(exprs: ParsedExpression[]): boolean {
  return exprs.some(exprContainsAggregate);
}

export function exprContainsAggregate(expr: ParsedExpression): boolean {
  switch (expr.expression_class) {
    case ExpressionClass.FUNCTION: {
      const fn = expr as FunctionExpression;
      if (AGGREGATE_FUNCTIONS.has(fn.function_name.toUpperCase())) return true;
      return fn.children.some(exprContainsAggregate);
    }
    case ExpressionClass.COMPARISON: {
      const cmp = expr as ComparisonExpression;
      return exprContainsAggregate(cmp.left) || exprContainsAggregate(cmp.right);
    }
    case ExpressionClass.CONJUNCTION:
      return (expr as ConjunctionExpression).children.some(exprContainsAggregate);
    case ExpressionClass.OPERATOR:
      return (expr as OperatorExpression).children.some(exprContainsAggregate);
    case ExpressionClass.CASE: {
      const cs = expr as CaseExpression;
      for (const check of cs.case_checks) {
        if (exprContainsAggregate(check.when_expr) || exprContainsAggregate(check.then_expr))
          return true;
      }
      return cs.else_expr ? exprContainsAggregate(cs.else_expr) : false;
    }
    case ExpressionClass.BETWEEN: {
      const bt = expr as BetweenExpression;
      return exprContainsAggregate(bt.input) || exprContainsAggregate(bt.lower) || exprContainsAggregate(bt.upper);
    }
    case ExpressionClass.CAST:
      return exprContainsAggregate((expr as CastExpression).child);
    default:
      return false;
  }
}

export function checkNoAggregates(expr: ParsedExpression, context = 'WHERE clause'): void {
  if (exprContainsAggregate(expr)) {
    throw new BindError(`Aggregate function not allowed in ${context}`);
  }
}

export function extractAggregates(
  ctx: BindContext,
  exprs: ParsedExpression[],
  scope: BindScope,
): BoundAggregateExpression[] {
  const result: BoundAggregateExpression[] = [];
  for (const expr of exprs) {
    collectAggregates(ctx, expr, scope, result);
  }
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
  switch (expr.expression_class) {
    case ExpressionClass.FUNCTION: {
      const fn = expr as FunctionExpression;
      if (AGGREGATE_FUNCTIONS.has(fn.function_name.toUpperCase())) {
        const bound = bindAggregate(ctx, fn, scope);
        if (!result.some((a) => sameAggregate(a, bound))) {
          bound.aggregateIndex = result.length;
          result.push(bound);
        }
        return;
      }
      for (const child of fn.children) {
        collectAggregates(ctx, child, scope, result);
      }
      return;
    }
    case ExpressionClass.COMPARISON: {
      const cmp = expr as ComparisonExpression;
      collectAggregates(ctx, cmp.left, scope, result);
      collectAggregates(ctx, cmp.right, scope, result);
      return;
    }
    case ExpressionClass.CONJUNCTION:
      for (const child of (expr as ConjunctionExpression).children) {
        collectAggregates(ctx, child, scope, result);
      }
      return;
    case ExpressionClass.OPERATOR:
      for (const child of (expr as OperatorExpression).children) {
        collectAggregates(ctx, child, scope, result);
      }
      return;
    case ExpressionClass.CASE: {
      const cs = expr as CaseExpression;
      for (const check of cs.case_checks) {
        collectAggregates(ctx, check.when_expr, scope, result);
        collectAggregates(ctx, check.then_expr, scope, result);
      }
      if (cs.else_expr) collectAggregates(ctx, cs.else_expr, scope, result);
      return;
    }
    case ExpressionClass.BETWEEN: {
      const bt = expr as BetweenExpression;
      collectAggregates(ctx, bt.input, scope, result);
      collectAggregates(ctx, bt.lower, scope, result);
      collectAggregates(ctx, bt.upper, scope, result);
      return;
    }
    case ExpressionClass.CAST:
      collectAggregates(ctx, (expr as CastExpression).child, scope, result);
      return;
  }
}

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
