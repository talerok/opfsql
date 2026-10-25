import type {
  BetweenExpression,
  CaseExpression,
  CastExpression,
  ComparisonExpression,
  ConjunctionExpression,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from "../../parser/types.js";
import {
  aggregateFunction,
  isAggregateFunction,
  resolveCall,
  staticType,
} from "../../../functions/index.js";
import { ExpressionClass } from "../../parser/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import type { BindScope } from "../core/scope.js";
import type {
  AggregateFunctionName,
  BoundAggregateExpression,
} from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { bindExpression } from "./index.js";
import { sameAggregate } from "./same-expression.js";

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
    if (isAggregateFunction((expr as FunctionExpression).function_name)) {
      return true;
    }
  }
  return getChildren(expr).some(exprContainsAggregate);
}

export function checkNoAggregates(
  expr: ParsedExpression,
  context = "WHERE clause",
): void {
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
    if (isAggregateFunction(fn.function_name)) {
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
        throw new BindError("Nested aggregate functions are not allowed");
      }
    }
  }

  const children = expr.is_star
    ? []
    : expr.children.map((c) => bindExpression(ctx, c, scope));

  // The registry checks the arguments: SUM over TEXT is a bind error.
  const definition = aggregateFunction(name);
  const returnType = resolveCall(
    definition,
    children.map((child) => staticType(child.returnType)),
  );

  return {
    expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
    functionName: name,
    function: definition,
    children,
    distinct: expr.distinct,
    isStar: expr.is_star,
    aggregateIndex: 0,
    returnType,
  };
}
