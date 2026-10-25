import { childExpressions } from "../../parser/index.js";
import type {
  FunctionExpression,
  ParsedExpression,
} from "../../parser/types.js";
import {
  aggregateFunction,
  isAggregateFunction,
  argumentTypes,
  resolveCall,
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
function getChildren(expr: ParsedExpression): readonly ParsedExpression[] {
  return childExpressions(expr);
}

// ---------------------------------------------------------------------------
// Detection & validation
// ---------------------------------------------------------------------------

export function detectAggregates(exprs: readonly ParsedExpression[]): boolean {
  return exprs.some(exprContainsAggregate);
}

export function exprContainsAggregate(expr: ParsedExpression): boolean {
  if (expr.expressionClass === ExpressionClass.FUNCTION) {
    if (isAggregateFunction((expr as FunctionExpression).functionName)) {
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
  exprs: readonly ParsedExpression[],
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
  if (expr.expressionClass === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    if (isAggregateFunction(fn.functionName)) {
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
  const name = expr.functionName.toUpperCase() as AggregateFunctionName;

  if (!expr.isStar) {
    for (const child of expr.children) {
      if (exprContainsAggregate(child)) {
        throw new BindError("Nested aggregate functions are not allowed");
      }
    }
  }

  const children = expr.isStar
    ? []
    : expr.children.map((c) => bindExpression(ctx, c, scope));

  // The registry checks the arguments: SUM over TEXT is a bind error.
  const definition = aggregateFunction(name);
  const returnType = resolveCall(definition, argumentTypes(children));

  return {
    expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
    functionName: name,
    function: definition,
    children,
    distinct: expr.isDistinct,
    isStar: expr.isStar,
    aggregateIndex: 0,
    returnType,
  };
}
