import { childExpressions } from "../../parser/index.js";
import type {
  FunctionExpression,
  ParsedExpression,
} from "../../parser/types.js";
import { ExpressionClass } from "../../parser/types.js";
import type { LogicalType } from "../../../store/types.js";
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

export const AGGREGATE_FUNCTIONS = new Set([
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
]);

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
    if (
      AGGREGATE_FUNCTIONS.has(
        (expr as FunctionExpression).functionName.toUpperCase(),
      )
    ) {
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
    if (AGGREGATE_FUNCTIONS.has(fn.functionName.toUpperCase())) {
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

  let returnType: LogicalType;
  switch (name) {
    case "COUNT":
      returnType = "INTEGER";
      break;
    case "SUM":
    case "AVG": {
      const childType = children.length > 0 ? children[0].returnType : "ANY";
      if (childType === "BLOB" || childType === "JSON") {
        throw new BindError(`Cannot apply ${name} to ${childType} type`);
      }
      returnType = "REAL";
      break;
    }
    case "MIN":
    case "MAX":
      returnType = children.length > 0 ? children[0].returnType : "ANY";
      break;
    default:
      returnType = "ANY";
  }

  return {
    expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
    functionName: name,
    children,
    distinct: expr.distinct,
    isStar: expr.isStar,
    aggregateIndex: 0,
    returnType,
  };
}
