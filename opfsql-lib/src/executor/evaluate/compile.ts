import { InternalError } from "../../errors.js";
import {
  type BoundAggregateExpression,
  type BoundCastExpression,
  type BoundExpression,
  BoundExpressionClass,
} from "../../sql/binder/types.js";
import { castValue, sqlTypeOf, truthOf } from "../../values/index.js";
import type { Resolver } from "../resolve.js";
import type { Tuple } from "../types.js";
import { compileCase } from "./case.js";
import { compileColumn, compileJsonAccess } from "./column.js";
import type { ExecutionContext } from "./context.js";
import { compileFunction } from "./function.js";
import {
  compileBetween,
  compileComparison,
  compileConjunction,
} from "./logic.js";
import { compileOperator } from "./operator.js";
import { parameterValue } from "./parameter.js";
import type { CompiledExpression, Scope } from "./scope.js";
import { compileSubquery } from "./subquery.js";

// ---------------------------------------------------------------------------
// The expression compiler: the one way opfsql evaluates an expression.
// The tree is walked once, when a plan node is built; what runs per row is
// a closure that already knows where its columns are and what its
// operators and functions do. Closures, never generated source: a strict
// Content Security Policy forbids `new Function`.
// ---------------------------------------------------------------------------

/** Compiles `expression` for rows laid out as `resolver` describes. */
export function compileExpression(
  expression: BoundExpression,
  resolver: Resolver,
  context: ExecutionContext,
): CompiledExpression {
  const scope: Scope = {
    resolver,
    context,
    compile: (child) => compileNode(child, scope),
  };
  return scope.compile(expression);
}

/**
 * Compiles a condition (WHERE, ON, HAVING): true only when the condition
 * is TRUE — not when it is FALSE or NULL.
 */
export function compilePredicate(
  expression: BoundExpression,
  resolver: Resolver,
  context: ExecutionContext,
): (row: Tuple) => boolean {
  const condition = compileExpression(expression, resolver, context);
  return (row) => truthOf(condition(row)) === true;
}

function compileNode(
  expression: BoundExpression,
  scope: Scope,
): CompiledExpression {
  switch (expression.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      return compileColumn(expression.binding, scope);
    }
    case BoundExpressionClass.BOUND_CONSTANT: {
      const { value } = expression;
      return () => value;
    }
    case BoundExpressionClass.BOUND_PARAMETER: {
      const value = parameterValue(scope.context, expression.index);
      return () => value;
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      return compileComparison(expression, scope);
    }
    case BoundExpressionClass.BOUND_CONJUNCTION: {
      return compileConjunction(expression, scope);
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      return compileOperator(expression, scope);
    }
    case BoundExpressionClass.BOUND_BETWEEN: {
      return compileBetween(expression, scope);
    }
    case BoundExpressionClass.BOUND_FUNCTION: {
      return compileFunction(expression, scope);
    }
    case BoundExpressionClass.BOUND_AGGREGATE: {
      return compileAggregateResult(expression, scope);
    }
    case BoundExpressionClass.BOUND_SUBQUERY: {
      return compileSubquery(expression, scope);
    }
    case BoundExpressionClass.BOUND_CASE: {
      return compileCase(expression, scope);
    }
    case BoundExpressionClass.BOUND_CAST: {
      return compileCast(expression, scope);
    }
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      return compileJsonAccess(expression, scope);
    }
  }
}

/** An aggregate above its aggregation: a column of the aggregate's output. */
function compileAggregateResult(
  expression: BoundAggregateExpression,
  scope: Scope,
): CompiledExpression {
  if (expression.binding === undefined) {
    throw new InternalError(
      `aggregate ${expression.functionName} has no output column`,
    );
  }
  return compileColumn(expression.binding, scope);
}

function compileCast(
  expression: BoundCastExpression,
  scope: Scope,
): CompiledExpression {
  const child = scope.compile(expression.child);
  const type = sqlTypeOf(expression.castType);
  return (row) => castValue(child(row), type);
}
