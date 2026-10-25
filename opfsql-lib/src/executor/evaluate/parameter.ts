import { InternalError, MisuseError } from "../../errors.js";
import {
  type BoundExpression,
  BoundExpressionClass,
} from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import type { ExecutionContext } from "./context.js";

/**
 * Parameter `$index + 1`. Statements check their parameter count before
 * running; a context built without that check still gets MISUSE here.
 */
export function parameterValue(
  context: ExecutionContext,
  index: number,
): Value {
  const value = context.parameters[index];
  if (value === undefined) {
    throw new MisuseError(
      `parameter $${String(index + 1)} was not given (${String(context.parameters.length)} given)`,
    );
  }
  return value;
}

/**
 * The value of a constant or a parameter — the same for every row of a
 * statement — or undefined for any other expression.
 */
export function fixedValue(
  expression: BoundExpression,
  context: ExecutionContext,
): Value | undefined {
  switch (expression.expressionClass) {
    case BoundExpressionClass.BOUND_CONSTANT: {
      return expression.value;
    }
    case BoundExpressionClass.BOUND_PARAMETER: {
      return parameterValue(context, expression.index);
    }
    case BoundExpressionClass.BOUND_COLUMN_REF:
    case BoundExpressionClass.BOUND_COMPARISON:
    case BoundExpressionClass.BOUND_CONJUNCTION:
    case BoundExpressionClass.BOUND_OPERATOR:
    case BoundExpressionClass.BOUND_BETWEEN:
    case BoundExpressionClass.BOUND_FUNCTION:
    case BoundExpressionClass.BOUND_AGGREGATE:
    case BoundExpressionClass.BOUND_SUBQUERY:
    case BoundExpressionClass.BOUND_CASE:
    case BoundExpressionClass.BOUND_CAST:
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      return undefined;
    }
  }
}

/**
 * `value`, which the binder guarantees: a missing one is a bug in opfsql,
 * not a NULL to compute with.
 */
export function required<T>(value: T | null | undefined, what: string): T {
  if (value === null || value === undefined) {
    throw new InternalError(`${what} is missing`);
  }
  return value;
}
