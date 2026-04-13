import type { BoundExpression } from "../../binder/types.js";
import { BoundExpressionClass } from "../../binder/types.js";
import { evalColumnRef } from "../../executor/evaluate/column-ref.js";
import { ExecutorError } from "../errors.js";
import type { Resolver } from "../resolve.js";
import type { Tuple, Value } from "../types.js";
import { evalAggregate } from "./aggregate.js";
import { evalBetween } from "./between.js";
import { evalCase } from "./case.js";
import { evalCast } from "./cast.js";
import { evalComparison } from "./comparison.js";
import { evalConjunction } from "./conjunction.js";
import type { SyncEvalContext } from "./context.js";
import { evalFunction } from "./function.js";
import { evalJsonAccess } from "./json-access.js";
import { evalOperator } from "./operator.js";
import { evalSubquery } from "./subquery.js";

export { type SyncEvalContext } from "./context.js";

export function evaluateExpression(
  expr: BoundExpression,
  tuple: Tuple,
  resolver: Resolver,
  ctx: SyncEvalContext,
): Value {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF:
      return evalColumnRef(expr, tuple, resolver, ctx as never);
    case BoundExpressionClass.BOUND_CONSTANT:
      return expr.value;
    case BoundExpressionClass.BOUND_PARAMETER: {
      const params = ctx.params;
      if (!params)
        throw new ExecutorError(
          "No parameters provided for parameterized query",
        );
      if (expr.index >= params.length) {
        throw new ExecutorError(
          `Parameter $${expr.index + 1} not provided (${
            params.length
          } params given)`,
        );
      }
      return params[expr.index];
    }
    case BoundExpressionClass.BOUND_COMPARISON:
      return evalComparison(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CONJUNCTION:
      return evalConjunction(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_OPERATOR:
      return evalOperator(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_BETWEEN:
      return evalBetween(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_FUNCTION:
      return evalFunction(expr, tuple, resolver, ctx as never);
    case BoundExpressionClass.BOUND_AGGREGATE:
      return evalAggregate(expr, tuple, resolver);
    case BoundExpressionClass.BOUND_SUBQUERY:
      return evalSubquery(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CASE:
      return evalCase(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_CAST:
      return evalCast(expr, tuple, resolver, ctx);
    case BoundExpressionClass.BOUND_JSON_ACCESS:
      return evalJsonAccess(expr, tuple, resolver, ctx);
  }
}
