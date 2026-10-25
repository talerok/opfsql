import { staticType } from "../../functions/index.js";
import {
  type BoundExpression,
  BoundExpressionClass,
  type BoundFunctionExpression,
} from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import type { ExecutionContext } from "./context.js";
import { parameterValue } from "./parameter.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// Function calls: the binder resolved the function; here it is told its
// argument types and constant arguments once, and called per row.
// ---------------------------------------------------------------------------

export function compileFunction(
  expression: BoundFunctionExpression,
  scope: Scope,
): CompiledExpression {
  const implementation = expression.function.implement({
    argumentTypes: expression.children.map((child) =>
      staticType(child.returnType),
    ),
    constants: expression.children.map((child) =>
      constantValue(child, scope.context),
    ),
  });
  const operands = expression.children.map((child) => scope.compile(child));
  switch (implementation.kind) {
    case "eager": {
      const { call } = implementation;
      return (row) => call(operands.map((operand) => operand(row)));
    }
    case "lazy": {
      const { call } = implementation;
      return (row) => call(operands, row);
    }
  }
}

/** The value of an argument that is the same for every row, if it is. */
function constantValue(
  expression: BoundExpression,
  context: ExecutionContext,
): Value | undefined {
  if (expression.expressionClass === BoundExpressionClass.BOUND_CONSTANT) {
    return expression.value;
  }
  return expression.expressionClass === BoundExpressionClass.BOUND_PARAMETER
    ? parameterValue(context, expression.index)
    : undefined;
}
