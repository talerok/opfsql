import { argumentTypes } from "../../functions/index.js";
import type { BoundFunctionExpression } from "../../sql/binder/types.js";
import { fixedValue } from "./parameter.js";
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
    argumentTypes: argumentTypes(expression.children),
    constants: expression.children.map((child) =>
      fixedValue(child, scope.context),
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
