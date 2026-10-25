import type { FunctionCall, physical } from "../sql/plan/index.js";
import { fixedValue } from "./parameter.js";
import type { CompiledExpression, Compiler } from "./scope.js";

/**
 * A function call: the binder resolved the function; here it is told its
 * argument types and its constant arguments once — LIKE builds its matcher
 * then — and called per row.
 */
export function compileFunction(
  expression: FunctionCall<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const { run } = compiler.scope;
  const implementation = expression.function.implement({
    argumentTypes: expression.arguments.map((argument) => argument.type),
    constants: expression.arguments.map((argument) =>
      fixedValue(argument, run),
    ),
  });
  const operands = expression.arguments.map((argument) =>
    compiler.compile(argument),
  );
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
