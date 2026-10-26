import type { StaticType } from "../functions/index.js";
import type {
  Arithmetic,
  ArithmeticOperator,
  Concatenate,
  Negate,
  physical,
} from "../sql/plan/index.js";
import {
  arithmetic,
  concatenate,
  negate,
  type NumericMode,
  type ArithmeticOperator as ValueOperator,
} from "../values/index.js";
import type { CompiledExpression, Compiler } from "./scope.js";

// ---------------------------------------------------------------------------
// Arithmetic, negation and `||`, as values defines them: NULL in, NULL out;
// INTEGER results outside the safe range fail with INTEGER_OVERFLOW.
// ---------------------------------------------------------------------------

/** The expressions that compute a number or a text from their operands. */
export type Operation = Extract<
  physical.Expression,
  { readonly kind: "arithmetic" | "negate" | "concatenate" }
>;

export function compileOperation(
  expression: Operation,
  compiler: Compiler,
): CompiledExpression {
  switch (expression.kind) {
    case "arithmetic": {
      return compileArithmetic(expression, compiler);
    }
    case "negate": {
      return compileNegate(expression, compiler);
    }
    case "concatenate": {
      return compileConcatenate(expression, compiler);
    }
  }
}

function compileArithmetic(
  expression: Arithmetic<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const { left, right } = expression;
  const apply = arithmetic(
    valueOperator(expression.operator),
    numericMode(expression.type),
  );
  const leftValue = compiler.compile(left);
  const rightValue = compiler.compile(right);
  return (row) => apply(leftValue(row), rightValue(row));
}

function compileNegate(
  expression: Negate<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const operand = compiler.compile(expression.operand);
  return (row) => negate(operand(row));
}

function compileConcatenate(
  expression: Concatenate<physical.Plan>,
  compiler: Compiler,
): CompiledExpression {
  const left = compiler.compile(expression.left);
  const right = compiler.compile(expression.right);
  return (row) => concatenate(left(row), right(row));
}

/**
 * Arithmetic as the binder typed its result: INTEGER for an INTEGER, REAL
 * — exact division — for a REAL, and decided per row otherwise (ANY, NULL).
 */
function numericMode(type: StaticType): NumericMode {
  if (type === "INTEGER") return "integer";
  return type === "REAL" ? "real" : "dynamic";
}

function valueOperator(operator: ArithmeticOperator): ValueOperator {
  switch (operator) {
    case "ADD": {
      return "add";
    }
    case "SUBTRACT": {
      return "subtract";
    }
    case "MULTIPLY": {
      return "multiply";
    }
    case "DIVIDE": {
      return "divide";
    }
    case "MOD": {
      return "remainder";
    }
  }
}
