import type {
  BoundExpression,
  BoundOperatorExpression,
} from "../../sql/binder/types.js";
import {
  type ArithmeticOperator,
  arithmetic,
  concatenate,
  negate,
  not,
  type NumericMode,
  sqlTypeOf,
  truthOf,
} from "../../values/index.js";
import { compileInList } from "./in.js";
import { required } from "./parameter.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// Unary and binary operators: NOT, IS [NOT] NULL, IN, arithmetic, ||.
// ---------------------------------------------------------------------------

export function compileOperator(
  expression: BoundOperatorExpression,
  scope: Scope,
): CompiledExpression {
  const compileOperand = (): CompiledExpression =>
    scope.compile(operandAt(expression, 0));
  switch (expression.operatorType) {
    case "NOT": {
      const operand = compileOperand();
      return (row) => not(truthOf(operand(row)));
    }
    case "IS_NULL": {
      const operand = compileOperand();
      return (row) => operand(row) === null;
    }
    case "IS_NOT_NULL": {
      const operand = compileOperand();
      return (row) => operand(row) !== null;
    }
    case "NEGATE": {
      const operand = compileOperand();
      return (row) => negate(operand(row));
    }
    case "IN": {
      return compileInList(expression, scope);
    }
    case "NOT_IN": {
      const isIn = compileInList(expression, scope);
      return (row) => not(truthOf(isIn(row)));
    }
    case "CONCAT": {
      const [left, right] = compileBinary(expression, scope);
      return (row) => concatenate(left(row), right(row));
    }
    case "ADD":
    case "SUBTRACT":
    case "MULTIPLY":
    case "DIVIDE":
    case "MOD": {
      const apply = arithmetic(
        arithmeticOperator(expression.operatorType),
        numericMode(operandAt(expression, 0), operandAt(expression, 1)),
      );
      const [left, right] = compileBinary(expression, scope);
      return (row) => apply(left(row), right(row));
    }
  }
}

function arithmeticOperator(
  operator: "ADD" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "MOD",
): ArithmeticOperator {
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

function compileBinary(
  expression: BoundOperatorExpression,
  scope: Scope,
): [CompiledExpression, CompiledExpression] {
  return [
    scope.compile(operandAt(expression, 0)),
    scope.compile(operandAt(expression, 1)),
  ];
}

/** Operand `index` of an operator, which the binder always supplies. */
function operandAt(
  expression: BoundOperatorExpression,
  index: number,
): BoundExpression {
  return required(
    expression.children[index],
    `operand ${String(index + 1)} of ${expression.operatorType}`,
  );
}

/**
 * INTEGER arithmetic when both operands are statically INTEGER, REAL when
 * either is REAL, and decided per row otherwise (ANY, JSON, NULL).
 */
export function numericMode(
  left: BoundExpression,
  right: BoundExpression,
): NumericMode {
  const types = [sqlTypeOf(left.returnType), sqlTypeOf(right.returnType)];
  if (types.every((type) => type === "INTEGER")) return "integer";
  return types.includes("REAL") ? "real" : "dynamic";
}
