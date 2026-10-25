import type {
  BoundExpression,
  BoundOperatorExpression,
  LogicalType,
} from "../../sql/binder/types.js";
import {
  type ArithmeticOperator,
  arithmetic,
  concatenate,
  negate,
  not,
  type NumericMode,
  truthOf,
} from "../../values/index.js";
import { compileIn } from "./in.js";
import type { CompiledExpression, Scope } from "./scope.js";

// ---------------------------------------------------------------------------
// Unary and binary operators: NOT, IS [NOT] NULL, IN, arithmetic, ||.
// ---------------------------------------------------------------------------

export function compileOperator(
  expression: BoundOperatorExpression,
  scope: Scope,
): CompiledExpression {
  const [operand = NULL, right = NULL] = expression.children.map((child) =>
    scope.compile(child),
  );
  switch (expression.operatorType) {
    case "NOT": {
      return (row) => not(truthOf(operand(row)));
    }
    case "IS_NULL": {
      return (row) => operand(row) === null;
    }
    case "IS_NOT_NULL": {
      return (row) => operand(row) !== null;
    }
    case "NEGATE": {
      return (row) => negate(operand(row));
    }
    case "IN":
    case "NOT_IN": {
      const found = compileIn(expression, scope);
      return expression.operatorType === "IN"
        ? found
        : (row) => not(truthOf(found(row)));
    }
    case "CONCAT": {
      return (row) => concatenate(operand(row), right(row));
    }
    case "ADD":
    case "SUBTRACT":
    case "MULTIPLY":
    case "DIVIDE":
    case "MOD": {
      const [left, other] = expression.children;
      const apply = arithmetic(
        arithmeticOperator(expression.operatorType),
        numericMode(left, other),
      );
      return (row) => apply(operand(row), right(row));
    }
  }
}

/** A missing operand: never, for an expression the binder built. */
const NULL: CompiledExpression = () => null;

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

/**
 * INTEGER arithmetic when both operands are statically INTEGER, REAL when
 * either is REAL, and decided per row otherwise (ANY, JSON, NULL).
 */
export function numericMode(
  left: BoundExpression | undefined,
  right: BoundExpression | undefined,
): NumericMode {
  const types = [left?.returnType, right?.returnType];
  if (types.every((type) => isIntegerType(type))) return "integer";
  return types.includes("REAL") ? "real" : "dynamic";
}

function isIntegerType(type: LogicalType | undefined): boolean {
  return type === "INTEGER" || type === "BIGINT";
}
