// How tightly printed expressions hold together, in the binding powers of
// precedence.ts, so the printer knows where the parser needs parentheses.

import { power } from "../precedence.js";
import {
  ExpressionClass,
  ExpressionType,
  type ComparisonType,
  type OperatorExpression,
  type ParsedExpression,
  type SubqueryExpression,
} from "./types.js";

const RELATIONAL: ReadonlySet<ComparisonType | ExpressionType.COMPARE_LIKE> =
  new Set([
    ExpressionType.COMPARE_LESSTHAN,
    ExpressionType.COMPARE_LESSTHANOREQUALTO,
    ExpressionType.COMPARE_GREATERTHAN,
    ExpressionType.COMPARE_GREATERTHANOREQUALTO,
  ]);

export const OPERATOR_POWER: Readonly<
  Record<OperatorExpression["type"], number>
> = {
  [ExpressionType.OPERATOR_NOT]: power.not,
  [ExpressionType.OPERATOR_IS_NULL]: power.equality,
  [ExpressionType.OPERATOR_IS_NOT_NULL]: power.equality,
  [ExpressionType.OPERATOR_IN]: power.equality,
  [ExpressionType.OPERATOR_NOT_IN]: power.equality,
  [ExpressionType.OPERATOR_ADD]: power.additive,
  [ExpressionType.OPERATOR_SUBTRACT]: power.additive,
  [ExpressionType.OPERATOR_MULTIPLY]: power.multiplicative,
  [ExpressionType.OPERATOR_DIVIDE]: power.multiplicative,
  [ExpressionType.OPERATOR_MOD]: power.multiplicative,
  [ExpressionType.OPERATOR_NEGATE]: power.unary,
  [ExpressionType.OPERATOR_CONCAT]: power.concatenation,
};

/** How tightly `expression`, printed, holds together. */
export function powerOf(expression: ParsedExpression): number {
  switch (expression.expressionClass) {
    case ExpressionClass.COMPARISON: {
      return comparisonPower(expression.type);
    }
    case ExpressionClass.CONJUNCTION: {
      const isAnd = expression.type === ExpressionType.CONJUNCTION_AND;
      return isAnd ? power.and : power.or;
    }
    case ExpressionClass.OPERATOR: {
      return OPERATOR_POWER[expression.type];
    }
    case ExpressionClass.BETWEEN: {
      return power.equality;
    }
    case ExpressionClass.SUBQUERY: {
      return subqueryPower(expression);
    }
    case ExpressionClass.COLUMN_REF:
    case ExpressionClass.CONSTANT:
    case ExpressionClass.PARAMETER:
    case ExpressionClass.FUNCTION:
    case ExpressionClass.CASE:
    case ExpressionClass.CAST:
    case ExpressionClass.STAR: {
      return power.primary;
    }
  }
}

export function comparisonPower(
  type: ComparisonType | ExpressionType.COMPARE_LIKE,
): number {
  return RELATIONAL.has(type) ? power.relational : power.equality;
}

function subqueryPower(expression: SubqueryExpression): number {
  switch (expression.subqueryType) {
    case "SCALAR":
    case "EXISTS": {
      return power.primary;
    }
    case "NOT_EXISTS": {
      // It starts with NOT, whose operand would take what follows.
      return power.not;
    }
    case "ANY":
    case "ALL": {
      return comparisonPower(expression.comparisonType);
    }
  }
}
