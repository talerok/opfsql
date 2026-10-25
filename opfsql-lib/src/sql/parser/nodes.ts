// Builders for the syntax nodes more than one grammar file makes.

import {
  ExpressionClass,
  ExpressionType,
  type ComparisonExpression,
  type ConstantExpression,
  type LiteralType,
  type LiteralValue,
  type OperatorExpression,
  type ParsedExpression,
} from "./types.js";

export function constantNode(
  type: LiteralType,
  value: LiteralValue,
): ConstantExpression {
  return {
    expressionClass: ExpressionClass.CONSTANT,
    alias: null,
    type,
    value,
  };
}

export function operatorNode(
  type: ExpressionType,
  children: readonly ParsedExpression[],
): OperatorExpression {
  return {
    expressionClass: ExpressionClass.OPERATOR,
    alias: null,
    type,
    children,
  };
}

export function notNode(child: ParsedExpression): OperatorExpression {
  return operatorNode(ExpressionType.OPERATOR_NOT, [child]);
}

export function comparisonNode(
  type: ExpressionType,
  left: ParsedExpression,
  right: ParsedExpression,
): ComparisonExpression {
  return {
    expressionClass: ExpressionClass.COMPARISON,
    alias: null,
    type,
    left,
    right,
  };
}
