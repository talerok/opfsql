// Builders for the expression nodes more than one grammar rule makes.

import {
  ExpressionClass,
  ExpressionType,
  type ComparisonExpression,
  type ComparisonType,
  type ConstantExpression,
  type Literal,
  type BinaryOperatorExpression,
  type BinaryOperatorType,
  type InListExpression,
  type InListType,
  type ParsedExpression,
  type SelectStatement,
  type SubqueryExpression,
  type UnaryOperatorExpression,
  type UnaryOperatorType,
} from "./types.js";

export function constantNode(literal: Literal): ConstantExpression {
  return { expressionClass: ExpressionClass.CONSTANT, ...literal };
}

export function unaryNode(
  type: UnaryOperatorType,
  operand: ParsedExpression,
): UnaryOperatorExpression {
  return { expressionClass: ExpressionClass.OPERATOR, type, operand };
}

export function binaryNode(
  type: BinaryOperatorType,
  left: ParsedExpression,
  right: ParsedExpression,
): BinaryOperatorExpression {
  return { expressionClass: ExpressionClass.OPERATOR, type, left, right };
}

export function inListNode(
  type: InListType,
  needle: ParsedExpression,
  list: readonly ParsedExpression[],
): InListExpression {
  return { expressionClass: ExpressionClass.OPERATOR, type, needle, list };
}

export function notNode(operand: ParsedExpression): UnaryOperatorExpression {
  return unaryNode(ExpressionType.OPERATOR_NOT, operand);
}

export function comparisonNode(
  type: ComparisonType | ExpressionType.COMPARE_LIKE,
  left: ParsedExpression,
  right: ParsedExpression,
): ComparisonExpression {
  return {
    expressionClass: ExpressionClass.COMPARISON,
    type,
    left,
    right,
  };
}

/** A subquery as a value or an EXISTS test. */
export function subqueryNode(
  subqueryType: "SCALAR" | "EXISTS" | "NOT_EXISTS",
  subquery: SelectStatement,
): SubqueryExpression {
  return {
    expressionClass: ExpressionClass.SUBQUERY,
    subqueryType,
    subquery,
  };
}

/** `child <comparisonType> ANY|ALL (subquery)`. */
export function quantifiedNode(
  child: ParsedExpression,
  comparisonType: ComparisonType,
  subqueryType: "ANY" | "ALL",
  subquery: SelectStatement,
): SubqueryExpression {
  return {
    expressionClass: ExpressionClass.SUBQUERY,
    subqueryType,
    subquery,
    child,
    comparisonType,
  };
}
