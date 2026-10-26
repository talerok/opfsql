// Builders for the expression nodes more than one grammar rule makes.

import {
  ExpressionClass,
  ExpressionType,
  type ComparisonExpression,
  type ComparisonType,
  type ConstantExpression,
  type Literal,
  type OperatorExpression,
  type OperatorType,
  type ParsedExpression,
  type SelectStatement,
  type SubqueryExpression,
} from "./types.js";

export function constantNode(literal: Literal): ConstantExpression {
  return { expressionClass: ExpressionClass.CONSTANT, ...literal };
}

export function operatorNode(
  type: OperatorType,
  children: readonly ParsedExpression[],
): OperatorExpression {
  return {
    expressionClass: ExpressionClass.OPERATOR,
    type,
    children,
  };
}

export function notNode(child: ParsedExpression): OperatorExpression {
  return operatorNode(ExpressionType.OPERATOR_NOT, [child]);
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
