import { evaluateConstant } from "../../executor/evaluate/index.js";
import type { Value } from "../../values/index.js";
import type {
  BoundConjunctionExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundOperatorExpression,
  LogicalOperator,
  LogicalType,
} from "../binder/types.js";
import { BoundExpressionClass } from "../binder/types.js";
import {
  flipComparison,
  isComparison,
  isConjunction,
  isConstant,
  isFoldable,
  isOperator,
  makeBoolConstant,
  makeNullConstant,
  mapOperatorExpressions,
} from "./utils/index.js";

// ============================================================================
// Expression Rewriter — simplifications applied bottom-up. Every rule must
// keep the expression's value for every row, NULLs included: constants are
// folded by the executor's own compiler, and logic follows three-valued
// rules (NULL AND TRUE is NULL, not FALSE).
// ============================================================================

export function rewriteExpressions(plan: LogicalOperator): LogicalOperator {
  mapOperatorExpressions(plan, rewriteExpression);
  return plan;
}

function rewriteExpression(expression: BoundExpression): BoundExpression {
  let result = foldConstant(expression);
  result = simplifyComparison(result);
  result = simplifyConjunction(result);
  result = simplifyArithmetic(result);
  result = moveConstants(result);
  return result;
}

// ============================================================================
// Rule 1: constant folding, by evaluating with the query compiler
// ============================================================================

function foldConstant(expression: BoundExpression): BoundExpression {
  if (isConstant(expression) || !isFoldable(expression)) return expression;
  const value = evaluateConstant(expression);
  // An expression that fails is left to fail at run time, if it is reached.
  return value === undefined
    ? expression
    : constant(value, expression.returnType);
}

function constant(
  value: Value,
  returnType: LogicalType,
): BoundConstantExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType,
  };
}

// ============================================================================
// Rule 2: a comparison with NULL is NULL
// ============================================================================

function simplifyComparison(expression: BoundExpression): BoundExpression {
  if (!isComparison(expression)) return expression;
  return isNullConstant(expression.left) || isNullConstant(expression.right)
    ? makeNullConstant("BOOLEAN")
    : expression;
}

function isNullConstant(expression: BoundExpression): boolean {
  return isConstant(expression) && expression.value === null;
}

// ============================================================================
// Rule 3: AND / OR with constant operands. TRUE is AND's identity and
// decides OR; FALSE the reverse. A NULL operand stays: it is neither.
// ============================================================================

function simplifyConjunction(expression: BoundExpression): BoundExpression {
  if (!isConjunction(expression)) return expression;
  // AND's identity is TRUE, OR's is FALSE; the other value decides.
  const isAnd = expression.conjunctionType === "AND";
  const remaining: BoundExpression[] = [];
  for (const child of expression.children) {
    if (isConstant(child) && child.value === !isAnd) {
      return makeBoolConstant(!isAnd);
    }
    if (!isConstant(child) || child.value !== isAnd) remaining.push(child);
  }
  if (remaining.length === expression.children.length) return expression;
  if (remaining.length === 0) return makeBoolConstant(isAnd);
  const [only] = remaining;
  return remaining.length === 1
    ? only
    : ({
        ...expression,
        children: remaining,
      } satisfies BoundConjunctionExpression);
}

// ============================================================================
// Rule 4: arithmetic identities, only on operands statically INTEGER or
// REAL — on anything else they could hide an error or change the type.
// x * 0 is not 0: Infinity * 0 is NULL.
// ============================================================================

function simplifyArithmetic(expression: BoundExpression): BoundExpression {
  if (!isOperator(expression)) return expression;
  // Only binary operators read `right`.
  const [left, right] = expression.children;
  switch (expression.operatorType) {
    case "ADD": {
      return (
        identity(expression, left, right, 0) ??
        identity(expression, right, left, 0) ??
        expression
      );
    }
    case "SUBTRACT": {
      return identity(expression, left, right, 0) ?? expression;
    }
    case "MULTIPLY": {
      return (
        identity(expression, left, right, 1) ??
        identity(expression, right, left, 1) ??
        expression
      );
    }
    case "DIVIDE": {
      // Not x / 0 to NULL: x is still evaluated, and may fail.
      return identity(expression, left, right, 1) ?? expression;
    }
    case "MOD":
    case "NOT":
    case "IS_NULL":
    case "IS_NOT_NULL":
    case "NEGATE":
    case "IN":
    case "NOT_IN":
    case "CONCAT": {
      return expression;
    }
  }
}

/** `operand` if `other` is the constant `neutral` and types allow it. */
function identity(
  expression: BoundOperatorExpression,
  operand: BoundExpression,
  other: BoundExpression,
  neutral: number,
): BoundExpression | null {
  return isConstant(other) &&
    other.value === neutral &&
    isNumeric(operand.returnType) &&
    operand.returnType === expression.returnType
    ? operand
    : null;
}

const NUMERIC_TYPES: ReadonlySet<LogicalType> = new Set<LogicalType>([
  "INTEGER",
  "BIGINT",
  "REAL",
]);

function isNumeric(type: LogicalType): boolean {
  return NUMERIC_TYPES.has(type);
}

// ============================================================================
// Rule 5: constants to the right — `C op x` is `x op' C`. Not `(x + C1) op
// C2` to `x op (C2 - C1)`: x + C1 can overflow where x cannot.
// ============================================================================

function moveConstants(expression: BoundExpression): BoundExpression {
  if (!isComparison(expression)) return expression;
  return isConstant(expression.left) && !isConstant(expression.right)
    ? {
        ...expression,
        left: expression.right,
        right: expression.left,
        comparisonType: flipComparison(expression.comparisonType),
      }
    : expression;
}
