import { ExpressionClass, type ParsedExpression } from "./types.js";

/**
 * The expressions directly inside one, in the order written. A subquery's
 * own expressions are not among them: they belong to another query.
 */
export function childExpressions(
  expression: ParsedExpression,
): readonly ParsedExpression[] {
  switch (expression.expressionClass) {
    case ExpressionClass.COLUMN_REF:
    case ExpressionClass.CONSTANT:
    case ExpressionClass.PARAMETER:
    case ExpressionClass.STAR: {
      return [];
    }
    case ExpressionClass.COMPARISON: {
      return [expression.left, expression.right];
    }
    case ExpressionClass.CONJUNCTION:
    case ExpressionClass.OPERATOR:
    case ExpressionClass.FUNCTION: {
      return expression.children;
    }
    case ExpressionClass.BETWEEN: {
      return [expression.input, expression.lower, expression.upper];
    }
    case ExpressionClass.SUBQUERY: {
      return expression.child === null ? [] : [expression.child];
    }
    case ExpressionClass.CASE: {
      return [
        ...(expression.operand === null ? [] : [expression.operand]),
        ...expression.caseChecks.flatMap((check) => [
          check.whenExpression,
          check.thenExpression,
        ]),
        ...(expression.elseExpression === null
          ? []
          : [expression.elseExpression]),
      ];
    }
    case ExpressionClass.CAST: {
      return [expression.child];
    }
  }
}
