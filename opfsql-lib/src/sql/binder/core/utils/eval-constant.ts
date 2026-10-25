import { NotSupportedError, SemanticError } from "../../../../errors.js";
import {
  childExpressions,
  ExpressionClass,
  type LiteralValue,
  type ParsedExpression,
} from "../../../parser/index.js";
import { BindError } from "../errors.js";

export function evalConstantInt(expr: ParsedExpression): number {
  if (
    expr.expressionClass === ExpressionClass.CONSTANT &&
    typeof expr.value === "number"
  ) {
    if (expr.value < 0) {
      throw new BindError("Expected non-negative integer constant");
    }
    return expr.value;
  }
  throw new BindError("Expected integer constant");
}

/**
 * The value of a column's DEFAULT. It must be constant: a column,
 * parameter or subquery in it is an error. A constant expression that is
 * not a literal, such as `DEFAULT (1 + 2)`, is valid SQL that opfsql does
 * not evaluate yet.
 */
export function evalDefaultValue(
  column: string,
  expression: ParsedExpression,
): LiteralValue {
  if (!isConstant(expression)) {
    throw new SemanticError(`the DEFAULT of column ${column} is not constant`);
  }
  if (expression.expressionClass !== ExpressionClass.CONSTANT) {
    throw new NotSupportedError("a DEFAULT other than a literal");
  }
  return expression.value;
}

const NOT_CONSTANT: ReadonlySet<ExpressionClass> = new Set([
  ExpressionClass.COLUMN_REF,
  ExpressionClass.PARAMETER,
  ExpressionClass.SUBQUERY,
  ExpressionClass.STAR,
]);

function isConstant(expression: ParsedExpression): boolean {
  return (
    !NOT_CONSTANT.has(expression.expressionClass) &&
    childExpressions(expression).every((child) => isConstant(child))
  );
}
