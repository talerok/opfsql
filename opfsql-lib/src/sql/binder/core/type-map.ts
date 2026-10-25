import { SemanticError } from "../../../errors.js";
import { typeFromName } from "../../../values/index.js";
import { ExpressionType } from "../../parser/types.js";
import type { LogicalType } from "../../../store/types.js";
import type { ComparisonType, OperatorType } from "../types.js";
import { BindError } from "./errors.js";

/**
 * The type a declared type name stands for (docs/semantics.md, Column
 * types); no declared type is ANY. The store still tells BIGINT from
 * INTEGER, so BIGINT keeps its own type here.
 */
export function mapTypeName(declared: string | null): LogicalType {
  if (declared === null) return "ANY";
  const type = typeFromName(declared);
  if (type === null) throw new SemanticError(`unknown type ${declared}`);
  return type === "INTEGER" && /^\s*bigint\b/i.test(declared) ? "BIGINT" : type;
}

export function mapComparisonType(exprType: ExpressionType): ComparisonType {
  switch (exprType) {
    case ExpressionType.COMPARE_EQUAL:
      return "EQUAL";
    case ExpressionType.COMPARE_NOTEQUAL:
      return "NOT_EQUAL";
    case ExpressionType.COMPARE_LESSTHAN:
      return "LESS";
    case ExpressionType.COMPARE_GREATERTHAN:
      return "GREATER";
    case ExpressionType.COMPARE_LESSTHANOREQUALTO:
      return "LESS_EQUAL";
    case ExpressionType.COMPARE_GREATERTHANOREQUALTO:
      return "GREATER_EQUAL";
    default:
      throw new BindError(`Unknown comparison type: ${exprType}`);
  }
}

export function mapOperatorType(exprType: ExpressionType): OperatorType {
  switch (exprType) {
    case ExpressionType.OPERATOR_NOT:
      return "NOT";
    case ExpressionType.OPERATOR_IS_NULL:
      return "IS_NULL";
    case ExpressionType.OPERATOR_IS_NOT_NULL:
      return "IS_NOT_NULL";
    case ExpressionType.OPERATOR_IN:
      return "IN";
    case ExpressionType.OPERATOR_NOT_IN:
      return "NOT_IN";
    case ExpressionType.OPERATOR_NEGATE:
      return "NEGATE";
    case ExpressionType.OPERATOR_ADD:
      return "ADD";
    case ExpressionType.OPERATOR_SUBTRACT:
      return "SUBTRACT";
    case ExpressionType.OPERATOR_MULTIPLY:
      return "MULTIPLY";
    case ExpressionType.OPERATOR_DIVIDE:
      return "DIVIDE";
    case ExpressionType.OPERATOR_MOD:
      return "MOD";
    case ExpressionType.OPERATOR_CONCAT:
      return "CONCAT";
    default:
      throw new BindError(`Unknown operator type: ${exprType}`);
  }
}
