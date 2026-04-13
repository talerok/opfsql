import { ExpressionType, LogicalTypeId } from "../../parser/types.js";
import type { LogicalType } from "../../store/types.js";
import type { ComparisonType, OperatorType } from "../types.js";
import { BindError } from "./errors.js";

export function mapParserType(parserType: { id: string }): LogicalType {
  switch (parserType.id) {
    case LogicalTypeId.INTEGER:
      return "INTEGER";
    case LogicalTypeId.BIGINT:
      return "BIGINT";
    case LogicalTypeId.SMALLINT:
      return "INTEGER";
    case LogicalTypeId.FLOAT:
    case LogicalTypeId.DOUBLE:
      return "REAL";
    case LogicalTypeId.VARCHAR:
      return "TEXT";
    case LogicalTypeId.BLOB:
      return "BLOB";
    case LogicalTypeId.BOOLEAN:
      return "BOOLEAN";
    case LogicalTypeId.JSON:
      return "JSON";
    default:
      return "ANY";
  }
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
