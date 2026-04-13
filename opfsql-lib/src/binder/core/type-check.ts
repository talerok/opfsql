import type { LogicalType } from "../../store/types.js";
import type { BoundExpression } from "../types.js";
import { BindError } from "./errors.js";

const NUMERIC_TYPES: ReadonlySet<LogicalType> = new Set<LogicalType>([
  "INTEGER",
  "BIGINT",
  "REAL",
]);

export function checkTypeCompatibility(
  left: LogicalType,
  right: LogicalType,
): LogicalType {
  if (left === "NULL" || left === "ANY") return right;
  if (right === "NULL" || right === "ANY") return left;
  if (left === right) return left;

  if (NUMERIC_TYPES.has(left) && NUMERIC_TYPES.has(right)) {
    if (left === "REAL" || right === "REAL") return "REAL";
    if (left === "BIGINT" || right === "BIGINT") return "BIGINT";
    return "INTEGER";
  }

  // JSON path access returns JSON but may contain scalar values at runtime
  if (left === "JSON" || right === "JSON") {
    return "JSON";
  }

  throw new BindError(`Type mismatch: cannot compare ${left} and ${right}`);
}

export function resolveArithmeticType(
  left: LogicalType,
  right: LogicalType,
): LogicalType {
  if (left === "NULL" || left === "ANY") return right;
  if (right === "NULL" || right === "ANY") return left;

  if (left === "JSON" || right === "JSON") {
    throw new BindError(`Cannot perform arithmetic on JSON type`);
  }

  if (NUMERIC_TYPES.has(left) && NUMERIC_TYPES.has(right)) {
    if (left === "REAL" || right === "REAL") return "REAL";
    if (left === "BIGINT" || right === "BIGINT") return "BIGINT";
    return "INTEGER";
  }

  throw new BindError(
    `Type mismatch: cannot perform arithmetic on ${left} and ${right}`,
  );
}

export function resolveScalarFunctionReturnType(
  name: string,
  children: BoundExpression[],
): LogicalType {
  switch (name) {
    case "UPPER":
    case "LOWER":
    case "TRIM":
    case "LTRIM":
    case "RTRIM":
    case "SUBSTR":
    case "SUBSTRING":
    case "REPLACE":
    case "CONCAT":
      return "TEXT";
    case "LENGTH":
      return "INTEGER";
    case "ABS":
      return children.length > 0 ? children[0].returnType : "INTEGER";
    case "COALESCE":
      return children.find((c) => c.returnType !== "NULL")?.returnType ?? "ANY";
    case "TYPEOF":
      return "TEXT";
    default:
      return children.length > 0 ? children[0].returnType : "ANY";
  }
}
