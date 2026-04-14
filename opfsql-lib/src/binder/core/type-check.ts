import type { LogicalType } from "../../store/types.js";
import type { BoundExpression } from "../types.js";
import { BindError } from "./errors.js";

const NUMERIC_TYPES: ReadonlySet<LogicalType> = new Set<LogicalType>([
  "INTEGER",
  "BIGINT",
  "REAL",
]);

const NON_SCALAR_TYPES: ReadonlySet<LogicalType> = new Set<LogicalType>([
  "BLOB",
  "JSON",
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

  // BLOB can only be compared with BLOB
  if (left === "BLOB" || right === "BLOB") {
    if (left === "BLOB" && right === "BLOB") return "BLOB";
    throw new BindError(`Type mismatch: cannot compare ${left} and ${right}`);
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

  if (left === "BLOB" || right === "BLOB") {
    throw new BindError(`Cannot perform arithmetic on BLOB type`);
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
      return requireTextArg(name, children);
    case "CONCAT":
      return requireTextArgs(name, children);
    case "LENGTH":
      return "INTEGER";
    case "ABS":
      return resolveAbsType(children);
    case "ROUND":
    case "FLOOR":
    case "CEIL":
    case "CEILING":
      return requireNumericArg(name, children);
    case "COALESCE":
      return resolveCoalesceType(children);
    case "TYPEOF":
      return "TEXT";
    default:
      return inferFromFirst(children);
  }
}

function requireTextArg(name: string, children: BoundExpression[]): "TEXT" {
  if (children.length > 0 && NON_SCALAR_TYPES.has(children[0].returnType)) {
    throw new BindError(`Cannot apply ${name} to ${children[0].returnType} type`);
  }
  return "TEXT";
}

function requireTextArgs(name: string, children: BoundExpression[]): "TEXT" {
  const bad = children.find((c) => NON_SCALAR_TYPES.has(c.returnType));
  if (bad) throw new BindError(`Cannot apply ${name} to ${bad.returnType} type`);
  return "TEXT";
}

function requireNumericArg(name: string, children: BoundExpression[]): "REAL" {
  if (children.length > 0 && NON_SCALAR_TYPES.has(children[0].returnType)) {
    throw new BindError(`Cannot apply ${name} to ${children[0].returnType} type`);
  }
  return "REAL";
}

function resolveAbsType(children: BoundExpression[]): LogicalType {
  const t = children.length > 0 ? children[0].returnType : "ANY";
  if (NON_SCALAR_TYPES.has(t)) {
    throw new BindError(`Cannot apply ABS to ${t} type`);
  }
  return t !== "NULL" && t !== "ANY" ? t : "INTEGER";
}

function resolveCoalesceType(children: BoundExpression[]): LogicalType {
  return children.find((c) => c.returnType !== "NULL")?.returnType ?? "ANY";
}

function inferFromFirst(children: BoundExpression[]): LogicalType {
  return children.length > 0 ? children[0].returnType : "ANY";
}
