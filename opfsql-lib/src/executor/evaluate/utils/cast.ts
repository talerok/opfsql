import { ExecutorError } from "../../errors.js";
import type { Value } from "../../types.js";

/** Cast a value to a target type. */
export function castValue(v: Value, targetType: string): Value {
  if (v === null) return null;
  switch (targetType) {
    case "INTEGER":
    case "BIGINT":
      return castNumber(v, targetType);
    case "REAL":
      return castReal(v);
    case "TEXT":
      return castText(v);
    case "BOOLEAN":
      return castBoolean(v);
    case "JSON":
      return castJson(v);
    default:
      return v;
  }
}

function castNumber(v: Value, targetType: string) {
  if (typeof v === "boolean") return v ? 1 : 0;
  const n = typeof v === "number" ? Math.trunc(v) : parseInt(String(v), 10);
  if (Number.isNaN(n)) {
    throw new ExecutorError(`Cannot cast '${v}' to ${targetType}`);
  }
  return n;
}

function castReal(v: Value) {
  const n = typeof v === "number" ? v : parseFloat(String(v));
  if (Number.isNaN(n)) {
    throw new ExecutorError(`Cannot cast '${v}' to REAL`);
  }
  return n;
}

export function castText(v: Value): string {
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "object" && v !== null) return JSON.stringify(v);
  return String(v);
}

export function castJson(v: Value) {
  if (typeof v === "object" && v !== null) return v;
  if (typeof v === "string") {
    try { return JSON.parse(v); }
    catch { throw new ExecutorError(`Cannot cast '${v}' to JSON: invalid JSON`); }
  }
  // Numbers and booleans are valid JSON scalars
  if (typeof v === "number" || typeof v === "boolean") return v;
  throw new ExecutorError(`Cannot cast '${typeof v}' value to JSON`);
}

function castBoolean(v: Value) {
  if (typeof v === "boolean") return v;
  const s = String(v).toLowerCase();
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  throw new ExecutorError(`Cannot cast '${v}' to BOOLEAN`);
}
