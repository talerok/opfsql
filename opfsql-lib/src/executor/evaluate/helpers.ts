import type { ComparisonType } from "../../binder/types.js";
import { ExecutorError } from "../errors.js";
import type { Value } from "../types.js";

/** SQL truthiness: only boolean true passes filters */
export function isTruthy(v: Value): boolean {
  return v === true;
}

/** Compare two non-null values. Numbers compare numerically, else as strings. */
export function compareValues(a: Value, b: Value): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  if (typeof a === "boolean" && typeof b === "boolean")
    return (a ? 1 : 0) - (b ? 1 : 0);
  // If one side is a number and the other a numeric string, compare numerically
  if (typeof a === "number" && typeof b === "string") {
    const nb = Number(b);
    if (!Number.isNaN(nb)) return a - nb;
  }
  if (typeof b === "number" && typeof a === "string") {
    const na = Number(a);
    if (!Number.isNaN(na)) return na - b;
  }
  const sa = String(a);
  const sb = String(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

/** Apply comparison operator to two values with NULL propagation. */
export function applyComparison(
  left: Value,
  right: Value,
  comparisonType: ComparisonType,
): Value {
  if (left === null || right === null) return null;
  const cmp = compareValues(left, right);
  switch (comparisonType) {
    case "EQUAL":
      return cmp === 0;
    case "NOT_EQUAL":
      return cmp !== 0;
    case "LESS":
      return cmp < 0;
    case "GREATER":
      return cmp > 0;
    case "LESS_EQUAL":
      return cmp <= 0;
    case "GREATER_EQUAL":
      return cmp >= 0;
  }
}

/** Convert SQL LIKE pattern to RegExp. */
export function likeToRegex(pattern: string): RegExp {
  let re = "^";
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (ch === "%") {
      re += ".*";
    } else if (ch === "_") {
      re += ".";
    } else if (/[.*+?^${}()|[\]\\]/.test(ch)) {
      re += "\\" + ch;
    } else {
      re += ch;
    }
  }
  re += "$";
  return new RegExp(re);
}

/** Serialize a value to a string for hashing (joins, distinct, group by). */
export function serializeValue(v: Value): string {
  if (v === null) return "\0NULL\0";
  if (typeof v === "string") return `s:${v}`;
  if (typeof v === "number") return `n:${v}`;
  return `b:${v}`;
}

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
    case "BOOLEAN": {
      return castBolean(v);
    }
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

function castText(v: Value) {
  if (typeof v === "boolean") return v ? "true" : "false";
  return String(v);
}

function castBolean(v: Value) {
  if (typeof v === "boolean") return v;
  const s = String(v).toLowerCase();
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  throw new ExecutorError(`Cannot cast '${v}' to BOOLEAN`);
}
