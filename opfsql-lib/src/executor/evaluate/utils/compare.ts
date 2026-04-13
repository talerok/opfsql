import type { ComparisonType } from "../../../binder/types.js";
import type { Value } from "../../types.js";

/** SQL truthiness: only boolean true passes filters */
export function isTruthy(v: Value): boolean {
  return v === true;
}

/** Compare two non-null values. Numbers compare numerically, else as strings. */
export function compareValues(a: Value, b: Value): number {
  // JSON object comparison via stringify (lexicographic for ordering)
  if (typeof a === "object" && a !== null && typeof b === "object" && b !== null) {
    const sa = JSON.stringify(a);
    const sb = JSON.stringify(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  }
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
  const sa = typeof a === "object" && a !== null ? JSON.stringify(a) : String(a);
  const sb = typeof b === "object" && b !== null ? JSON.stringify(b) : String(b);
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
