import { canonicalJson } from "./order.js";
import type { Value } from "./value.js";

// ---------------------------------------------------------------------------
// Hash keys for grouping, DISTINCT, UNION and hash joins. Two values get the
// same key exactly when compareValues says they are equal, so every operator
// that groups agrees with the ones that sort.
// ---------------------------------------------------------------------------

/** A string that is equal for two values exactly when the values are equal. */
export function valueKey(value: Value): string {
  if (value === null) return "n";
  if (typeof value === "boolean") return value ? "t" : "f";
  // String() is exact and unique per number, and prints -0 as "0".
  if (typeof value === "number") return `#${String(value)}`;
  if (typeof value === "string") return `s${value}`;
  if (value instanceof Uint8Array) return `b${bytesToBinaryString(value)}`;
  return `j${canonicalJson(value)}`;
}

/** A key for a list of values: equal exactly when every pair is equal. */
export function rowKey(values: readonly Value[]): string {
  return values.map((value) => lengthPrefixed(valueKey(value))).join("");
}

/** Length prefixes keep ["ab", "c"] apart from ["a", "bc"]. */
function lengthPrefixed(key: string): string {
  return `${String(key.length)}:${key}`;
}

function bytesToBinaryString(bytes: Uint8Array): string {
  let result = "";
  for (const byte of bytes) result += String.fromCodePoint(byte);
  return result;
}
