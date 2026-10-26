import { DatatypeMismatchError } from "../errors.js";
import { describeValue } from "./convert.js";
import { valueKey } from "./key.js";
import { compareValues } from "./order.js";
import type { Value } from "./value.js";

// ---------------------------------------------------------------------------
// Three-valued logic: a condition is true, false or unknown (NULL).
// ---------------------------------------------------------------------------

export type Truth = boolean | null;

/**
 * A value used as a condition: a BOOLEAN or NULL. Anything else is a
 * DATATYPE_MISMATCH — conditions are never judged by truthiness.
 */
export function truthOf(value: Value): Truth {
  if (value === null || typeof value === "boolean") return value;
  throw new DatatypeMismatchError(
    `${describeValue(value)} is not a BOOLEAN condition`,
  );
}

export function and(a: Truth, b: Truth): Truth {
  if (a === false || b === false) return false;
  if (a === null || b === null) return null;
  return true;
}

export function or(a: Truth, b: Truth): Truth {
  if (a === true || b === true) return true;
  if (a === null || b === null) return null;
  return false;
}

/** AND and OR as folds: where they start, and the answer that ends them. */
const CONJUNCTIONS = {
  and: { combine: and, identity: true, decisive: false },
  or: { combine: or, identity: false, decisive: true },
} as const satisfies Record<
  string,
  {
    combine: (a: Truth, b: Truth) => Truth;
    identity: Truth;
    decisive: Truth;
  }
>;

/**
 * `truths` combined by AND or OR, three-valued, stopping at the first that
 * decides (false for AND, true for OR) — the rest are never computed.
 */
export function foldTruth(
  kind: keyof typeof CONJUNCTIONS,
  truths: Iterable<Truth>,
): Truth {
  const { combine, identity, decisive } = CONJUNCTIONS[kind];
  let result: Truth = identity;
  for (const truth of truths) {
    result = combine(result, truth);
    if (result === decisive) return result;
  }
  return result;
}

export function not(a: Truth): Truth {
  return a === null ? null : !a;
}

/** `a <op> b` as SQL compares: NULL when either side is NULL. */
export function compareSql(a: Value, b: Value): number | null {
  return a === null || b === null ? null : compareValues(a, b);
}

/**
 * `needle IN (items)` for a list known in advance: true if an item equals
 * the needle; otherwise NULL if the needle or any item is NULL; otherwise
 * false, and false for an empty list. The list is hashed once (by
 * valueKey, which agrees with compareValues), so each test is one lookup.
 */
export function membership(items: readonly Value[]): (needle: Value) => Truth {
  if (items.length === 0) return () => false;
  const nonNull = items.filter((item) => item !== null);
  const keys = new Set(nonNull.map((item) => valueKey(item)));
  const hasNull = nonNull.length < items.length;
  return (needle) => {
    if (needle === null) return null;
    if (keys.has(valueKey(needle))) return true;
    return hasNull ? null : false;
  };
}
