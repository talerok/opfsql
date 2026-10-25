import { compareIndexKeys } from "./compare.js";
import type { IndexKey, IndexKeyValue, RangeOptions } from "./types.js";

export interface SearchPredicate {
  columnPosition: number;
  comparisonType: "EQUAL" | "LESS" | "GREATER" | "LESS_EQUAL" | "GREATER_EQUAL";
  value: IndexKeyValue;
}

function applyRangePred(
  bounds: RangeOptions,
  key: IndexKey,
  type: SearchPredicate["comparisonType"],
): void {
  switch (type) {
    case "GREATER":
      bounds.lower = key;
      bounds.lowerInclusive = false;
      break;
    case "GREATER_EQUAL":
      bounds.lower = key;
      bounds.lowerInclusive = true;
      break;
    case "LESS":
      bounds.upper = key;
      bounds.upperInclusive = false;
      break;
    case "LESS_EQUAL":
      bounds.upper = key;
      bounds.upperInclusive = true;
      break;
  }
}

export function computeBounds(predicates: SearchPredicate[]): RangeOptions {
  const bounds: RangeOptions = {
    lowerInclusive: true,
    upperInclusive: true,
  };

  const eqValues: Array<{ pos: number; value: IndexKeyValue }> = [];
  const rangePreds: SearchPredicate[] = [];

  for (const pred of predicates) {
    if (pred.comparisonType === "EQUAL") {
      eqValues.push({ pos: pred.columnPosition, value: pred.value });
    } else {
      rangePreds.push(pred);
    }
  }

  eqValues.sort((a, b) => a.pos - b.pos);
  const prefix = eqValues.map((eq) => eq.value);

  for (const pred of rangePreds) {
    const fullKey = prefix.length > 0 ? [...prefix, pred.value] : [pred.value];
    applyRangePred(bounds, fullKey, pred.comparisonType);
  }

  // Equality prefix fills any unbounded side: all-equality collapses to
  // lower=upper=prefix (prefix-scan via bound-length slicing in
  // SyncBTree.isBelowLower/isAboveUpper); equality + one-sided range anchors
  // the open side to the prefix.
  if (prefix.length > 0) {
    if (!bounds.lower) {
      bounds.lower = prefix;
      bounds.lowerInclusive = true;
    }
    if (!bounds.upper) {
      bounds.upper = prefix;
      bounds.upperInclusive = true;
    }
  }

  return bounds;
}

// Bounds are user keys, compared with the stored key's prefix of their length.

export function isBelowLower(key: IndexKey, opts: RangeOptions): boolean {
  if (!opts.lower) return false;
  const prefix = key.slice(0, opts.lower.length);
  const cmp = compareIndexKeys(prefix, opts.lower);
  return opts.lowerInclusive !== false ? cmp < 0 : cmp <= 0;
}

export function isAboveUpper(key: IndexKey, opts: RangeOptions): boolean {
  if (!opts.upper) return false;
  const prefix = key.slice(0, opts.upper.length);
  const cmp = compareIndexKeys(prefix, opts.upper);
  return opts.upperInclusive !== false ? cmp > 0 : cmp >= 0;
}
