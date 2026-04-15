import type { IndexKey, IndexKeyValue } from "./types.js";

export interface SearchPredicate {
  columnPosition: number;
  comparisonType:
    | "EQUAL"
    | "LESS"
    | "GREATER"
    | "LESS_EQUAL"
    | "GREATER_EQUAL";
  value: string | number | boolean | null;
}

export interface ScanBounds {
  lowerKey: IndexKey | null;
  lowerInclusive: boolean;
  upperKey: IndexKey | null;
  upperInclusive: boolean;
  exactKey: IndexKey | null;
  prefixScan?: boolean;
}

function applyRangePred(
  bounds: ScanBounds,
  key: IndexKey,
  type: SearchPredicate["comparisonType"],
): void {
  switch (type) {
    case "GREATER":
      bounds.lowerKey = key;
      bounds.lowerInclusive = false;
      break;
    case "GREATER_EQUAL":
      bounds.lowerKey = key;
      bounds.lowerInclusive = true;
      break;
    case "LESS":
      bounds.upperKey = key;
      bounds.upperInclusive = false;
      break;
    case "LESS_EQUAL":
      bounds.upperKey = key;
      bounds.upperInclusive = true;
      break;
  }
}

export function computeBounds(
  predicates: SearchPredicate[],
  totalColumns?: number,
): ScanBounds {
  const bounds: ScanBounds = {
    lowerKey: null,
    lowerInclusive: true,
    upperKey: null,
    upperInclusive: true,
    exactKey: null,
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

  if (rangePreds.length === 0 && eqValues.length > 0) {
    const isPrefixOnly =
      totalColumns !== undefined && eqValues.length < totalColumns;
    if (isPrefixOnly) {
      const prefix = eqValues.map((eq) => eq.value);
      bounds.lowerKey = prefix;
      bounds.lowerInclusive = true;
      bounds.upperKey = prefix;
      bounds.upperInclusive = true;
      bounds.prefixScan = true;
      return bounds;
    }
    bounds.exactKey = eqValues.map((eq) => eq.value);
    return bounds;
  }

  const prefix = eqValues.map((eq) => eq.value);

  for (const pred of rangePreds) {
    const fullKey =
      prefix.length > 0 ? [...prefix, pred.value] : [pred.value];
    applyRangePred(bounds, fullKey, pred.comparisonType);
  }

  if (prefix.length > 0) {
    if (!bounds.lowerKey) {
      bounds.lowerKey = prefix;
      bounds.lowerInclusive = true;
    }
    if (!bounds.upperKey) {
      bounds.upperKey = prefix;
      bounds.upperInclusive = true;
      bounds.prefixScan = true;
    }
  }

  return bounds;
}
