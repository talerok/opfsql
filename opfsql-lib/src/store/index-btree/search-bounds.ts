import type { IndexKey, IndexKeyValue } from "./types.js";

export interface SearchPredicate {
  columnPosition: number;
  comparisonType: "EQUAL" | "LESS" | "GREATER" | "LESS_EQUAL" | "GREATER_EQUAL";
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

  for (const p of predicates) {
    if (p.comparisonType === "EQUAL") {
      eqValues.push({ pos: p.columnPosition, value: p.value });
    } else {
      rangePreds.push(p);
    }
  }

  eqValues.sort((a, b) => a.pos - b.pos);

  if (rangePreds.length === 0 && eqValues.length > 0) {
    const isPrefixOnly =
      totalColumns !== undefined && eqValues.length < totalColumns;
    if (isPrefixOnly) {
      const prefix = eqValues.map((e) => e.value);
      bounds.lowerKey = prefix;
      bounds.lowerInclusive = true;
      bounds.upperKey = prefix;
      bounds.upperInclusive = true;
      bounds.prefixScan = true;
      return bounds;
    }
    bounds.exactKey = eqValues.map((e) => e.value);
    return bounds;
  }

  if (eqValues.length > 0) {
    const prefix = eqValues.map((e) => e.value);
    for (const rp of rangePreds) {
      const fullKey = [...prefix, rp.value];
      switch (rp.comparisonType) {
        case "GREATER":
          bounds.lowerKey = fullKey;
          bounds.lowerInclusive = false;
          break;
        case "GREATER_EQUAL":
          bounds.lowerKey = fullKey;
          bounds.lowerInclusive = true;
          break;
        case "LESS":
          bounds.upperKey = fullKey;
          bounds.upperInclusive = false;
          break;
        case "LESS_EQUAL":
          bounds.upperKey = fullKey;
          bounds.upperInclusive = true;
          break;
      }
    }
    if (!bounds.lowerKey) {
      bounds.lowerKey = prefix;
      bounds.lowerInclusive = true;
    }
    if (!bounds.upperKey) {
      bounds.upperKey = prefix;
      bounds.upperInclusive = true;
      bounds.prefixScan = true;
    }
  } else {
    for (const rp of rangePreds) {
      const fullKey: IndexKey = [rp.value];
      switch (rp.comparisonType) {
        case "GREATER":
          bounds.lowerKey = fullKey;
          bounds.lowerInclusive = false;
          break;
        case "GREATER_EQUAL":
          bounds.lowerKey = fullKey;
          bounds.lowerInclusive = true;
          break;
        case "LESS":
          bounds.upperKey = fullKey;
          bounds.upperInclusive = false;
          break;
        case "LESS_EQUAL":
          bounds.upperKey = fullKey;
          bounds.upperInclusive = true;
          break;
      }
    }
  }

  return bounds;
}
