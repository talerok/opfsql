import type { IndexKey } from "./types.js";

// Inlined from executor/evaluate/helpers.ts to keep store-sync self-contained.
function compareValues(
  a: string | number | boolean,
  b: string | number | boolean,
): number {
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }
  if (typeof a === "boolean" && typeof b === "boolean") {
    return (a ? 1 : 0) - (b ? 1 : 0);
  }

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

export function compareIndexKeys(a: IndexKey, b: IndexKey): number {
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const av = a[i];
    const bv = b[i];
    if (av === null && bv === null) continue;
    if (av === null) return 1;
    if (bv === null) return -1;
    const cmp = compareValues(av, bv);
    if (cmp !== 0) return cmp;
  }
  return a.length - b.length;
}

export function keyHasNull(key: IndexKey): boolean {
  return key.some((v) => v === null);
}
