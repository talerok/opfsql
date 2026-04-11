import { compareValues } from '../../executor/evaluate/helpers.js';
import type { IndexKey } from './types.js';

/**
 * Compare two composite index keys lexicographically.
 * NULL sorts LAST (greater than all non-null values).
 * Two NULLs in the same position are considered equal.
 */
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

/**
 * Check if a key contains any NULL components.
 * Used for UNIQUE constraint: rows with NULL in any indexed column
 * are exempt from uniqueness checks (SQL standard).
 */
export function keyHasNull(key: IndexKey): boolean {
  return key.some((v) => v === null);
}
