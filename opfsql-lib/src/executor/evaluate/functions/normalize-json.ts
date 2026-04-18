import type { JsonValue } from '../../../types.js';

/**
 * Recursively normalize a JSON value for canonical serialization:
 * - Object keys are sorted lexicographically
 * - Array order is preserved
 * - Primitives pass through unchanged
 */
export function normalizeJson(value: JsonValue): JsonValue {
  if (value === null || typeof value !== 'object') return value;

  if (Array.isArray(value)) {
    return value.map(normalizeJson);
  }

  const sorted: Record<string, JsonValue> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = normalizeJson(value[key]);
  }
  return sorted;
}
