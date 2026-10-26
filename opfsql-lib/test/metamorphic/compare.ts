import type { ResultRow } from "../../src/engine/api/index.js";
import type { Value } from "../../src/values/index.js";

// ---------------------------------------------------------------------------
// Comparing results. Each value gets a key of its own making — not the
// engine's — that is equal exactly when docs/semantics.md says two values
// are: by kind, numbers numerically, JSON by its text with keys sorted.
// ---------------------------------------------------------------------------

export type Rows = readonly ResultRow[];

/** Differences shown in a failure, at most. */
const SHOWN = 5;

export function valueKey(value: Value): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return String(value);
  if (typeof value === "number") return `number ${String(value)}`;
  if (typeof value === "string") return `text ${JSON.stringify(value)}`;
  if (value instanceof Uint8Array) return `blob ${[...value].join(",")}`;
  return `json ${sortedJson(value)}`;
}

function sortedJson(value: unknown): string {
  if (Array.isArray(value))
    return `[${value.map((item) => sortedJson(item)).join(",")}]`;
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  const entries = Object.entries(value).toSorted(([a], [b]) =>
    a < b ? -1 : 1,
  );
  const fields = entries.map(
    ([key, item]) => `${JSON.stringify(key)}:${sortedJson(item)}`,
  );
  return `{${fields.join(",")}}`;
}

export function rowKey(row: ResultRow): string {
  return row.map((value) => valueKey(value)).join(" | ");
}

/** The rows once each, in their first order. */
export function distinctRows(rows: Rows): Rows {
  const seen = new Set<string>();
  return rows.filter((row) => {
    const key = rowKey(row);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** What the two sides compared are called in a report: [first, second]. */
export type Sides = readonly [string, string];

/** What tells two multisets of rows apart, or null when they are equal. */
export function multisetDifference(
  expected: Rows,
  actual: Rows,
  [first, second]: Sides,
): string | null {
  const counts = new Map<string, number>();
  for (const row of expected)
    counts.set(rowKey(row), (counts.get(rowKey(row)) ?? 0) + 1);
  for (const row of actual)
    counts.set(rowKey(row), (counts.get(rowKey(row)) ?? 0) - 1);
  const differing = [...counts].filter(([, count]) => count !== 0);
  if (differing.length === 0) return null;
  const shown = differing.slice(0, SHOWN).map(([key, count]) => {
    const side = count > 0 ? `only in ${first}` : `only in ${second}`;
    return `  ${side} (×${String(Math.abs(count))}): ${key}`;
  });
  const sizes = `${String(expected.length)} rows in ${first} against ${String(actual.length)} in ${second}`;
  return [sizes, ...shown].join("\n");
}

/** What tells two sequences of rows apart, or null when they are equal. */
export function sequenceDifference(
  expected: Rows,
  actual: Rows,
  [first, second]: Sides,
): string | null {
  const length = Math.max(expected.length, actual.length);
  for (let index = 0; index < length; index++) {
    const expectedRow = expected[index];
    const actualRow = actual[index];
    const isSame =
      expectedRow !== undefined &&
      actualRow !== undefined &&
      rowKey(expectedRow) === rowKey(actualRow);
    if (isSame) continue;
    const shown = (row: ResultRow | undefined) =>
      row === undefined ? "none" : rowKey(row);
    return `row ${String(index + 1)}: ${shown(expectedRow)} in ${first} against ${shown(actualRow)} in ${second}`;
  }
  return null;
}
