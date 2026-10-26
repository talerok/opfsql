import { md5 } from "../../src/functions/md5.js";
import type { Value } from "../../src/values/index.js";
import type { Expected, SortMode } from "./records.js";

// ---------------------------------------------------------------------------
// A query's result as sqllogictest's SQLite client prints it: each value by
// its column's type letter, then sorted as the record asks, then compared
// value by value, or by count and MD5 when the file gives a hash.
//
//   I   as sqlite3_column_int64 reads it: a number truncated toward zero;
//       text, the integer it starts with, or 0;
//   R   "%.3f" of sqlite3_column_double, text read as the number it starts
//       with;
//   T   the text, each character outside ' '…'~' printed as '@'; empty text
//       is `(empty)`.
//
// NULL is `NULL` whatever the letter. A BOOLEAN reads as the 1 or 0 SQLite
// would have given for it: how it compares is another matter, which
// docs/semantics.md settles. Trailing spaces are not compared: the corpus
// files do not keep them.
// ---------------------------------------------------------------------------

type Row = readonly Value[];

/** ' ' and '~': the printable ASCII range. */
const LOWEST_PRINTABLE = 0x20;
const HIGHEST_PRINTABLE = 0x7e;
const LEADING_INTEGER = /^\s*[+-]?\d+/u;
const LEADING_NUMBER = /^\s*[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/u;

/** The values of `rows` as the file lists them: formatted, sorted, row after row. */
export function resultValues(
  rows: readonly Row[],
  types: string,
  sort: SortMode,
): string[] {
  const formatted = rows.map((row) =>
    row.map((value, column) => formatValue(value, types[column] ?? "T")),
  );
  switch (sort) {
    case "nosort": {
      return formatted.flat();
    }
    case "rowsort": {
      return formatted.toSorted(compareRows).flat();
    }
    case "valuesort": {
      return formatted.flat().toSorted(compareText);
    }
  }
}

/** Why `values` are not what the file expects, or null if they are. */
export function mismatchOf(
  values: readonly string[],
  expected: Expected,
): string | null {
  if (expected.kind === "hash") {
    const hash = hashOf(values);
    const isSame = values.length === expected.count && hash === expected.hash;
    if (isSame) return null;
    return `expected ${String(expected.count)} values hashing to ${expected.hash}, got ${String(values.length)} hashing to ${hash}`;
  }
  const isSame =
    values.length === expected.values.length &&
    values.every((value, index) => value === expected.values[index]);
  if (isSame) return null;
  return `expected [${expected.values.join(", ")}], got [${values.join(", ")}]`;
}

/** The MD5 of the values, each followed by a newline, as sqllogictest hashes them. */
export function hashOf(values: readonly string[]): string {
  const text = values.map((value) => `${value}\n`).join("");
  return md5(new TextEncoder().encode(text));
}

export function formatValue(value: Value, type: string): string {
  if (value === null) return "NULL";
  switch (type) {
    case "I": {
      return String(Math.trunc(numberOf(value, LEADING_INTEGER)));
    }
    case "R": {
      return numberOf(value, LEADING_NUMBER).toFixed(3);
    }
    default: {
      return textOf(value);
    }
  }
}

/** A value as a number, text read as the number `leading` finds at its start. */
function numberOf(value: Value, leading: RegExp): number {
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value ? 1 : 0;
  const found = leading.exec(textOf(value));
  return found === null ? 0 : Number(found[0]);
}

function textOf(value: Value): string {
  if (typeof value === "boolean") return value ? "1" : "0";
  if (value instanceof Uint8Array)
    return printable(new TextDecoder().decode(value));
  const text =
    typeof value === "object" ? JSON.stringify(value) : String(value);
  return text === "" ? "(empty)" : printable(text).trimEnd();
}

/** The text's UTF-8 bytes, each outside ' '…'~' printed as '@', as the C client prints them. */
function printable(text: string): string {
  const bytes = new TextEncoder().encode(text);
  return Array.from(bytes, (byte) =>
    isPrintable(byte) ? String.fromCodePoint(byte) : "@",
  ).join("");
}

function isPrintable(byte: number): boolean {
  return byte >= LOWEST_PRINTABLE && byte <= HIGHEST_PRINTABLE;
}

/** strcmp on ASCII: what the C client sorts by. */
function compareText(a: string, b: string): number {
  if (a < b) return -1;
  return a > b ? 1 : 0;
}

function compareRows(a: readonly string[], b: readonly string[]): number {
  for (const [index, value] of a.entries()) {
    const order = compareText(value, b[index] ?? "");
    if (order !== 0) return order;
  }
  return a.length - b.length;
}
