import { InternalError } from "../errors.js";
import { bytesToHex } from "./convert.js";
import type { Value } from "./value.js";

// ---------------------------------------------------------------------------
// Values as SQL literals: how the schema table writes defaults, how the
// printer writes expressions, how plans print constants — one form, which
// reads back as the same value.
// ---------------------------------------------------------------------------

/**
 * `value` as a SQL literal that reads back as the same value: NULL, TRUE,
 * -1.5, 'it''s', X'cafe'. SQL has no infinity; 9e999 overflows to it, as in
 * SQLite. A JSON object or array has no literal: JSON text is its form.
 */
export function sqlLiteral(value: Value): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return numberLiteral(value);
  if (typeof value === "string") return textLiteral(value);
  if (value instanceof Uint8Array) return `X'${bytesToHex(value)}'`;
  throw new InternalError("a JSON value has no SQL literal outside JSON text");
}

/** A REAL as a literal that reads back as REAL: 2.0, not the INTEGER 2. */
export function realLiteral(value: number): string {
  const text = numberLiteral(value);
  return /[.e]/.test(text) ? text : `${text}.0`;
}

/** Text as a quoted literal, its quotes doubled. */
export function textLiteral(text: string): string {
  return `'${text.replaceAll("'", "''")}'`;
}

function numberLiteral(value: number): string {
  if (value === Infinity) return "9e999";
  if (value === -Infinity) return "-9e999";
  return String(value);
}
