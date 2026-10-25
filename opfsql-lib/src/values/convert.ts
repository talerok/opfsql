import { DatatypeMismatchError } from "../errors.js";
import type { SqlType } from "./types.js";
import type { JsonValue, Value } from "./value.js";
import { kindOf } from "./value.js";

// ---------------------------------------------------------------------------
// Conversions between kinds (docs/semantics.md, Assignment and Casts).
// Assignment converts only what it can convert without loss or guessing;
// CAST also converts between booleans, numbers, text and hex BLOBs.
// ---------------------------------------------------------------------------

/** A number as SQL text writes it: digits, an optional fraction and exponent. */
const NUMERIC_TEXT = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/iu;

/**
 * Normalizes a value coming from outside: NaN becomes NULL and -0 becomes 0.
 * Text with a lone surrogate is rejected: UTF-8 cannot store it unchanged.
 */
export function normalizeValue(value: Value): Value {
  if (typeof value === "number") return normalizeNumber(value);
  if (typeof value === "string") checkWellFormed(value);
  return value;
}

/** Converts `value` for storage in a column of `type`, or throws. */
export function assignValue(value: Value, type: SqlType): Value {
  const normalized = normalizeValue(value);
  if (normalized === null) return null;
  switch (type) {
    case "INTEGER": {
      return toInteger(normalized, type);
    }
    case "REAL": {
      return toReal(normalized, type);
    }
    case "TEXT": {
      return toText(normalized, type);
    }
    case "BLOB": {
      return toBlob(normalized, type);
    }
    case "BOOLEAN": {
      return toBoolean(normalized, type);
    }
    case "JSON": {
      return toJson(normalized, type);
    }
    case "ANY": {
      return normalized;
    }
  }
}

/** CAST(value AS type), or throws. */
export function castValue(value: Value, type: SqlType): Value {
  const normalized = normalizeValue(value);
  if (normalized === null) return null;
  switch (type) {
    case "INTEGER": {
      return toInteger(truncate(castToNumber(normalized, type)), type);
    }
    case "REAL": {
      return castToNumber(normalized, type);
    }
    case "TEXT": {
      return castToText(normalized);
    }
    case "BLOB": {
      return castToBlob(normalized);
    }
    case "BOOLEAN": {
      return castToBoolean(normalized);
    }
    case "JSON":
    case "ANY": {
      return assignValue(normalized, type);
    }
  }
}

/** The number text writes exactly, or null if it is not a number. */
export function numberFromText(text: string): number | null {
  return NUMERIC_TEXT.test(text) ? Number(text) : null;
}

function normalizeNumber(value: number): number | null {
  if (Number.isNaN(value)) return null;
  return value === 0 ? 0 : value; // -0 becomes 0
}

function checkWellFormed(text: string): void {
  if (!text.isWellFormed()) {
    throw new DatatypeMismatchError(
      "text contains a lone surrogate, which is not valid Unicode",
    );
  }
}

// --- Assignment --------------------------------------------------------------

function toInteger(value: Value, type: SqlType): number {
  const number = numberIn(value);
  if (number !== null && Number.isSafeInteger(number)) return number;
  throw mismatch(value, type);
}

function toReal(value: Value, type: SqlType): number {
  const number = numberIn(value);
  if (number !== null) return number;
  throw mismatch(value, type);
}

/** A number, or the number text writes exactly; null for anything else. */
function numberIn(value: Value): number | null {
  if (typeof value === "number") return value;
  if (typeof value === "string") return numberFromText(value);
  return null;
}

function toText(value: Value, type: SqlType): string {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  throw mismatch(value, type);
}

function toBlob(value: Value, type: SqlType): Uint8Array {
  if (value instanceof Uint8Array) return value;
  throw mismatch(value, type);
}

function toBoolean(value: Value, type: SqlType): boolean {
  if (typeof value === "boolean") return value;
  if (value === 0 || value === 1) return value === 1;
  throw mismatch(value, type);
}

function toJson(value: Value, type: SqlType): Value {
  if (value instanceof Uint8Array) throw mismatch(value, type);
  if (typeof value !== "string") return value;
  try {
    return normalizeValue(JSON.parse(value) as JsonValue);
  } catch {
    throw new DatatypeMismatchError(`${describe(value)} is not valid JSON`);
  }
}

// --- Casts -------------------------------------------------------------------

function castToNumber(value: Value, type: SqlType): number {
  if (typeof value === "boolean") return Number(value);
  return toReal(value, type);
}

/** Rounds toward zero, never to -0. */
function truncate(number: number): number {
  const truncated = Math.trunc(number);
  return truncated === 0 ? 0 : truncated;
}

function castToText(value: Value): string {
  if (typeof value === "boolean") return String(value);
  if (value instanceof Uint8Array) return bytesToHex(value);
  if (kindOf(value) === "json") return JSON.stringify(value);
  return toText(value, "TEXT");
}

/** Text casts to a BLOB as hexadecimal. */
function castToBlob(value: Value): Uint8Array {
  if (typeof value === "string") return hexToBytes(value);
  return toBlob(value, "BLOB");
}

function castToBoolean(value: Value): boolean {
  const word = typeof value === "string" ? value.toLowerCase() : null;
  if (word === "true") return true;
  if (word === "false") return false;
  return toBoolean(value, "BOOLEAN");
}

// --- Hex -----------------------------------------------------------------------

export function bytesToHex(bytes: Uint8Array): string {
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return hex;
}

export function hexToBytes(hex: string): Uint8Array {
  if (!/^(?:[\da-f]{2})*$/iu.test(hex)) {
    throw new DatatypeMismatchError(`${describe(hex)} is not hexadecimal`);
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

// --- Errors ------------------------------------------------------------------

function mismatch(value: Value, type: SqlType): DatatypeMismatchError {
  return new DatatypeMismatchError(
    `cannot store ${describe(value)} as ${type}`,
  );
}

function describe(value: Value): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean" || typeof value === "number") {
    return String(value);
  }
  if (typeof value === "string") return JSON.stringify(shorten(value));
  if (value instanceof Uint8Array) {
    return `a ${String(value.length)}-byte BLOB`;
  }
  return Array.isArray(value) ? "a JSON array" : "a JSON object";
}

const DESCRIBED_TEXT_LENGTH = 40;

function shorten(text: string): string {
  if (text.length <= DESCRIBED_TEXT_LENGTH) return text;
  return `${text.slice(0, DESCRIBED_TEXT_LENGTH)}…`;
}
