import { DatatypeMismatchError } from "../errors.js";
import { areEqual, canonicalJson } from "./order.js";
import type { SqlType } from "./types.js";
import type { JsonValue, Value } from "./value.js";

// ---------------------------------------------------------------------------
// Conversions between kinds (docs/semantics.md, Assignment and Casts).
// Assignment converts only what it can convert without loss or guessing;
// CAST also converts between booleans, numbers, text and hex BLOBs.
//
// Each conversion returns undefined when it does not apply; assignValue and
// castValue turn that into the error, so every refusal reads the same.
// ---------------------------------------------------------------------------

/** A number as SQL text writes it: digits, an optional fraction and exponent. */
const NUMERIC_TEXT = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/iu;
const HEX_TEXT = /^(?:[\da-f]{2})*$/iu;
/** JSON text that may parse to -0 or to a lone surrogate. */
const MAY_NEED_NORMALIZING = /-0|\\u[dD][89a-fA-F]/u;
const DESCRIBED_TEXT_LENGTH = 40;

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
  const assigned = assignmentOf(normalized, type);
  if (assigned !== undefined) return assigned;
  throw new DatatypeMismatchError(
    `cannot store ${describeValue(normalized)} as ${type}`,
  );
}

/**
 * True if a column of `type` stores `value` as it is: what assignment would
 * keep unchanged. A JSON column stores any value but a BLOB as it is — text
 * already stored is a JSON string, not JSON text to parse again.
 */
export function isStoredAs(value: Value, type: SqlType): boolean {
  if (value === null) return true;
  if (type === "JSON") return !(value instanceof Uint8Array);
  try {
    return areEqual(assignValue(value, type), value);
  } catch {
    return false;
  }
}

/** CAST(value AS type), or throws. */
export function castValue(value: Value, type: SqlType): Value {
  const normalized = normalizeValue(value);
  if (normalized === null) return null;
  const cast = castOf(normalized, type);
  if (cast !== undefined) return cast;
  throw new DatatypeMismatchError(
    `cannot cast ${describeValue(normalized)} to ${type}`,
  );
}

/**
 * How a parameter is converted to the type of what it is compared with
 * (docs/semantics.md, Comparisons): `numeric` turns text that is exactly a
 * number into that number, `text` turns a number into text.
 */
export type Affinity = "numeric" | "text";

/** `value` converted for comparison with a side of `affinity`, if lossless. */
export function applyAffinity(value: Value, affinity: Affinity): Value {
  if (affinity === "numeric" && typeof value === "string") {
    return numberFromText(value) ?? value;
  }
  if (affinity === "text" && typeof value === "number") return String(value);
  return value;
}

/** The number text writes exactly, or null if it is not a number; `-0` is 0. */
export function numberFromText(text: string): number | null {
  return NUMERIC_TEXT.test(text) ? withoutNegativeZero(Number(text)) : null;
}

/**
 * The JSON value SQL text holds, normalized as every value is
 * (docs/semantics.md, Values) at any depth: -0 is 0, and a string with a
 * lone surrogate, which an escape such as `\ud800` writes, is refused.
 */
export function parseJsonText(text: string): JsonValue {
  let parsed: JsonValue;
  try {
    parsed = JSON.parse(text) as JsonValue;
  } catch {
    throw new DatatypeMismatchError(`${describeValue(text)} is not valid JSON`);
  }
  // Only text that writes -0 or a surrogate's escape can hold either, so
  // most documents are returned as parsed, without a walk over them.
  return MAY_NEED_NORMALIZING.test(text) ? normalizedJson(parsed) : parsed;
}

/** `number`, with -0 turned into 0: they compare equal, so they must be one value. */
export function withoutNegativeZero(number: number): number {
  return number === 0 ? 0 : number;
}

/** Rounds toward zero, never to -0. */
export function truncate(number: number): number {
  return withoutNegativeZero(Math.trunc(number));
}

/** A computed number as a value: NaN is NULL, -0 is 0. */
export function normalizeNumber(value: number): number | null {
  return Number.isNaN(value) ? null : withoutNegativeZero(value);
}

function checkWellFormed(text: string): void {
  if (!text.isWellFormed()) {
    throw new DatatypeMismatchError(
      "text contains a lone surrogate, which is not valid Unicode",
    );
  }
}

// --- Assignment --------------------------------------------------------------

function assignmentOf(value: Value, type: SqlType): Value | undefined {
  switch (type) {
    case "INTEGER": {
      return integerOf(value);
    }
    case "REAL": {
      return numberOf(value);
    }
    case "TEXT": {
      return textOf(value);
    }
    case "BLOB": {
      return value instanceof Uint8Array ? value : undefined;
    }
    case "BOOLEAN": {
      return booleanOf(value);
    }
    case "JSON": {
      return jsonOf(value);
    }
    case "ANY": {
      return value;
    }
  }
}

/** A safe integer, or text that writes one exactly. */
function integerOf(value: Value): number | undefined {
  const number = numberOf(value);
  if (number === undefined || !Number.isSafeInteger(number)) return undefined;
  return number;
}

/** A number, or text that writes one exactly. */
function numberOf(value: Value): number | undefined {
  if (typeof value === "number") return value;
  if (typeof value === "string") return numberFromText(value) ?? undefined;
  return undefined;
}

/** Text, or a number as JavaScript prints it. */
function textOf(value: Value): string | undefined {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  return undefined;
}

/** A boolean, or 0 and 1. */
function booleanOf(value: Value): boolean | undefined {
  if (typeof value === "boolean") return value;
  if (value === 0 || value === 1) return value === 1;
  return undefined;
}

/** Any value but a BLOB; text is parsed as JSON text. */
function jsonOf(value: Value): Value | undefined {
  if (value instanceof Uint8Array) return undefined;
  if (typeof value !== "string") return value;
  return parseJsonText(value);
}

/** A parsed JSON document, every number and string in it normalized. */
function normalizedJson(value: JsonValue): JsonValue {
  if (typeof value === "number") return withoutNegativeZero(value);
  if (typeof value === "string") return wellFormedJsonText(value);
  if (value === null || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.map((item) => normalizedJson(item));
  const normalized: Record<string, JsonValue> = {};
  for (const [key, item] of Object.entries(value)) {
    normalized[wellFormedJsonText(key)] = normalizedJson(item);
  }
  return normalized;
}

function wellFormedJsonText(text: string): string {
  if (text.isWellFormed()) return text;
  throw new DatatypeMismatchError(
    "JSON text holds a lone surrogate, which is not valid Unicode",
  );
}

// --- Casts -------------------------------------------------------------------

function castOf(value: Value, type: SqlType): Value | undefined {
  switch (type) {
    case "INTEGER": {
      return castToInteger(value);
    }
    case "REAL": {
      return castToNumber(value);
    }
    case "TEXT": {
      return castToText(value);
    }
    case "BLOB": {
      return castToBlob(value);
    }
    case "BOOLEAN": {
      return castToBoolean(value);
    }
    case "JSON":
    case "ANY": {
      return assignmentOf(value, type);
    }
  }
}

/** Numbers truncate toward zero. */
function castToInteger(value: Value): number | undefined {
  const number = castToNumber(value);
  if (number === undefined) return undefined;
  return integerOf(truncate(number));
}

/** Booleans are 0 and 1. */
function castToNumber(value: Value): number | undefined {
  if (typeof value === "boolean") return Number(value);
  return numberOf(value);
}

/** Booleans are 'true' and 'false', BLOBs hex, JSON its canonical text. */
function castToText(value: Value): string | undefined {
  if (typeof value === "boolean") return String(value);
  if (value instanceof Uint8Array) return bytesToHex(value);
  if (typeof value === "object" && value !== null) return canonicalJson(value);
  return textOf(value);
}

/** Text is read as hex. */
function castToBlob(value: Value): Uint8Array | undefined {
  if (typeof value === "string") {
    return HEX_TEXT.test(value) ? hexToBytes(value) : undefined;
  }
  return value instanceof Uint8Array ? value : undefined;
}

/** 'true' and 'false' in any case. */
function castToBoolean(value: Value): boolean | undefined {
  const word = typeof value === "string" ? value.toLowerCase() : undefined;
  if (word === "true") return true;
  if (word === "false") return false;
  return booleanOf(value);
}

// --- Hex -----------------------------------------------------------------------

export function bytesToHex(bytes: Uint8Array): string {
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return hex;
}

export function hexToBytes(hex: string): Uint8Array {
  if (!HEX_TEXT.test(hex)) {
    throw new DatatypeMismatchError(`${describeValue(hex)} is not hexadecimal`);
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

// --- Errors ------------------------------------------------------------------

/** A short description of a value for error messages. */
export function describeValue(value: Value): string {
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

function shorten(text: string): string {
  if (text.length <= DESCRIBED_TEXT_LENGTH) return text;
  return `${text.slice(0, DESCRIBED_TEXT_LENGTH)}…`;
}
