import { ExecutorError } from "../../errors.js";
import type { Value } from "../../types.js";

/** Cast a value to a target type. */
export function castValue(v: Value, targetType: string): Value {
  if (v === null) return null;
  switch (targetType) {
    case "INTEGER":
    case "BIGINT":
      return castNumber(v, targetType);
    case "REAL":
      return castReal(v);
    case "TEXT":
      return castText(v);
    case "BOOLEAN":
      return castBoolean(v);
    case "JSON":
      return castJson(v);
    case "BLOB":
      return castBlob(v);
    default:
      return v;
  }
}

function castNumber(v: Value, targetType: string) {
  if (typeof v === "boolean") return v ? 1 : 0;
  if (v instanceof Uint8Array) throw new ExecutorError(`Cannot cast BLOB to ${targetType}`);
  const n = typeof v === "number" ? Math.trunc(v) : parseInt(String(v), 10);
  if (Number.isNaN(n)) {
    throw new ExecutorError(`Cannot cast '${v}' to ${targetType}`);
  }
  return n;
}

function castReal(v: Value) {
  if (v instanceof Uint8Array) throw new ExecutorError(`Cannot cast BLOB to REAL`);
  const n = typeof v === "number" ? v : parseFloat(String(v));
  if (Number.isNaN(n)) {
    throw new ExecutorError(`Cannot cast '${v}' to REAL`);
  }
  return n;
}

export function castText(v: Value): string {
  if (v instanceof Uint8Array) return blobToHex(v);
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "object" && v !== null) return JSON.stringify(v);
  return String(v);
}

export function castJson(v: Value) {
  if (v instanceof Uint8Array) throw new ExecutorError(`Cannot cast BLOB to JSON`);
  if (typeof v === "object" && v !== null) return v;
  if (typeof v === "string") {
    try { return JSON.parse(v); }
    catch { throw new ExecutorError(`Cannot cast '${v}' to JSON: invalid JSON`); }
  }
  // Numbers and booleans are valid JSON scalars
  if (typeof v === "number" || typeof v === "boolean") return v;
  throw new ExecutorError(`Cannot cast '${typeof v}' value to JSON`);
}

function castBoolean(v: Value) {
  if (v instanceof Uint8Array) throw new ExecutorError(`Cannot cast BLOB to BOOLEAN`);
  if (typeof v === "boolean") return v;
  const s = String(v).toLowerCase();
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  throw new ExecutorError(`Cannot cast '${v}' to BOOLEAN`);
}

function castBlob(v: Value): Uint8Array {
  if (v instanceof Uint8Array) return v;
  if (typeof v === "string") return hexToBlob(v);
  throw new ExecutorError(`Cannot cast ${typeof v} to BLOB`);
}

export function blobToHex(buf: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < buf.length; i++) {
    hex += buf[i].toString(16).padStart(2, '0').toUpperCase();
  }
  return hex;
}

export function hexToBlob(hex: string): Uint8Array {
  const clean = hex.replace(/\s/g, '');
  if (clean.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(clean)) {
    throw new ExecutorError(`Cannot cast '${hex}' to BLOB: invalid hex string`);
  }
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < clean.length; i += 2) {
    bytes[i / 2] = parseInt(clean.substring(i, i + 2), 16);
  }
  return bytes;
}
