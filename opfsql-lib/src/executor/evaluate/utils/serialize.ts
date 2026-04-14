import type { Value } from "../../types.js";
import { blobToHex } from "./cast.js";

/** Serialize a value to a string for hashing (joins, distinct, group by). */
export function serializeValue(v: Value): string {
  if (v === null) return "\0NULL\0";
  if (v instanceof Uint8Array) return `x:${blobToHex(v)}`;
  if (typeof v === "object") return `j:${JSON.stringify(v)}`;
  if (typeof v === "string") return `s:${v}`;
  if (typeof v === "number") return `n:${v}`;
  return `b:${v}`;
}
