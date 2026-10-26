// Values as docs/semantics.md defines them: one JavaScript representation per
// kind, normalized on the way in (no NaN, no -0, no undefined).

export type JsonValue =
  null | boolean | number | string | JsonValue[] | JsonObject;

export interface JsonObject {
  [key: string]: JsonValue;
}

/** A JSON object or array: JSON values that are not also SQL scalars. */
export type JsonComposite = JsonValue[] | JsonObject;

export type Value =
  null | boolean | number | string | Uint8Array | JsonComposite;

export type ValueKind =
  "null" | "boolean" | "number" | "text" | "blob" | "json";

/** Whether a value is a JSON object or array: one its holder can change. */
export function isJsonComposite(value: Value): value is JsonComposite {
  const isObject = typeof value === "object" && value !== null;
  return isObject && !(value instanceof Uint8Array);
}

/** Whether its holder can change a value: a BLOB, a JSON object or array. */
export function isMutable(value: Value): value is Uint8Array | JsonComposite {
  return typeof value === "object" && value !== null;
}

/**
 * A value of one's own: a BLOB or a JSON object or array copied, so that
 * changing it changes nothing anyone else holds; any other value as it is.
 */
export function copyValue(value: Value): Value {
  if (value instanceof Uint8Array) return value.slice();
  return isJsonComposite(value) ? structuredClone(value) : value;
}

export function kindOf(value: Value): ValueKind {
  if (value === null) return "null";
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "number") return "number";
  if (typeof value === "string") return "text";
  return value instanceof Uint8Array ? "blob" : "json";
}
