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

export function kindOf(value: Value): ValueKind {
  if (value === null) return "null";
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "number") return "number";
  if (typeof value === "string") return "text";
  return value instanceof Uint8Array ? "blob" : "json";
}
